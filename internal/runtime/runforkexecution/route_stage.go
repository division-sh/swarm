package runforkexecution

import (
	"context"
	"errors"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type selectedFlowRoutePublisher interface {
	StageFlowInstanceRouteContext(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest) (runtimebus.FlowInstanceRouteTopologyResult, error)
	PublishPersistedFlowInstanceRouteForAttempt(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest, runtimepipeline.DynamicFlowRuntimeActivationAttempt) (runtimebus.FlowRoutePublicationHandle, error)
	VerifyFlowInstanceRoute(context.Context, runtimeflowidentity.RunScopedFlowInstance) error
}

type selectedFlowRouteRetirer interface {
	RetireFlowInstanceRouteForAttempt(runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
}

type selectedFlowActivationRetirementStore interface {
	RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.RunScopedFlowInstance) error
	AbandonDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
}

type selectedFlowActivation struct {
	attempt         runtimepipeline.DynamicFlowRuntimeActivationAttempt
	identity        runtimeflowidentity.RunScopedFlowInstance
	plan            runtimepipeline.DynamicFlowRuntimeReadinessPlan
	publication     runtimebus.FlowRoutePublicationHandle
	timersProjected bool
}

func admitSelectedContractFlowRoute(ctx context.Context, workflow *runtimepipeline.PipelineCoordinator, bus selectedFlowRoutePublisher, binding runtimeprocessbinding.Binding, route runtimebus.FlowInstanceRouteMaterializationRequest, diagnostics *selectedForkCommitDiagnostics, published *[]selectedFlowActivation) error {
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(ctx, route.Identity.RunID, route.Identity.Route)
	if err != nil {
		return err
	}
	if !found || !readiness.Eligible() || readiness.PlanRevision == 0 {
		return errors.New("selected flow route has no eligible durable readiness plan")
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if plan.RunID != route.Identity.RunID || plan.Identity.Route() != route.Identity.Route || plan.BundleHash != binding.BundleHash {
		return errors.New("selected flow route differs from its exact generation readiness plan")
	}
	admitted, commitErr := workflow.BeginDynamicFlowRuntimeActivation(ctx, plan, readiness.PlanRevision, binding)
	if !admitted.Acknowledged || admitted.Reused {
		return errors.Join(commitErr, errors.New("selected flow activation attempt was not freshly admitted"))
	}
	if commitErr != nil {
		diagnostics.add(commitErr)
	}
	if err := workflow.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
		*published = append(*published, selectedFlowActivation{attempt: admitted.Attempt, identity: route.Identity})
		return err
	}
	err = publishSelectedContractFlowRoute(ctx, bus, route, admitted.Attempt, diagnostics, published)
	(*published)[len(*published)-1].plan = plan
	if err != nil {
		return err
	}
	return workflow.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt)
}

func completeSelectedContractFlowRoutes(ctx context.Context, workflow *runtimepipeline.PipelineCoordinator, published []selectedFlowActivation, diagnostics *selectedForkCommitDiagnostics) error {
	for i := range published {
		activation := &published[i]
		activation.timersProjected = true
		if err := workflow.ReconcileInitialEntryTimersForAttempt(ctx, activation.identity, activation.attempt, activation.plan); err != nil {
			return err
		}
		result, err := workflow.MarkDynamicFlowRuntimeTopologyReadyForAttempt(ctx, activation.attempt, activation.plan, time.Now().UTC())
		if !result.Acknowledged {
			return errors.Join(err, errors.New("selected flow topology completion was not acknowledged"))
		}
		if err != nil {
			diagnostics.add(err)
		}
	}
	return nil
}

func retireSelectedFlowActivations(ctx context.Context, bus selectedFlowRouteRetirer, workflow selectedFlowActivationRetirementStore, activations []selectedFlowActivation) ([]selectedFlowActivation, error) {
	var retained []selectedFlowActivation
	var result error
	for _, activation := range activations {
		var err error
		if activation.publication != nil {
			err = activation.publication.Retire()
		} else if bus != nil {
			err = bus.RetireFlowInstanceRouteForAttempt(activation.attempt)
		} else {
			err = errors.New("selected flow route retirement owner is required")
		}
		if err == nil && activation.timersProjected {
			err = workflow.RetireInitialEntryTimerWakeups(ctx, activation.identity)
		}
		if err == nil {
			err = workflow.AbandonDynamicFlowRuntimeActivationAttempt(ctx, activation.attempt)
		}
		if err != nil {
			result = errors.Join(result, err)
			retained = append(retained, activation)
		}
	}
	return retained, result
}

func publishSelectedContractFlowRoute(ctx context.Context, bus selectedFlowRoutePublisher, route runtimebus.FlowInstanceRouteMaterializationRequest, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, diagnostics *selectedForkCommitDiagnostics, published *[]selectedFlowActivation) error {
	*published = append(*published, selectedFlowActivation{attempt: attempt, identity: route.Identity})
	staged, stageErr := bus.StageFlowInstanceRouteContext(ctx, route)
	if !staged.Acknowledged {
		return errors.Join(stageErr, errors.New("selected-contract flow route stage was not acknowledged"))
	}
	if stageErr != nil {
		if diagnostics == nil {
			return stageErr
		}
		diagnostics.add(stageErr)
	}
	publication, err := bus.PublishPersistedFlowInstanceRouteForAttempt(ctx, route, attempt)
	if err != nil {
		return err
	}
	(*published)[len(*published)-1].publication = publication
	return bus.VerifyFlowInstanceRoute(ctx, route.Identity)
}
