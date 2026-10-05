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
	RetireFlowInstanceRouteForAttempt(runtimeflowidentity.RunScopedFlowInstance, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
}

type selectedFlowActivationRetirementStore interface {
	ResolveDynamicFlowRuntimeActivation(context.Context, runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error)
	RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.RunScopedFlowInstance) error
	AbandonDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
}

type selectedFlowActivationAdmissionStore interface {
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	BeginDynamicFlowRuntimeActivation(context.Context, runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error)
	ResolveDynamicFlowRuntimeActivation(context.Context, runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationResolution, error)
	VerifyDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error
}

type selectedFlowActivation struct {
	pending         *runtimepipeline.DynamicFlowRuntimeActivationRequest
	attempt         runtimepipeline.DynamicFlowRuntimeActivationAttempt
	identity        runtimeflowidentity.RunScopedFlowInstance
	plan            runtimepipeline.DynamicFlowRuntimeReadinessPlan
	route           runtimebus.FlowInstanceRouteMaterializationRequest
	publication     runtimebus.FlowRoutePublicationHandle
	timersProjected bool
}

func admitSelectedContractFlowRoute(ctx context.Context, workflow selectedFlowActivationAdmissionStore, bus selectedFlowRoutePublisher, binding runtimeprocessbinding.Binding, route runtimebus.FlowInstanceRouteMaterializationRequest, diagnostics *selectedForkCommitDiagnostics, published *[]selectedFlowActivation) error {
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(ctx, route.Identity.RunID, route.Identity.Route)
	if err != nil {
		return err
	}
	if !found || !readiness.Eligible() || readiness.AttemptOrdinal == 0 {
		return errors.New("selected flow route has no eligible durable readiness plan")
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if plan.RunID != route.Identity.RunID || plan.Identity.Route() != route.Identity.Route || plan.BundleHash != binding.BundleHash {
		return errors.New("selected flow route differs from its exact generation readiness plan")
	}
	request := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan, readiness.AttemptOrdinal, readiness.AttemptState, binding)
	if err := request.Validate(); err != nil {
		return err
	}
	index := len(*published)
	*published = append(*published, selectedFlowActivation{pending: &request, identity: route.Identity, plan: plan, route: route})
	admitted, commitErr := workflow.BeginDynamicFlowRuntimeActivation(ctx, request)
	if !admitted.Acknowledged {
		resolved, resolveErr := workflow.ResolveDynamicFlowRuntimeActivation(ctx, request)
		commitErr = errors.Join(commitErr, resolveErr)
		if err := request.ValidateResolution(resolved); err != nil {
			return errors.Join(commitErr, err)
		}
		if resolved.Disposition != runtimepipeline.FlowActivationAdmitted {
			return errors.Join(commitErr, errors.New("selected flow activation admission requires exact resolution"))
		}
		admitted.Attempt = resolved.Attempt
	}
	if err := admitted.Attempt.Validate(); err != nil {
		return errors.Join(commitErr, err)
	}
	if admitted.Attempt.RunID() != plan.RunID || admitted.Attempt.InstancePath() != plan.Identity.InstancePath || !admitted.Attempt.ProcessBinding().Equal(binding) {
		return errors.Join(commitErr, errors.New("selected flow activation acknowledgment differs from its retained request"))
	}
	(*published)[index].attempt, (*published)[index].pending = admitted.Attempt, nil
	if commitErr != nil {
		diagnostics.add(commitErr)
	}
	if err := workflow.VerifyDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
		return err
	}
	return nil
}

func completeSelectedContractFlowRoutes(ctx context.Context, workflow *runtimepipeline.PipelineCoordinator, bus selectedFlowRoutePublisher, published []selectedFlowActivation, diagnostics *selectedForkCommitDiagnostics) error {
	for i := range published {
		activation := &published[i]
		advance := func(previous runtimepipeline.FlowAttachmentPhase) error {
			result, err := workflow.AdvanceFlowAttachment(ctx, activation.attempt, previous, time.Now().UTC())
			if !result.Admitted() {
				return errors.Join(err, errors.New("selected flow attachment progress was not admitted"))
			}
			if err != nil {
				diagnostics.add(err)
			}
			return nil
		}
		if err := advance(runtimepipeline.FlowAttachmentPlanned); err != nil {
			return err
		}
		if err := publishSelectedContractFlowRoute(ctx, bus, activation.route, activation.attempt, diagnostics, activation); err != nil {
			return err
		}
		if err := advance(runtimepipeline.FlowAttachmentAgentsRegistered); err != nil {
			return err
		}
		activation.timersProjected = true
		if err := workflow.ReconcileInitialEntryTimersForAttempt(ctx, activation.identity, activation.attempt, activation.plan); err != nil {
			return err
		}
		if err := advance(runtimepipeline.FlowAttachmentRouteInstalled); err != nil {
			return err
		}
		if err := advance(runtimepipeline.FlowAttachmentTimersArmed); err != nil {
			return err
		}
	}
	return nil
}

func retireSelectedFlowActivations(ctx context.Context, bus selectedFlowRouteRetirer, workflow selectedFlowActivationRetirementStore, activations []selectedFlowActivation) ([]selectedFlowActivation, error) {
	var retained []selectedFlowActivation
	var result error
	for _, activation := range activations {
		var err error
		if activation.pending != nil {
			resolved, resolveErr := workflow.ResolveDynamicFlowRuntimeActivation(ctx, *activation.pending)
			resolveErr = errors.Join(resolveErr, activation.pending.ValidateResolution(resolved))
			if resolveErr != nil || resolved.Disposition == runtimepipeline.FlowActivationUnresolved {
				result = errors.Join(result, resolveErr, errors.New("selected flow activation retains unresolved admission"))
				retained = append(retained, activation)
				continue
			}
			if resolved.Disposition != runtimepipeline.FlowActivationAdmitted {
				continue
			}
			activation.attempt, activation.pending = resolved.Attempt, nil
			// A pending request never installs resources before acknowledgment.
			err = workflow.AbandonDynamicFlowRuntimeActivationAttempt(ctx, activation.attempt)
			if err != nil {
				result = errors.Join(result, err)
				retained = append(retained, activation)
			}
			continue
		}
		if activation.publication != nil {
			err = activation.publication.Retire()
		} else if bus != nil {
			err = bus.RetireFlowInstanceRouteForAttempt(activation.identity, activation.attempt)
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

func publishSelectedContractFlowRoute(ctx context.Context, bus selectedFlowRoutePublisher, route runtimebus.FlowInstanceRouteMaterializationRequest, attempt runtimepipeline.DynamicFlowRuntimeActivationAttempt, diagnostics *selectedForkCommitDiagnostics, activation *selectedFlowActivation) error {
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
	activation.publication = publication
	return bus.VerifyFlowInstanceRoute(ctx, route.Identity)
}
