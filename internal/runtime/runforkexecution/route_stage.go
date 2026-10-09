package runforkexecution

import (
	"context"
	"errors"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

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
	timersProjected bool
}

func admitSelectedContractFlowActivation(ctx context.Context, workflow selectedFlowActivationAdmissionStore, binding runtimeprocessbinding.Binding, identity runtimeflowidentity.RunScopedFlowInstance, diagnostics *selectedForkCommitDiagnostics, published *[]selectedFlowActivation) error {
	readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(ctx, identity.RunID, identity.Route)
	if err != nil {
		return err
	}
	if !found || !readiness.Eligible() || readiness.AttemptOrdinal == 0 {
		return errors.New("selected flow activation has no eligible durable readiness plan")
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	if plan.RunID != identity.RunID || plan.Identity.Route() != identity.Route || plan.BundleHash != binding.BundleHash {
		return errors.New("selected flow activation differs from its exact generation readiness plan")
	}
	request := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan, readiness.AttemptOrdinal, readiness.AttemptState, binding)
	if err := request.Validate(); err != nil {
		return err
	}
	index := len(*published)
	*published = append(*published, selectedFlowActivation{pending: &request, identity: identity, plan: plan})
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

func completeSelectedContractFlowActivations(ctx context.Context, workflow *runtimepipeline.PipelineCoordinator, published []selectedFlowActivation, diagnostics *selectedForkCommitDiagnostics) error {
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
		if err := workflow.VerifyDynamicFlowRuntimeActivationAttempt(ctx, activation.attempt); err != nil {
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

func retireSelectedFlowActivations(ctx context.Context, workflow selectedFlowActivationRetirementStore, activations []selectedFlowActivation) ([]selectedFlowActivation, error) {
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
		if activation.timersProjected {
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
