package runtime

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (rt *Runtime) observeStandingSessionBinding(ctx context.Context, declaration StandingTargetDeclaration, binding StandingIngressBinding) (standingBindingCredentials, error) {
	result := standingBindingCredentials{alias: declaration.Alias, plan: binding.AdmissionPlan,
		blockReason: runtimerunlifecycle.StandingBindingSessionRequired}
	store := rt.Options.ChannelOnboardingStore
	if store == nil {
		return result, nil
	}
	operations, err := store.ListChannelOnboardingOperations(ctx)
	if err != nil || len(operations) == 0 {
		return result, err
	}
	bundle, ok := semanticview.Bundle(rt.Options.WorkflowModule.SemanticSource())
	if !ok || bundle == nil || bundle.PackInventory == nil {
		return result, fmt.Errorf("session standing admission requires the exact admitted source inventory")
	}
	selector := standingIngressSelector(declaration.FlowPath, binding.Provider)
	for _, plan := range rt.Options.ChannelPlans {
		if plan.Transport() != packs.ChannelTransportSession || plan.Provider() != binding.Provider {
			continue
		}
		identity, err := plan.InterfaceIdentity()
		if err != nil {
			return result, err
		}
		generation, err := plan.Generation()
		if err != nil {
			return result, err
		}
		for _, op := range operations {
			if op.Posture != channelonboarding.ActivationSessionConnection || op.Provider != binding.Provider ||
				op.TargetSelector != selector || op.Interface.Normalized() != identity.Normalized() ||
				op.Coordinate.BundleHash != rt.Options.SourceArtifactFact.BundleHash() ||
				op.Coordinate.PackInventoryGeneration != bundle.PackInventory.Digest() || !op.Coordinate.PlanGeneration.Equal(generation) {
				continue
			}
			current, err := store.SessionStandingBindingCurrent(ctx, op)
			if err != nil {
				return result, err
			}
			if !current {
				continue
			}
			if result.sessionOperation != nil {
				return result, fmt.Errorf("ingress %s has competing confirmed session owners", selector)
			}
			selected := op
			result.enabled, result.blockReason = true, ""
			result.operationID, result.sessionOperation = op.OperationID, &selected
		}
	}
	return result, nil
}

func (rt *Runtime) validateStandingSessionBinding(ctx context.Context, binding standingBindingCredentials) error {
	if binding.sessionOperation == nil {
		return nil
	}
	current, err := rt.Options.ChannelOnboardingStore.SessionStandingBindingCurrent(ctx, *binding.sessionOperation)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("standing session binding %s no longer owns its confirmed responsibility", binding.operationID)
	}
	return nil
}
