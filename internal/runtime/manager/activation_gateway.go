package manager

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// ProveUnpublishedActivation is an observation of the existing lifecycle owner,
// not a second admission grant. An old generation or published route cannot use
// the pre-publication probe entrance.
func (am *AgentManager) ProveUnpublishedActivation(token effects.LifecycleToken) error {
	_, err := am.unpublishedActivationRevision(token)
	return err
}

func (am *AgentManager) unpublishedActivationRevision(token effects.LifecycleToken) (string, error) {
	revision, _, err := am.unpublishedActivationState(token)
	return revision, err
}

func (am *AgentManager) unpublishedActivationState(token effects.LifecycleToken) (string, agenttopology.Admission, error) {
	if am != nil && am.lifecycle != nil && token.Valid() {
		am.lifecycle.mu.Lock()
		defer am.lifecycle.mu.Unlock()
		cell := am.lifecycle.cells[token.Identity]
		if am.lifecycle.phase == runtimeLifecycleRunning && cell != nil && cell.view.Phase == AgentLifecycleRunning && cell.execution != nil {
			execution := cell.execution
			if execution.token == token && !execution.fenced && execution.routeToken == (effects.LifecycleToken{}) && execution.route == nil && execution.loopDone != nil && execution.generationCtx != nil && execution.generationCtx.Err() == nil {
				return cell.configRevision, cell.topology, nil
			}
		}
	}
	return "", agenttopology.Admission{}, failures.New(failures.ClassLifecycleConflict, "activation_probe_generation_not_current", "agent-lifecycle", "observe_workspace_gateway", nil)
}

// ProveUnpublishedFlowActor observes an exact current construction attempt. The
// returned planning coordinates do not grant execution or extend a fork census.
func (am *AgentManager) ProveUnpublishedFlowActor(ctx context.Context, token effects.LifecycleToken, actor actors.AgentConfig) (agenttopology.FlowReadinessPlan, error) {
	if err := am.proveUnpublishedActivationActor(ctx, token, actor); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	_, topology, err := am.unpublishedActivationState(token)
	if err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	if err := topology.Validate(); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	if topology.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan || topology.Authority.Readiness.Preparation || am.workflowInstances == nil {
		return agenttopology.FlowReadinessPlan{}, fmt.Errorf("activation observation requires a current constructed-flow attempt")
	}
	admission := *topology.Authority.Readiness
	if admission.RunID != actor.Identity.RunID || admission.InstancePath != actor.Identity.FlowInstance() {
		return agenttopology.FlowReadinessPlan{}, fmt.Errorf("activation observation differs from its constructed-flow owner")
	}
	if err := am.requireRunExecutionOwnership(ctx, admission.RunID); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	key := dynamicFlowRuntimeReadinessKey{runID: admission.RunID, instancePath: admission.InstancePath}
	am.dynamicFlowReadinessMu.Lock()
	active := am.dynamicFlowActiveAttempts[key]
	if active == nil || active.retiring || active.pending != nil || active.receipt.ID() != admission.AttemptID {
		am.dynamicFlowReadinessMu.Unlock()
		return agenttopology.FlowReadinessPlan{}, fmt.Errorf("activation observation has no exact current attachment")
	}
	attempt, identity := active.receipt, active.identity
	am.dynamicFlowReadinessMu.Unlock()
	if !identity.MatchesAgentRoute(actor.Identity) {
		return agenttopology.FlowReadinessPlan{}, fmt.Errorf("activation observation differs from its exact constructed identity")
	}
	if err := am.workflowInstances.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, admission.RunID, identity.Route)
	if err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	if !found {
		return agenttopology.FlowReadinessPlan{}, fmt.Errorf("activation observation has no matching current readiness")
	}
	if err := validateActivationActorReadiness(readiness, admission, attempt, identity); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	if _, err := am.verifyConstructedFlowTree(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: am.semanticReadinessSource.source, Instance: readiness.Plan.Identity,
	}, admission.RunID); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	if err := proveActivationActorExpectation(actor, readiness.Plan.Agents); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	am.dynamicFlowReadinessMu.Lock()
	current := am.dynamicFlowActiveAttempts[key] == active && !active.retiring && active.pending == nil && active.receipt == attempt
	am.dynamicFlowReadinessMu.Unlock()
	if !current {
		return agenttopology.FlowReadinessPlan{}, fmt.Errorf("activation observation attachment changed during admission")
	}
	if err := am.proveUnpublishedActivationActor(ctx, token, actor); err != nil {
		return agenttopology.FlowReadinessPlan{}, err
	}
	return admission, am.requireRunExecutionOwnership(ctx, admission.RunID)
}

func validateActivationActorReadiness(readiness pipeline.DynamicFlowRuntimeReadiness, admission agenttopology.FlowReadinessPlan, attempt pipeline.DynamicFlowRuntimeActivationAttempt, identity flowidentity.RunScopedFlowInstance) error {
	if readiness.PlanHash != admission.PlanFingerprint || readiness.AttemptOrdinal != attempt.Ordinal() || readiness.AttemptState != "accepted" || !readiness.Eligible() {
		return fmt.Errorf("activation observation has no matching current readiness")
	}
	if readiness.Plan.RunID != admission.RunID || readiness.Plan.Identity.Route() != identity.Route || readiness.Plan.BundleHash != attempt.ProcessBinding().BundleHash || readiness.OwningRunSource.BundleHash() != readiness.Plan.BundleHash {
		return fmt.Errorf("activation observation readiness differs from its admitted source or identity")
	}
	return nil
}

func proveActivationActorExpectation(actor actors.AgentConfig, expected []pipeline.DynamicFlowRuntimeAgentExpectation) error {
	var exact []pipeline.DynamicFlowRuntimeAgentExpectation
	for _, candidate := range expected {
		if candidate.Identity == actor.Identity {
			exact = append(exact, candidate)
		}
	}
	if len(exact) != 1 {
		return fmt.Errorf("activation observation requires exactly one admitted actor")
	}
	return verifyDynamicFlowAgentExpectations([]PersistedAgent{{Config: actor}}, exact)
}

func (am *AgentManager) proveUnpublishedActivationActor(ctx context.Context, token effects.LifecycleToken, actor actors.AgentConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !token.Valid() || actor.Identity != token.Identity {
		return fmt.Errorf("activation observation requires its exact actor and lifecycle token")
	}
	revision, err := lifecycleConfigRevision(PersistedAgent{Config: actor})
	if err != nil {
		return err
	}
	current, err := am.unpublishedActivationRevision(token)
	if err != nil {
		return err
	}
	if revision != current {
		return failures.New(failures.ClassLifecycleConflict, "activation_probe_configuration_not_current", "agent-lifecycle", "observe_workspace_gateway", nil)
	}
	return ctx.Err()
}

func (am *AgentManager) observeUnpublishedActivation(ctx context.Context, token effects.LifecycleToken, actor actors.AgentConfig) error {
	if err := am.proveUnpublishedActivationActor(ctx, token, actor); err != nil {
		return err
	}
	if err := am.workspaceGatewayAdmission(ctx, actor); err != nil {
		return err
	}
	return am.proveUnpublishedActivationActor(ctx, token, actor)
}
