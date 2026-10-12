package manager

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// An absent actor can defer only to its live, durably accepted attachment.
// This observation grants neither agent construction nor delivery execution.
func (am *AgentManager) committedAgentAttachmentInstalling(ctx context.Context, identity agentidentity.Identity) (bool, error) {
	ownerCtx, _, running := am.lifecycle.runSnapshot()
	if !running || ownerCtx == nil || ownerCtx.Err() != nil {
		return false, nil
	}
	key, active, executor, receipt, hash := am.installingAgentAttachment(identity)
	if active == nil || am.workflowInstances == nil {
		return false, nil
	}
	if err := receipt.Validate(); err != nil {
		return false, err
	}
	if receipt.RunID() != key.runID || receipt.InstancePath() != key.instancePath {
		return false, errors.New("accepted agent attachment receipt differs from its exact owner")
	}
	provider, ok := am.lifecycle.persistence().(processExecutionBindingProvider)
	if !ok {
		return false, errors.New("accepted agent attachment requires its generation binding")
	}
	binding, err := provider.ProcessExecutionBinding()
	if err != nil {
		return false, err
	}
	if !receipt.ProcessBinding().Equal(binding) {
		return false, pipeline.ErrFlowAttachmentStale
	}
	row, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, active.identity.Route)
	if err != nil {
		return false, err
	}
	installing, err := installingAgentReadiness(row, found, active, receipt, hash, identity)
	if err != nil || !installing {
		return false, err
	}
	if err := am.workflowInstances.VerifyDynamicFlowRuntimeActivationAttempt(ctx, receipt); err != nil {
		return false, err
	}
	// Native verification never runs under the process mutex. Recheck the
	// executor afterwards so retirement/successor handoff cannot lend its wake.
	am.dynamicFlowReadinessMu.Lock()
	defer am.dynamicFlowReadinessMu.Unlock()
	return ownerCtx.Err() == nil && am.dynamicFlowActiveAttempts[key] == active && am.dynamicFlowReadinessAttempts[key] == executor &&
		active.receipt == receipt && active.planHash == hash && agentAttachmentInstalling(active, executor), nil
}

func installingAgentReadiness(row pipeline.DynamicFlowRuntimeReadiness, found bool, active *dynamicFlowActiveAttempt, receipt pipeline.DynamicFlowRuntimeActivationAttempt, hash string, identity agentidentity.Identity) (bool, error) {
	if !found || row.Plan.RunID != receipt.RunID() || row.InstancePath != receipt.InstancePath() || row.Plan.Identity.Route() != active.identity.Route ||
		row.PlanHash != hash || row.AttemptOrdinal != receipt.Ordinal() {
		return false, errors.New("accepted agent attachment disagrees with current readiness evidence")
	}
	if !row.Eligible() || row.AttemptState != "accepted" || !row.Phase.Valid() || row.Phase.Includes(pipeline.FlowAttachmentAgentsRegistered) {
		return false, nil
	}
	return readinessPlanOwnsAgent(row.Plan, identity), nil
}

func readinessPlanOwnsAgent(plan pipeline.DynamicFlowRuntimeReadinessPlan, identity agentidentity.Identity) bool {
	for _, expected := range plan.Agents {
		if expected.Identity == identity {
			return true
		}
	}
	return false
}

func (am *AgentManager) installingAgentAttachment(identity agentidentity.Identity) (dynamicFlowRuntimeReadinessKey, *dynamicFlowActiveAttempt, *dynamicFlowRuntimeReadinessAttempt, pipeline.DynamicFlowRuntimeActivationAttempt, string) {
	am.dynamicFlowReadinessMu.Lock()
	defer am.dynamicFlowReadinessMu.Unlock()
	for key, active := range am.dynamicFlowActiveAttempts {
		if active == nil || !active.identity.MatchesAgentRoute(identity) {
			continue
		}
		executor := am.dynamicFlowReadinessAttempts[key]
		if agentAttachmentInstalling(active, executor) {
			return key, active, executor, active.receipt, active.planHash
		}
	}
	return dynamicFlowRuntimeReadinessKey{}, nil, nil, pipeline.DynamicFlowRuntimeActivationAttempt{}, ""
}

func agentAttachmentInstalling(active *dynamicFlowActiveAttempt, executor *dynamicFlowRuntimeReadinessAttempt) bool {
	if active == nil || executor == nil || active.pending != nil || active.complete || active.retiring || active.retirementKind != 0 ||
		active.locallyRetired || executor.successorRequired || executor.attemptOrdinal != active.receipt.Ordinal() || executor.planHash != active.planHash {
		return false
	}
	select {
	case <-executor.done:
		return false
	case <-executor.retiring:
		return false
	default:
		return true
	}
}
