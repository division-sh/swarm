package agentpersistence

import (
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func selfReleasePhaseOnly(req manager.AgentLifecycleTransition, previous manager.AgentLifecycleState) bool {
	return previous.Phase == manager.AgentLifecycleRunning && req.TargetPhase == manager.AgentLifecycleRegistered &&
		req.TargetEpoch == previous.RuntimeEpoch && req.TargetGeneration == previous.Generation &&
		req.ConfigRevision == previous.ConfigRevision && req.RunMode == manager.AgentRunModeStopped &&
		req.Agent == nil && req.Subordinate.Action == sessions.LifecycleMutationNone && previous.Topology.Equal(req.Topology)
}

func sourceSetReplacementShape(req manager.AgentLifecycleTransition) bool {
	if req.TargetPhase == manager.AgentLifecycleRunning {
		return req.Agent != nil
	}
	return req.TargetPhase == manager.AgentLifecycleRegistered && req.RunMode == manager.AgentRunModeStopped
}

// Generation and topology owners have already authorized the native writer in
// the original transaction. This arm cannot replace execution or its resources.
func sourceSetSurvivorRebind(req manager.AgentLifecycleTransition, previous manager.AgentLifecycleState, instanceRebind bool) bool {
	if req.OperationKind != "source_set_rebind" || req.Agent != nil || req.Subordinate.Action != sessions.LifecycleMutationNone ||
		req.Identity.Normalize() != previous.Identity.Normalize() ||
		req.TargetEpoch != previous.RuntimeEpoch || req.TargetGeneration != previous.Generation || req.TargetPhase != previous.Phase ||
		req.ConfigRevision != previous.ConfigRevision || req.RunMode != previous.RunMode {
		return false
	}
	before, after := previous.ProcessBinding, req.ProcessBinding
	if before.Validate() != nil || after.Validate() != nil {
		return false
	}
	if !before.SameProcessExecution(after) || before.GenerationGrantID == after.GenerationGrantID {
		return false
	}
	if instanceRebind {
		return previous.Topology.Authority.Kind == agenttopology.AuthorityFlowReadinessPlan && previous.Topology.Equal(req.Topology)
	}
	if previous.Topology.Authority.Kind != agenttopology.AuthorityStaticDeclarationPlan ||
		req.Topology.Authority.Kind != agenttopology.AuthorityStaticDeclarationPlan {
		return false
	}
	old, next := previous.Topology.Authority.Static, req.Topology.Authority.Static
	return old.BundleHash == next.BundleHash && old.SourceSetRevision != next.SourceSetRevision && previous.Topology.Lifetime == req.Topology.Lifetime
}
