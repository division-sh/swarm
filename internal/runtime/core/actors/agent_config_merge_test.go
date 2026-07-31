package actors

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestMergeAgentConfigBuildsCompleteParentBearingCandidate(t *testing.T) {
	identity := agentidentitytest.Runtime(
		t, "worker", "agent-config-merge-test", "review", "inst-1", "review/inst-1",
	)
	base := AgentConfig{
		ID: "worker", Identity: identity, Role: "worker", Model: "regular",
		ParentAgent: "old-parent", ManagerFallback: "old-parent", FlowPath: identity.FlowInstance(),
	}
	patch := AgentConfig{
		Model: "fast", ParentAgent: "new-parent", ManagerFallback: "fallback-parent",
	}

	candidate := MergeAgentConfig(base, patch)
	if candidate.Model != "fast" ||
		candidate.ParentAgent != "new-parent" ||
		candidate.ManagerFallback != "fallback-parent" {
		t.Fatalf("merged candidate = %+v", candidate)
	}
	if candidate.ID != base.ID || candidate.Identity != base.Identity || candidate.FlowPath != base.FlowPath {
		t.Fatalf("merge changed concrete child identity: before=%+v after=%+v", base, candidate)
	}
}
