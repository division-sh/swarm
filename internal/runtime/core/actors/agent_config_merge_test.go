package actors

import (
	"encoding/json"
	"strings"
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

func TestValidateNoAuthoredSystemPromptRejectsEveryNestedIngress(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "top_level", raw: `{"system_prompt":"obsolete"}`, want: "config.system_prompt"},
		{name: "nested_map", raw: `{"outer":{"system_prompt":"obsolete"}}`, want: "config.outer.system_prompt"},
		{name: "nested_array", raw: `{"outer":[{"system_prompt":"obsolete"}]}`, want: "config.outer[0].system_prompt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateNoAuthoredSystemPrompt(json.RawMessage(tc.raw))
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateNoAuthoredSystemPrompt error = %v, want retired path %q", err, tc.want)
			}
		})
	}
}

func TestValidateNoAuthoredSystemPromptAllowsOpaqueNonIntentConfig(t *testing.T) {
	if err := ValidateNoAuthoredSystemPrompt(json.RawMessage(`{"priority":"high","nested":[{"value":1}]}`)); err != nil {
		t.Fatalf("ValidateNoAuthoredSystemPrompt: %v", err)
	}
}
