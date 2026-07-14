package agentmemory

import (
	"strings"
	"testing"
)

func TestPlanPreservesValueAndProvenance(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		source  Source
	}{
		{name: "omitted false", enabled: false, source: SourcePlatformDefault},
		{name: "authored false", enabled: false, source: SourceAuthored},
		{name: "authored true", enabled: true, source: SourceAuthored},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := NewPlan(tt.enabled, tt.source)
			if err != nil {
				t.Fatalf("NewPlan: %v", err)
			}
			if plan.Enabled != tt.enabled || plan.Source != tt.source {
				t.Fatalf("plan = %#v, want enabled=%v source=%q", plan, tt.enabled, tt.source)
			}
		})
	}
}

func TestIdentityRequiresExactRunAgentAndFlowInstance(t *testing.T) {
	valid := Identity{RunID: "run-a", AgentID: "agent-a", FlowInstance: "support/chat-a"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid identity: %v", err)
	}

	for _, tc := range []struct {
		name     string
		identity Identity
		want     string
	}{
		{name: "run", identity: Identity{AgentID: "agent-a", FlowInstance: "support/chat-a"}, want: "run_id"},
		{name: "agent", identity: Identity{RunID: "run-a", FlowInstance: "support/chat-a"}, want: "agent_id"},
		{name: "flow instance", identity: Identity{RunID: "run-a", AgentID: "agent-a"}, want: "flow_instance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.identity.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestIdentityKeyIsolatesRunsAndFlowInstances(t *testing.T) {
	base := Identity{RunID: "run-a", AgentID: "agent-a", FlowInstance: "support/chat-a"}
	otherRun := Identity{RunID: "run-b", AgentID: "agent-a", FlowInstance: "support/chat-a"}
	otherFlow := Identity{RunID: "run-a", AgentID: "agent-a", FlowInstance: "support/chat-b"}
	if base.Key() == otherRun.Key() {
		t.Fatal("different runs produced the same memory identity key")
	}
	if base.Key() == otherFlow.Key() {
		t.Fatal("different flow instances produced the same memory identity key")
	}
}
