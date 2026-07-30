package agentmemory

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
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

func TestPlanRejectsEnabledPlatformDefaultProvenance(t *testing.T) {
	for _, tc := range []struct {
		name string
		plan Plan
	}{
		{name: "explicit platform default", plan: Plan{Enabled: true, Source: SourcePlatformDefault}},
		{name: "empty source normalizes to platform default", plan: Plan{Enabled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.plan.Normalize(); err == nil || !strings.Contains(err.Error(), `requires source "authored"`) {
				t.Fatalf("Normalize error = %v, want authored-source requirement", err)
			}
		})
	}
}

func TestIdentityRequiresExactRunAgentAndFlowInstance(t *testing.T) {
	validAgent := agentidentitytest.Runtime(t, "agent-a", "test-fixture", "support", "chat-a", "support/chat-a")
	valid := Identity{RunID: "run-a", Agent: validAgent}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid identity: %v", err)
	}

	for _, tc := range []struct {
		name     string
		identity Identity
		want     string
	}{
		{name: "run", identity: Identity{Agent: validAgent}, want: "run_id"},
		{name: "agent", identity: Identity{RunID: "run-a"}, want: "agent_id"},
		{name: "flow instance", identity: Identity{
			RunID: "run-a",
			Agent: agentidentity.Identity{
				Name:  validAgent.Name,
				Route: agentidentity.RootRoute(),
			},
		}, want: "flow_instance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.identity.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestIdentityValueIsolatesRunsAndConcreteAgents(t *testing.T) {
	base := Identity{RunID: "run-a", Agent: agentidentitytest.Runtime(t, "agent-a", "test-fixture", "support", "chat-a", "support/chat-a")}
	otherRun := Identity{RunID: "run-b", Agent: base.Agent}
	otherFlow := Identity{RunID: "run-a", Agent: agentidentitytest.Runtime(t, "agent-a", "test-fixture", "support", "chat-b", "support/chat-b")}
	if base == otherRun {
		t.Fatal("different runs produced the same memory identity key")
	}
	if base == otherFlow {
		t.Fatal("different flow instances produced the same memory identity key")
	}
}
