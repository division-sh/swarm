package agentmemory

import (
	"strings"
	"testing"

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
	valid := agentidentitytest.RuntimeForRun(t, "run-a", "agent-a", "test-fixture", "support", "chat-a", "support/chat-a")
	if err := ValidateIdentity(valid, true); err != nil {
		t.Fatalf("valid identity: %v", err)
	}
	missingRun := valid
	missingRun.RunID = ""
	root := agentidentitytest.RootRuntimeForRun(t, "run-a", "agent-a", "test-fixture")

	for _, tc := range []struct {
		name     string
		identity Identity
		want     string
	}{
		{name: "run", identity: missingRun, want: "run_id"},
		{name: "agent", identity: Identity{RunID: "run-a"}, want: "agent_id"},
		{name: "flow instance", identity: root, want: "flow_instance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateIdentity(tc.identity, true); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestIdentityValueIsolatesRunsAndConcreteAgents(t *testing.T) {
	base := agentidentitytest.RuntimeForRun(t, "run-a", "agent-a", "test-fixture", "support", "chat-a", "support/chat-a")
	otherRun := base
	otherRun.RunID = "run-b"
	otherFlow := agentidentitytest.RuntimeForRun(t, "run-a", "agent-a", "test-fixture", "support", "chat-b", "support/chat-b")
	if base == otherRun {
		t.Fatal("different runs produced the same memory identity key")
	}
	if base == otherFlow {
		t.Fatal("different flow instances produced the same memory identity key")
	}
}
