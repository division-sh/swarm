package requiredagents

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestResolveAgentRequiresMapKeyIdentity(t *testing.T) {
	agents := map[string]runtimecontracts.AgentRegistryEntry{
		"alias": {
			ID:   "worker",
			Role: "worker",
		},
	}

	if agentID, _, ok := ResolveAgent(agents, runtimecontracts.FlowRequiredAgent{Role: "worker"}); ok {
		t.Fatalf("ResolveAgent matched %q through non-key identity", agentID)
	}
}

func TestCheckScopeReportsSubscriptionAndEmitCoverage(t *testing.T) {
	findings := CheckScope(Scope{
		ID: "child",
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				Subscriptions: []string{"work.requested"},
				EmitEvents:    []string{"work.completed"},
			},
		},
		Required: []runtimecontracts.FlowRequiredAgent{{
			Role:         "worker",
			SubscribesTo: []string{"work.requested", "work.escalated"},
			Emits:        []string{"work.completed", "work.failed"},
		}},
	})

	if len(findings) != 2 {
		t.Fatalf("findings = %#v, want subscription and emit findings", findings)
	}
	if findings[0].Kind != FindingMissingSubscriptions || findings[0].Missing[0] != "work.escalated" {
		t.Fatalf("subscription finding = %#v", findings[0])
	}
	if findings[1].Kind != FindingMissingEmits || findings[1].Missing[0] != "work.failed" {
		t.Fatalf("emit finding = %#v", findings[1])
	}
}
