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

func TestEffectiveRequirementsInferOnlyWhenOmitted(t *testing.T) {
	agents := map[string]runtimecontracts.AgentRegistryEntry{
		"worker": {
			Subscriptions: []string{"work.requested"},
			EmitEvents:    []string{"work.completed"},
		},
	}

	inferred := EffectiveRequirements(agents, nil, false)
	if len(inferred) != 1 || inferred[0].Role != "worker" || inferred[0].SubscribesTo[0] != "work.requested" || inferred[0].Emits[0] != "work.completed" {
		t.Fatalf("inferred = %#v, want worker from agents.yaml facts", inferred)
	}

	explicitEmpty := EffectiveRequirements(agents, nil, true)
	if len(explicitEmpty) != 0 {
		t.Fatalf("explicit empty = %#v, want no inference", explicitEmpty)
	}
}
