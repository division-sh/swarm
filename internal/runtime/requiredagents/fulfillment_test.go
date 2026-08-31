package requiredagents

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCheckScopeReportsSubscriptionAndEmitCoverage(t *testing.T) {
	findings := CheckScope(Scope{
		ID: "child",
		Declarations: []semanticview.AgentDeclaration{
			{
				LocalID: "worker",
				Entry: runtimecontracts.AgentRegistryEntry{
					Subscriptions: []string{"work.requested"},
					EmitEvents:    []string{"work.completed"},
				},
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

func TestCheckScopeRejectsAmbiguousSameRoleDeclarations(t *testing.T) {
	findings := CheckScope(Scope{
		ID: "child",
		Declarations: []semanticview.AgentDeclaration{
			{LocalID: "worker", Source: runtimecontracts.ContractItemSource{File: "left/agents.yaml"}},
			{LocalID: "worker", Source: runtimecontracts.ContractItemSource{File: "right/agents.yaml"}},
		},
		Required: []runtimecontracts.FlowRequiredAgent{{Role: "worker"}},
	})

	if len(findings) != 1 || findings[0].Kind != FindingAmbiguousAgent {
		t.Fatalf("findings = %#v, want one ambiguous-agent finding", findings)
	}
	if want := []string{"left/agents.yaml", "right/agents.yaml"}; !reflect.DeepEqual(findings[0].Candidates, want) {
		t.Fatalf("candidates = %#v, want %#v", findings[0].Candidates, want)
	}
}
