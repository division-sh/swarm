package commgraph

import (
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	models "empireai/internal/runtime/core/actors"
	"empireai/internal/runtime/semanticview"
)

func TestSourcePolicyUsesBootedSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control-plane": {
				Role:       "control-plane",
				ToolsTier2: []string{"message_all", "agent_hire", "configure_routing"},
				EmitEvents: []string{"system.created"},
			},
			"reviewer": {
				Role:       "reviewer",
				ToolsTier2: []string{"message_domain", "human_task_decide"},
			},
			"worker": {
				Role:       "worker",
				EmitEvents: []string{"work.completed"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"system.created": {EmitterType: "agent"},
			"work.completed": {EmitterType: "agent"},
		},
	})

	SetContractSource(source)
	SetDefaultPolicyFactory(func() Policy {
		return NewSourcePolicy(source)
	})
	t.Cleanup(func() {
		SetContractSource(nil)
		SetDefaultPolicyFactory(func() Policy {
			return NewGenericTestPolicy()
		})
	})

	if got := ProducerEventsForRole("control-plane"); len(got) != 1 || got[0] != "system.created" {
		t.Fatalf("control-plane producer events = %#v", got)
	}
	if got := ProducerEventsForRole("worker"); len(got) != 1 || got[0] != "work.completed" {
		t.Fatalf("worker producer events = %#v", got)
	}

	controlPlane := models.AgentConfig{ID: "cp", Role: "control-plane"}
	reviewer := models.AgentConfig{ID: "rv", Role: "reviewer", EntityID: "ent-1"}
	worker := models.AgentConfig{ID: "wk", Role: "worker", EntityID: "ent-2"}

	if !HasMessageAuthority(controlPlane, reviewer) {
		t.Fatal("expected control-plane message authority from source policy")
	}
	if !CanDecideHumanTasks("reviewer") {
		t.Fatal("expected reviewer to decide human tasks from source policy")
	}
	if err := AuthorizeRouting(controlPlane, worker, "active"); err != nil {
		t.Fatalf("expected control-plane routing authority: %v", err)
	}
	if err := AuthorizeManagement(controlPlane, worker.Role, worker.EntityID); err != nil {
		t.Fatalf("expected control-plane management authority: %v", err)
	}
	if err := AuthorizeMailboxSend(worker); err != nil {
		t.Fatalf("expected mailbox send authority for worker: %v", err)
	}
}
