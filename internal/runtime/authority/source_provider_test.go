package authority

import (
	"encoding/json"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	models "empireai/internal/runtime/core/actors"
	"empireai/internal/runtime/semanticview"
)

func TestNewSourceProvider_UsesDeclaredAgentEmitEventsOnly(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:         "worker",
				Role:       "worker",
				EmitEvents: []string{"work.completed"},
			},
		},
	}

	provider := NewSourceProvider(semanticview.Wrap(bundle))
	got := provider.ProducerEventsForRole("worker")
	if len(got) != 1 || got[0] != "work.completed" {
		t.Fatalf("ProducerEventsForRole(worker) = %#v, want [work.completed]", got)
	}
	if got := provider.ProducerEventsForRole("dashboard"); len(got) != 0 {
		t.Fatalf("ProducerEventsForRole(dashboard) = %#v, want nil/empty", got)
	}
	if got := provider.ProducerEventsForRole("actor-agent"); len(got) != 0 {
		t.Fatalf("ProducerEventsForRole(actor-agent) = %#v, want nil/empty", got)
	}
}

func TestNewSourceProvider_AuthorityMatrix(t *testing.T) {
	provider := NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control-plane": {ID: "control-plane", Role: "control-plane"},
			"reviewer":      {ID: "reviewer", Role: "reviewer"},
			"worker":        {ID: "worker", Role: "worker"},
		},
	}))

	controlPlane := testAgentConfig(
		"control-plane",
		"control-plane",
		[]string{"message_flow", "configure_routing", "agent_hire", "mailbox_send"},
		"",
		"review/inst-1",
		"",
	)
	reviewer := testAgentConfig(
		"reviewer",
		"reviewer",
		[]string{"message_peers", "mailbox_send"},
		"",
		"review/inst-1",
		"control-plane",
	)
	worker := testAgentConfig(
		"worker-a",
		"worker",
		[]string{"message_peers"},
		"",
		"review/inst-1",
		"control-plane",
	)
	otherFlowWorker := testAgentConfig(
		"worker-b",
		"worker",
		[]string{"message_peers"},
		"",
		"review/inst-2",
		"control-plane",
	)

	if !provider.HasMessageAuthority(controlPlane, reviewer) {
		t.Fatal("expected control-plane to message reviewer in same flow instance")
	}
	if !provider.HasMessageAuthority(reviewer, worker) {
		t.Fatal("expected peers with same manager_fallback to message each other")
	}
	if provider.HasMessageAuthority(worker, otherFlowWorker) {
		t.Fatal("expected cross-flow peer messaging to be denied")
	}
	if err := provider.AuthorizeRouting(controlPlane, worker, "active"); err != nil {
		t.Fatalf("expected control-plane routing authority: %v", err)
	}
	if err := provider.AuthorizeManagement(controlPlane, reviewer); err != nil {
		t.Fatalf("expected control-plane to manage reviewer: %v", err)
	}
	if err := provider.AuthorizeManagement(reviewer, controlPlane); err == nil {
		t.Fatal("expected reviewer ancestor management to be denied")
	}
	if err := provider.AuthorizeMailboxSend(reviewer); err != nil {
		t.Fatalf("expected reviewer mailbox permission: %v", err)
	}
}

func testAgentConfig(id, role string, permissions []string, entityID, flowPath, managerFallback string) models.AgentConfig {
	payload := map[string]any{
		"flow_path": flowPath,
	}
	if managerFallback != "" {
		payload["manager_fallback"] = managerFallback
	}
	raw, _ := json.Marshal(payload)
	return models.AgentConfig{
		ID:          id,
		Role:        role,
		Permissions: permissions,
		EntityID:    entityID,
		ParentAgent: managerFallback,
		Config:      raw,
	}
}
