package tools

import (
	"context"
	"testing"

	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

type managerStub struct {
	agents map[string]models.AgentConfig
}

func (m managerStub) GetAgentConfig(agentID string) (models.AgentConfig, bool) {
	cfg, ok := m.agents[agentID]
	return cfg, ok
}

func (managerStub) SpawnAgentForEntity(string, models.AgentConfig) error { return nil }
func (managerStub) TeardownAgent(string) error                           { return nil }
func (managerStub) ReconfigureAgent(string, models.AgentConfig) error    { return nil }

type publishDirectBusStub struct {
	recipients []string
}

func (b *publishDirectBusStub) Publish(context.Context, events.Event) error { return nil }

func (b *publishDirectBusStub) PublishDirect(_ context.Context, _ events.Event, recipients []string) error {
	b.recipients = append([]string{}, recipients...)
	return nil
}

func TestAuthorizeManage_AllowsAncestorManagerChain(t *testing.T) {
	runtimeauthority.SetProvider(runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control": {
				ID:   "control",
				Role: "control",
			},
			"reviewer": {
				ID:              "reviewer",
				Role:            "reviewer",
				ManagerFallback: "control",
			},
			"worker": {
				ID:              "worker",
				Role:            "worker",
				ManagerFallback: "reviewer",
			},
		},
	})))
	defer runtimeauthority.SetProvider(nil)

	manager := managerStub{
		agents: map[string]models.AgentConfig{
			"control": {ID: "control"},
			"reviewer": {
				ID:          "reviewer",
				ParentAgent: "control",
				Config:      []byte(`{"flow_path":"review/inst-1","manager_fallback":"control"}`),
			},
			"worker": {
				ID:          "worker",
				ParentAgent: "reviewer",
				Config:      []byte(`{"flow_path":"review/inst-1","manager_fallback":"reviewer"}`),
			},
		},
	}
	actor := models.AgentConfig{
		ID:          "control",
		Role:        "control",
		Permissions: []string{"agent_fire"},
		Config:      []byte(`{"flow_path":"review/inst-1"}`),
	}
	target := manager.agents["worker"]

	if err := authorizeManage(actor, target, manager); err != nil {
		t.Fatalf("expected ancestor manager to be allowed, got %v", err)
	}
}

func TestExecAgentMessage_AllowsCrossEntityWhenAuthorityPermits(t *testing.T) {
	runtimeauthority.SetProvider(runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"control": {
				ID:    "control",
				Role:  "control",
				Tools: []string{"message_flow"},
			},
			"reviewer": {
				ID:    "reviewer",
				Role:  "reviewer",
				Tools: []string{"message_peers"},
			},
		},
	})))
	defer runtimeauthority.SetProvider(nil)

	bus := &publishDirectBusStub{}
	manager := managerStub{
		agents: map[string]models.AgentConfig{
			"target-1": {
				ID:       "target-1",
				Role:     "reviewer",
				EntityID: "entity-b",
				Config:   []byte(`{"flow_path":"review/inst-1","manager_fallback":"control"}`),
			},
		},
	}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{Manager: manager})
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:          "control",
		Role:        "control",
		Permissions: []string{"message_flow"},
		EntityID:    "entity-a",
		Config:      []byte(`{"flow_path":"review/inst-1"}`),
	})

	if _, err := exec.execAgentMessage(ctx, models.AgentConfig{
		ID:          "control",
		Role:        "control",
		Permissions: []string{"message_flow"},
		EntityID:    "entity-a",
		Config:      []byte(`{"flow_path":"review/inst-1"}`),
	}, map[string]any{
		"target_agent_id": "target-1",
		"message":         "hello",
	}); err != nil {
		t.Fatalf("expected cross-entity agent_message to be allowed, got %v", err)
	}
	if len(bus.recipients) != 1 || bus.recipients[0] != "target-1" {
		t.Fatalf("recipients = %#v, want [target-1]", bus.recipients)
	}
}
