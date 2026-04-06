package tools

import (
	"context"
	"strings"
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

type captureManagerStub struct {
	agents            map[string]models.AgentConfig
	spawnedEntityID   string
	spawnedConfig     models.AgentConfig
	spawnCalled       bool
	reconfiguredID    string
	reconfiguredPatch models.AgentConfig
	reconfigureCalled bool
}

func (m *captureManagerStub) GetAgentConfig(agentID string) (models.AgentConfig, bool) {
	cfg, ok := m.agents[agentID]
	return cfg, ok
}

func (m *captureManagerStub) SpawnAgentForEntity(entityID string, cfg models.AgentConfig) error {
	m.spawnedEntityID = entityID
	m.spawnedConfig = cfg
	m.spawnCalled = true
	if m.agents == nil {
		m.agents = map[string]models.AgentConfig{}
	}
	m.agents[cfg.ID] = cfg
	return nil
}

func (m *captureManagerStub) TeardownAgent(string) error { return nil }

func (m *captureManagerStub) ReconfigureAgent(agentID string, cfg models.AgentConfig) error {
	m.reconfiguredID = agentID
	m.reconfiguredPatch = cfg
	m.reconfigureCalled = true
	if m.agents == nil {
		m.agents = map[string]models.AgentConfig{}
	}
	current := m.agents[agentID]
	current = mergeDelegablePrivilegeConfig(current, cfg)
	current.ID = agentID
	m.agents[agentID] = current
	return nil
}

func TestAuthorizeManage_AllowsAncestorManagerChain(t *testing.T) {
	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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
	}))

	manager := managerStub{
		agents: map[string]models.AgentConfig{
			"control": {ID: "control"},
			"reviewer": {
				ID:              "reviewer",
				ParentAgent:     "control",
				FlowPath:        "review/inst-1",
				ManagerFallback: "control",
			},
			"worker": {
				ID:              "worker",
				ParentAgent:     "reviewer",
				FlowPath:        "review/inst-1",
				ManagerFallback: "reviewer",
			},
		},
	}
	actor := models.AgentConfig{
		ID:          "control",
		Role:        "control",
		Permissions: []string{"agent_fire"},
		FlowPath:    "review/inst-1",
	}
	target := manager.agents["worker"]

	if err := authorizeManage(provider, actor, target, manager); err != nil {
		t.Fatalf("expected ancestor manager to be allowed, got %v", err)
	}
}

func TestExecAgentMessage_AllowsCrossEntityWhenAuthorityPermits(t *testing.T) {
	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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
	}))

	bus := &publishDirectBusStub{}
	manager := managerStub{
		agents: map[string]models.AgentConfig{
			"target-1": {
				ID:              "target-1",
				Role:            "reviewer",
				EntityID:        "entity-b",
				FlowPath:        "review/inst-1",
				ManagerFallback: "control",
			},
		},
	}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:          "control",
		Role:        "control",
		Permissions: []string{"message_flow"},
		EntityID:    "entity-a",
		FlowPath:    "review/inst-1",
	})

	if _, err := exec.execAgentMessage(ctx, models.AgentConfig{
		ID:          "control",
		Role:        "control",
		Permissions: []string{"message_flow"},
		EntityID:    "entity-a",
		FlowPath:    "review/inst-1",
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

func TestExecAgentHire_DeniesDelegatedPermissionEscalation(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ID:          "manager-1",
		Role:        "manager",
		Permissions: []string{"agent_hire"},
		FlowPath:    "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
			"flow_path":        "review/inst-1",
			"permissions":      []any{"agent_fire"},
		},
	})
	if err == nil {
		t.Fatal("expected delegated permission escalation to be denied")
	}
	if !strings.Contains(err.Error(), `delegated permission "agent_fire"`) {
		t.Fatalf("error = %q, want delegated permission denial", err.Error())
	}
	if manager.spawnCalled {
		t.Fatal("expected denied hire to fail closed before spawning")
	}
}

func TestExecAgentHire_DeniesDelegatedToolEscalation(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ID:          "manager-1",
		Role:        "manager",
		Permissions: []string{"agent_hire"},
		Tools:       []string{"lookup_data"},
		FlowPath:    "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
			"flow_path":        "review/inst-1",
			"tools":            []any{"deploy_prod"},
		},
	})
	if err == nil {
		t.Fatal("expected delegated tool escalation to be denied")
	}
	if !strings.Contains(err.Error(), `delegated tool "deploy_prod"`) {
		t.Fatalf("error = %q, want delegated tool denial", err.Error())
	}
	if manager.spawnCalled {
		t.Fatal("expected denied hire to fail closed before spawning")
	}
}

func TestExecAgentHire_DeniesRoleBasedEmitEscalation(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager":   {ID: "manager", Role: "manager", EmitEvents: []string{"review.started"}},
			"worker":    {ID: "worker", Role: "worker", ManagerFallback: "manager", EmitEvents: []string{"review.started"}},
			"escalated": {ID: "escalated", Role: "escalated", ManagerFallback: "manager", EmitEvents: []string{"security.root"}},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ID:          "manager-1",
		Role:        "manager",
		Permissions: []string{"agent_hire"},
		FlowPath:    "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "escalated",
			"manager_fallback": "manager",
			"flow_path":        "review/inst-1",
		},
	})
	if err == nil {
		t.Fatal("expected role-based emit authority escalation to be denied")
	}
	if !strings.Contains(err.Error(), `delegated emit authority "security.root"`) {
		t.Fatalf("error = %q, want delegated emit authority denial", err.Error())
	}
	if manager.spawnCalled {
		t.Fatal("expected denied hire to fail closed before spawning")
	}
}

func TestExecAgentHire_AllowsDelegablePrivileges(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager", EmitEvents: []string{"review.started"}},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ID:          "manager-1",
		Role:        "manager",
		Permissions: []string{"agent_hire", "schedule"},
		Tools:       []string{"lookup_data"},
		NativeTools: models.NativeToolConfig{FileIO: true},
		EmitEvents:  []string{"review.started"},
		FlowPath:    "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
			"flow_path":        "review/inst-1",
			"permissions":      []any{"schedule"},
			"tools":            []any{"lookup_data"},
			"native_tools": map[string]any{
				"file_io": true,
			},
			"emit_events": []any{"review.started"},
		},
	})
	if err != nil {
		t.Fatalf("expected delegable privilege set to be allowed, got %v", err)
	}
	if !manager.spawnCalled {
		t.Fatal("expected allowed hire to spawn agent")
	}
	if manager.spawnedEntityID != "" {
		t.Fatalf("spawned entity id = %q, want empty", manager.spawnedEntityID)
	}
	if len(manager.spawnedConfig.Permissions) != 1 || manager.spawnedConfig.Permissions[0] != "schedule" {
		t.Fatalf("spawned permissions = %#v, want [schedule]", manager.spawnedConfig.Permissions)
	}
	if len(manager.spawnedConfig.Tools) != 1 || manager.spawnedConfig.Tools[0] != "lookup_data" {
		t.Fatalf("spawned tools = %#v, want [lookup_data]", manager.spawnedConfig.Tools)
	}
	if !manager.spawnedConfig.NativeTools.FileIO {
		t.Fatalf("spawned native tools = %#v, want file_io enabled", manager.spawnedConfig.NativeTools)
	}
	if len(manager.spawnedConfig.EmitEvents) != 1 || manager.spawnedConfig.EmitEvents[0] != "review.started" {
		t.Fatalf("spawned emit events = %#v, want [review.started]", manager.spawnedConfig.EmitEvents)
	}
}

func TestExecAgentReconfigure_DeniesNativeToolEscalation(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{
		agents: map[string]models.AgentConfig{
			"worker-1": {
				ID:              "worker-1",
				Role:            "worker",
				ManagerFallback: "manager",
				FlowPath:        "review/inst-1",
			},
		},
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
		ID:          "manager-1",
		Role:        "manager",
		Permissions: []string{"agent_reconfigure"},
		FlowPath:    "review/inst-1",
	}, map[string]any{
		"agent_id": "worker-1",
		"config": map[string]any{
			"native_tools": map[string]any{
				"bash": true,
			},
		},
	})
	if err == nil {
		t.Fatal("expected delegated native tool escalation to be denied")
	}
	if !strings.Contains(err.Error(), "delegated native_tools.bash") {
		t.Fatalf("error = %q, want delegated native tool denial", err.Error())
	}
	if manager.reconfigureCalled {
		t.Fatal("expected denied reconfigure to fail closed before persistence")
	}
}
