package tools

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
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
	tornDownID        string
	teardownCalled    bool
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

func (m *captureManagerStub) TeardownAgent(agentID string) error {
	m.tornDownID = agentID
	m.teardownCalled = true
	delete(m.agents, agentID)
	return nil
}

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
		ExecutionMode: "live",
		ID:            "control",
		Role:          "control",
		Permissions:   []string{"agent_fire"},
		FlowPath:      "review/inst-1",
	}
	target := manager.agents["worker"]

	if err := authorizeManage(provider, actor, target, manager); err != nil {
		t.Fatalf("expected ancestor manager to be allowed, got %v", err)
	}
}

func TestExecAgentFire_UsesAuthorizedManagerLifecyclePath(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{agents: map[string]models.AgentConfig{
		"worker-1": {ID: "worker-1", Role: "worker", ManagerFallback: "manager", FlowPath: "review/inst-1"},
	}}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
	})

	result, err := exec.ExecAgentFireDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager-1", Role: "manager", Permissions: []string{"agent_fire"}, FlowPath: "review/inst-1",
	}, map[string]any{"agent_id": "worker-1"})
	if err != nil {
		t.Fatalf("ExecAgentFireDirect: %v", err)
	}
	if !manager.teardownCalled || manager.tornDownID != "worker-1" {
		t.Fatalf("teardown called=%v agent=%q, want worker-1", manager.teardownCalled, manager.tornDownID)
	}
	if got := result.(map[string]any)["status"]; got != "fired" {
		t.Fatalf("status = %v, want fired", got)
	}
}

func TestExecAgentReconfigure_UsesAuthorizedManagerLifecyclePath(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{agents: map[string]models.AgentConfig{
		"worker-1": {ID: "worker-1", Role: "worker", ManagerFallback: "manager", FlowPath: "review/inst-1"},
	}}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
	})

	result, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager-1", Role: "manager", Permissions: []string{"agent_reconfigure"}, FlowPath: "review/inst-1",
	}, map[string]any{"agent_id": "worker-1", "model": "fast"})
	if err != nil {
		t.Fatalf("ExecAgentReconfigureDirect: %v", err)
	}
	if !manager.reconfigureCalled || manager.reconfiguredID != "worker-1" || manager.reconfiguredPatch.Model != "fast" {
		t.Fatalf("reconfigure called=%v agent=%q patch=%+v", manager.reconfigureCalled, manager.reconfiguredID, manager.reconfiguredPatch)
	}
	if got := result.(map[string]any)["status"]; got != "reconfigured" {
		t.Fatalf("status = %v, want reconfigured", got)
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
	ctx := WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "control",
		Role:          "control",
		Permissions:   []string{"message_flow"},
		EntityID:      "entity-a",
		FlowPath:      "review/inst-1",
	})

	if _, err := exec.execAgentMessage(ctx, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "control",
		Role:          "control",
		Permissions:   []string{"message_flow"},
		EntityID:      "entity-a",
		FlowPath:      "review/inst-1",
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
		FlowPath:      "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
			"permissions":      []any{"agent_fire"},
		},
	})
	permissionFailure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "delegated_permission_forbidden")
	if permissionFailure.Detail.Attributes["permission"] != "agent_fire" {
		t.Fatalf("permission failure attributes = %#v", permissionFailure.Detail.Attributes)
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
		FlowPath:      "review/inst-1",
		Tools:         []string{"lookup_data"},
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
			"tools":            []any{"deploy_prod"},
		},
	})
	toolFailure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "delegated_tool_forbidden")
	if toolFailure.Detail.Attributes["tool"] != "deploy_prod" {
		t.Fatalf("tool failure attributes = %#v", toolFailure.Detail.Attributes)
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
		FlowPath:      "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "escalated",
			"manager_fallback": "manager",
		},
	})
	emitFailure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "delegated_emit_forbidden")
	if emitFailure.Detail.Attributes["event"] != "security.root" {
		t.Fatalf("emit failure attributes = %#v", emitFailure.Detail.Attributes)
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
		ModelRuntime:      nativeCapabilityRuntimeStub{},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_hire", "schedule"},
		Tools:         []string{"lookup_data"},
		NativeTools:   models.NativeToolConfig{FileIO: true},
		EmitEvents:    []string{"review.started"},
		FlowPath:      "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
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

func TestExecAgentHire_FailsClosedWhenNativeToolFallbackIsNotAdmitted(t *testing.T) {
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
		NativeTools:   models.NativeToolConfig{FileIO: true},
		FlowPath:      "review/inst-1",
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
			"native_tools": map[string]any{
				"file_io": true,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "native_tools.file_io") {
		t.Fatalf("ExecAgentHireDirect error = %v, want native_tools.file_io admission failure", err)
	}
	if manager.spawnCalled {
		t.Fatal("expected native tool admission failure before spawning")
	}
}

func TestExecAgentHire_PreservesMemoryPresenceAndProvenance(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		memory     any
		wantMemory agentmemory.Plan
	}{
		{name: "omitted", wantMemory: agentmemory.PlatformDefault()},
		{name: "authored false", memory: false, wantMemory: agentmemory.Authored(false)},
		{name: "authored true", memory: true, wantMemory: agentmemory.Authored(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			input := map[string]any{
				"config": map[string]any{
					"id":               "worker-1",
					"role":             "worker",
					"manager_fallback": "manager",
				},
			}
			if tc.memory != nil {
				input["memory"] = tc.memory
			}
			_, err := exec.ExecAgentHireDirect(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "manager-1",
				Role:          "manager",
				Permissions:   []string{"agent_hire"},
				FlowPath:      "review/inst-1",
			}, input)
			if err != nil {
				t.Fatalf("ExecAgentHireDirect: %v", err)
			}
			if manager.spawnedConfig.Memory != tc.wantMemory {
				t.Fatalf("spawned memory = %+v, want %+v", manager.spawnedConfig.Memory, tc.wantMemory)
			}
			if manager.spawnedConfig.FlowPath != "review/inst-1" {
				t.Fatalf("spawned flow path = %q, want inherited review/inst-1", manager.spawnedConfig.FlowPath)
			}
		})
	}
}

func TestExecAgentHire_RejectsMemoryWithoutFlowInstanceOwner(t *testing.T) {
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
	}, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "manager",
		},
		"memory": true,
	})
	if err == nil {
		t.Fatal("expected root memory hire to be denied")
	}
	if !strings.Contains(err.Error(), "memory: true requires a flow-instance owner") {
		t.Fatalf("error = %q, want flow-instance ownership denial", err.Error())
	}
	if manager.spawnCalled {
		t.Fatal("expected denied hire to fail closed before spawning")
	}
}

func TestExecAgentHireRejectsRetiredAndInvalidMemoryModeInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    map[string]any
		contains string
	}{
		{name: "top_level_conversation_mode", input: map[string]any{"conversation_mode": "task", "config": map[string]any{"id": "worker-1", "role": "worker"}}, contains: "input.conversation_mode is retired; use memory"},
		{name: "top_level_session_scope", input: map[string]any{"session_scope": "flow", "config": map[string]any{"id": "worker-1", "role": "worker"}}, contains: "input.session_scope is retired; use memory"},
		{name: "config_conversation_mode", input: map[string]any{"config": map[string]any{"id": "worker-1", "role": "worker", "conversation_mode": "task"}}, contains: "input.config.conversation_mode is retired; use memory"},
		{name: "config_session_scope_authority", input: map[string]any{"config": map[string]any{"id": "worker-1", "role": "worker", "session_scope_authority": "platform_internal"}}, contains: "input.config.session_scope_authority is retired; use memory"},
		{name: "opaque_config_session_scope", input: map[string]any{"config": map[string]any{"id": "worker-1", "role": "worker", "config": map[string]any{"session_scope": "global"}}}, contains: "input.config.config.session_scope is retired; use memory"},
		{name: "opaque_config_mode", input: map[string]any{"config": map[string]any{"id": "worker-1", "role": "worker", "config": map[string]any{"mode": "entity"}}}, contains: "input.config.config.mode is retired; use memory"},
		{name: "config_mode", input: map[string]any{"config": map[string]any{"id": "worker-1", "role": "worker", "mode": "session"}}, contains: "input.config.mode is retired; use memory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"manager": {ID: "manager", Role: "manager"},
					"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
				},
			})
			exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
				Manager:           &captureManagerStub{},
				AuthorityProvider: runtimeauthority.NewSourceProvider(source),
				WorkflowSource:    source,
			})
			_, err := exec.ExecAgentHireDirect(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "manager-1",
				Role:          "manager",
				Permissions:   []string{"agent_hire"},
				FlowPath:      "review/inst-1",
			}, tt.input)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("ExecAgentHireDirect error = %v, want %q", err, tt.contains)
			}
		})
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_reconfigure"},
		FlowPath:      "review/inst-1",
	}, map[string]any{
		"agent_id": "worker-1",
		"config": map[string]any{
			"native_tools": map[string]any{
				"bash": true,
			},
		},
	})
	nativeFailure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "delegated_native_tool_forbidden")
	if nativeFailure.Detail.Attributes["capability"] != "bash" {
		t.Fatalf("native failure attributes = %#v", nativeFailure.Detail.Attributes)
	}
	if manager.reconfigureCalled {
		t.Fatal("expected denied reconfigure to fail closed before persistence")
	}
}

func TestExecAgentReconfigure_FailsClosedWhenNativeToolFallbackIsNotAdmitted(t *testing.T) {
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
		ExecutionMode: "live",
		ID:            "manager-1",
		Role:          "manager",
		Permissions:   []string{"agent_reconfigure"},
		NativeTools:   models.NativeToolConfig{FileIO: true},
		FlowPath:      "review/inst-1",
	}, map[string]any{
		"agent_id": "worker-1",
		"config": map[string]any{
			"native_tools": map[string]any{
				"file_io": true,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "native_tools.file_io") {
		t.Fatalf("ExecAgentReconfigureDirect error = %v, want native_tools.file_io admission failure", err)
	}
	if manager.reconfigureCalled {
		t.Fatal("expected native tool admission failure before reconfigure")
	}
}

func TestExecAgentReconfigure_PreservesMemoryPresence(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		input      map[string]any
		wantMemory agentmemory.Plan
	}{
		{name: "omitted retains through empty patch", input: map[string]any{"agent_id": "worker-1", "config": map[string]any{"tools": []any{"agent_message"}}}},
		{name: "explicit false", input: map[string]any{"agent_id": "worker-1", "memory": false}, wantMemory: agentmemory.Authored(false)},
		{name: "explicit true", input: map[string]any{"agent_id": "worker-1", "config": map[string]any{"memory": true}}, wantMemory: agentmemory.Authored(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"manager": {ID: "manager", Role: "manager"},
					"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
				},
			})
			manager := &captureManagerStub{agents: map[string]models.AgentConfig{
				"worker-1": {
					ID: "worker-1", Role: "worker", ManagerFallback: "manager",
					Memory: agentmemory.Authored(true), FlowPath: "review/inst-1",
				},
			}}
			exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
				Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
			})
			_, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "manager-1", Role: "manager", Permissions: []string{"agent_reconfigure"}, FlowPath: "review/inst-1",
			}, tc.input)
			if err != nil {
				t.Fatalf("ExecAgentReconfigureDirect: %v", err)
			}
			if manager.reconfiguredPatch.Memory != tc.wantMemory {
				t.Fatalf("memory patch = %+v, want %+v", manager.reconfiguredPatch.Memory, tc.wantMemory)
			}
		})
	}
}

func TestExecAgentReconfigure_RejectsRetiredMemoryInterpreters(t *testing.T) {
	_, err := decodeAgentMutationInput("agent_reconfigure", map[string]any{
		"agent_id": "worker-1", "config": map[string]any{"session_scope": "global"},
	})
	if err == nil || !strings.Contains(err.Error(), "input.config.session_scope is retired; use memory") {
		t.Fatalf("decode error = %v, want retired session_scope denial", err)
	}
}
