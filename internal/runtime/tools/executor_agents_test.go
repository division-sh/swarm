package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func toolTestRootAgentIdentity(t testing.TB, agentID string) agentidentity.Identity {
	t.Helper()
	return agentidentitytest.RootRuntime(t, agentID, "runtime-tools-test")
}

func toolTestAgentIdentity(t testing.TB, agentID, flowID, flowPath string) agentidentity.Identity {
	t.Helper()
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if flowID == "" && flowPath == "" {
		return toolTestRootAgentIdentity(t, agentID)
	}
	return agentidentitytest.Runtime(t, agentID, "runtime-tools-test", flowID, "test-instance", flowPath)
}

type managerStub struct {
	agents map[string]models.AgentConfig
}

func (m managerStub) ResolveAgentConfig(agentID, flowInstance string) (models.AgentConfig, error) {
	cfg, ok := m.agents[agentID]
	if !ok || (flowInstance != "" && cfg.CanonicalFlowPath() != flowInstance) {
		return models.AgentConfig{}, fmt.Errorf("agent not found")
	}
	return cfg, nil
}

func (managerStub) SpawnAgentForEntity(string, models.AgentConfig) error { return nil }
func (managerStub) TeardownAgentTarget(
	_, _ string,
	beforeCommit func(models.AgentConfig) error,
	_ func(models.AgentConfig) error,
) error {
	if beforeCommit != nil {
		return beforeCommit(models.AgentConfig{})
	}
	return nil
}
func (managerStub) ReconfigureAgentTarget(_ string, _ string, cfg models.AgentConfig, onCommitted func(models.AgentConfig) error) error {
	if onCommitted != nil {
		return onCommitted(cfg)
	}
	return nil
}

type publishDirectBusStub struct {
	recipients []string
	routes     []events.DeliveryRoute
	event      events.Event
}

type captureScheduleScheduler struct {
	schedule Schedule
}

func (s *captureScheduleScheduler) Register(_ context.Context, schedule Schedule) error {
	if err := schedule.ValidateRoutingSource(); err != nil {
		return err
	}
	s.schedule = schedule
	return nil
}

func (*captureScheduleScheduler) Stop() {}

func (b *publishDirectBusStub) Publish(context.Context, events.Event) error { return nil }

func (b *publishDirectBusStub) PublishDirect(_ context.Context, event events.Event, recipients []string) error {
	b.recipients = append([]string{}, recipients...)
	b.event = event
	return nil
}

func (b *publishDirectBusStub) PublishDirectRoutes(_ context.Context, event events.Event, routes []events.DeliveryRoute) error {
	b.routes = append([]events.DeliveryRoute(nil), routes...)
	b.event = event
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
	resolveCalled     bool
	tornDownID        string
	teardownCalled    bool
	allowCrossRoute   bool
	reconfigureMu     sync.Mutex
	reconfigureArrive chan<- struct{}
	reconfigureStart  <-chan struct{}
}

func (m *captureManagerStub) ResolveAgentConfig(agentID, flowInstance string) (models.AgentConfig, error) {
	m.resolveCalled = true
	cfg, ok := m.agents[agentID]
	if !ok || (!m.allowCrossRoute && flowInstance != "" && cfg.CanonicalFlowPath() != flowInstance) {
		return models.AgentConfig{}, fmt.Errorf("agent not found")
	}
	return withRuntimeToolsTestIdentity(cfg)
}

func (m *captureManagerStub) SpawnAgentForEntity(entityID string, cfg models.AgentConfig) error {
	var err error
	cfg, err = withRuntimeToolsTestIdentity(cfg)
	if err != nil {
		return err
	}
	m.spawnedEntityID = entityID
	m.spawnedConfig = cfg
	m.spawnCalled = true
	if m.agents == nil {
		m.agents = map[string]models.AgentConfig{}
	}
	m.agents[cfg.ID] = cfg
	return nil
}

func (m *captureManagerStub) TeardownAgentTarget(
	agentID, flowInstance string,
	beforeCommit func(models.AgentConfig) error,
	_ func(models.AgentConfig) error,
) error {
	cfg, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(cfg); err != nil {
			return err
		}
	}
	m.tornDownID = agentID
	m.teardownCalled = true
	delete(m.agents, agentID)
	return nil
}

func (m *captureManagerStub) ReconfigureAgentTarget(
	agentID, flowInstance string,
	cfg models.AgentConfig,
	onCommitted func(models.AgentConfig) error,
) error {
	if m.reconfigureArrive != nil {
		m.reconfigureArrive <- struct{}{}
	}
	if m.reconfigureStart != nil {
		<-m.reconfigureStart
	}
	m.reconfigureMu.Lock()
	defer m.reconfigureMu.Unlock()

	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return err
	}
	m.reconfiguredID = agentID
	m.reconfiguredPatch = cfg
	m.reconfigureCalled = true
	if m.agents == nil {
		m.agents = map[string]models.AgentConfig{}
	}
	current = models.MergeAgentConfig(current, cfg)
	current.ID = agentID
	current, err = withRuntimeToolsTestIdentity(current)
	if err != nil {
		return err
	}
	if onCommitted != nil {
		if err := onCommitted(current); err != nil {
			return err
		}
	}
	m.agents[agentID] = current
	return nil
}

type staleAuthorizationManagerStub struct {
	stateMu        sync.Mutex
	reconfigure    sync.Mutex
	agents         map[string]models.AgentConfig
	arrivals       chan struct{}
	firstApplied   chan struct{}
	releaseFirst   chan struct{}
	first          bool
	teardownArrive chan<- struct{}
	teardownStart  <-chan struct{}
	teardownFail   bool
}

func (m *staleAuthorizationManagerStub) ResolveAgentConfig(agentID, flowInstance string) (models.AgentConfig, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	cfg, ok := m.agents[agentID]
	if !ok || (flowInstance != "" && cfg.CanonicalFlowPath() != flowInstance) {
		return models.AgentConfig{}, fmt.Errorf("agent not found")
	}
	return cfg, nil
}

func (*staleAuthorizationManagerStub) SpawnAgentForEntity(string, models.AgentConfig) error {
	return nil
}

func (m *staleAuthorizationManagerStub) TeardownAgentTarget(
	agentID, flowInstance string,
	beforeCommit func(models.AgentConfig) error,
	restoreBeforeCommit func(models.AgentConfig) error,
) error {
	if m.teardownArrive != nil {
		m.teardownArrive <- struct{}{}
	}
	if m.teardownStart != nil {
		<-m.teardownStart
	}
	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(current); err != nil {
			return err
		}
	}
	if m.teardownFail {
		if restoreBeforeCommit != nil {
			if err := restoreBeforeCommit(current); err != nil {
				return err
			}
		}
		return errors.New("injected teardown persistence failure")
	}
	m.stateMu.Lock()
	delete(m.agents, agentID)
	m.stateMu.Unlock()
	return nil
}

func (m *staleAuthorizationManagerStub) ReconfigureAgentTarget(
	agentID, flowInstance string,
	patch models.AgentConfig,
	onCommitted func(models.AgentConfig) error,
) error {
	m.arrivals <- struct{}{}
	m.reconfigure.Lock()
	defer m.reconfigure.Unlock()

	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return err
	}
	candidate := models.MergeAgentConfig(current, patch)
	if !m.first {
		m.first = true
		if err := onCommitted(candidate); err != nil {
			_ = onCommitted(current)
			return err
		}
		close(m.firstApplied)
		<-m.releaseFirst
		if err := onCommitted(current); err != nil {
			return err
		}
		return errors.New("injected persistence failure")
	}
	if err := onCommitted(candidate); err != nil {
		_ = onCommitted(current)
		return err
	}
	m.stateMu.Lock()
	m.agents[agentID] = candidate
	m.stateMu.Unlock()
	return nil
}

func withRuntimeToolsTestIdentity(cfg models.AgentConfig) (models.AgentConfig, error) {
	if !cfg.Identity.IsZero() {
		return cfg, nil
	}
	name, err := agentidentity.RuntimeName(cfg.ID, "runtime-tools-test")
	if err != nil {
		return models.AgentConfig{}, err
	}
	flowPath := cfg.CanonicalFlowPath()
	route := agentidentity.RootRoute()
	if flowPath != "" {
		parts := strings.Split(flowPath, "/")
		route, err = agentidentity.PresentRoute(parts[0], parts[len(parts)-1], flowPath)
		if err != nil {
			return models.AgentConfig{}, err
		}
	}
	cfg.Identity, err = agentidentity.New(name, route)
	return cfg, err
}

type concreteManagerStub struct {
	agents   map[agentidentity.Identity]models.AgentConfig
	tornDown []agentidentity.Identity
}

func (m *concreteManagerStub) ResolveAgentConfig(agentID, flowInstance string) (models.AgentConfig, error) {
	matches := make([]models.AgentConfig, 0, 2)
	for identity, cfg := range m.agents {
		if identity.AgentID() != strings.TrimSpace(agentID) {
			continue
		}
		if flowInstance != "" && identity.FlowInstance() != strings.Trim(strings.TrimSpace(flowInstance), "/") {
			continue
		}
		matches = append(matches, cfg)
	}
	if len(matches) != 1 {
		return models.AgentConfig{}, fmt.Errorf("agent resolution matched %d concrete identities", len(matches))
	}
	return matches[0], nil
}

func (m *concreteManagerStub) SpawnAgentForEntity(_ string, cfg models.AgentConfig) error {
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		return err
	}
	m.agents[identity] = cfg
	return nil
}

func (m *concreteManagerStub) TeardownAgentTarget(
	agentID, flowInstance string,
	beforeCommit func(models.AgentConfig) error,
	_ func(models.AgentConfig) error,
) error {
	cfg, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(cfg); err != nil {
			return err
		}
	}
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		return err
	}
	delete(m.agents, identity)
	m.tornDown = append(m.tornDown, identity)
	return nil
}

func (m *concreteManagerStub) ReconfigureAgentTarget(
	agentID, flowInstance string,
	cfg models.AgentConfig,
	onCommitted func(models.AgentConfig) error,
) error {
	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return err
	}
	identity, err := current.ConcreteIdentity()
	if err != nil {
		return err
	}
	committed := models.MergeAgentConfig(current, cfg)
	m.agents[identity] = committed
	if onCommitted != nil {
		return onCommitted(committed)
	}
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
		"worker-1": {
			ID: "worker-1", Identity: agentidentitytest.Runtime(t, "worker-1", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
			Role: "worker", ManagerFallback: "manager", FlowPath: "review/inst-1",
		},
	}}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
	})

	result, err := exec.ExecAgentFireDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager-1",
		Identity:      agentidentitytest.Runtime(t, "manager-1", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
		Role:          "manager", Permissions: []string{"agent_fire"}, FlowPath: "review/inst-1",
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

func TestExecAgentFire_RemovesOnlySelectedSameSlugAuthority(t *testing.T) {
	t.Parallel()

	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	managerA := models.AgentConfig{
		ID: "manager", Identity: agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-a", "review/inst-a"),
		Role: "manager", Permissions: []string{"agent_fire"}, FlowPath: "review/inst-a",
	}
	managerB := models.AgentConfig{
		ID: "manager", Identity: agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-b", "review/inst-b"),
		Role: "manager", Permissions: []string{"agent_fire"}, FlowPath: "review/inst-b",
	}
	workerA := models.AgentConfig{
		ID: "worker", Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-a", "review/inst-a"),
		Role: "worker", ParentAgent: "manager", FlowPath: "review/inst-a",
	}
	workerB := models.AgentConfig{
		ID: "worker", Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-b", "review/inst-b"),
		Role: "worker", ParentAgent: "manager", FlowPath: "review/inst-b",
	}
	if err := runtimeauthority.UpsertManagedAgent(provider, workerA.Identity, managerA.Identity); err != nil {
		t.Fatalf("upsert first worker authority: %v", err)
	}
	if err := runtimeauthority.UpsertManagedAgent(provider, workerB.Identity, managerB.Identity); err != nil {
		t.Fatalf("upsert sibling worker authority: %v", err)
	}
	manager := &concreteManagerStub{agents: map[agentidentity.Identity]models.AgentConfig{
		managerA.Identity: managerA,
		managerB.Identity: managerB,
		workerA.Identity:  workerA,
		workerB.Identity:  workerB,
	}}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

	if _, err := exec.ExecAgentFireDirect(managerA, map[string]any{
		"agent_id": "worker", "flow_instance": "review/inst-a",
	}); err != nil {
		t.Fatalf("fire first concrete worker: %v", err)
	}
	if len(manager.tornDown) != 1 || manager.tornDown[0] != workerA.Identity {
		t.Fatalf("torn down identities = %#v, want first worker only", manager.tornDown)
	}
	if err := provider.AuthorizeManagement(managerB, workerB); err != nil {
		t.Fatalf("same-slug sibling authority was removed by fire: %v", err)
	}
}

func TestExecAgentFire_RevalidatesQueuedActorAtSerializedCommit(t *testing.T) {
	const route = "review/inst-1"
	newAgent := func(id, role string) models.AgentConfig {
		return models.AgentConfig{
			ExecutionMode: "live",
			ID:            id,
			Identity:      agentidentitytest.Runtime(t, id, "runtime-tools-test", "review", "inst-1", route),
			Role:          role,
			Permissions:   []string{"agent_fire"},
			FlowPath:      route,
		}
	}
	oldParent := newAgent("old-parent", "manager")
	newParent := newAgent("new-parent", "manager")
	target := newAgent("worker", "worker")
	target.ParentAgent = oldParent.ID

	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, oldParent.Identity); err != nil {
		t.Fatalf("seed authority: %v", err)
	}
	arrived := make(chan struct{}, 1)
	start := make(chan struct{})
	manager := &staleAuthorizationManagerStub{
		agents: map[string]models.AgentConfig{
			oldParent.ID: oldParent,
			newParent.ID: newParent,
			target.ID:    target,
		},
		teardownArrive: arrived,
		teardownStart:  start,
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

	fireErr := make(chan error, 1)
	go func() {
		_, err := exec.ExecAgentFireDirect(oldParent, map[string]any{
			"agent_id": target.ID, "flow_instance": route,
		})
		fireErr <- err
	}()
	<-arrived

	manager.stateMu.Lock()
	updated := manager.agents[target.ID]
	updated.ParentAgent = newParent.ID
	manager.agents[target.ID] = updated
	manager.stateMu.Unlock()
	if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, newParent.Identity); err != nil {
		t.Fatalf("transfer authority while fire waits: %v", err)
	}
	close(start)

	if err := <-fireErr; err == nil || !strings.Contains(err.Error(), "revalidate serialized fire authority") {
		t.Fatalf("queued fire error = %v, want serialized authority rejection", err)
	}
	stored, err := manager.ResolveAgentConfig(target.ID, route)
	if err != nil {
		t.Fatalf("queued stale fire removed target: %v", err)
	}
	if err := provider.AuthorizeManagement(newParent, stored); err != nil {
		t.Fatalf("new parent lacks authority after rejected stale fire: %v", err)
	}
	if err := provider.AuthorizeManagement(oldParent, stored); err == nil {
		t.Fatal("stale parent retained authority after rejected fire")
	}
}

func TestExecAgentFire_PersistenceFailureRestoresPriorAuthority(t *testing.T) {
	const route = "review/inst-1"
	parent := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", route),
		Role:          "manager",
		Permissions:   []string{"agent_fire"},
		FlowPath:      route,
	}
	target := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "worker",
		Identity:      agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-1", route),
		Role:          "worker",
		ParentAgent:   parent.ID,
		FlowPath:      route,
	}
	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, parent.Identity); err != nil {
		t.Fatalf("seed authority: %v", err)
	}
	manager := &staleAuthorizationManagerStub{
		agents: map[string]models.AgentConfig{
			parent.ID: parent,
			target.ID: target,
		},
		teardownFail: true,
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

	_, err := exec.ExecAgentFireDirect(parent, map[string]any{
		"agent_id": target.ID, "flow_instance": route,
	})
	if err == nil || !strings.Contains(err.Error(), "injected teardown persistence failure") {
		t.Fatalf("fire error = %v, want persistence failure", err)
	}
	stored, err := manager.ResolveAgentConfig(target.ID, route)
	if err != nil {
		t.Fatalf("failed fire removed target: %v", err)
	}
	if err := provider.AuthorizeManagement(parent, stored); err != nil {
		t.Fatalf("failed fire did not restore prior authority: %v", err)
	}
}

func TestRetiredDynamicAgentMutationFailsBeforeAnyInterpreterOrManagerSideEffect(t *testing.T) {
	for _, toolName := range []string{"agent_hire", "agent_reconfigure"} {
		for _, tc := range []struct {
			name  string
			input any
		}{
			{name: "ordinary", input: map[string]any{"agent_id": "worker"}},
			{name: "top_level_raw_prompt", input: map[string]any{"system_prompt": "obsolete"}},
			{name: "nested_raw_prompt", input: map[string]any{"config": map[string]any{"nested": map[string]any{"system_prompt": "obsolete"}}}},
			{name: "malformed", input: make(chan struct{})},
		} {
			t.Run(toolName+"/"+tc.name, func(t *testing.T) {
				manager := &captureManagerStub{}
				exec := NewExecutor(nil, nil, manager)
				var err error
				if tc.name == "ordinary" {
					_, err = exec.Execute(context.Background(), toolName, tc.input)
				} else if toolName == "agent_hire" {
					_, err = exec.ExecAgentHireDirect(models.AgentConfig{}, tc.input)
				} else {
					_, err = exec.ExecAgentReconfigureDirect(models.AgentConfig{}, tc.input)
				}
				if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), toolName) {
					t.Fatalf("error = %v, want unconditional %s retirement", err, toolName)
				}
				if manager.resolveCalled || manager.spawnCalled || manager.reconfigureCalled || manager.teardownCalled {
					t.Fatalf("retired %s reached manager: %#v", toolName, manager)
				}
			})
		}
	}
}

func TestExecAgentMessage_AllowsCrossEntityWhenAuthorityPermits(t *testing.T) {
	agents := map[string]runtimecontracts.AgentRegistryEntry{
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
	}
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:   "review",
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Agents: agents,
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: reviewFlow,
			ByID: map[string]*runtimecontracts.FlowContractView{"review": reviewFlow},
		},
	})
	provider := runtimeauthority.NewSourceProvider(source)

	bus := &publishDirectBusStub{}
	manager := managerStub{
		agents: map[string]models.AgentConfig{
			"target-1": {
				ID:              "target-1",
				Identity:        agentidentitytest.Runtime(t, "target-1", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
				Role:            "reviewer",
				EntityID:        "entity-b",
				FlowPath:        "review/inst-1",
				ManagerFallback: "control",
			},
		},
	}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider, WorkflowSource: source})
	actor := models.AgentConfig{
		ExecutionMode: "mock",
		ID:            "control",
		Identity:      agentidentitytest.Runtime(t, "control", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
		Role:          "control",
		Permissions:   []string{"message_flow"},
		EntityID:      "entity-a",
		FlowID:        "review",
		FlowPath:      "review/inst-1",
	}
	ctx := runtimeeffects.WithExecutionMode(WithActor(toolEventTestContext(actor), actor), runtimeeffects.ExecutionModeMock)

	if _, err := exec.execAgentMessage(ctx, actor, map[string]any{
		"target_agent_id": "target-1",
		"message":         "hello",
	}); err != nil {
		t.Fatalf("expected cross-entity agent_message to be allowed, got %v", err)
	}
	if len(bus.recipients) != 0 {
		t.Fatalf("slug-only recipients = %#v, want none", bus.recipients)
	}
	if len(bus.routes) != 1 || bus.routes[0].AgentIdentity != manager.agents["target-1"].Identity {
		t.Fatalf("exact routes = %#v, want target concrete identity", bus.routes)
	}
	if bus.event.ExecutionMode() != runtimeeffects.ExecutionModeMock {
		t.Fatalf("agent_message event execution mode = %q, want mock", bus.event.ExecutionMode())
	}
}

func TestExecSchedulePreservesRootAgentRoutingSource(t *testing.T) {
	scheduler := &captureScheduleScheduler{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	exec := NewExecutorWithOptions(nil, scheduler, ExecutorOptions{WorkflowSource: source})
	actor := models.AgentConfig{
		ID:       "root-agent",
		Identity: toolTestRootAgentIdentity(t, "root-agent"),
		EntityID: "entity-root",
	}

	if _, err := exec.execSchedule(context.Background(), actor, map[string]any{
		"event_type": "root.timer.fired",
		"mode":       "once",
		"at":         time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"payload":    map[string]any{"reason": "root-owned"},
	}); err != nil {
		t.Fatalf("execSchedule(root agent): %v", err)
	}
	if scheduler.schedule.RoutingSource.Kind() != events.RoutingSourceRoot {
		t.Fatalf("schedule routing source kind = %q, want root", scheduler.schedule.RoutingSource.Kind().StorageCode())
	}
	if got := scheduler.schedule.RoutingSource.Route(); got != (events.RouteIdentity{EntityID: "entity-root"}) {
		t.Fatalf("schedule routing source route = %#v, want exact root entity", got)
	}
	if scheduler.schedule.FlowInstance != "" {
		t.Fatalf("schedule flow instance = %q, want absent for root agent", scheduler.schedule.FlowInstance)
	}
}

func TestExecAgentMessage_PublishesOnlyResolvedSameSlugRoute(t *testing.T) {
	t.Parallel()

	agents := map[string]runtimecontracts.AgentRegistryEntry{
		"manager": {ID: "manager", Role: "manager", Tools: []string{"message_flow"}},
		"worker":  {ID: "worker", Role: "worker"},
	}
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review", Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate}, Agents: agents,
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: reviewFlow,
			ByID: map[string]*runtimecontracts.FlowContractView{"review": reviewFlow},
		},
	})
	workerA := models.AgentConfig{
		ID: "worker", Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-a", "review/inst-a"),
		Role: "worker", EntityID: "entity-a", FlowPath: "review/inst-a",
	}
	workerB := models.AgentConfig{
		ID: "worker", Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-b", "review/inst-b"),
		Role: "worker", EntityID: "entity-b", FlowPath: "review/inst-b",
	}
	manager := &concreteManagerStub{agents: map[agentidentity.Identity]models.AgentConfig{
		workerA.Identity: workerA,
		workerB.Identity: workerB,
	}}
	bus := &publishDirectBusStub{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{
		Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
	})
	actor := models.AgentConfig{
		ExecutionMode: "mock",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-b", "review/inst-b"),
		Role:          "manager",
		Permissions:   []string{"message_flow"},
		EntityID:      "entity-manager",
		FlowID:        "review",
		FlowPath:      "review/inst-b",
	}
	ctx := runtimeeffects.WithExecutionMode(WithActor(toolEventTestContext(actor), actor), runtimeeffects.ExecutionModeMock)

	if _, err := exec.execAgentMessage(ctx, actor, map[string]any{
		"target_agent_id": "worker",
		"flow_instance":   "review/inst-b",
		"message":         "exact sibling",
	}); err != nil {
		t.Fatalf("send exact same-slug agent message: %v", err)
	}
	if len(bus.recipients) != 0 {
		t.Fatalf("slug-only recipients = %#v, want none", bus.recipients)
	}
	if len(bus.routes) != 1 || bus.routes[0].AgentIdentity != workerB.Identity {
		t.Fatalf("published routes = %#v, want second concrete worker only", bus.routes)
	}
	if bus.routes[0].AgentIdentity == workerA.Identity {
		t.Fatal("message route crossed to unrelated same-slug sibling")
	}
}

func TestAuthorizeAgentMessageSelfRequiresExactConcreteIdentity(t *testing.T) {
	t.Parallel()

	workerA := models.AgentConfig{
		ID:       "worker",
		Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-a", "review/inst-a"),
		Role:     "worker",
		FlowPath: "review/inst-a",
	}
	workerB := models.AgentConfig{
		ID:       "worker",
		Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-b", "review/inst-b"),
		Role:     "worker",
		FlowPath: "review/inst-b",
	}
	if err := authorizeAgentMessage(runtimeauthority.NoopProvider(), workerA, workerA, nil); err != nil {
		t.Fatalf("exact self authorization: %v", err)
	}
	if err := authorizeAgentMessage(runtimeauthority.NoopProvider(), workerA, workerB, nil); err == nil {
		t.Fatal("same-slug sibling bypassed message authorization")
	}
	malformed := workerA
	malformed.Identity = agentidentity.Identity{}
	if err := authorizeAgentMessage(runtimeauthority.NoopProvider(), malformed, malformed, nil); err == nil {
		t.Fatal("malformed identity bypassed message authorization")
	}
}
