package tools

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
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
	_ *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	return models.AgentTargetMutationResult{Transitioned: true}, nil
}
func (managerStub) ReconfigureAgentTarget(_ string, _ string, cfg models.AgentConfig, _ *models.AgentConfig) (models.AgentTargetMutationResult, error) {
	return models.AgentTargetMutationResult{CurrentConfig: cfg, Transitioned: true}, nil
}

type publishDirectBusStub struct {
	recipients []string
	routes     []events.DeliveryRoute
	event      events.Event
}

type captureScheduleScheduler struct {
	command runtimegenericschedule.AdmissionCommand
	calls   int
}

func (s *captureScheduleScheduler) Admit(_ context.Context, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error) {
	if err := command.Validate(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	now := time.Now().UTC()
	due, err := command.Due.FirstDue(now)
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	activation := runtimegenericschedule.Activation{
		ID: "3fcdf85b-30e9-41db-ace4-c82c330b5760", Command: command, ImmutableHash: hash,
		AdmittedAt: now, InitialDueAt: due, CurrentDueAt: due,
		Status: runtimegenericschedule.StatusActive,
	}
	if err := activation.Validate(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	s.command = command
	s.calls++
	return runtimegenericschedule.AdmissionResult{Outcome: runtimegenericschedule.AdmissionCreated, Activation: activation}, nil
}

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
	tornDownID        string
	teardownCalled    bool
	allowCrossRoute   bool
	reconfigureMu     sync.Mutex
	reconfigureArrive chan<- struct{}
	reconfigureStart  <-chan struct{}
}

func (m *captureManagerStub) ResolveAgentConfig(agentID, flowInstance string) (models.AgentConfig, error) {
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
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	cfg, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	if expected != nil && !reflect.DeepEqual(cfg, *expected) {
		return models.AgentTargetMutationResult{}, errors.New("agent_config_changed")
	}
	m.tornDownID = agentID
	m.teardownCalled = true
	delete(m.agents, agentID)
	return models.AgentTargetMutationResult{PreviousConfig: cfg, Transitioned: true}, nil
}

func (m *captureManagerStub) ReconfigureAgentTarget(
	agentID, flowInstance string,
	cfg models.AgentConfig,
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
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
		return models.AgentTargetMutationResult{}, err
	}
	if expected != nil && !reflect.DeepEqual(current, *expected) {
		return models.AgentTargetMutationResult{}, errors.New("agent_config_changed")
	}
	previous := current
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
		return models.AgentTargetMutationResult{}, err
	}
	m.agents[agentID] = current
	return models.AgentTargetMutationResult{PreviousConfig: previous, CurrentConfig: current, Transitioned: true}, nil
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
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	if m.teardownArrive != nil {
		m.teardownArrive <- struct{}{}
	}
	if m.teardownStart != nil {
		<-m.teardownStart
	}
	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	if expected != nil && !reflect.DeepEqual(current, *expected) {
		return models.AgentTargetMutationResult{}, errors.New("agent_config_changed")
	}
	if m.teardownFail {
		return models.AgentTargetMutationResult{}, errors.New("injected teardown persistence failure")
	}
	m.stateMu.Lock()
	delete(m.agents, agentID)
	m.stateMu.Unlock()
	return models.AgentTargetMutationResult{PreviousConfig: current, Transitioned: true}, nil
}

func (m *staleAuthorizationManagerStub) ReconfigureAgentTarget(
	agentID, flowInstance string,
	patch models.AgentConfig,
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	m.arrivals <- struct{}{}
	m.reconfigure.Lock()
	defer m.reconfigure.Unlock()

	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	if expected != nil && !reflect.DeepEqual(current, *expected) {
		return models.AgentTargetMutationResult{}, errors.New("agent_config_changed")
	}
	candidate := models.MergeAgentConfig(current, patch)
	if !m.first {
		m.first = true
		close(m.firstApplied)
		<-m.releaseFirst
		return models.AgentTargetMutationResult{}, errors.New("injected persistence failure")
	}
	m.stateMu.Lock()
	m.agents[agentID] = candidate
	m.stateMu.Unlock()
	return models.AgentTargetMutationResult{PreviousConfig: current, CurrentConfig: candidate, Transitioned: true}, nil
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
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	cfg, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	if expected != nil && !reflect.DeepEqual(cfg, *expected) {
		return models.AgentTargetMutationResult{}, errors.New("agent_config_changed")
	}
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	delete(m.agents, identity)
	m.tornDown = append(m.tornDown, identity)
	return models.AgentTargetMutationResult{PreviousConfig: cfg, Transitioned: true}, nil
}

func (m *concreteManagerStub) ReconfigureAgentTarget(
	agentID, flowInstance string,
	cfg models.AgentConfig,
	expected *models.AgentConfig,
) (models.AgentTargetMutationResult, error) {
	current, err := m.ResolveAgentConfig(agentID, flowInstance)
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	if expected != nil && !reflect.DeepEqual(current, *expected) {
		return models.AgentTargetMutationResult{}, errors.New("agent_config_changed")
	}
	identity, err := current.ConcreteIdentity()
	if err != nil {
		return models.AgentTargetMutationResult{}, err
	}
	committed := models.MergeAgentConfig(current, cfg)
	m.agents[identity] = committed
	return models.AgentTargetMutationResult{PreviousConfig: current, CurrentConfig: committed, Transitioned: true}, nil
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

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

func TestExecAgentFire_RejectsConfigChangedBeforeSerializedCommit(t *testing.T) {
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

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

	if err := <-fireErr; err == nil || !strings.Contains(err.Error(), "agent_config_changed") {
		t.Fatalf("queued fire error = %v, want exact expected-config rejection", err)
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
	})

	result, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager", Identity: agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
		Role: "manager", Permissions: []string{"agent_reconfigure"}, FlowPath: "review/inst-1",
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

func TestExecAgentReconfigure_RejectsInvalidParentCandidatesBeforeMutation(t *testing.T) {
	route := "review/inst-1"
	otherRoute := "review/inst-2"
	for _, tc := range []struct {
		name            string
		parentID        string
		wantError       string
		allowCrossRoute bool
		parentConfig    *models.AgentConfig
	}{
		{name: "unresolved", parentID: "missing-parent", wantError: "resolve managed parent missing-parent"},
		{
			name: "malformed", parentID: "malformed-parent",
			wantError: "resolve concrete managed parent malformed-parent identity",
			parentConfig: &models.AgentConfig{
				ID: "malformed-parent", Role: "manager", FlowPath: route,
				Identity: agentidentity.Identity{
					Name:  agentidentity.Name{AgentID: "malformed-parent", Owner: "runtime-tools-test", Source: agentidentity.NameSourceRuntimeCreated},
					Route: agentidentity.Route{Presence: agentidentity.RoutePresent, ScopeKey: "review", InstanceID: "inst-1"},
				},
			},
		},
		{
			name: "cross_route", parentID: "cross-route-parent",
			wantError: "is not in target flow route " + route, allowCrossRoute: true,
			parentConfig: &models.AgentConfig{
				ID: "cross-route-parent", Role: "manager", FlowPath: otherRoute,
				Identity: agentidentitytest.Runtime(t, "cross-route-parent", "runtime-tools-test", "review", "inst-2", otherRoute),
			},
		},
		{name: "self_parent", parentID: "worker", wantError: "managed agent identity cannot be its own parent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldParent := models.AgentConfig{
				ExecutionMode: "live", ID: "old-parent",
				Identity: agentidentitytest.Runtime(t, "old-parent", "runtime-tools-test", "review", "inst-1", route),
				Role:     "manager", Permissions: []string{"agent_reconfigure"}, FlowPath: route,
			}
			target := models.AgentConfig{
				ID: "worker", Identity: agentidentitytest.Runtime(t, "worker", "runtime-tools-test", "review", "inst-1", route),
				Role: "worker", ParentAgent: oldParent.ID, ManagerFallback: oldParent.ID, Model: "regular", FlowPath: route,
			}
			provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
			if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, oldParent.Identity); err != nil {
				t.Fatalf("seed managed authority: %v", err)
			}
			agents := map[string]models.AgentConfig{oldParent.ID: oldParent, target.ID: target}
			if tc.parentConfig != nil {
				agents[tc.parentConfig.ID] = *tc.parentConfig
			}
			manager := &captureManagerStub{agents: agents, allowCrossRoute: tc.allowCrossRoute}
			exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

			_, err := exec.ExecAgentReconfigureDirect(oldParent, map[string]any{
				"agent_id": target.ID, "flow_instance": route,
				"config": map[string]any{"model": "fast", "parent_agent_id": tc.parentID},
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("reconfigure error = %v, want %q", err, tc.wantError)
			}
			if manager.reconfigureCalled {
				t.Fatal("invalid parent candidate reached manager mutation")
			}
			stored := manager.agents[target.ID]
			if stored.Model != target.Model || stored.ParentAgent != target.ParentAgent || stored.ManagerFallback != target.ManagerFallback {
				t.Fatalf("invalid parent changed manager state: before=%+v after=%+v", target, stored)
			}
			if err := provider.AuthorizeManagement(oldParent, target); err != nil {
				t.Fatalf("invalid parent changed prior authority mapping: %v", err)
			}
		})
	}
}

func TestExecAgentReconfigure_ParentCandidateAndAuthorityGraphAgree(t *testing.T) {
	const route = "review/inst-1"
	newAgent := func(t *testing.T, id, role string) models.AgentConfig {
		t.Helper()
		return models.AgentConfig{
			ExecutionMode: "live", ID: id,
			Identity: agentidentitytest.Runtime(t, id, "runtime-tools-test", "review", "inst-1", route),
			Role:     role, Permissions: []string{"agent_reconfigure"}, FlowPath: route,
		}
	}

	t.Run("parent_agent_precedes_manager_fallback", func(t *testing.T) {
		oldParent := newAgent(t, "old-parent", "manager")
		newParent := newAgent(t, "new-parent", "manager")
		ignoredFallback := newAgent(t, "ignored-fallback", "manager")
		target := newAgent(t, "worker", "worker")
		target.ParentAgent = oldParent.ID
		target.ManagerFallback = oldParent.ID

		provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
		if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, oldParent.Identity); err != nil {
			t.Fatalf("seed managed authority: %v", err)
		}
		manager := &captureManagerStub{agents: map[string]models.AgentConfig{
			oldParent.ID:       oldParent,
			newParent.ID:       newParent,
			ignoredFallback.ID: ignoredFallback,
			target.ID:          target,
		}}
		exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

		if _, err := exec.ExecAgentReconfigureDirect(oldParent, map[string]any{
			"agent_id": target.ID, "flow_instance": route,
			"config": map[string]any{
				"model": "fast", "parent_agent_id": newParent.ID, "manager_fallback": ignoredFallback.ID,
			},
		}); err != nil {
			t.Fatalf("reconfigure exact parent: %v", err)
		}
		stored := manager.agents[target.ID]
		if stored.Model != "fast" || stored.ParentAgent != newParent.ID || stored.ManagerFallback != ignoredFallback.ID {
			t.Fatalf("stored candidate = %+v", stored)
		}
		if err := provider.AuthorizeManagement(newParent, stored); err != nil {
			t.Fatalf("new exact parent lacks authority: %v", err)
		}
		if err := provider.AuthorizeManagement(oldParent, stored); err == nil {
			t.Fatal("old parent retained authority after replacement")
		}
		if err := provider.AuthorizeManagement(ignoredFallback, stored); err == nil {
			t.Fatal("manager_fallback overrode explicit parent_agent_id")
		}
	})

	t.Run("no_parent_clears_managed_authority", func(t *testing.T) {
		oldParent := newAgent(t, "old-parent", "manager")
		target := newAgent(t, "worker", "worker")
		provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
		if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, oldParent.Identity); err != nil {
			t.Fatalf("seed stale managed authority: %v", err)
		}
		manager := &captureManagerStub{agents: map[string]models.AgentConfig{
			oldParent.ID: oldParent,
			target.ID:    target,
		}}
		exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

		if _, err := exec.ExecAgentReconfigureDirect(oldParent, map[string]any{
			"agent_id": target.ID, "flow_instance": route, "config": map[string]any{"model": "fast"},
		}); err != nil {
			t.Fatalf("reconfigure no-parent candidate: %v", err)
		}
		stored := manager.agents[target.ID]
		if stored.Model != "fast" || stored.ParentAgent != "" || stored.ManagerFallback != "" {
			t.Fatalf("stored no-parent candidate = %+v", stored)
		}
		if err := provider.AuthorizeManagement(oldParent, stored); err == nil {
			t.Fatal("no-parent candidate retained stale managed authority")
		}
	})
}

func TestExecAgentReconfigure_RejectsIndirectParentCycleAtCommit(t *testing.T) {
	const route = "review/inst-1"
	newAgent := func(id, role string) models.AgentConfig {
		return models.AgentConfig{
			ExecutionMode: "live",
			ID:            id,
			Identity:      agentidentitytest.Runtime(t, id, "runtime-tools-test", "review", "inst-1", route),
			Role:          role,
			Permissions:   []string{"agent_reconfigure"},
			FlowPath:      route,
		}
	}
	root := newAgent("root", "manager")
	first := newAgent("first", "manager")
	second := newAgent("second", "worker")
	first.ParentAgent = root.ID
	second.ParentAgent = first.ID

	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err := runtimeauthority.UpsertManagedAgent(provider, first.Identity, root.Identity); err != nil {
		t.Fatalf("seed first parent: %v", err)
	}
	if err := runtimeauthority.UpsertManagedAgent(provider, second.Identity, first.Identity); err != nil {
		t.Fatalf("seed second parent: %v", err)
	}
	manager := &captureManagerStub{agents: map[string]models.AgentConfig{
		root.ID:   root,
		first.ID:  first,
		second.ID: second,
	}}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

	_, err := exec.ExecAgentReconfigureDirect(root, map[string]any{
		"agent_id": first.ID,
		"config":   map[string]any{"parent_agent_id": second.ID},
	})
	if err == nil || !strings.Contains(err.Error(), "managed parent update would create a cycle") {
		t.Fatalf("cycle reconfigure error = %v, want cycle rejection", err)
	}
	stored := manager.agents[first.ID]
	if stored.ParentAgent != root.ID {
		t.Fatalf("cycle rejection changed target parent to %q", stored.ParentAgent)
	}
	if err := provider.AuthorizeManagement(root, stored); err != nil {
		t.Fatalf("cycle rejection changed prior authority: %v", err)
	}
	if err := provider.AuthorizeManagement(second, stored); err == nil {
		t.Fatal("cycle rejection granted reverse authority")
	}
}

func TestExecAgentReconfigure_ConcurrentCommitsRejectStaleExpectedConfig(t *testing.T) {
	const route = "review/inst-1"
	newAgent := func(id, role string) models.AgentConfig {
		return models.AgentConfig{
			ExecutionMode: "live", ID: id,
			Identity: agentidentitytest.Runtime(t, id, "runtime-tools-test", "review", "inst-1", route),
			Role:     role, Permissions: []string{"agent_reconfigure"}, FlowPath: route,
		}
	}

	oldParent := newAgent("old-parent", "manager")
	parentA := newAgent("parent-a", "manager")
	parentB := newAgent("parent-b", "manager")
	target := newAgent("worker", "worker")
	target.ParentAgent = oldParent.ID
	target.ManagerFallback = oldParent.ID

	provider := runtimeauthority.NewSourceProvider(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err := runtimeauthority.UpsertManagedAgent(provider, target.Identity, oldParent.Identity); err != nil {
		t.Fatalf("seed managed authority: %v", err)
	}
	arrivals := make(chan struct{}, 2)
	start := make(chan struct{})
	manager := &captureManagerStub{
		agents: map[string]models.AgentConfig{
			oldParent.ID: oldParent,
			parentA.ID:   parentA,
			parentB.ID:   parentB,
			target.ID:    target,
		},
		reconfigureArrive: arrivals,
		reconfigureStart:  start,
	}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

	type reconfigureResult struct {
		parentID string
		err      error
	}
	results := make(chan reconfigureResult, 2)
	for _, parent := range []models.AgentConfig{parentA, parentB} {
		parent := parent
		go func() {
			_, err := exec.ExecAgentReconfigureDirect(oldParent, map[string]any{
				"agent_id": target.ID, "flow_instance": route,
				"config": map[string]any{"parent_agent_id": parent.ID},
			})
			results <- reconfigureResult{parentID: parent.ID, err: err}
		}()
	}
	for range 2 {
		select {
		case <-arrivals:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent reconfigure did not reach the manager after preflight")
		}
	}
	close(start)
	successfulParent := ""
	denials := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			if successfulParent != "" {
				t.Fatalf("both stale-parent reconfigurations succeeded: %q and %q", successfulParent, result.parentID)
			}
			successfulParent = result.parentID
			continue
		}
		if !strings.Contains(result.err.Error(), "agent_config_changed") {
			t.Fatalf("concurrent reconfigure error = %v, want expected-config denial", result.err)
		}
		denials++
	}
	if successfulParent == "" || denials != 1 {
		t.Fatalf("successful parent = %q denials = %d, want one commit and one stale-authority denial", successfulParent, denials)
	}

	stored := manager.agents[target.ID]
	parents := map[string]models.AgentConfig{parentA.ID: parentA, parentB.ID: parentB}
	finalParent, ok := parents[stored.ParentAgent]
	if !ok || stored.ParentAgent != successfulParent {
		t.Fatalf("committed parent = %q, successful request = %q", stored.ParentAgent, successfulParent)
	}
	if err := provider.AuthorizeManagement(finalParent, stored); err != nil {
		t.Fatalf("last committed parent lacks authority: %v", err)
	}
	for parentID, parent := range parents {
		if parentID == stored.ParentAgent {
			continue
		}
		if err := provider.AuthorizeManagement(parent, stored); err == nil {
			t.Fatalf("superseded parent %q retained authority", parentID)
		}
	}
	if err := provider.AuthorizeManagement(oldParent, stored); err == nil {
		t.Fatal("original parent retained authority")
	}
}

func TestExecAgentReconfigure_PersistenceFailureDoesNotPublishUncommittedAuthority(t *testing.T) {
	const route = "review/inst-1"
	newAgent := func(id, role string) models.AgentConfig {
		return models.AgentConfig{
			ExecutionMode: "live",
			ID:            id,
			Identity:      agentidentitytest.Runtime(t, id, "runtime-tools-test", "review", "inst-1", route),
			Role:          role,
			Permissions:   []string{"agent_reconfigure"},
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
	manager := &staleAuthorizationManagerStub{
		agents: map[string]models.AgentConfig{
			oldParent.ID: oldParent,
			newParent.ID: newParent,
			target.ID:    target,
		},
		arrivals:     make(chan struct{}, 2),
		firstApplied: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, AuthorityProvider: provider})

	firstErr := make(chan error, 1)
	go func() {
		_, err := exec.ExecAgentReconfigureDirect(oldParent, map[string]any{
			"agent_id": target.ID,
			"config":   map[string]any{"parent_agent_id": newParent.ID},
		})
		firstErr <- err
	}()
	<-manager.arrivals
	<-manager.firstApplied

	secondErr := make(chan error, 1)
	go func() {
		_, err := exec.ExecAgentReconfigureDirect(oldParent, map[string]any{
			"agent_id": target.ID,
			"config":   map[string]any{"model": "committed-after-rollback"},
		})
		secondErr <- err
	}()
	<-manager.arrivals
	close(manager.releaseFirst)

	if err := <-firstErr; err == nil || !strings.Contains(err.Error(), "injected persistence failure") {
		t.Fatalf("first reconfigure error = %v, want injected persistence failure", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("queued reconfigure after rollback: %v", err)
	}
	stored, err := manager.ResolveAgentConfig(target.ID, route)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Model != "committed-after-rollback" || stored.ParentAgent != oldParent.ID {
		t.Fatalf("queued committed mutation = %+v, want prior parent and committed model", stored)
	}
	if err := provider.AuthorizeManagement(oldParent, stored); err != nil {
		t.Fatalf("predecessor parent authority was not restored: %v", err)
	}
	if err := provider.AuthorizeManagement(newParent, stored); err == nil {
		t.Fatal("rolled-back candidate parent retained authority")
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
	exec := NewExecutorWithOptions(bus, ExecutorOptions{Manager: manager, AuthorityProvider: provider, WorkflowSource: source})
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, GenericSchedules: scheduler})
	actor := models.AgentConfig{
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ID:            "root-agent",
		Identity:      toolTestRootAgentIdentity(t, "root-agent"),
		EntityID:      "entity-root",
	}

	ctx := runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163")
	if _, err := exec.execSchedule(ctx, actor, map[string]any{
		"schedule_key": "root-proof",
		"event_type":   "root.timer.fired",
		"mode":         "absolute",
		"at":           time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"payload":      map[string]any{"reason": "root-owned"},
	}); err != nil {
		t.Fatalf("execSchedule(root agent): %v", err)
	}
	if scheduler.command.RoutingSource.Kind() != events.RoutingSourceRoot {
		t.Fatalf("schedule routing source kind = %q, want root", scheduler.command.RoutingSource.Kind().StorageCode())
	}
	if got := scheduler.command.RoutingSource.Route(); got != (events.RouteIdentity{EntityID: "entity-root"}) {
		t.Fatalf("schedule routing source route = %#v, want exact root entity", got)
	}
	if scheduler.command.FlowInstance != "" {
		t.Fatalf("schedule flow instance = %q, want absent for root agent", scheduler.command.FlowInstance)
	}
}

func TestExecScheduleAdmissionGatesAndTypedDueBasis(t *testing.T) {
	actor := models.AgentConfig{
		ID:       "root-agent",
		Identity: toolTestRootAgentIdentity(t, "root-agent"),
		EntityID: "entity-root",
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163")
	newExecutor := func() (*Executor, *captureScheduleScheduler) {
		t.Helper()
		scheduler := &captureScheduleScheduler{}
		return NewExecutorWithOptions(nil, ExecutorOptions{
			WorkflowSource:   semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
			GenericSchedules: scheduler,
		}), scheduler
	}
	for _, tc := range []struct {
		name  string
		input map[string]any
	}{
		{name: "missing key", input: map[string]any{
			"event_type": "root.timer.fired", "mode": "once", "at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		}},
		{name: "different agent", input: map[string]any{
			"schedule_key": "foreign-agent", "agent_id": "other", "event_type": "root.timer.fired",
			"mode": "absolute", "at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		}},
		{name: "different entity", input: map[string]any{
			"schedule_key": "foreign-entity", "entity_id": "entity-other", "event_type": "root.timer.fired",
			"mode": "absolute", "at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, scheduler := newExecutor()
			if _, err := exec.execSchedule(ctx, actor, tc.input); err == nil {
				t.Fatal("schedule admission gate accepted invalid request")
			}
			if scheduler.calls != 0 {
				t.Fatalf("rejected request reached generic admission %d time(s)", scheduler.calls)
			}
		})
	}

	for _, tc := range []struct {
		name     string
		input    map[string]any
		wantKind runtimegenericschedule.DueBasisKind
	}{
		{name: "absolute", wantKind: runtimegenericschedule.DueAbsolute, input: map[string]any{
			"schedule_key": "absolute", "event_type": "root.timer.fired", "mode": "absolute",
			"at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		}},
		{name: "delay", wantKind: runtimegenericschedule.DueDelay, input: map[string]any{
			"schedule_key": "delay", "event_type": "root.timer.fired", "mode": "delay", "delay": "45m",
		}},
		{name: "cron", wantKind: runtimegenericschedule.DueCron, input: map[string]any{
			"schedule_key": "cron", "event_type": "root.timer.fired", "mode": "cron", "cron": "17 * * * *",
		}},
		{name: "every", wantKind: runtimegenericschedule.DueEvery, input: map[string]any{
			"schedule_key": "every", "event_type": "root.timer.fired", "mode": "every", "every": "15m",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, scheduler := newExecutor()
			result, err := exec.execSchedule(ctx, actor, tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if scheduler.calls != 1 || scheduler.command.Due.Kind != tc.wantKind {
				t.Fatalf("typed admission = calls:%d command:%#v", scheduler.calls, scheduler.command)
			}
			projection, ok := result.(map[string]any)
			if !ok || projection["activation_id"] == "" || projection["outcome"] != string(runtimegenericschedule.AdmissionCreated) || projection["schedule_key"] != tc.input["schedule_key"] {
				t.Fatalf("schedule result = %#v", result)
			}
		})
	}
	if scheduler.schedule.ExecutionMode != runtimeeffects.ExecutionModeLive {
		t.Fatalf("schedule execution mode = %q, want live", scheduler.schedule.ExecutionMode)
	}
}

func TestExecSchedulePreservesMockAuthority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		actorMode  runtimeeffects.ExecutionMode
		causalMode runtimeeffects.ExecutionMode
		withCausal bool
	}{
		{name: "mock actor", actorMode: runtimeeffects.ExecutionModeMock},
		{name: "mock causal event", actorMode: runtimeeffects.ExecutionModeLive, causalMode: runtimeeffects.ExecutionModeMock, withCausal: true},
		{name: "mock actor and causal event", actorMode: runtimeeffects.ExecutionModeMock, causalMode: runtimeeffects.ExecutionModeMock, withCausal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := &captureScheduleScheduler{}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
			exec := NewExecutorWithOptions(nil, scheduler, ExecutorOptions{WorkflowSource: source})
			actor := models.AgentConfig{
				ExecutionMode: tc.actorMode,
				ID:            "root-agent",
				Identity:      toolTestRootAgentIdentity(t, "root-agent"),
				EntityID:      "entity-root",
			}
			ctx := context.Background()
			if tc.withCausal {
				ctx = runtimeeffects.WithExecutionMode(ctx, tc.causalMode)
			}

			if _, err := exec.execSchedule(ctx, actor, map[string]any{
				"event_type": "root.timer.fired",
				"mode":       "once",
				"at":         time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
				"payload":    map[string]any{"reason": "mock-owned"},
			}); err != nil {
				t.Fatalf("execSchedule: %v", err)
			}
			if scheduler.schedule.ExecutionMode != runtimeeffects.ExecutionModeMock {
				t.Fatalf("schedule execution mode = %q, want mock", scheduler.schedule.ExecutionMode)
			}
		})
	}
}

func TestScheduleBuiltinContractDeliversValidatesAndDispatches(t *testing.T) {
	scheduler := &captureScheduleScheduler{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		WorkflowSource:   semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		GenericSchedules: scheduler,
	})
	actor := models.AgentConfig{
		ID:          "root-agent",
		Identity:    toolTestRootAgentIdentity(t, "root-agent"),
		EntityID:    "00000000-0000-4000-8000-000000002164",
		Permissions: []string{"schedule"},
	}
	definitions := exec.ToolDefinitionsForActor(actor)
	var scheduleDefinitionFound bool
	for _, definition := range definitions {
		if definition.Name == "schedule" {
			scheduleDefinitionFound = true
			break
		}
	}
	if !scheduleDefinitionFound {
		t.Fatalf("ToolDefinitionsForActor omitted schedule: %#v", definitions)
	}

	ctx := WithActor(runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163"), actor)
	result, err := exec.Execute(ctx, "schedule", map[string]any{
		"schedule_key": "supported-surface",
		"event_type":   "root.timer.fired",
		"mode":         "delay",
		"delay":        "10m",
		"payload":      map[string]any{"source": "supported"},
	})
	if err != nil {
		t.Fatalf("Execute(schedule): %v", err)
	}
	projection, ok := result.(map[string]any)
	if !ok || projection["schedule_key"] != "supported-surface" || scheduler.calls != 1 || scheduler.command.Due.Kind != runtimegenericschedule.DueDelay {
		t.Fatalf("supported schedule result=%#v command=%#v calls=%d", result, scheduler.command, scheduler.calls)
	}

	for _, legacy := range []map[string]any{
		{"schedule_key": "legacy-action", "action": "root.timer.fired", "mode": "delay", "delay": "10m"},
		{"schedule_key": "legacy-context", "event_type": "root.timer.fired", "mode": "delay", "delay": "10m", "context": map[string]any{}},
		{"schedule_key": "legacy-seconds", "event_type": "root.timer.fired", "mode": "delay", "delay": "10m", "delay_seconds": 600},
		{"schedule_key": "legacy-once", "event_type": "root.timer.fired", "mode": "once", "at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)},
	} {
		if _, err := exec.Execute(ctx, "schedule", legacy); err == nil {
			t.Fatalf("legacy schedule spelling was admitted: %#v", legacy)
		}
	}
	if scheduler.calls != 1 {
		t.Fatalf("rejected legacy requests reached admission: calls=%d", scheduler.calls)
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
	exec := NewExecutorWithOptions(bus, ExecutorOptions{
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

func TestExecAgentHire_DeniesDelegatedPermissionEscalation(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
		ModelRuntimes:     staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
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

func TestAgentHireHandlerRejectsMockCausalLiveAuthorityBeforeSpawn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		actorMode     runtimeeffects.ExecutionMode
		causalMode    runtimeeffects.ExecutionMode
		hasCausalMode bool
	}{
		{name: "selected mock actor and mock causality", actorMode: runtimeeffects.ExecutionModeMock, causalMode: runtimeeffects.ExecutionModeMock, hasCausalMode: true},
		{name: "selected mock actor without contextual mode", actorMode: runtimeeffects.ExecutionModeMock},
		{name: "mock causality with inconsistent live actor", actorMode: runtimeeffects.ExecutionModeLive, causalMode: runtimeeffects.ExecutionModeMock, hasCausalMode: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"manager": {ID: "manager", Role: "manager", Permissions: []string{"agent_hire"}},
					"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
				},
			})
			manager := &captureManagerStub{}
			exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
				Manager:           manager,
				AuthorityProvider: runtimeauthority.NewSourceProvider(source),
				WorkflowSource:    source,
			})
			actor := models.AgentConfig{
				ExecutionMode: tc.actorMode,
				ID:            "manager",
				Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
				Role:          "manager",
				Permissions:   []string{"agent_hire"},
				FlowPath:      "review/inst-1",
			}
			ctx := WithActor(context.Background(), actor)
			if tc.hasCausalMode {
				ctx = runtimeeffects.WithExecutionMode(ctx, tc.causalMode)
			}
			handler := exec.buildToolHandlers()["agent_hire"]

			_, err := handler(ctx, actor, map[string]any{
				"config": map[string]any{
					"id":               "live-worker-1",
					"role":             "worker",
					"manager_fallback": "manager",
				},
			})
			failure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "mock_agent_hire_forbidden")
			if failure.Operation != "agent_hire.authorize_execution_mode" {
				t.Fatalf("failure operation = %q, want agent_hire.authorize_execution_mode", failure.Operation)
			}
			if manager.spawnCalled {
				t.Fatal("mock-causal hire reached the agent manager")
			}
		})
	}
}

func TestAgentMutationHandlersRejectMockCausalStateChangesBeforeManagerMutation(t *testing.T) {
	t.Parallel()

	authorityCases := []struct {
		name          string
		actorMode     runtimeeffects.ExecutionMode
		causalMode    runtimeeffects.ExecutionMode
		hasCausalMode bool
	}{
		{name: "selected mock actor and mock causality", actorMode: runtimeeffects.ExecutionModeMock, causalMode: runtimeeffects.ExecutionModeMock, hasCausalMode: true},
		{name: "selected mock actor without contextual mode", actorMode: runtimeeffects.ExecutionModeMock},
		{name: "mock causality with inconsistent live actor", actorMode: runtimeeffects.ExecutionModeLive, causalMode: runtimeeffects.ExecutionModeMock, hasCausalMode: true},
	}
	mutations := []struct {
		name  string
		input map[string]any
	}{
		{name: "agent_hire", input: map[string]any{"config": map[string]any{"id": "new-worker", "role": "worker"}}},
		{name: "agent_reconfigure", input: map[string]any{"agent_id": "live-worker", "config": map[string]any{"tools": []any{"agent_message"}}}},
		{name: "agent_fire", input: map[string]any{"agent_id": "live-worker", "reason": "test"}},
	}

	for _, authorityCase := range authorityCases {
		for _, mutation := range mutations {
			t.Run(authorityCase.name+"/"+mutation.name, func(t *testing.T) {
				manager := &captureManagerStub{agents: map[string]models.AgentConfig{
					"live-worker": {ID: "live-worker", ExecutionMode: runtimeeffects.ExecutionModeLive, FlowPath: "review/inst-1"},
				}}
				exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{Manager: manager})
				actor := models.AgentConfig{
					ExecutionMode: authorityCase.actorMode,
					ID:            "manager",
					Permissions:   []string{"agent_hire", "agent_reconfigure", "agent_fire"},
				}
				ctx := WithActor(context.Background(), actor)
				if authorityCase.hasCausalMode {
					ctx = runtimeeffects.WithExecutionMode(ctx, authorityCase.causalMode)
				}

				_, err := exec.buildToolHandlers()[mutation.name](ctx, actor, mutation.input)
				failure := requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "mock_"+mutation.name+"_forbidden")
				if want := mutation.name + ".authorize_execution_mode"; failure.Operation != want {
					t.Fatalf("failure operation = %q, want %q", failure.Operation, want)
				}
				if manager.spawnCalled || manager.reconfigureCalled || manager.teardownCalled {
					t.Fatalf("mock-causal %s reached manager mutation: spawn=%v reconfigure=%v teardown=%v", mutation.name, manager.spawnCalled, manager.reconfigureCalled, manager.teardownCalled)
				}
			})
		}
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
		ModelRuntimes:     staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
		ModelRuntimes:     staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
		ModelRuntimes:     staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})

	_, err := exec.ExecAgentHireDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
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

func TestExecAgentHire_RejectsUnresolvedParentBeforeSpawn(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
		ModelRuntimes:     staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
		FlowPath:      "review/inst-1",
	}

	_, err := exec.ExecAgentHireDirect(actor, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "missing-manager",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "resolve managed parent missing-manager") {
		t.Fatalf("ExecAgentHireDirect error = %v, want unresolved parent preflight", err)
	}
	if manager.spawnCalled {
		t.Fatal("unresolved parent was discovered after spawning")
	}
}

func TestExecAgentHire_RejectsSelfParentBeforeSpawn(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"manager": {ID: "manager", Role: "manager"},
			"worker":  {ID: "worker", Role: "worker", ManagerFallback: "manager"},
		},
	})
	manager := &captureManagerStub{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager",
		Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
		Role:          "manager",
		Permissions:   []string{"agent_hire"},
		FlowPath:      "review/inst-1",
	}

	_, err := exec.ExecAgentHireDirect(actor, map[string]any{
		"config": map[string]any{
			"id":               "worker-1",
			"role":             "worker",
			"manager_fallback": "worker-1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be its own parent") {
		t.Fatalf("ExecAgentHireDirect error = %v, want self-parent preflight", err)
	}
	if manager.spawnCalled {
		t.Fatal("self-parent was discovered after spawning")
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
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
			exec := NewExecutorWithOptions(nil, ExecutorOptions{
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
				ID:            "manager",
				Identity:      agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
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
			exec := NewExecutorWithOptions(nil, ExecutorOptions{
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager-1",
		Identity:      agentidentitytest.Runtime(t, "manager-1", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
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
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		Manager:           manager,
		AuthorityProvider: runtimeauthority.NewSourceProvider(source),
		WorkflowSource:    source,
	})

	_, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "manager-1",
		Identity:      agentidentitytest.Runtime(t, "manager-1", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
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
			exec := NewExecutorWithOptions(nil, ExecutorOptions{
				Manager: manager, AuthorityProvider: runtimeauthority.NewSourceProvider(source), WorkflowSource: source,
			})
			_, err := exec.ExecAgentReconfigureDirect(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "manager", Identity: agentidentitytest.Runtime(t, "manager", "runtime-tools-test", "review", "inst-1", "review/inst-1"),
				Role: "manager", Permissions: []string{"agent_reconfigure"}, FlowPath: "review/inst-1",
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
