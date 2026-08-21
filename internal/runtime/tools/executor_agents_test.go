package tools

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func toolTestRootAgentIdentity(t testing.TB, agentID string) agentidentity.Identity {
	t.Helper()
	return agentidentitytest.RootDeclared(t, agentID, "swarm-test://root/agents/"+strings.TrimSpace(agentID))
}

func toolTestAgentIdentity(t testing.TB, agentID, flowID, flowPath string) agentidentity.Identity {
	t.Helper()
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if flowID == "" && flowPath == "" {
		return toolTestRootAgentIdentity(t, agentID)
	}
	return agentidentitytest.Declared(t, agentID, "swarm-test://"+flowID+"/"+strings.TrimSpace(agentID), flowID, "test-instance", flowPath)
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

type concreteManagerStub struct {
	agents map[agentidentity.Identity]models.AgentConfig
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
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: reviewFlow,
			ByID: map[string]*runtimecontracts.FlowContractView{"review": reviewFlow},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "control", "review")
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
		Identity:      toolTestAgentIdentity(t, "control", "review", "review/inst-1"),
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
	wantSourceRoute := events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1", EntityID: "entity-a"}
	if got := bus.event.RoutingSource().Route().Normalized(); got != wantSourceRoute {
		t.Fatalf("agent_message routing source = %#v, want %#v", got, wantSourceRoute)
	}
}

func TestExecSchedulePreservesRootAgentRoutingSource(t *testing.T) {
	scheduler := &captureScheduleScheduler{}
	source := toolTestSourceWithDeclaredAgent(t, &runtimecontracts.WorkflowContractBundle{}, "root-agent", "")
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, GenericSchedules: scheduler})
	actor := models.AgentConfig{
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ID:            "root-agent",
		Identity:      toolTestRootAgentIdentity(t, "root-agent"),
		EntityID:      "entity-root",
	}

	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163"), runtimeeffects.ExecutionModeLive)
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

func TestExecSchedulePreservesImportedTemplateAgentRoutingSource(t *testing.T) {
	const (
		flowID       = "telegram-chat"
		flowPath     = "telegram-ingress/telegram-chat"
		instancePath = flowPath + "/chat-1"
		agentID      = "scheduler-agent"
	)
	flow := runtimecontracts.FlowContractView{
		Path: flowPath,
		Paths: runtimecontracts.FlowContractPaths{
			ID: flowID, Flow: flowID, PackageKey: "bot", AgentsFile: "/contracts/bot/flows/telegram-chat/agents.yaml",
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			agentID: runtimecontracts.EffectiveAgentRegistryEntry(agentID, runtimecontracts.AgentRegistryEntry{ID: agentID, Role: agentID}),
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: runtimecontracts.FlowTree{
		Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root.Children[0]},
	}}
	source := toolTestSourceWithDeclaredAgent(t, bundle, agentID, flowID)
	declarations := semanticview.AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("agent declarations = %#v, want one", declarations)
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
	if err != nil {
		t.Fatalf("agent declaration name: %v", err)
	}
	actor := models.AgentConfig{
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ID:            agentID,
		Identity:      agentidentitytest.Declared(t, agentID, plan.OwnerURI, flowPath, "chat-1", instancePath),
		FlowID:        flowID,
		FlowPath:      instancePath,
		EntityID:      "entity-chat",
	}
	scheduler := &captureScheduleScheduler{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, GenericSchedules: scheduler})
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002164"), runtimeeffects.ExecutionModeLive)
	if _, err := exec.execSchedule(ctx, actor, map[string]any{
		"schedule_key": "imported-proof",
		"event_type":   flowPath + "/telegram.followup.requested",
		"mode":         "absolute",
		"at":           time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"payload":      map[string]any{"chat_id": "42"},
	}); err != nil {
		t.Fatalf("execSchedule(imported template agent): %v", err)
	}
	wantRoute := events.RouteIdentity{FlowID: flowID, FlowInstance: instancePath, EntityID: actor.EntityID}
	if got := scheduler.command.RoutingSource.Route().Normalized(); got != wantRoute {
		t.Fatalf("schedule routing source = %#v, want %#v", got, wantRoute)
	}
	if scheduler.command.FlowInstance != instancePath {
		t.Fatalf("schedule flow instance = %q, want %q", scheduler.command.FlowInstance, instancePath)
	}
}

func TestExecScheduleAdmissionGatesAndTypedDueBasis(t *testing.T) {
	actor := models.AgentConfig{
		ID:       "root-agent",
		Identity: toolTestRootAgentIdentity(t, "root-agent"),
		EntityID: "entity-root",
	}
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163"), runtimeeffects.ExecutionModeLive)
	newExecutor := func() (*Executor, *captureScheduleScheduler) {
		t.Helper()
		scheduler := &captureScheduleScheduler{}
		return NewExecutorWithOptions(nil, ExecutorOptions{
			WorkflowSource:   toolTestSourceWithDeclaredAgent(t, &runtimecontracts.WorkflowContractBundle{}, "root-agent", ""),
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
}

func TestExecSchedulePreservesExactCausalMode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      runtimeeffects.ExecutionMode
		wantError bool
	}{
		{name: "live causal event", mode: runtimeeffects.ExecutionModeLive},
		{name: "mock causal event", mode: runtimeeffects.ExecutionModeMock},
		{name: "missing causal mode", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := &captureScheduleScheduler{}
			source := toolTestSourceWithDeclaredAgent(t, &runtimecontracts.WorkflowContractBundle{}, "root-agent", "")
			exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, GenericSchedules: scheduler})
			actor := models.AgentConfig{
				ExecutionMode: runtimeeffects.ExecutionModeLive,
				ID:            "root-agent",
				Identity:      toolTestRootAgentIdentity(t, "root-agent"),
				EntityID:      "entity-root",
			}
			ctx := runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163")
			if tc.mode.Valid() {
				ctx = runtimeeffects.WithExecutionMode(ctx, tc.mode)
			}

			if _, err := exec.execSchedule(ctx, actor, map[string]any{
				"schedule_key": "mode-proof",
				"event_type":   "root.timer.fired",
				"mode":         "absolute",
				"at":           time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
				"payload":      map[string]any{"reason": "mock-owned"},
			}); tc.wantError != (err != nil) {
				t.Fatalf("execSchedule: %v", err)
			}
			if tc.wantError {
				if scheduler.calls != 0 {
					t.Fatalf("missing causal mode reached admission %d time(s)", scheduler.calls)
				}
				return
			}
			if scheduler.command.ExecutionMode != tc.mode {
				t.Fatalf("schedule execution mode = %q, want %q", scheduler.command.ExecutionMode, tc.mode)
			}
		})
	}
}

func TestScheduleBuiltinContractDeliversValidatesAndDispatches(t *testing.T) {
	scheduler := &captureScheduleScheduler{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		WorkflowSource:   toolTestSourceWithDeclaredAgent(t, &runtimecontracts.WorkflowContractBundle{}, "root-agent", ""),
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

	ctx := runtimeeffects.WithExecutionMode(
		WithActor(runtimecorrelation.WithRunID(context.Background(), "00000000-0000-4000-8000-000000002163"), actor),
		runtimeeffects.ExecutionModeLive,
	)
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
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: reviewFlow,
			ByID: map[string]*runtimecontracts.FlowContractView{"review": reviewFlow},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "manager", "review")
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
		Identity:      toolTestAgentIdentity(t, "manager", "review", "review/inst-b"),
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
