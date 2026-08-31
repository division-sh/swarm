package tools

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const toolTestRunID = "22222222-2222-4222-8222-222222222222"

func toolTestRootAgentIdentity(t testing.TB, agentID string) agentidentity.Identity {
	t.Helper()
	return agentidentitytest.RootDeclaredForRun(t, toolTestRunID, agentID, "swarm-test://root/agents/"+strings.TrimSpace(agentID))
}

func toolTestAgentIdentity(t testing.TB, agentID, flowID, flowPath string) agentidentity.Identity {
	t.Helper()
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if flowID == "" && flowPath == "" {
		return toolTestRootAgentIdentity(t, agentID)
	}
	return agentidentitytest.DeclaredForRun(t, toolTestRunID, agentID, "swarm-test://"+flowID+"/"+strings.TrimSpace(agentID), flowID, "test-instance", flowPath)
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

	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(context.Background(), toolTestRunID), runtimeeffects.ExecutionModeLive)
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
		Identity:      agentidentitytest.DeclaredForRun(t, toolTestRunID, agentID, plan.OwnerURI, flowPath, "chat-1", instancePath),
		FlowID:        flowID,
		FlowPath:      instancePath,
		EntityID:      "entity-chat",
	}
	scheduler := &captureScheduleScheduler{}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, GenericSchedules: scheduler})
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(context.Background(), toolTestRunID), runtimeeffects.ExecutionModeLive)
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
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(context.Background(), toolTestRunID), runtimeeffects.ExecutionModeLive)
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
			ctx := runtimecorrelation.WithRunID(context.Background(), toolTestRunID)
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
		WithActor(runtimecorrelation.WithRunID(context.Background(), toolTestRunID), actor),
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
