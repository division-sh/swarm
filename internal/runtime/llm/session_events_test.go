package llm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/sessions"
)

type eventPublisherStub struct {
	events []events.Event
}

func (s *eventPublisherStub) Publish(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func TestAnthropicAPIRuntime_StartSessionPublishesAgentStarted(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(sessions.WithScope(context.Background(), sessions.RuntimeModeTask, "task-1"), runtimeactors.AgentConfig{
		ID:       "agent-1",
		Type:     "sonnet",
		EntityID: "entity-1",
	})

	s, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session")
	}
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 platform.agent_started event, got %d", len(publisher.events))
	}
	evt := publisher.events[0]
	if evt.Type != events.EventType("platform.agent_started") {
		t.Fatalf("event type = %s, want platform.agent_started", evt.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if got := payload["agent_id"]; got != "agent-1" {
		t.Fatalf("agent_id = %#v, want agent-1", got)
	}
	if got := payload["conversation_mode"]; got != sessions.RuntimeModeTask {
		t.Fatalf("conversation_mode = %#v, want task", got)
	}
	if got := payload["model_tier"]; got != "sonnet" {
		t.Fatalf("model_tier = %#v, want sonnet", got)
	}
	if evt.EntityID() != "entity-1" {
		t.Fatalf("entity_id = %q, want entity-1", evt.EntityID())
	}
}

func TestClaudeCLIRuntime_StartSessionPublishesAgentStarted(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(sessions.WithScope(context.Background(), sessions.RuntimeModeTask, "task-1"), runtimeactors.AgentConfig{
		ID:   "agent-2",
		Type: "haiku",
	})

	s, err := runtime.StartSession(ctx, "agent-2", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session")
	}
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 platform.agent_started event, got %d", len(publisher.events))
	}
	evt := publisher.events[0]
	if evt.Type != events.EventType("platform.agent_started") {
		t.Fatalf("event type = %s, want platform.agent_started", evt.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if got := payload["agent_id"]; got != "agent-2" {
		t.Fatalf("agent_id = %#v, want agent-2", got)
	}
	if got := payload["model_tier"]; got != "haiku" {
		t.Fatalf("model_tier = %#v, want haiku", got)
	}
}

func TestClaudeCLIRuntime_StartSessionAugmentsSystemPromptWithSwarmTools(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil)

	s, err := runtime.StartSession(context.Background(), "agent-2", "base prompt", []ToolDefinition{
		{Name: "emit_market_research_scan_complete"},
		{Name: "read_file"},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session")
	}
	if !strings.Contains(s.SystemPrompt, cliToolInvocationMarker) {
		t.Fatalf("expected CLI tool note in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "emit_market_research_scan_complete") {
		t.Fatalf("expected emit tool name in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "read_file") {
		t.Fatalf("expected native fallback tool name in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "Do not write JSON files under `/workspace/events`") {
		t.Fatalf("expected emit workaround warning in system prompt, got %q", s.SystemPrompt)
	}
}

func TestAnthropicAPIRuntime_PersistTurnIncludesTaskMode(t *testing.T) {
	turns := &turnCapture{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", turns, nil, nil, nil)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:     "agent-1",
		RuntimeMode: sessions.RuntimeModeTask,
		SessionID:   "session-1",
	})

	if len(turns.records) != 1 {
		t.Fatalf("expected task-mode turn to persist, got %d records", len(turns.records))
	}
	if turns.records[0].RuntimeMode != sessions.RuntimeModeTask {
		t.Fatalf("runtime_mode = %q, want task", turns.records[0].RuntimeMode)
	}
}

func TestClaudeCLIRuntime_PersistTurnIncludesTaskMode(t *testing.T) {
	turns := &turnCapture{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", turns, nil, nil, nil, nil)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:     "agent-2",
		RuntimeMode: sessions.RuntimeModeTask,
		SessionID:   "session-2",
	})

	if len(turns.records) != 1 {
		t.Fatalf("expected task-mode turn to persist, got %d records", len(turns.records))
	}
	if turns.records[0].RuntimeMode != sessions.RuntimeModeTask {
		t.Fatalf("runtime_mode = %q, want task", turns.records[0].RuntimeMode)
	}
}

func TestEnrichTurnRecordIncludesTraceTriggerToolsAndEmits(t *testing.T) {
	ctx := runtimecorrelation.WithTraceID(context.Background(), "trace-123")
	ctx = runtimebus.WithInboundEvent(ctx, events.Event{
		ID:      "11111111-1111-1111-1111-111111111111",
		Type:    events.EventType("scan.requested"),
		Payload: []byte(`{"entity_id":"22222222-2222-2222-2222-222222222222"}`),
	})
	recorder := runtimebus.NewEmittedEventsRecorder()
	recorder.Append(events.Event{Type: events.EventType("discovery/category.assessed")})
	recorder.Append(events.Event{Type: events.EventType("discovery/category.assessed")})
	recorder.Append(events.Event{Type: events.EventType("discovery/scan_complete")})
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)

	session := &Session{
		ID:       "33333333-3333-3333-3333-333333333333",
		ScopeKey: "global",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "read_file"},
		},
	}
	resp := &Response{
		MCPServers: map[string]string{
			"runtime-tools": "connected",
		},
		MCPVisibleTools: []string{
			"mcp__runtime-tools__emit_category_assessed",
		},
		ToolCalls: []ToolCall{
			{Name: "emit_category_assessed", Arguments: map[string]any{"subcategory": "x"}},
		},
	}

	rec := enrichTurnRecord(ctx, session, AgentTurnRecord{
		AgentID:     "market-research-agent",
		RuntimeMode: sessions.RuntimeModeSession,
		SessionID:   session.ID,
	}, resp)

	if rec.TraceID != "trace-123" {
		t.Fatalf("trace_id = %q, want trace-123", rec.TraceID)
	}
	if rec.TriggerEventID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("trigger_event_id = %q", rec.TriggerEventID)
	}
	if rec.TriggerEventType != "scan.requested" {
		t.Fatalf("trigger_event_type = %q", rec.TriggerEventType)
	}
	if rec.EntityID != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("entity_id = %q", rec.EntityID)
	}
	if len(rec.AvailableTools) != 2 || rec.AvailableTools[0] != "emit_category_assessed" || rec.AvailableTools[1] != "read_file" {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
	if len(rec.ToolCalls) != 1 || rec.ToolCalls[0].Name != "emit_category_assessed" {
		t.Fatalf("tool_calls = %#v", rec.ToolCalls)
	}
	if got := rec.MCPServers["runtime-tools"]; got != "connected" {
		t.Fatalf("mcp_servers = %#v", rec.MCPServers)
	}
	if len(rec.MCPToolsListed) != 2 || rec.MCPToolsListed[0] != "mcp__runtime-tools__emit_category_assessed" || rec.MCPToolsListed[1] != "mcp__runtime-tools__read_file" {
		t.Fatalf("mcp_tools_listed = %#v", rec.MCPToolsListed)
	}
	if len(rec.MCPToolsVisible) != 1 || rec.MCPToolsVisible[0] != "mcp__runtime-tools__emit_category_assessed" {
		t.Fatalf("mcp_tools_visible = %#v", rec.MCPToolsVisible)
	}
	if len(rec.EmittedEvents) != 2 || rec.EmittedEvents[0] != "discovery/category.assessed" || rec.EmittedEvents[1] != "discovery/scan_complete" {
		t.Fatalf("emitted_events = %#v", rec.EmittedEvents)
	}
}
