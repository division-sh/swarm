package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/sessions"
)

type eventPublisherStub struct {
	events      []events.Event
	marks       []string
	runtimeLogs []runtimepipeline.RuntimeLogEntry
	publishErr  error
	markErr     error
}

func (s *eventPublisherStub) Publish(_ context.Context, evt events.Event) error {
	if s.publishErr != nil {
		return s.publishErr
	}
	s.events = append(s.events, evt)
	return nil
}

func (s *eventPublisherStub) MarkDeliveryInProgress(_ context.Context, agentID, sessionID string) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.marks = append(s.marks, strings.TrimSpace(agentID)+"|"+strings.TrimSpace(sessionID))
	return nil
}

func (s *eventPublisherStub) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) {
	s.runtimeLogs = append(s.runtimeLogs, entry)
}

type failingConversationStore struct {
	err error
}

func (s *failingConversationStore) UpsertConversation(context.Context, ConversationRecord) error {
	return s.err
}

func (s *failingConversationStore) LoadActiveConversation(context.Context, string, string, string) (ConversationRecord, bool, error) {
	return ConversationRecord{}, false, nil
}

type failingTurnStore struct {
	err error
}

func (s *failingTurnStore) AppendAgentTurn(context.Context, AgentTurnRecord) error {
	return s.err
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
	if len(publisher.marks) != 1 {
		t.Fatalf("expected 1 delivery mark, got %d", len(publisher.marks))
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
	if len(publisher.marks) != 1 {
		t.Fatalf("expected 1 delivery mark, got %d", len(publisher.marks))
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

func TestPublishAgentStarted_LogsRuntimeFailures(t *testing.T) {
	publisher := &eventPublisherStub{
		markErr:    errors.New("mark boom"),
		publishErr: errors.New("publish boom"),
	}
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID:       "agent-1",
		EntityID: "entity-1",
	})

	publishAgentStarted(ctx, publisher, &Session{
		ID:                "session-1",
		AgentID:           "agent-1",
		ConversationMode:  sessions.RuntimeModeTask,
		RuntimeMode:       sessions.RuntimeModeTask,
		ProviderSessionID: "provider-1",
	}, events.EventType("platform.agent_started"))

	if len(publisher.runtimeLogs) != 2 {
		t.Fatalf("runtime log count = %d, want 2", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "mark_delivery_in_progress_failed" {
		t.Fatalf("first action = %q", publisher.runtimeLogs[0].Action)
	}
	if publisher.runtimeLogs[1].Action != "publish_agent_started_failed" {
		t.Fatalf("second action = %q", publisher.runtimeLogs[1].Action)
	}
}

func TestClaudeCLIRuntime_PersistTurnFailureLogsRuntime(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", &failingTurnStore{err: errors.New("turn boom")}, nil, nil, nil, publisher)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:   "agent-2",
		SessionID: "session-2",
		EntityID:  "entity-2",
	})

	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "persist_cli_turn_failed" {
		t.Fatalf("action = %q", publisher.runtimeLogs[0].Action)
	}
}

func TestAnthropicAPIRuntime_PersistConversationFailureLogsRuntime(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, &failingConversationStore{err: errors.New("conversation boom")}, nil, publisher)

	runtime.persistConversation(context.Background(), &Session{
		ID:               "session-3",
		AgentID:          "agent-3",
		ConversationMode: sessions.RuntimeModeTask,
		ScopeKey:         "task-3",
	})

	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "persist_api_conversation_failed" {
		t.Fatalf("action = %q", publisher.runtimeLogs[0].Action)
	}
}

func TestEnrichTurnRecordIncludesTriggerToolsAndEmits(t *testing.T) {
	ctx := runtimecorrelation.WithRunID(context.Background(), "run-123")
	ctx = runtimebus.WithInboundEvent(ctx, events.Event{
		ID:      "11111111-1111-1111-1111-111111111111",
		RunID:   "run-123",
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

	if rec.RunID != "run-123" {
		t.Fatalf("run_id = %q, want run-123", rec.RunID)
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
