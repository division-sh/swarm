package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"time"
)

type eventPublisherStub struct {
	events      []events.Event
	marks       []string
	runtimeLogs []runtimepipeline.RuntimeLogEntry
	publishErr  error
	markErr     error
	markChanged bool
}

func (s *eventPublisherStub) Publish(_ context.Context, evt events.Event) error {
	if s.publishErr != nil {
		return s.publishErr
	}
	s.events = append(s.events, evt)
	return nil
}

func (s *eventPublisherStub) MarkDeliveryInProgress(_ context.Context, agentID, sessionID string) (bool, error) {
	if s.markErr != nil {
		return false, s.markErr
	}
	s.marks = append(s.marks, strings.TrimSpace(agentID)+"|"+strings.TrimSpace(sessionID))
	return s.markChanged, nil
}

func (s *eventPublisherStub) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	s.runtimeLogs = append(s.runtimeLogs, entry)
	return nil
}

type failingConversationStore struct {
	err error
}

func (s *failingConversationStore) UpsertConversation(context.Context, ConversationRecord) error {
	return s.err
}

func (s *failingConversationStore) LoadActiveConversation(context.Context, string, string, string, string) (ConversationRecord, bool, error) {
	return ConversationRecord{}, false, nil
}

func (s *failingConversationStore) UpdateLiveSessionWatchdog(context.Context, ConversationWatchdogUpdate) error {
	return s.err
}

type captureConversationStore struct {
	record         ConversationRecord
	load           ConversationRecord
	loadOK         bool
	watchdogUpdate ConversationWatchdogUpdate
}

func (s *captureConversationStore) UpsertConversation(_ context.Context, rec ConversationRecord) error {
	s.record = rec
	return nil
}

func (s *captureConversationStore) LoadActiveConversation(context.Context, string, string, string, string) (ConversationRecord, bool, error) {
	return s.load, s.loadOK, nil
}

func (s *captureConversationStore) UpdateLiveSessionWatchdog(_ context.Context, update ConversationWatchdogUpdate) error {
	s.watchdogUpdate = update
	return nil
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
	ctx := runtimeactors.WithActor(sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1"), runtimeactors.AgentConfig{
		ID:       "agent-1",
		Model:    "regular",
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
	if len(publisher.runtimeLogs) != 0 {
		t.Fatalf("expected no delivery lifecycle log without a real delivery mark, got %d", len(publisher.runtimeLogs))
	}
	evt := publisher.events[0]
	if evt.Type() != events.EventType("platform.agent_started") {
		t.Fatalf("event type = %s, want platform.agent_started", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if got := payload["agent_id"]; got != "agent-1" {
		t.Fatalf("agent_id = %#v, want agent-1", got)
	}
	if got := payload["mode"]; got != sessions.RuntimeModeTask.String() {
		t.Fatalf("mode = %#v, want task", got)
	}
	if got := payload["model"]; got != "regular" {
		t.Fatalf("model = %#v, want regular", got)
	}
	if evt.EntityID() != "entity-1" {
		t.Fatalf("entity_id = %q, want entity-1", evt.EntityID())
	}
}

func TestClaudeCLIRuntime_StartSessionPublishesAgentStarted(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1"), runtimeactors.AgentConfig{
		ID:    "agent-2",
		Model: "cheap",
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
	if len(publisher.runtimeLogs) != 0 {
		t.Fatalf("expected no delivery lifecycle log without a real delivery mark, got %d", len(publisher.runtimeLogs))
	}
	evt := publisher.events[0]
	if evt.Type() != events.EventType("platform.agent_started") {
		t.Fatalf("event type = %s, want platform.agent_started", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if got := payload["agent_id"]; got != "agent-2" {
		t.Fatalf("agent_id = %#v, want agent-2", got)
	}
	if got := payload["model"]; got != "cheap" {
		t.Fatalf("model = %#v, want cheap", got)
	}
}

func TestClaudeCLIRuntime_StartSessionAugmentsSystemPromptWithSwarmTools(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil)
	ctx := sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1")

	s, err := runtime.StartSession(ctx, "agent-2", "base prompt", []ToolDefinition{
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
	if !strings.Contains(s.SystemPrompt, "mcp__runtime-tools__read_file") {
		t.Fatalf("expected provider-visible runtime tool name in system prompt, got %q", s.SystemPrompt)
	}
	if strings.Contains(s.SystemPrompt, "\n- read_file\n") {
		t.Fatalf("did not expect raw non-emit runtime tool fallback in system prompt, got %q", s.SystemPrompt)
	}
	if strings.Contains(s.SystemPrompt, "Claude CLI native tools available in this turn") {
		t.Fatalf("did not expect native builtin prompt section, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "Do not write JSON files under `/workspace/events`") {
		t.Fatalf("expected emit workaround warning in system prompt, got %q", s.SystemPrompt)
	}
}

func TestAnthropicAPIRuntime_StartSessionAugmentsSystemPromptWithDerivedToolSurface(t *testing.T) {
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil)
	ctx := runtimeactors.WithActor(
		sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1"),
		runtimeactors.AgentConfig{
			ID: "agent-3",
			NativeTools: runtimeactors.NativeToolConfig{
				FileIO: true,
			},
		},
	)

	s, err := runtime.StartSession(ctx, "agent-3", "base prompt", []ToolDefinition{
		{Name: "emit_market_research_scan_complete"},
		{Name: "query_entities"},
		{
			Name: "save_entity_field",
			Schema: map[string]any{
				"properties": map[string]any{
					"field": map[string]any{
						"enum": []any{"metadata", "metadata.region", "status"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session")
	}
	if !strings.Contains(s.SystemPrompt, deliveryToolSurfaceMarker) {
		t.Fatalf("expected derived delivery section in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "Available emit tools in this turn: emit_market_research_scan_complete") {
		t.Fatalf("expected emit tool summary in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "Available non-emit tools in this turn: query_entities") {
		t.Fatalf("expected non-emit tool summary in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "Writable entity paths for save_entity_field in this turn: metadata, metadata.region, status") {
		t.Fatalf("expected writable path summary in system prompt, got %q", s.SystemPrompt)
	}
	if !strings.Contains(s.SystemPrompt, "Available native CLI tools in this turn: Edit, Read, Write") {
		t.Fatalf("expected native tool summary in system prompt, got %q", s.SystemPrompt)
	}
	if strings.Contains(s.SystemPrompt, "mcp__runtime-tools__query_entities") {
		t.Fatalf("did not expect CLI-only MCP narration in API system prompt, got %q", s.SystemPrompt)
	}
}

func TestPublishAgentStarted_LogsActiveTransitionOnlyAfterRealDeliveryMark(t *testing.T) {
	publisher := &eventPublisherStub{markChanged: true}
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID:       "agent-1",
		EntityID: "entity-1",
	})

	publishAgentStarted(ctx, publisher, &Session{
		ID:               "session-1",
		AgentID:          "agent-1",
		ConversationMode: sessions.RuntimeModeTask.String(),
		RuntimeMode:      sessions.RuntimeModeTask.String(),
	}, events.EventType("platform.agent_started"))

	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("expected 1 runtime log, got %d", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "delivery_lifecycle_transition" {
		t.Fatalf("runtime log action = %q, want delivery_lifecycle_transition", publisher.runtimeLogs[0].Action)
	}
	detail, ok := publisher.runtimeLogs[0].Detail.(map[string]any)
	if !ok {
		t.Fatalf("runtime log detail = %#v", publisher.runtimeLogs[0].Detail)
	}
	if detail["delivery_state"] != "active" || detail["delivery_previous_state"] != "launching" || detail["delivery_reason"] != "session_started" {
		t.Fatalf("active delivery detail = %#v", detail)
	}
}

func TestEnrichTurnRecord_UsesCanonicalVisibleToolsForNativeCapabilities(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
			Bash:   true,
		},
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, nil)

	if !slices.Equal(rec.AvailableTools, []string{"bash", "emit_category_assessed", "read_file", "write_file"}) {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
}

func TestEnrichTurnRecord_CarriesInboundFlowInstance(t *testing.T) {
	ctx := runtimebus.WithInboundEvent(context.Background(), eventtest.RootIngress(
		"11111111-1111-1111-1111-111111111111",
		events.EventType("analysis.requested"),
		"tester",
		"",
		nil,
		0,
		"run-123",
		"",
		events.EventEnvelope{FlowInstance: "review/inst-1"},
		time.Now(),
	))
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, nil)

	if rec.FlowInstance != "review/inst-1" {
		t.Fatalf("flow_instance = %q, want review/inst-1", rec.FlowInstance)
	}
	if rec.EntityID != "" {
		t.Fatalf("entity_id = %q, want empty for flow-only inbound event", rec.EntityID)
	}
}

func TestEnrichTurnRecord_FiltersCLIControlToolsFromObservedVisibleTools(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, &Response{
		VisibleTools: []string{"ExitPlanMode", "emit_category_assessed", "read_file", "write_file"},
	})

	if !slices.Equal(rec.AvailableTools, []string{"emit_category_assessed", "read_file", "write_file"}) {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
}

func TestEnrichTurnRecord_UsesEmitFallbackWhenObservedCLISurfaceExistsWithoutVisibleNonEmitTools(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "read_file"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, &Response{
		MCPServers: map[string]string{
			"runtime-tools": "failed",
		},
	})

	if !slices.Equal(rec.AvailableTools, []string{"emit_category_assessed"}) {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
}

func TestEnrichTurnRecord_PreservesEmitFallbackAlongsideObservedSupportedReadFileSurface(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "read_file"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, &Response{
		MCPServers:      map[string]string{"runtime-tools": "connected"},
		MCPVisibleTools: []string{"mcp__runtime-tools__read_file"},
	})

	if !slices.Equal(rec.AvailableTools, []string{"emit_category_assessed", "read_file"}) {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
}

func TestEnrichTurnRecord_NativeBuiltinsDoNotLeakIntoMCPToolsListed(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO:    true,
			Bash:      true,
			WebSearch: true,
		},
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "read_file"},
			{Name: "write_file"},
			{Name: "bash"},
			{Name: "web_search"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, nil)

	if !slices.Equal(rec.MCPToolsListed, []string{"mcp__runtime-tools__emit_category_assessed"}) {
		t.Fatalf("mcp_tools_listed = %#v", rec.MCPToolsListed)
	}
	if !slices.Equal(rec.AvailableTools, []string{"bash", "emit_category_assessed", "read_file", "web_search", "write_file"}) {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
}

func TestEnrichTurnRecord_UsesPlannedConfiguredSurfaceWhenObservedMetadataIsAbsent(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "query_entities"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, &Response{
		ToolCalls: []ToolCall{
			{Name: "query_entities", Arguments: map[string]any{"entity_type": "company"}},
		},
	})

	if !slices.Equal(rec.AvailableTools, []string{"emit_category_assessed", "query_entities"}) {
		t.Fatalf("available_tools = %#v", rec.AvailableTools)
	}
}

func TestEnrichTurnRecord_PrefersObservedToolCallsForPersistenceWhenExecutionCallsAreSuppressed(t *testing.T) {
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID: "analysis-agent",
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
		},
	}, AgentTurnRecord{
		AgentID:     "analysis-agent",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	}, &Response{
		ToolCalls: []ToolCall{},
		ObservedToolCalls: []ToolCall{
			{Name: "emit_category_assessed", Arguments: map[string]any{"category": "payments"}},
		},
	})

	if len(rec.ToolCalls) != 1 || rec.ToolCalls[0].Name != "emit_category_assessed" {
		t.Fatalf("tool_calls = %#v", rec.ToolCalls)
	}
}

func TestClaudeCLIRuntimePrompt_HidesNativeCapabilityFallbackToolsFromPostamble(t *testing.T) {
	actor := runtimeactors.AgentConfig{
		ID: "analysis-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
			Bash:   true,
		},
	}
	prompt := augmentCLISystemPrompt("base prompt", actor, []ToolDefinition{
		{Name: "emit_market_research_scan_complete"},
		{Name: "read_file"},
		{Name: "write_file"},
		{Name: "bash"},
	})

	if !strings.Contains(prompt, "emit_market_research_scan_complete") {
		t.Fatalf("expected non-native runtime tool in prompt, got %q", prompt)
	}
	for _, name := range []string{"read_file", "write_file", "bash"} {
		if strings.Contains(prompt, "\n- "+name+"\n") {
			t.Fatalf("did not expect native capability tool %q in prompt, got %q", name, prompt)
		}
	}
	for _, name := range []string{"mcp__runtime-tools__read_file", "mcp__runtime-tools__write_file", "mcp__runtime-tools__bash"} {
		if strings.Contains(prompt, name) {
			t.Fatalf("did not expect fallback MCP tool %q in prompt, got %q", name, prompt)
		}
	}
	if strings.Contains(prompt, "Claude CLI native tools available in this turn") {
		t.Fatalf("did not expect native builtin prompt section, got %q", prompt)
	}
}

func TestClaudeCLIRuntimePrompt_IncludesWritableEntityPathSummary(t *testing.T) {
	actor := runtimeactors.AgentConfig{
		ID:   "analysis-agent",
		Role: "analysis",
	}
	prompt := augmentCLISystemPrompt("base prompt", actor, []ToolDefinition{
		{Name: "save_entity_field", Schema: map[string]any{
			"properties": map[string]any{
				"field": map[string]any{
					"enum": []any{"status", "metadata.region", "metadata"},
				},
			},
		}},
	})

	if !strings.Contains(prompt, "Writable entity paths for save_entity_field in this turn: metadata, metadata.region, status") {
		t.Fatalf("expected writable path summary in prompt, got %q", prompt)
	}
}

func TestAnthropicAPIRuntime_PersistTurnIncludesTaskMode(t *testing.T) {
	turns := &turnCapture{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", turns, nil, nil, nil)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:     "agent-1",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-1",
	})

	if len(turns.records) != 1 {
		t.Fatalf("expected task-mode turn to persist, got %d records", len(turns.records))
	}
	if turns.records[0].RuntimeMode != sessions.RuntimeModeTask.String() {
		t.Fatalf("runtime_mode = %q, want task", turns.records[0].RuntimeMode)
	}
}

func TestAnthropicAPIRuntime_PersistTurnDefersTurnBlockCanonicalizationToStore(t *testing.T) {
	turns := &turnCapture{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", turns, nil, nil, nil)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:        "agent-1",
		RuntimeMode:    sessions.RuntimeModeTask.String(),
		SessionID:      "session-1",
		ResponseRaw:    []byte(`{"result":"done"}`),
		TriggerEventID: "evt-1",
	})

	if len(turns.records) != 1 {
		t.Fatalf("persisted turn count = %d, want 1", len(turns.records))
	}
	if len(turns.records[0].TurnBlocks) != 0 {
		t.Fatalf("persisted turn blocks = %#v, want store-side canonicalization", turns.records[0].TurnBlocks)
	}
}

func TestClaudeCLIRuntime_PersistTurnIncludesTaskMode(t *testing.T) {
	turns := &turnCapture{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", turns, nil, nil, nil, nil)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:     "agent-2",
		RuntimeMode: sessions.RuntimeModeTask.String(),
		SessionID:   "session-2",
	})

	if len(turns.records) != 1 {
		t.Fatalf("expected task-mode turn to persist, got %d records", len(turns.records))
	}
	if turns.records[0].RuntimeMode != sessions.RuntimeModeTask.String() {
		t.Fatalf("runtime_mode = %q, want task", turns.records[0].RuntimeMode)
	}
}

func TestClaudeCLIRuntime_PersistTurnDefersTurnBlockCanonicalizationToStore(t *testing.T) {
	turns := &turnCapture{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", turns, nil, nil, nil, nil)

	runtime.persistTurn(context.Background(), AgentTurnRecord{
		AgentID:        "agent-2",
		RuntimeMode:    sessions.RuntimeModeTask.String(),
		SessionID:      "session-2",
		ResponseRaw:    []byte(`{"result":"done"}`),
		TriggerEventID: "evt-2",
	})

	if len(turns.records) != 1 {
		t.Fatalf("persisted turn count = %d, want 1", len(turns.records))
	}
	if len(turns.records[0].TurnBlocks) != 0 {
		t.Fatalf("persisted turn blocks = %#v, want store-side canonicalization", turns.records[0].TurnBlocks)
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
		ConversationMode:  sessions.RuntimeModeTask.String(),
		RuntimeMode:       sessions.RuntimeModeTask.String(),
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
		ConversationMode: sessions.RuntimeModeTask.String(),
		ScopeKey:         "task-3",
	})

	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "persist_api_conversation_failed" {
		t.Fatalf("action = %q", publisher.runtimeLogs[0].Action)
	}
}

func TestAnthropicAPIRuntime_PersistConversationIncludesSessionScope(t *testing.T) {
	store := &captureConversationStore{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, store, nil, nil)

	runtime.persistConversation(context.Background(), &Session{
		ID:               "session-3",
		AgentID:          "agent-3",
		ConversationMode: sessions.RuntimeModeSession.String(),
		SessionScope:     sessions.SessionScopeFlow.String(),
		ScopeKey:         "review/inst-1",
	})

	if store.record.SessionScope != sessions.SessionScopeFlow.String() {
		t.Fatalf("SessionScope = %q, want %q", store.record.SessionScope, sessions.SessionScopeFlow)
	}
}

func TestClaudeCLIRuntime_PersistConversationIncludesSessionScope(t *testing.T) {
	store := &captureConversationStore{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, store, nil)

	runtime.persistConversation(context.Background(), &Session{
		ID:               "session-4",
		AgentID:          "agent-4",
		ConversationMode: sessions.RuntimeModeSessionPerEntity.String(),
		SessionScope:     sessions.SessionScopeEntity.String(),
		ScopeKey:         "entity-1",
	})

	if store.record.SessionScope != sessions.SessionScopeEntity.String() {
		t.Fatalf("SessionScope = %q, want %q", store.record.SessionScope, sessions.SessionScopeEntity)
	}
}

func TestClaudeCLIRuntime_PersistConversationSuccessDoesNotLogFailure(t *testing.T) {
	store := &captureConversationStore{}
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, store, publisher)

	runtime.persistConversation(context.Background(), &Session{
		ID:               "session-task",
		AgentID:          "agent-task",
		ConversationMode: sessions.RuntimeModeTask.String(),
		ScopeKey:         "task-scope",
		Messages:         []Message{{Role: "assistant", Content: "done"}},
		TurnCount:        1,
	})

	if store.record.Mode != sessions.RuntimeModeTask.String() || store.record.SessionID != "session-task" {
		t.Fatalf("stored conversation = %+v, want task session snapshot", store.record)
	}
	if len(publisher.runtimeLogs) != 0 {
		t.Fatalf("runtime log count = %d, want 0 after successful task conversation persistence", len(publisher.runtimeLogs))
	}
}

func TestClaudeCLIRuntime_StartSessionLoadsRetryLineage(t *testing.T) {
	store := &captureConversationStore{
		load: ConversationRecord{
			SessionID:            "session-previous",
			Messages:             []Message{{Role: "assistant", Content: "hello again"}},
			TurnCount:            2,
			RetryReason:          "session not found",
			RetriesFromSessionID: "session-original",
		},
		loadOK: true,
	}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, store, nil)
	ctx := sessions.WithScope(context.Background(), sessions.RuntimeModeSession.String(), sessions.SessionScopeGlobal.String(), "global")

	s, err := runtime.StartSession(ctx, "agent-4", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.RetryReason != "session not found" {
		t.Fatalf("RetryReason = %q, want session not found", s.RetryReason)
	}
	if s.RetriesFromSessionID != "session-original" {
		t.Fatalf("RetriesFromSessionID = %q, want session-original", s.RetriesFromSessionID)
	}
	if s.TurnCount != 2 || len(s.Messages) != 1 {
		t.Fatalf("unexpected restored session: %+v", s)
	}
}

func TestAnthropicAPIRuntime_ContinueSessionReMarksInboundDeliveryForReusedSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"claude-test",
			"usage":{"input_tokens":1,"output_tokens":1},
			"content":[{"type":"text","text":"ok"}]
		}`))
	}))
	defer server.Close()

	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{
		LLM: config.LLMConfig{
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-test",
			},
		},
	}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, publisher)
	runtime.apiURL = server.URL
	runtime.apiKey = "test-key"
	runtime.httpClient = server.Client()

	ctx := runtimeactors.WithActor(
		runtimebus.WithInboundEvent(
			sessions.WithScope(context.Background(), sessions.RuntimeModeSession.String(), sessions.SessionScopeFlow.String(), "support/inst-1"), eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		),
		runtimeactors.AgentConfig{
			ID:           "agent-1",
			Model:        "regular",
			SessionScope: sessions.SessionScopeFlow.String(),
			FlowPath:     "support/inst-1",
		},
	)

	s, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if len(publisher.marks) != 1 {
		t.Fatalf("start-session marks = %d, want 1", len(publisher.marks))
	}
	publisher.marks = nil

	if _, err := runtime.ContinueSession(ctx, s, Message{Role: "user", Content: "hello"}); err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if len(publisher.marks) != 1 {
		t.Fatalf("continue-session marks = %d, want 1", len(publisher.marks))
	}
	if got := publisher.marks[0]; got != "agent-1|"+s.ID {
		t.Fatalf("continue-session mark = %q, want %q", got, "agent-1|"+s.ID)
	}
}

func TestAnthropicAPIRuntime_ContinueSessionFailsClosedWhenDeliveryRestampFails(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{
		LLM: config.LLMConfig{
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-test",
			},
		},
	}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(
		runtimebus.WithInboundEvent(
			sessions.WithScope(context.Background(), sessions.RuntimeModeSession.String(), sessions.SessionScopeFlow.String(), "support/inst-1"), eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		),
		runtimeactors.AgentConfig{
			ID:           "agent-1",
			Model:        "regular",
			SessionScope: sessions.SessionScopeFlow.String(),
			FlowPath:     "support/inst-1",
		},
	)

	s, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	publisher.marks = nil
	publisher.markErr = errors.New("mark boom")

	_, err = runtime.ContinueSession(ctx, s, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "mark inbound delivery active for reused api session: mark boom") {
		t.Fatalf("ContinueSession err = %v, want mark failure", err)
	}
	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "mark_delivery_in_progress_failed" {
		t.Fatalf("runtime log action = %q, want mark_delivery_in_progress_failed", publisher.runtimeLogs[0].Action)
	}
}

func TestClaudeCLIRuntime_ContinueSessionFailsClosedWhenDeliveryRestampFails(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(
		runtimebus.WithInboundEvent(
			sessions.WithScope(context.Background(), sessions.RuntimeModeSession.String(), sessions.SessionScopeFlow.String(), "support/inst-1"), eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		),
		runtimeactors.AgentConfig{
			ID:           "agent-1",
			Model:        "regular",
			SessionScope: sessions.SessionScopeFlow.String(),
			FlowPath:     "support/inst-1",
		},
	)

	s, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	publisher.marks = nil
	publisher.markErr = errors.New("mark boom")

	_, err = runtime.ContinueSession(ctx, s, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "mark inbound delivery active for reused cli session: mark boom") {
		t.Fatalf("ContinueSession err = %v, want mark failure", err)
	}
	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "mark_delivery_in_progress_failed" {
		t.Fatalf("runtime log action = %q, want mark_delivery_in_progress_failed", publisher.runtimeLogs[0].Action)
	}
}

func TestEnrichTurnRecordIncludesTriggerToolsAndEmits(t *testing.T) {
	ctx := runtimecorrelation.WithRunID(context.Background(), "run-123")
	ctx = runtimebus.WithInboundEvent(ctx, eventtest.RootIngress(
		"11111111-1111-1111-1111-111111111111",
		events.EventType("scan.requested"),
		"",
		"",
		[]byte(`{"entity_id":"22222222-2222-2222-2222-222222222222"}`),
		0,
		"run-123",
		"",
		events.EventEnvelope{EntityID: "22222222-2222-2222-2222-222222222222"},
		time.Time{},
	))
	recorder := runtimebus.NewEmittedEventsRecorder()
	recorder.Append(eventtest.RootIngress("", events.EventType("discovery/category.assessed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	recorder.Append(eventtest.RootIngress("", events.EventType("discovery/category.assessed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	recorder.Append(eventtest.RootIngress("", events.EventType("discovery/scan_complete"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	recorder.AppendPublish(runtimebus.PublishDiagnostic{
		EventID:   "44444444-4444-4444-4444-444444444444",
		EventType: "discovery/category.assessed",
		EntityID:  "22222222-2222-2222-2222-222222222222",
		RoutedRecipients: []runtimebus.PublishDiagnosticRecipient{
			{
				ID:             "scan-orchestrator",
				Type:           "node",
				Path:           "discovery",
				MatchedPattern: "producer/category.assessed",
				RouteSource:    "pin_auto_wire",
				LocalizedEvent: "category.assessed",
			},
		},
		SubscriptionRecipients: []string{"direct-agent"},
	})
	recorder.AppendRuntimeLog(diaglog.RunEntry{
		Level:     "info",
		Message:   "Emit tool target was resolved",
		Component: "tool-executor",
		Action:    "emit_target_resolved",
		AgentID:   "market-research-agent",
		EntityID:  "22222222-2222-2222-2222-222222222222",
		Detail: map[string]any{
			"tool_name": "emit_category_assessed",
		},
	})
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
		RuntimeMode: sessions.RuntimeModeSession.String(),
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
	if len(rec.AvailableTools) != 1 || rec.AvailableTools[0] != "emit_category_assessed" {
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
	if len(rec.FlightRecorder) != 2 {
		t.Fatalf("flight_recorder = %#v", rec.FlightRecorder)
	}
	if rec.FlightRecorder[0].Kind != "publish" || rec.FlightRecorder[0].EventType != "discovery/category.assessed" {
		t.Fatalf("flight_recorder[0] = %#v", rec.FlightRecorder[0])
	}
	if rec.FlightRecorder[1].Message != "Emit tool target was resolved" {
		t.Fatalf("flight_recorder[1].message = %q", rec.FlightRecorder[1].Message)
	}
}
