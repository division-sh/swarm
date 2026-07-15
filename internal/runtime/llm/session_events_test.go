package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
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
	upsertCount    int
	load           ConversationRecord
	loadOK         bool
	watchdogMu     sync.Mutex
	watchdogUpdate ConversationWatchdogUpdate
}

type atomicLiveSessionTestRegistry struct {
	sessions.Registry
	hydrated ConversationRecord
}

func (r atomicLiveSessionTestRegistry) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*sessions.Lease, ConversationRecord, error) {
	lease, err := r.Registry.Acquire(ctx, identity, lockOwner)
	if err != nil {
		return nil, ConversationRecord{}, err
	}
	record := r.hydrated
	record.SessionID = lease.SessionID
	record.AgentID = identity.AgentID
	record.Identity = identity
	record.Memory = testMemory()
	record.Status = "active"
	return lease, record, nil
}

type exactAcquireFailureRegistry struct {
	*sessions.InMemoryRegistry
	err error
}

func (r exactAcquireFailureRegistry) AcquireLiveSession(context.Context, agentmemory.Identity, string) (*sessions.Lease, ConversationRecord, error) {
	return nil, ConversationRecord{}, r.err
}

type sessionStarter interface {
	StartSession(context.Context, string, string, []ToolDefinition) (*Session, error)
}

func TestProviderRuntimesFailBeforeAgentStartedWhenExactAcquireHydrateFails(t *testing.T) {
	wantErr := errors.New("exact acquire-hydrate failed")
	constructors := map[string]func(sessions.Registry, EventPublisher) sessionStarter{
		"anthropic_api": func(registry sessions.Registry, publisher EventPublisher) sessionStarter {
			return NewAnthropicAPIRuntime(&config.Config{}, registry, "worker-1", nil, publisher)
		},
		"claude_cli": func(registry sessions.Registry, publisher EventPublisher) sessionStarter {
			return NewClaudeCLIRuntime(&config.Config{}, registry, "worker-1", nil, nil, publisher)
		},
		"openai_compatible": func(registry sessions.Registry, publisher EventPublisher) sessionStarter {
			return NewOpenAICompatibleRuntime(&config.Config{}, registry, "worker-1", nil, publisher)
		},
		"openai_responses": func(registry sessions.Registry, publisher EventPublisher) sessionStarter {
			return NewOpenAIResponsesRuntime(&config.Config{}, registry, "worker-1", nil, publisher)
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			publisher := &eventPublisherStub{}
			registry := exactAcquireFailureRegistry{InMemoryRegistry: sessions.NewInMemoryRegistry(0), err: wantErr}
			runtime := construct(registry, publisher)
			ctx := withTestMemory(unmanagedLLMTestContext(), "agent-1", "support/instance-1")
			ctx = runtimeactors.WithActor(ctx, runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1", Model: "regular", Memory: testMemory(), FlowPath: "support/instance-1",
			})
			if _, err := runtime.StartSession(ctx, "agent-1", "system", nil); !errors.Is(err, wantErr) {
				t.Fatalf("StartSession error = %v, want exact acquire-hydrate failure", err)
			}
			if len(publisher.events) != 0 || len(publisher.marks) != 0 {
				t.Fatalf("failed acquire published startup surface: events=%d marks=%d", len(publisher.events), len(publisher.marks))
			}
		})
	}
}

func (s *captureConversationStore) UpsertConversation(_ context.Context, rec ConversationRecord) error {
	s.record = rec
	s.upsertCount++
	return nil
}

func (s *captureConversationStore) LoadActiveConversation(context.Context, string, string, string, string) (ConversationRecord, bool, error) {
	return s.load, s.loadOK, nil
}

func (s *captureConversationStore) UpdateLiveSessionWatchdog(_ context.Context, update ConversationWatchdogUpdate) error {
	s.watchdogMu.Lock()
	defer s.watchdogMu.Unlock()
	s.watchdogUpdate = update
	return nil
}

func (s *captureConversationStore) capturedWatchdogUpdate() ConversationWatchdogUpdate {
	s.watchdogMu.Lock()
	defer s.watchdogMu.Unlock()
	return s.watchdogUpdate
}

func TestAnthropicAPIRuntime_StartSessionPublishesAgentStarted(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, publisher)
	ctx := runtimeactors.WithActor(withTestStatelessMemory(unmanagedLLMTestContext()), runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Model:         "regular",
		EntityID:      "entity-1",
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
	if got := payload["memory_enabled"]; got != false {
		t.Fatalf("memory_enabled = %#v, want false", got)
	}
	if got := payload["memory_source"]; got != string(agentmemory.SourceAuthored) {
		t.Fatalf("memory_source = %#v, want authored", got)
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
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, publisher)
	ctx := runtimeactors.WithActor(withTestStatelessMemory(unmanagedLLMTestContext()), runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-2",
		Model:         "cheap",
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

func TestClaudeCLIRuntime_StartSessionPreservesBusinessPrompt(t *testing.T) {
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil)
	ctx := withTestStatelessMemory(unmanagedLLMTestContext())

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
	if s.SystemPrompt != "base prompt" {
		t.Fatalf("system prompt = %q, want business intent unchanged", s.SystemPrompt)
	}
}

func TestAnthropicAPIRuntime_StartSessionPreservesBusinessPrompt(t *testing.T) {
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil)
	ctx := runtimeactors.WithActor(
		withTestStatelessMemory(unmanagedLLMTestContext()),
		runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-3",
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
	if s.SystemPrompt != "base prompt" {
		t.Fatalf("system prompt = %q, want business intent unchanged", s.SystemPrompt)
	}
}

func TestPublishAgentStarted_LogsActiveTransitionOnlyAfterRealDeliveryMark(t *testing.T) {
	publisher := &eventPublisherStub{markChanged: true}
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		EntityID:      "entity-1",
	})

	publishAgentStarted(ctx, publisher, &Session{
		ID:      "session-1",
		AgentID: "agent-1",
		Memory:  agentmemory.Authored(false),
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

func TestEnrichTurnRecord_CarriesInboundFlowInstance(t *testing.T) {
	ctx := runtimebus.WithInboundEvent(unmanagedLLMTestContext(), eventtest.RootIngress(
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
		AgentID:   "analysis-agent",
		SessionID: "session-1",
	}, nil)

	if rec.FlowInstance != "review/inst-1" {
		t.Fatalf("flow_instance = %q, want review/inst-1", rec.FlowInstance)
	}
	if rec.EntityID != "" {
		t.Fatalf("entity_id = %q, want empty for flow-only inbound event", rec.EntityID)
	}
}

func TestEnrichTurnRecord_PrefersObservedToolCallsForPersistenceWhenExecutionCallsAreSuppressed(t *testing.T) {
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "analysis-agent",
	})
	rec := enrichTurnRecord(ctx, &Session{
		ID: "session-1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
		},
	}, AgentTurnRecord{
		AgentID:   "analysis-agent",
		SessionID: "session-1",
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

func TestEnrichTurnRecordIncludesSessionIdentity(t *testing.T) {
	record := enrichTurnRecord(unmanagedLLMTestContext(), &Session{
		ID: "session-1",
	}, AgentTurnRecord{AgentID: "agent-1"}, nil)

	if record.SessionID != "session-1" {
		t.Fatalf("session_id = %q, want session-1", record.SessionID)
	}
}

func TestEnrichTurnRecordDefersTurnBlockCanonicalizationToStore(t *testing.T) {
	record := enrichTurnRecord(unmanagedLLMTestContext(), &Session{
		ID: "session-1",
	}, AgentTurnRecord{
		AgentID:        "agent-1",
		ResponseRaw:    []byte(`{"result":"done"}`),
		TriggerEventID: "evt-1",
	}, nil)

	if len(record.TurnBlocks) != 0 {
		t.Fatalf("persisted turn blocks = %#v, want store-side canonicalization", record.TurnBlocks)
	}
}

func TestPublishAgentStarted_LogsRuntimeFailures(t *testing.T) {
	publisher := &eventPublisherStub{
		markErr:    errors.New("mark boom"),
		publishErr: errors.New("publish boom"),
	}
	ctx := runtimeactors.WithActor(unmanagedLLMTestContext(), runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		EntityID:      "entity-1",
	})

	publishAgentStarted(ctx, publisher, &Session{
		ID:                "session-1",
		AgentID:           "agent-1",
		Memory:            agentmemory.Authored(false),
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

func TestAnthropicAPIRuntime_PersistConversationFailureLogsRuntime(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", &failingConversationStore{err: errors.New("conversation boom")}, publisher)

	runtime.persistConversation(unmanagedLLMTestContext(), &Session{
		ID:             "session-3",
		AgentID:        "agent-3",
		Memory:         testMemory(),
		MemoryIdentity: testMemoryIdentity("agent-3", "review/inst-3"),
	})

	if len(publisher.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(publisher.runtimeLogs))
	}
	if publisher.runtimeLogs[0].Action != "persist_api_conversation_failed" {
		t.Fatalf("action = %q", publisher.runtimeLogs[0].Action)
	}
}

func TestAnthropicAPIRuntime_PersistConversationIncludesExactMemoryIdentity(t *testing.T) {
	store := &captureConversationStore{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", store, nil)

	runtime.persistConversation(unmanagedLLMTestContext(), &Session{
		ID:             "session-3",
		AgentID:        "agent-3",
		Memory:         testMemory(),
		MemoryIdentity: testMemoryIdentity("agent-3", "review/inst-1"),
	})

	if store.record.Identity != testMemoryIdentity("agent-3", "review/inst-1") || store.record.Memory != testMemory() {
		t.Fatalf("stored memory = identity=%+v plan=%+v", store.record.Identity, store.record.Memory)
	}
}

func TestClaudeCLIRuntime_PersistConversationIncludesExactMemoryIdentity(t *testing.T) {
	store := &captureConversationStore{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, atomicLiveSessionTestRegistry{Registry: sessions.NewInMemoryRegistry(0), hydrated: store.load}, "worker-1", nil, store, nil)

	runtime.persistConversation(unmanagedLLMTestContext(), &Session{
		ID:             "session-4",
		AgentID:        "agent-4",
		Memory:         testMemory(),
		MemoryIdentity: testMemoryIdentity("agent-4", "support/inst-4"),
	})

	if store.record.Identity != testMemoryIdentity("agent-4", "support/inst-4") || store.record.Memory != testMemory() {
		t.Fatalf("stored memory = identity=%+v plan=%+v", store.record.Identity, store.record.Memory)
	}
}

func TestClaudeCLIRuntime_StatelessConversationIsNotPersisted(t *testing.T) {
	store := &captureConversationStore{}
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, store, publisher)

	runtime.persistConversation(unmanagedLLMTestContext(), &Session{
		ID:        "session-stateless",
		AgentID:   "agent-stateless",
		Memory:    agentmemory.Authored(false),
		Messages:  []Message{{Role: "assistant", Content: "done"}},
		TurnCount: 1,
	})

	if store.upsertCount != 0 {
		t.Fatalf("conversation upserts = %d, want zero for stateless execution", store.upsertCount)
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
	runtime := NewClaudeCLIRuntime(&config.Config{}, atomicLiveSessionTestRegistry{Registry: sessions.NewInMemoryRegistry(0), hydrated: store.load}, "worker-1", nil, store, nil)
	ctx := withTestMemory(unmanagedLLMTestContext(), "agent-4", "review/inst-4")

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
	effects := effecttest.New()
	effects.Token.AgentID = "agent-1"
	runtime := NewAnthropicAPIRuntime(&config.Config{
		LLM: config.LLMConfig{
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-test",
			},
		},
	}, sessions.NewInMemoryRegistry(0), "worker-1", nil, publisher)

	runtime.apiURL = server.URL
	runtime.apiKey = "test-key"
	runtime.httpClient = server.Client()
	runtime.completionController = runtimeeffects.NewCompletionController(effects, effects)

	ctx := runtimeactors.WithActor(
		runtimebus.WithInboundEvent(
			withTestMemory(effects.Context("anthropic-reused-session"), "agent-1", "support/inst-1"), eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		),
		runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Model:         "regular",
			Memory:        testMemory(),
			FlowPath:      "support/inst-1",
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
	ctx = managedProviderTestContext(t, ctx, runtime, s, nil)

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
	}, sessions.NewInMemoryRegistry(0), "worker-1", nil, publisher)

	ctx := runtimeactors.WithActor(
		runtimebus.WithInboundEvent(
			withTestMemory(unmanagedLLMTestContext(), "agent-1", "support/inst-1"), eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		),
		runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Model:         "regular",
			Memory:        testMemory(),
			FlowPath:      "support/inst-1",
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
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, publisher)
	ctx := runtimeactors.WithActor(
		runtimebus.WithInboundEvent(
			withTestMemory(unmanagedLLMTestContext(), "agent-1", "support/inst-1"), eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		),
		runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Model:         "regular",
			Memory:        testMemory(),
			FlowPath:      "support/inst-1",
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
	ctx := runtimecorrelation.WithRunID(unmanagedLLMTestContext(), "run-123")
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
		ID: "33333333-3333-3333-3333-333333333333",
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
		AgentID:   "market-research-agent",
		SessionID: session.ID,
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
	if len(rec.ToolCalls) != 1 || rec.ToolCalls[0].Name != "emit_category_assessed" {
		t.Fatalf("tool_calls = %#v", rec.ToolCalls)
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
