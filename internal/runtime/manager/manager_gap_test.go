package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimebus "empireai/internal/runtime/bus"
	"empireai/internal/runtime/sessions"
)

type stubAgent struct {
	id   string
	typ  string
	subs []events.EventType

	mu        sync.Mutex
	events    []events.Event
	onEventFn func(ctx context.Context, evt events.Event) ([]events.Event, error)
	boardFn   func(ctx context.Context, directive string) (string, error)
}

func (a *stubAgent) ID() string   { return a.id }
func (a *stubAgent) Type() string { return a.typ }
func (a *stubAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subs...)
}

func (a *stubAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.mu.Lock()
	a.events = append(a.events, evt)
	fn := a.onEventFn
	a.mu.Unlock()
	if fn != nil {
		return fn(ctx, evt)
	}
	return nil, nil
}

func (a *stubAgent) BoardStep(ctx context.Context, directive string) (string, error) {
	if a.boardFn == nil {
		return "", errors.New("no board fn")
	}
	return a.boardFn(ctx, directive)
}

func (a *stubAgent) eventCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.events)
}

type rotateStubRegistry struct {
	rotations int
}

func (r *rotateStubRegistry) Acquire(_ context.Context, _, runtimeMode, _ string, scopeKey string) (*sessions.Lease, error) {
	return &sessions.Lease{SessionID: "sess-1", AgentID: "a1", RuntimeMode: runtimeMode, LockOwner: "owner", ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) Release(_ context.Context, _ *sessions.Lease) error { return nil }

func (r *rotateStubRegistry) Rotate(_ context.Context, agentID, runtimeMode, lockOwner, _ string, scopeKey string) (*sessions.Lease, error) {
	r.rotations++
	return &sessions.Lease{SessionID: "sess-rotated", AgentID: agentID, RuntimeMode: runtimeMode, LockOwner: lockOwner, ScopeKey: scopeKey}, nil
}

func (r *rotateStubRegistry) IncrementTurn(_ context.Context, _, _, _, _ string) error { return nil }
func (r *rotateStubRegistry) ResetAll(string) error                                    { return nil }

type managerStoreStub struct {
	mu sync.Mutex

	rules  []PersistedRoutingRule
	agents []PersistedAgent

	pendingDirect map[string][]events.Event
	pendingSub    map[string][]events.Event

	receipts             []EventReceipt
	receiptMap           map[string]EventReceipt // event|agent => receipt
	receiptErrOnCanceled bool
	receiptUpsertCalls   int

	setTplCalls []struct{ verticalID, version string }
}

func (s *managerStoreStub) UpsertAgent(_ context.Context, rec PersistedAgent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents = append(s.agents, rec)
	return nil
}

func (s *managerStoreStub) LoadAgents(_ context.Context) ([]PersistedAgent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PersistedAgent(nil), s.agents...), nil
}

func (s *managerStoreStub) MarkAgentTerminated(context.Context, string) error  { return nil }
func (s *managerStoreStub) EnsureVerticalSchema(context.Context, string) error { return nil }
func (s *managerStoreStub) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, errors.New("not implemented")
}
func (s *managerStoreStub) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, errors.New("not implemented")
}
func (s *managerStoreStub) SetVerticalTemplateVersion(_ context.Context, verticalID, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setTplCalls = append(s.setTplCalls, struct{ verticalID, version string }{verticalID, version})
	return nil
}
func (s *managerStoreStub) UpsertRoutingRule(_ context.Context, rule PersistedRoutingRule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = append(s.rules, rule)
	return nil
}
func (s *managerStoreStub) LoadRoutingRules(_ context.Context) ([]PersistedRoutingRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PersistedRoutingRule(nil), s.rules...), nil
}
func (s *managerStoreStub) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}

func (s *managerStoreStub) UpsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receiptUpsertCalls++
	if s.receiptErrOnCanceled && ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if s.receiptMap == nil {
		s.receiptMap = make(map[string]EventReceipt)
	}
	key := eventID + "|" + agentID
	// Simulate store retry bookkeeping: if we see repeated errors, mark dead_letter.
	r := s.receiptMap[key]
	r.EventID = eventID
	r.AgentID = agentID
	r.Error = errText
	if status == "error" {
		r.RetryCount++
		if r.RetryCount >= 3 {
			r.Status = "dead_letter"
		} else {
			r.Status = "error"
		}
	} else {
		r.Status = status
	}
	s.receiptMap[key] = r
	s.receipts = append(s.receipts, r)
	return nil
}

func TestAgentManager_WriteReceiptRetriesOnCanceledContext(t *testing.T) {
	store := &managerStoreStub{receiptErrOnCanceled: true}
	am := NewAgentManager(runtimebus.NewEventBus(runtimebus.InMemoryEventStore{}), nil, store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	am.writeReceipt(ctx, "evt-1", "agent-1", "processed", "")

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.receiptUpsertCalls < 2 {
		t.Fatalf("expected fallback receipt write attempt, got %d calls", store.receiptUpsertCalls)
	}
	if len(store.receipts) != 1 {
		t.Fatalf("expected one persisted receipt, got %d", len(store.receipts))
	}
	got := store.receipts[0]
	if got.EventID != "evt-1" || got.AgentID != "agent-1" || got.Status != "processed" {
		t.Fatalf("unexpected receipt %+v", got)
	}
}

func (s *managerStoreStub) ListPendingEventsForAgent(_ context.Context, agentID string, _ time.Time, _ int) ([]events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Event(nil), s.pendingDirect[agentID]...), nil
}

func (s *managerStoreStub) ListPendingSubscribedEvents(_ context.Context, agentID string, _ []events.EventType, _ time.Time, _ int) ([]events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Event(nil), s.pendingSub[agentID]...), nil
}

func (s *managerStoreStub) GetEventReceipt(_ context.Context, eventID, agentID string) (EventReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receiptMap == nil {
		return EventReceipt{}, false, nil
	}
	r, ok := s.receiptMap[eventID+"|"+agentID]
	return r, ok, nil
}

func TestAgentManager_RunRestartChatIsRunning(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})

	agents := make(map[string]*stubAgent)
	factory := func(cfg models.AgentConfig) (Agent, error) {
		subs := make([]events.EventType, 0, len(cfg.Subscriptions))
		for _, s := range cfg.Subscriptions {
			if s == "" {
				continue
			}
			subs = append(subs, events.EventType(s))
		}
		a := &stubAgent{
			id:   cfg.ID,
			typ:  cfg.Type,
			subs: subs,
		}
		if cfg.Role == "board" {
			a.boardFn = func(_ context.Context, directive string) (string, error) {
				if directive == "" {
					return "", errors.New("empty directive")
				}
				return "ok:" + directive, nil
			}
		}
		agents[cfg.ID] = a
		return a, nil
	}

	am := NewAgentManager(bus, factory)
	if am.IsRunning() {
		t.Fatal("expected not running initially")
	}

	if err := am.SpawnAgent(models.AgentConfig{ID: "a1", Type: "worker", Role: "worker", Mode: "holding", Subscriptions: []string{"ping"}}); err != nil {
		t.Fatalf("spawn a1: %v", err)
	}
	if err := am.SpawnAgent(models.AgentConfig{ID: "b1", Type: "worker", Role: "board", Mode: "holding"}); err != nil {
		t.Fatalf("spawn b1: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()

	if !am.IsRunning() {
		t.Fatal("expected running after Run")
	}

	// Deliver an event and ensure agent processes it.
	if err := bus.Publish(context.Background(), events.Event{ID: "e1", Type: events.EventType("ping"), SourceAgent: "x", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if agents["a1"].eventCount() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if agents["a1"].eventCount() < 1 {
		t.Fatal("expected a1 to process ping")
	}

	// Restart should keep processing.
	if err := am.RestartAgent("a1"); err != nil {
		t.Fatalf("RestartAgent: %v", err)
	}
	if err := bus.Publish(context.Background(), events.Event{ID: "e2", Type: events.EventType("ping"), SourceAgent: "x", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if agents["a1"].eventCount() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if agents["a1"].eventCount() < 2 {
		t.Fatal("expected a1 to process ping after restart")
	}

	// ChatWithAgent works for board interactive agent.
	out, err := am.ChatWithAgent(context.Background(), "b1", "hello")
	if err != nil {
		t.Fatalf("ChatWithAgent: %v", err)
	}
	if out != "ok:hello" {
		t.Fatalf("unexpected chat out: %q", out)
	}
	// Non-interactive agent errors.
	if _, err := am.ChatWithAgent(context.Background(), "a1", "x"); err == nil {
		t.Fatal("expected chat to fail for non-board agent")
	}
}

func TestAgentManager_SpawnAgent_RejectsMissingSystemPrompt(t *testing.T) {
	t.Setenv("EMPIREAI_PROMPTS_DIR", t.TempDir())

	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	factory := func(cfg models.AgentConfig) (Agent, error) {
		if strings.TrimSpace(ExtractSystemPromptFromConfig(cfg.Config)) == "" {
			return nil, errors.New("missing required system_prompt for agent " + strings.TrimSpace(cfg.ID))
		}
		return &stubAgent{id: cfg.ID, typ: cfg.Type}, nil
	}
	am := NewAgentManager(bus, factory)

	err := am.SpawnAgent(models.AgentConfig{
		ID:   "unknown-agent",
		Role: "unknown-agent",
		Mode: "factory",
	})
	if err == nil {
		t.Fatal("expected missing system_prompt failure when no contract prompt exists")
	}
}

func TestAgentManager_Recover_RejectsMissingSystemPrompt(t *testing.T) {
	t.Setenv("EMPIREAI_PROMPTS_DIR", t.TempDir())

	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		agents: []PersistedAgent{
			{
				Config: models.AgentConfig{
					ID:   "unknown-agent",
					Role: "unknown-agent",
					Mode: "factory",
				},
			},
		},
	}
	factory := func(cfg models.AgentConfig) (Agent, error) {
		if strings.TrimSpace(ExtractSystemPromptFromConfig(cfg.Config)) == "" {
			return nil, errors.New("missing required system_prompt for agent " + strings.TrimSpace(cfg.ID))
		}
		return &stubAgent{id: cfg.ID, typ: cfg.Type}, nil
	}
	am := NewAgentManager(bus, factory, store)

	err := am.Recover(context.Background())
	if err == nil {
		t.Fatal("expected recover failure for missing system_prompt when no contract prompt exists")
	}
}

func TestAgentManager_PendingEventsForAgent_DedupAndOperatingSkipsSubscribed(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		pendingDirect: make(map[string][]events.Event),
		pendingSub:    make(map[string][]events.Event),
	}
	am := NewAgentManager(bus, nil, store)

	t1 := time.Now().Add(-2 * time.Minute)
	t2 := time.Now().Add(-1 * time.Minute)
	e1 := events.Event{ID: "1", Type: "x", CreatedAt: t2}
	e2 := events.Event{ID: "2", Type: "x", CreatedAt: t1}
	e3 := events.Event{ID: "3", Type: "x", CreatedAt: t2.Add(1 * time.Second)}
	store.pendingDirect["a1"] = []events.Event{e1, e2}
	store.pendingSub["a1"] = []events.Event{e1, e3} // e1 duplicates direct

	agent := &stubAgent{id: "a1", subs: []events.EventType{"x"}}

	// Operating: only direct.
	pending, err := am.pendingEventsForAgent(context.Background(), "a1", models.AgentConfig{ID: "a1", Mode: "operating"}, agent, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("pending operating: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
	if pending[0].ID != "2" { // older created_at first
		t.Fatalf("expected sorted by created_at: %+v", pending)
	}

	// Holding: direct + subscribed dedup.
	pending, err = am.pendingEventsForAgent(context.Background(), "a1", models.AgentConfig{ID: "a1", Mode: "holding"}, agent, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("pending holding: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending, got %d", len(pending))
	}
	if pending[0].ID != "2" || pending[1].ID != "1" {
		t.Fatalf("unexpected order: %+v", pending)
	}
}

func TestAgentManager_ReplayBacklog_ReceiptsAndDeadLetterEscalationAndTransient(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		pendingDirect: make(map[string][]events.Event),
		pendingSub:    make(map[string][]events.Event),
		receiptMap:    make(map[string]EventReceipt),
	}

	// Subscribe the manager recipient of dead-letter escalation.
	mgrCh := bus.Subscribe("mgr")

	agents := make(map[string]*stubAgent)
	factory := func(cfg models.AgentConfig) (Agent, error) {
		subs := make([]events.EventType, 0, len(cfg.Subscriptions))
		for _, s := range cfg.Subscriptions {
			if s == "" {
				continue
			}
			subs = append(subs, events.EventType(s))
		}
		a := &stubAgent{
			id:   cfg.ID,
			typ:  cfg.Type,
			subs: subs,
		}
		a.onEventFn = func(_ context.Context, evt events.Event) ([]events.Event, error) {
			switch evt.ID {
			case "t0":
				return nil, fmt.Errorf("budget emergency: refusing llm execution")
			case "e1":
				return nil, errors.New("boom")
			case "e2":
				// emit a new event to exercise publish loop
				return []events.Event{{
					ID:          "emitted",
					Type:        "side.effect",
					SourceAgent: cfg.ID,
					VerticalID:  cfg.VerticalID,
					Payload:     mustJSON(map[string]any{"ok": true}),
					CreatedAt:   time.Now(),
				}}, nil
			default:
				return nil, nil
			}
		}
		agents[cfg.ID] = a
		return a, nil
	}

	am := NewAgentManager(bus, factory, store)
	// Ensure manager-id resolution prefers ParentAgent.
	if err := am.SpawnAgent(models.AgentConfig{ID: "a1", Type: "worker", Role: "worker", Mode: "holding", VerticalID: "v1", ParentAgent: "mgr"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Seed pending events (transient, error, success). replay uses direct list for holding too.
	store.pendingDirect["a1"] = []events.Event{
		{ID: "t0", Type: "x", CreatedAt: time.Now().Add(-3 * time.Minute)},
		{ID: "e1", Type: "x", CreatedAt: time.Now().Add(-2 * time.Minute)},
		{ID: "e2", Type: "x", CreatedAt: time.Now().Add(-1 * time.Minute)},
	}

	// Force the receipt to be dead-lettered by simulating multiple errors.
	_ = store.UpsertEventReceipt(context.Background(), "e1", "a1", "error", "boom")
	_ = store.UpsertEventReceipt(context.Background(), "e1", "a1", "error", "boom")

	if err := am.ReplayAgentBacklog(context.Background(), "a1"); err != nil {
		t.Fatalf("ReplayAgentBacklog: %v", err)
	}

	// Transient error should not write receipts from processEvent.
	// Error and processed should both be present.
	store.mu.Lock()
	defer store.mu.Unlock()
	gotError := false
	gotProcessed := false
	for _, r := range store.receipts {
		if r.EventID == "t0" {
			t.Fatalf("did not expect receipt for transient error event")
		}
		if r.EventID == "e1" && r.Status == "dead_letter" {
			gotError = true
		}
		if r.EventID == "e2" && r.Status == "processed" {
			gotProcessed = true
		}
	}
	if !gotError || !gotProcessed {
		t.Fatalf("expected receipts for e1(dead_letter) and e2(processed), got %+v", store.receipts)
	}

	// Dead-letter escalation should have been published to mgr.
	select {
	case evt := <-mgrCh:
		if string(evt.Type) != "ops.dead_letter_escalation" {
			t.Fatalf("unexpected escalation event: %s", evt.Type)
		}
		var payload map[string]any
		_ = json.Unmarshal(evt.Payload, &payload)
		if payload["agent_id"] != "a1" || payload["manager_id"] != "mgr" {
			t.Fatalf("unexpected payload: %+v", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected dead-letter escalation event")
	}
}

func TestAgentManager_ProcessEvent_TripsAuthCircuitBreaker(t *testing.T) {
	runtimebus.ResumeRuntimeIngress()
	defer runtimebus.ResumeRuntimeIngress()
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		pendingDirect: map[string][]events.Event{},
		pendingSub:    map[string][]events.Event{},
	}

	agents := make(map[string]*stubAgent)
	factory := func(cfg models.AgentConfig) (Agent, error) {
		a := &stubAgent{
			id:  cfg.ID,
			typ: cfg.Type,
			onEventFn: func(_ context.Context, _ events.Event) ([]events.Event, error) {
				return nil, errors.New("claude auth required")
			},
		}
		agents[cfg.ID] = a
		return a, nil
	}
	am := NewAgentManager(bus, factory, store)
	if err := am.SpawnAgent(models.AgentConfig{ID: "a1", Type: "worker", Role: "worker", Mode: "holding"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	authCh := bus.Subscribe("observer-auth", events.EventType("runtime.auth_required"))

	am.Run(context.Background())
	if !am.IsRunning() {
		t.Fatal("expected manager running")
	}

	err := am.processEvent(context.Background(), agents["a1"], events.Event{
		ID:        "e-auth",
		Type:      "x",
		CreatedAt: time.Now(),
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "claude auth required") {
		t.Fatalf("expected claude auth required, got %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for am.IsRunning() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if am.IsRunning() {
		t.Fatal("expected runtime paused after auth breaker")
	}
	if !runtimebus.RuntimeIngressPaused() {
		t.Fatal("expected runtime ingress paused after auth breaker")
	}

	select {
	case evt := <-authCh:
		if string(evt.Type) != "runtime.auth_required" {
			t.Fatalf("unexpected auth breaker event: %s", evt.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected runtime.auth_required event")
	}
}

func TestAgentManager_ProcessEvent_TripsCreditExhaustionPause(t *testing.T) {
	runtimebus.ResumeRuntimeIngress()
	defer runtimebus.ResumeRuntimeIngress()
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		pendingDirect: map[string][]events.Event{},
		pendingSub:    map[string][]events.Event{},
	}

	agents := make(map[string]*stubAgent)
	factory := func(cfg models.AgentConfig) (Agent, error) {
		a := &stubAgent{
			id:  cfg.ID,
			typ: cfg.Type,
			onEventFn: func(_ context.Context, _ events.Event) ([]events.Event, error) {
				return nil, errors.New("claude cli run failed: exit status 1, stderr=You've hit your limit · resets 4am (UTC)")
			},
		}
		agents[cfg.ID] = a
		return a, nil
	}
	am := NewAgentManager(bus, factory, store)
	if err := am.SpawnAgent(models.AgentConfig{ID: "a1", Type: "worker", Role: "worker", Mode: "holding"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	pauseCh := bus.Subscribe("observer-pause", events.EventType("runtime.paused"))

	am.Run(context.Background())
	if !am.IsRunning() {
		t.Fatal("expected manager running")
	}
	err := am.processEvent(context.Background(), agents["a1"], events.Event{
		ID:        "e-credit",
		Type:      "x",
		CreatedAt: time.Now(),
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "hit your limit") {
		t.Fatalf("expected out-of-credit error, got %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for am.IsRunning() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if am.IsRunning() {
		t.Fatal("expected runtime paused after credit breaker")
	}
	if !runtimebus.RuntimeIngressPaused() {
		t.Fatal("expected runtime ingress paused after credit breaker")
	}

	select {
	case evt := <-pauseCh:
		if string(evt.Type) != "runtime.paused" {
			t.Fatalf("unexpected pause event: %s", evt.Type)
		}
		var payload map[string]any
		_ = json.Unmarshal(evt.Payload, &payload)
		if strings.TrimSpace(asString(payload["reason"])) != "claude_credit_exhausted" {
			t.Fatalf("unexpected pause payload: %+v", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected runtime.paused event")
	}
}

func TestAgentManager_ProcessEvent_SkipsAlreadyProcessedReceipt(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		receiptMap: map[string]EventReceipt{
			"e1|a1": {EventID: "e1", AgentID: "a1", Status: "processed"},
		},
	}
	am := NewAgentManager(bus, nil, store)
	agent := &stubAgent{id: "a1", typ: "stub"}

	if err := am.processEvent(context.Background(), agent, events.Event{ID: "e1", Type: "x"}); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if got := agent.eventCount(); got != 0 {
		t.Fatalf("expected skipped event processing, got %d calls", got)
	}
	if len(store.receipts) != 0 {
		t.Fatalf("expected no new receipts for skipped event, got %+v", store.receipts)
	}
}

func TestAgentManager_ProcessEvent_DedupsInFlight(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{receiptMap: make(map[string]EventReceipt)}
	am := NewAgentManager(bus, nil, store)

	var calls int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	agent := &stubAgent{
		id:  "a1",
		typ: "stub",
		onEventFn: func(_ context.Context, _ events.Event) ([]events.Event, error) {
			atomic.AddInt32(&calls, 1)
			once.Do(func() { close(entered) })
			<-release
			return nil, nil
		},
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- am.processEvent(context.Background(), agent, events.Event{ID: "dup-1", Type: "x"})
	}()
	<-entered
	go func() {
		errCh <- am.processEvent(context.Background(), agent, events.Event{ID: "dup-1", Type: "x"})
	}()
	time.Sleep(25 * time.Millisecond)
	close(release)

	err1 := <-errCh
	err2 := <-errCh
	if err1 != nil {
		t.Fatalf("first processEvent err: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second processEvent err: %v", err2)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly one OnEvent call, got %d", got)
	}
}

func TestAgentManager_DeadLetterSelfEscalationSuppressed(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		pendingDirect: map[string][]events.Event{},
		pendingSub:    map[string][]events.Event{},
	}
	am := NewAgentManager(bus, nil, store)

	_ = store.UpsertEventReceipt(context.Background(), "e-self", "empire-coordinator", "error", "boom")
	_ = store.UpsertEventReceipt(context.Background(), "e-self", "empire-coordinator", "error", "boom")
	_ = store.UpsertEventReceipt(context.Background(), "e-self", "empire-coordinator", "error", "boom")

	ch := bus.Subscribe("empire-coordinator")
	am.maybeEscalateDeadLetter(context.Background(), "e-self", "empire-coordinator")

	select {
	case evt := <-ch:
		t.Fatalf("expected no self-escalation event, got %s", evt.Type)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAgentManager_ResetRuntimeState_ClearsAgentsAndStopsWorkspaces(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	ws := &workspaceLifecycleStub{}
	am.SetWorkspaceLifecycle(ws)

	_ = bus.SetRoutingTable("v1", &runtimebus.RoutingTable{VerticalID: "v1", Routes: []runtimebus.Route{{EventPattern: "x", SubscriberID: "a"}}})
	_ = am.SpawnAgent(models.AgentConfig{ID: "a1", Type: "worker", Role: "worker", Mode: "operating", VerticalID: "v1"})
	am.setRouteMeta(routeRuleKey("v1", "x", "a1"), PersistedRoutingRule{VerticalID: "v1", EventPattern: "x", SubscriberID: "a1"})

	if am.Count() != 1 {
		t.Fatalf("expected 1 agent, got %d", am.Count())
	}
	legacyCh := bus.Subscribe("a1", events.EventType("legacy.event"))
	if err := bus.Publish(context.Background(), events.Event{
		ID:          "legacy-1",
		Type:        events.EventType("legacy.event"),
		SourceAgent: "test",
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("queue legacy event: %v", err)
	}
	select {
	case <-legacyCh:
		// Put one back in the channel to simulate buffered pre-reset residue.
		// We only need to assert that after reset a fresh subscription has no residue.
		if err := bus.Publish(context.Background(), events.Event{
			ID:          "legacy-2",
			Type:        events.EventType("legacy.event"),
			SourceAgent: "test",
			CreatedAt:   time.Now(),
		}); err != nil {
			t.Fatalf("queue second legacy event: %v", err)
		}
	default:
	}

	if err := am.ResetRuntimeState(); err != nil {
		t.Fatalf("reset runtime state: %v", err)
	}

	if am.Count() != 0 {
		t.Fatalf("expected 0 agents after reset, got %d", am.Count())
	}
	rt := bus.GetRoutingTable("v1")
	if rt == nil || len(rt.Routes) != 0 {
		t.Fatalf("expected cleared routing table, got %+v", rt)
	}
	if ws.stopCount != 1 {
		t.Fatalf("expected workspace stop once, got %d", ws.stopCount)
	}
	if ws.killCount != 1 {
		t.Fatalf("expected orphan killer once, got %d", ws.killCount)
	}
	postResetCh := bus.Subscribe("a1", events.EventType("legacy.event"))
	select {
	case evt := <-postResetCh:
		t.Fatalf("expected no stale buffered events after reset, got %s", evt.ID)
	case <-time.After(120 * time.Millisecond):
	}
}

func TestAgentManager_SessionRegistryRotateOnReconfigureAndHelpers(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	reg := &rotateStubRegistry{}
	am.SetSessionRegistry(reg, "api")
	if err := am.SpawnAgent(models.AgentConfig{ID: "a1", Type: "worker", Role: "worker"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if err := am.ReconfigureAgent("a1", models.AgentConfig{Role: "worker2"}); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if reg.rotations == 0 {
		t.Fatal("expected session rotation on reconfigure")
	}

	// Transient errors.
	if !isTransientAgentError(errors.New("Session currently leased by another worker")) {
		t.Fatal("expected leased transient")
	}
	if !isTransientAgentError(errors.New("budget emergency: x")) {
		t.Fatal("expected budget transient")
	}
	if isTransientAgentError(errors.New("boom")) {
		t.Fatal("expected non-transient")
	}

	// resolveManagerAgentID.
	am.mu.Lock()
	am.agentCfg["child"] = models.AgentConfig{ID: "child", ParentAgent: "p1"}
	am.agentCfg["op"] = models.AgentConfig{ID: "op", Mode: "operating", VerticalID: "v1", Role: "vp-product"}
	am.agentCfg["misc"] = models.AgentConfig{ID: "misc"}
	am.mu.Unlock()
	if am.resolveManagerAgentID("child") != "p1" {
		t.Fatal("expected parent resolution")
	}
	if am.resolveManagerAgentID("op") != "opco-ceo-v1" {
		t.Fatal("expected opco-ceo resolution for operating non-ceo")
	}
	if am.resolveManagerAgentID("misc") != "empire-coordinator" {
		t.Fatal("expected empire-coordinator fallback")
	}

	// hydrateRoutingTables.
	rules := []PersistedRoutingRule{
		{VerticalID: "v1", EventPattern: "a.*", SubscriberID: "x", Status: "active"},
		{VerticalID: "v1", EventPattern: "b.*", SubscriberID: "y", Status: "active"},
		{VerticalID: "v2", EventPattern: "c.*", SubscriberID: "z", Status: "active"},
	}
	if err := am.hydrateRoutingTables(rules); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if rt := bus.GetRoutingTable("v1"); rt == nil || len(rt.Routes) != 2 {
		t.Fatalf("expected v1 table 2 routes, got %+v", rt)
	}

	// Cover genericAgent.Type accessor (was 0%).
	g := newGenericAgent(models.AgentConfig{ID: "g1", Type: "generic"})
	_ = g.Type()
}

func TestAgentManager_Recover_HydratesAndReplays(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	store := &managerStoreStub{
		pendingDirect: make(map[string][]events.Event),
		pendingSub:    make(map[string][]events.Event),
		receiptMap:    make(map[string]EventReceipt),
	}

	// Seed routing rules (Recover -> hydrateRoutingTables).
	store.rules = []PersistedRoutingRule{
		{VerticalID: "v1", EventPattern: "opco.*", SubscriberID: "opco-ceo-v1", Status: "active"},
	}

	// Seed agents to hydrate (Recover -> spawnAgentInternal persist=false).
	start1 := time.Now().Add(-2 * time.Hour)
	start2 := time.Now().Add(-1 * time.Hour)
	store.agents = []PersistedAgent{
		{
			Config: models.AgentConfig{
				ID:            "a1",
				Type:          "generic",
				Role:          "holding-test",
				Mode:          "holding",
				Subscriptions: []string{"sub1"},
				Config:        mustJSON(map[string]any{"subscriptions": []string{"sub2"}}),
			},
			StartedAt: start1,
		},
		{
			Config: models.AgentConfig{
				ID:            "a2",
				Type:          "generic",
				Role:          "operating-test",
				Mode:          "operating",
				VerticalID:    "v1",
				Subscriptions: []string{"opco.*"},
			},
			StartedAt: start2,
		},
	}

	// Backlog: direct + subscribed for holding agent.
	store.pendingDirect["a1"] = []events.Event{
		{ID: "d1", Type: events.EventType("sub1"), CreatedAt: time.Now().Add(-5 * time.Minute)},
	}
	store.pendingSub["a1"] = []events.Event{
		{ID: "s1", Type: events.EventType("sub2"), CreatedAt: time.Now().Add(-4 * time.Minute)},
	}
	// Backlog for operating agent is only direct.
	store.pendingDirect["a2"] = []events.Event{
		{ID: "o1", Type: events.EventType("opco.test"), VerticalID: "v1", CreatedAt: time.Now().Add(-3 * time.Minute)},
	}

	am := NewAgentManager(bus, nil, store)
	if err := am.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if am.Count() != 2 {
		t.Fatalf("expected 2 hydrated agents, got %d", am.Count())
	}
	if rt := bus.GetRoutingTable("v1"); rt == nil || len(rt.Routes) != 1 {
		t.Fatalf("expected hydrated routing table, got %+v", rt)
	}

	// Receipts should exist for replayed backlog events.
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.receiptMap["d1|a1"]; !ok {
		t.Fatal("expected receipt for d1|a1")
	}
	if _, ok := store.receiptMap["s1|a1"]; !ok {
		t.Fatal("expected receipt for s1|a1")
	}
	if _, ok := store.receiptMap["o1|a2"]; !ok {
		t.Fatal("expected receipt for o1|a2")
	}
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
