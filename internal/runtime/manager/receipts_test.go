package manager

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type recordingReceiptBus struct {
	published   []events.Event
	runtimeLogs []runtimepipeline.RuntimeLogEntry
}

func (b *recordingReceiptBus) Publish(ctx context.Context, evt events.Event) error {
	_, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	b.published = append(b.published, evt)
	return nil
}

func (*recordingReceiptBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*recordingReceiptBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*recordingReceiptBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*recordingReceiptBus) Unsubscribe(string)           {}
func (*recordingReceiptBus) Store() runtimebus.EventStore { return runtimebus.InMemoryEventStore{} }
func (*recordingReceiptBus) ResetInMemoryState() error    { return nil }
func (b *recordingReceiptBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}

type recordingCompletionReceiptBus struct {
	recordingReceiptBus
	normalCompletionEvents []string
}

func (b *recordingCompletionReceiptBus) ConvergeNormalRunCompletionForEvent(_ context.Context, eventID string) error {
	b.normalCompletionEvents = append(b.normalCompletionEvents, strings.TrimSpace(eventID))
	return nil
}

type receiptReaderStub struct {
	receipt     EventReceipt
	found       bool
	upsertErrs  []error
	upsertCalls int
}

func (*receiptReaderStub) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (*receiptReaderStub) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (*receiptReaderStub) MarkAgentTerminated(context.Context, string) error { return nil }
func (*receiptReaderStub) EnsureEntitySchema(context.Context, string) error  { return nil }
func (s *receiptReaderStub) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, string) error {
	s.upsertCalls++
	if len(s.upsertErrs) == 0 {
		return nil
	}
	err := s.upsertErrs[0]
	s.upsertErrs = s.upsertErrs[1:]
	return err
}
func (*receiptReaderStub) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*receiptReaderStub) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *receiptReaderStub) GetEventReceipt(context.Context, string, string) (EventReceipt, bool, error) {
	return s.receipt, s.found, nil
}

func TestWriteReceiptConvergesNormalRunCompletionAfterReceiptPersists(t *testing.T) {
	bus := &recordingCompletionReceiptBus{}
	store := &receiptReaderStub{
		receipt: EventReceipt{
			EventID: "event-1",
			AgentID: "agent-1",
			Status:  ReceiptStatusProcessed,
		},
		found: true,
	}
	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{}, store)

	am.writeReceipt(context.Background(), "event-1", "agent-1", ReceiptStatusProcessed, "")

	if store.upsertCalls != 1 {
		t.Fatalf("receipt upsert calls = %d, want 1", store.upsertCalls)
	}
	if len(bus.normalCompletionEvents) != 1 || bus.normalCompletionEvents[0] != "event-1" {
		t.Fatalf("normal completion events = %#v, want event-1", bus.normalCompletionEvents)
	}
}

type traceRecordingAgent struct{ parent string }

func (a *traceRecordingAgent) ID() string                        { return "trace-agent" }
func (a *traceRecordingAgent) Type() string                      { return "llm" }
func (a *traceRecordingAgent) Subscriptions() []events.EventType { return nil }
func (a *traceRecordingAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		a.parent = inbound.ID
	}
	return nil, nil
}

type outputRecordingAgent struct {
	calls int
}

func (a *outputRecordingAgent) ID() string                        { return "agent-a" }
func (a *outputRecordingAgent) Type() string                      { return "llm" }
func (a *outputRecordingAgent) Subscriptions() []events.EventType { return nil }
func (a *outputRecordingAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	a.calls++
	return []events.Event{{ID: "out-1", Type: "task.done", SourceAgent: "agent-a", Payload: []byte(`{}`)}}, nil
}

type panicStubAgent struct{ id string }

func (a panicStubAgent) ID() string                        { return a.id }
func (a panicStubAgent) Type() string                      { return "llm" }
func (a panicStubAgent) Subscriptions() []events.EventType { return nil }
func (a panicStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestMaybeTripAuthCircuitBreaker_PublishesFlowScopedAuthRequired(t *testing.T) {
	bus := &recordingReceiptBus{}
	pauseCalls := 0
	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeIngressSafetyPause: func(ctx context.Context, reason string) error {
			pauseCalls++
			return bus.Publish(ctx, events.Event{
				Type:        events.EventType("platform.paused"),
				SourceAgent: "runtime",
				Payload: mustJSON(map[string]any{
					"reason":    reason,
					"paused_by": "runtime",
					"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
				}),
				CreatedAt: time.Now().UTC(),
			})
		},
	})
	am.agentCfg["agent-a"] = runtimeactors.AgentConfig{
		ID:       "agent-a",
		EntityID: "ent-123",
		FlowPath: "review/inst-1",
	}

	inbound := events.Event{
		ID:    "evt-1",
		Type:  events.EventType("work.requested"),
		RunID: "run-1",
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), inbound)
	ctx = runtimecorrelation.WithRunID(ctx, inbound.RunID)
	am.maybeTripAuthCircuitBreaker(ctx, "agent-a", inbound.ID, errors.New("claude auth required"))

	if len(bus.published) != 2 {
		t.Fatalf("published events = %d, want 2", len(bus.published))
	}
	if pauseCalls != 1 {
		t.Fatalf("runtime ingress safety pause calls = %d, want 1", pauseCalls)
	}
	for _, evt := range bus.published {
		if got := evt.ParentEventID; got != inbound.ID {
			t.Fatalf("%s parent_event_id = %q, want %q", evt.Type, got, inbound.ID)
		}
	}
	var authEvt events.Event
	for _, evt := range bus.published {
		if evt.Type == events.EventType("platform.auth_required") {
			authEvt = evt
		}
	}
	if authEvt.Type != events.EventType("platform.auth_required") {
		t.Fatalf("published events = %#v, want platform.auth_required", bus.published)
	}
	if got := authEvt.ParentEventID; got != inbound.ID {
		t.Fatalf("auth event parent_event_id = %q, want %q", got, inbound.ID)
	}
	if got := authEvt.RunID; got != inbound.RunID {
		t.Fatalf("auth event run_id = %q, want %q", got, inbound.RunID)
	}
	if got := authEvt.EntityID(); got != "ent-123" {
		t.Fatalf("auth event entity_id = %q, want ent-123", got)
	}
	if got := authEvt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("auth event flow_instance = %q, want review/inst-1", got)
	}
	if got := authEvt.Scope(); got != events.EventScopeEntity {
		t.Fatalf("auth event scope = %q, want %q", got, events.EventScopeEntity)
	}
	var payload map[string]any
	if err := json.Unmarshal(authEvt.Payload, &payload); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got := payload["flow_instance"]; got != "review/inst-1" {
		t.Fatalf("auth event flow_instance = %#v, want review/inst-1", got)
	}
}

func TestMaybeTripAuthCircuitBreaker_PreservesCanceledEventLineage(t *testing.T) {
	bus := &recordingReceiptBus{}
	am := NewAgentManager(bus, nil)

	inbound := events.Event{
		ID:    "evt-canceled",
		Type:  events.EventType("work.requested"),
		RunID: "run-canceled",
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), inbound)
	ctx = runtimecorrelation.WithRunID(ctx, inbound.RunID)
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	am.maybeTripAuthCircuitBreaker(ctx, "agent-a", inbound.ID, errors.New("claude auth required"))

	var authEvt events.Event
	for _, evt := range bus.published {
		if evt.Type == events.EventType("platform.auth_required") {
			authEvt = evt
			break
		}
	}
	if authEvt.Type != events.EventType("platform.auth_required") {
		t.Fatalf("published events = %#v, want platform.auth_required", bus.published)
	}
	if got := authEvt.ParentEventID; got != inbound.ID {
		t.Fatalf("auth event parent_event_id = %q, want %q", got, inbound.ID)
	}
	if got := authEvt.RunID; got != inbound.RunID {
		t.Fatalf("auth event run_id = %q, want %q", got, inbound.RunID)
	}
}

func TestRecordDeadLetterEscalation_RequiresThreshold(t *testing.T) {
	am := NewAgentManager(nil, nil)
	now := time.Now().UTC()

	for i := 0; i < deadLetterEscalationThreshold-1; i++ {
		count, _, emit := am.recordDeadLetterEscalation("flow-1", deadLetterEscalationSample{
			at:         now.Add(time.Duration(i) * time.Minute),
			eventID:    "evt",
			agentID:    "agent",
			retryCount: i + 1,
		})
		if emit {
			t.Fatalf("unexpected escalation before threshold at count=%d", count)
		}
	}

	count, samples, emit := am.recordDeadLetterEscalation("flow-1", deadLetterEscalationSample{
		at:         now.Add(2 * time.Minute),
		eventID:    "evt-3",
		agentID:    "agent",
		retryCount: 3,
	})
	if !emit {
		t.Fatal("expected escalation at threshold")
	}
	if count != deadLetterEscalationThreshold {
		t.Fatalf("count = %d, want %d", count, deadLetterEscalationThreshold)
	}
	if len(samples) != deadLetterEscalationThreshold {
		t.Fatalf("sample count = %d, want %d", len(samples), deadLetterEscalationThreshold)
	}

	if _, _, emit := am.recordDeadLetterEscalation("flow-1", deadLetterEscalationSample{
		at:         now.Add(3 * time.Minute),
		eventID:    "evt-4",
		agentID:    "agent",
		retryCount: 4,
	}); emit {
		t.Fatal("expected escalation to stay suppressed inside the same window")
	}
}

func TestMaybeEscalateDeadLetter_PublishesTypedFlowInstanceEnvelope(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &receiptReaderStub{
		found: true,
		receipt: EventReceipt{
			EventID:    "evt-1",
			AgentID:    "agent-a",
			Status:     ReceiptStatusDeadLetter,
			RetryCount: 2,
			Error:      "boom",
		},
	}
	am := NewAgentManager(bus, nil)
	am.store = store
	am.agentCfg["agent-a"] = runtimeactors.AgentConfig{
		ID:       "agent-a",
		EntityID: "ent-123",
		FlowPath: "review/inst-1",
	}

	for i := 0; i < deadLetterEscalationThreshold; i++ {
		am.maybeEscalateDeadLetter(context.Background(), "evt-1", "agent-a")
	}

	if len(bus.published) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.published))
	}
	evt := bus.published[0]
	if evt.Type != events.EventType("platform.dead_letter_escalation") {
		t.Fatalf("event type = %s, want platform.dead_letter_escalation", evt.Type)
	}
	if got := evt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("dead-letter escalation flow_instance = %q, want review/inst-1", got)
	}
	if got := evt.Scope(); got != events.EventScopeFlow {
		t.Fatalf("dead-letter escalation scope = %q, want %q", got, events.EventScopeFlow)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got := payload["flow_instance"]; got != "review/inst-1" {
		t.Fatalf("dead-letter escalation payload flow_instance = %#v, want review/inst-1", got)
	}
}

func TestHandleAgentLoopPanic_PublishesTypedFlowInstanceEnvelope(t *testing.T) {
	bus := &recordingReceiptBus{}
	am := NewAgentManager(bus, nil)
	am.agentCfg["agent-a"] = runtimeactors.AgentConfig{
		ID:       "agent-a",
		EntityID: "ent-123",
		FlowPath: "review/inst-1",
	}

	am.handleAgentLoopPanic(context.Background(), panicStubAgent{id: "agent-a"}, 5, "scan.requested", "boom", "stack")

	if len(bus.published) != 2 {
		t.Fatalf("published events = %d, want 2", len(bus.published))
	}
	for i, evt := range bus.published {
		if got := evt.FlowInstance(); got != "review/inst-1" {
			t.Fatalf("event %d flow_instance = %q, want review/inst-1", i, got)
		}
		if got := evt.Scope(); got != events.EventScopeEntity {
			t.Fatalf("event %d scope = %q, want %q", i, got, events.EventScopeEntity)
		}
	}
}

func TestRecordPoisonQuarantine_RequiresDistinctEntities(t *testing.T) {
	am := NewAgentManager(nil, nil)

	if count, emit := am.recordPoisonQuarantine("item.failed", "ent-1"); emit || count != 1 {
		t.Fatalf("first poison count=%d emit=%v, want count=1 emit=false", count, emit)
	}
	if count, emit := am.recordPoisonQuarantine("item.failed", "ent-1"); emit || count != 1 {
		t.Fatalf("duplicate entity count=%d emit=%v, want count=1 emit=false", count, emit)
	}
	if count, emit := am.recordPoisonQuarantine("item.failed", "ent-2"); emit || count != 2 {
		t.Fatalf("second entity count=%d emit=%v, want count=2 emit=false", count, emit)
	}
	if count, emit := am.recordPoisonQuarantine("item.failed", "ent-3"); !emit || count != poisonEventEntityThreshold {
		t.Fatalf("third entity count=%d emit=%v, want count=%d emit=true", count, emit, poisonEventEntityThreshold)
	}
	if _, emit := am.recordPoisonQuarantine("item.failed", "ent-4"); emit {
		t.Fatal("expected poison quarantine event to emit only once per event name")
	}
}

func TestProcessEvent_PropagatesInboundParentWithoutTraceSeeding(t *testing.T) {
	agent := &traceRecordingAgent{}
	am := NewAgentManager(nil, nil)
	evt := events.Event{
		ID:   "evt-123",
		Type: events.EventType("discovery/market_research.scan_assigned"),
	}
	if err := am.processEvent(context.Background(), agent, evt); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if agent.parent != "evt-123" {
		t.Fatalf("parent event = %q, want evt-123", agent.parent)
	}
}

type deliveryLifecycleStoreStub struct {
	receiptReaderStub
	markCalls           []string
	quiescenceChecks    int
	quiescedAfterChecks int
}

func (s *deliveryLifecycleStoreStub) MarkEventDeliveryInProgress(_ context.Context, eventID, agentID, sessionID string) error {
	s.markCalls = append(s.markCalls, strings.TrimSpace(eventID)+"|"+strings.TrimSpace(agentID)+"|"+strings.TrimSpace(sessionID))
	return nil
}

func (s *deliveryLifecycleStoreStub) ActiveRunDeliveryQuiesced(context.Context, string, string, string) (string, bool, error) {
	s.quiescenceChecks++
	ok := s.quiescedAfterChecks > 0 && s.quiescenceChecks >= s.quiescedAfterChecks
	if !ok {
		return "", false, nil
	}
	return "runtime_nuke_cancelled", true, nil
}

func TestProcessEvent_LogsLaunchingDeliveryLifecycleTransition(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &deliveryLifecycleStoreStub{}
	am := NewAgentManager(bus, nil, store)
	evt := events.Event{ID: "evt-1", Type: events.EventType("task.started")}
	agent := traceRecordingAgent{parent: ""}

	if err := am.processEvent(context.Background(), &agent, evt); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if len(store.markCalls) != 1 {
		t.Fatalf("mark calls = %d, want 1", len(store.markCalls))
	}
	if len(bus.runtimeLogs) == 0 {
		t.Fatal("expected runtime logs")
	}
	entry := bus.runtimeLogs[0]
	if entry.Action != "delivery_lifecycle_transition" {
		t.Fatalf("action = %q, want delivery_lifecycle_transition", entry.Action)
	}
	detail := entry.Detail.(map[string]any)
	if detail["delivery_state"] != "launching" || detail["delivery_previous_state"] != "queued" || detail["delivery_reason"] != "agent_processing" {
		t.Fatalf("launching detail = %#v", detail)
	}
}

func TestProcessEvent_SkipsLateOutputAndReceiptAfterDestructiveResetQuiescence(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &deliveryLifecycleStoreStub{quiescedAfterChecks: 2}
	am := NewAgentManager(bus, nil, store)
	agent := &outputRecordingAgent{}

	result := am.processEventDetailed(context.Background(), agent, events.Event{ID: uuid.NewString(), Type: events.EventType("task.started")})
	if result.err != nil {
		t.Fatalf("processEventDetailed error = %v", result.err)
	}
	if agent.calls != 1 {
		t.Fatalf("agent calls = %d, want 1 before late quiescence guard", agent.calls)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none after quiescence", bus.published)
	}
	if store.upsertCalls != 0 {
		t.Fatalf("receipt upserts = %d, want none after quiescence", store.upsertCalls)
	}
	if result.record.ReasonCode != "runtime_nuke_cancelled" {
		t.Fatalf("reason = %q, want runtime_nuke_cancelled", result.record.ReasonCode)
	}
}

func TestWriteReceipt_LogsRetryingAndExhaustedDeliveryLifecycleTransitions(t *testing.T) {
	cases := []struct {
		name          string
		receipt       EventReceipt
		wantState     string
		wantTerminal  string
		wantRetry     int
		wantReasonRaw string
	}{
		{
			name:          "retrying",
			receipt:       EventReceipt{EventID: "evt-1", AgentID: "agent-a", Status: ReceiptStatusError, RetryCount: 1, Error: "boom"},
			wantState:     "retrying",
			wantTerminal:  "",
			wantRetry:     1,
			wantReasonRaw: "boom",
		},
		{
			name:          "exhausted",
			receipt:       EventReceipt{EventID: "evt-1", AgentID: "agent-a", Status: ReceiptStatusDeadLetter, RetryCount: 2, Error: "boom"},
			wantState:     "exhausted",
			wantTerminal:  "retry_exhausted",
			wantRetry:     2,
			wantReasonRaw: "boom",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bus := &recordingReceiptBus{}
			store := &deliveryLifecycleStoreStub{}
			store.receipt = tc.receipt
			store.found = true
			am := NewAgentManager(bus, nil, store)

			am.writeReceipt(context.Background(), "evt-1", "agent-a", ReceiptStatusError, "boom")

			if len(bus.runtimeLogs) != 1 {
				t.Fatalf("runtime logs = %d, want 1", len(bus.runtimeLogs))
			}
			entry := bus.runtimeLogs[0]
			if entry.Action != "delivery_lifecycle_transition" {
				t.Fatalf("action = %q, want delivery_lifecycle_transition", entry.Action)
			}
			detail := entry.Detail.(map[string]any)
			if detail["delivery_state"] != tc.wantState {
				t.Fatalf("delivery_state = %#v, want %q", detail["delivery_state"], tc.wantState)
			}
			if detail["retry_count"] != tc.wantRetry {
				t.Fatalf("retry_count = %#v, want %d", detail["retry_count"], tc.wantRetry)
			}
			if detail["delivery_reason"] != tc.wantReasonRaw {
				t.Fatalf("delivery_reason = %#v, want %q", detail["delivery_reason"], tc.wantReasonRaw)
			}
			if got := detail["delivery_terminal_outcome"]; got != tc.wantTerminal && !(got == nil && tc.wantTerminal == "") {
				t.Fatalf("delivery_terminal_outcome = %#v, want %q", got, tc.wantTerminal)
			}
		})
	}
}

func TestWriteReceipt_RetryAfterContextCancellationStillLogsLifecycleTransition(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &deliveryLifecycleStoreStub{}
	store.receipt = EventReceipt{
		EventID:    "evt-1",
		AgentID:    "agent-a",
		Status:     ReceiptStatusError,
		RetryCount: 1,
		Error:      "boom",
	}
	store.found = true
	store.upsertErrs = []error{context.Canceled, nil}
	am := NewAgentManager(bus, nil, store)

	am.writeReceipt(context.Background(), "evt-1", "agent-a", ReceiptStatusError, "boom")

	if store.upsertCalls != 2 {
		t.Fatalf("upsert calls = %d, want 2", store.upsertCalls)
	}
	if len(bus.runtimeLogs) != 1 {
		t.Fatalf("runtime logs = %d, want 1", len(bus.runtimeLogs))
	}
	detail := bus.runtimeLogs[0].Detail.(map[string]any)
	if detail["delivery_state"] != "retrying" || detail["delivery_reason"] != "boom" {
		t.Fatalf("retry lifecycle detail = %#v", detail)
	}
}
