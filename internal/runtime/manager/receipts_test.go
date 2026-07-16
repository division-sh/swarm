package manager

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
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

type projectedEmergencyBudgetGuard struct{}

func (projectedEmergencyBudgetGuard) ProjectRecoveryBudgetState(context.Context) error {
	return nil
}
func (projectedEmergencyBudgetGuard) IsEntityEmergency(string) bool { return true }
func (projectedEmergencyBudgetGuard) IsEntityThrottle(string) bool  { return true }
func (projectedEmergencyBudgetGuard) IsEmergency(string) bool       { return true }
func (projectedEmergencyBudgetGuard) IsThrottle(string) bool        { return true }

func TestProjectedBudgetEmergencySuppressesDeliveryButNotThresholdEvent(t *testing.T) {
	am := NewAgentManagerWithOptions(&recordingReceiptBus{}, nil, AgentManagerOptions{Budget: projectedEmergencyBudgetGuard{}})
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-a", EntityID: "entity-a"})
	work := eventtest.RootIngress("evt-work", events.EventType("work.requested"), "source", "", nil, 0, "", "", events.EventEnvelope{}, time.Now())
	if suppressed, reason := am.shouldSuppressForBudget("agent-a", work); !suppressed || reason != "suppressed by budget emergency guardrail" {
		t.Fatalf("projected emergency suppression=%v reason=%q", suppressed, reason)
	}
	threshold := eventtest.RootIngress("evt-budget", events.EventType("platform.budget_threshold_crossed"), "runtime", "", nil, 0, "", "", events.EventEnvelope{}, time.Now())
	if suppressed, reason := am.shouldSuppressForBudget("agent-a", threshold); suppressed || reason != "" {
		t.Fatalf("threshold event suppression=%v reason=%q, want exempt", suppressed, reason)
	}
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
	lastStatus  ReceiptStatus
	lastFailure *runtimefailures.Envelope
}

func (*receiptReaderStub) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (*receiptReaderStub) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (*receiptReaderStub) EnsureEntitySchema(context.Context, string) error { return nil }
func (s *receiptReaderStub) UpsertEventReceipt(_ context.Context, _, _ string, status ReceiptStatus, failure *runtimefailures.Envelope) error {
	s.upsertCalls++
	s.lastStatus = status
	s.lastFailure = runtimefailures.CloneEnvelope(failure)
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

	am.writeReceipt(testAuthorActivityContext(context.Background()), "event-1", "agent-1", ReceiptStatusProcessed, nil)

	if store.upsertCalls != 1 {
		t.Fatalf("receipt upsert calls = %d, want 1", store.upsertCalls)
	}
	if len(bus.normalCompletionEvents) != 1 || bus.normalCompletionEvents[0] != "event-1" {
		t.Fatalf("normal completion events = %#v, want event-1", bus.normalCompletionEvents)
	}
}

func TestWriteReceiptConvergesNormalRunCompletionAfterReceiptRetryPersists(t *testing.T) {
	bus := &recordingCompletionReceiptBus{}
	store := &receiptReaderStub{
		receipt: EventReceipt{
			EventID: "event-1",
			AgentID: "agent-1",
			Status:  ReceiptStatusProcessed,
		},
		found:      true,
		upsertErrs: []error{context.Canceled, nil},
	}
	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{}, store)

	am.writeReceipt(testAuthorActivityContext(context.Background()), "event-1", "agent-1", ReceiptStatusProcessed, nil)

	if store.upsertCalls != 2 {
		t.Fatalf("receipt upsert calls = %d, want 2", store.upsertCalls)
	}
	if len(bus.normalCompletionEvents) != 1 || bus.normalCompletionEvents[0] != "event-1" {
		t.Fatalf("normal completion events = %#v, want event-1", bus.normalCompletionEvents)
	}
}

type traceRecordingAgent struct {
	parent         string
	replyContextID string
}

func (a *traceRecordingAgent) ID() string                        { return "trace-agent" }
func (a *traceRecordingAgent) Type() string                      { return "llm" }
func (a *traceRecordingAgent) Subscriptions() []events.EventType { return nil }
func (a *traceRecordingAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		a.parent = inbound.ID()
	}
	a.replyContextID = events.DeliveryContextFromContext(ctx).ReplyContextID()
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
	return []events.Event{
		eventtest.RootIngress("out-1", events.EventType("task.done"), "agent-a", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
	}, nil
}

type panicStubAgent struct{ id string }

func (a panicStubAgent) ID() string                        { return a.id }
func (a panicStubAgent) Type() string                      { return "llm" }
func (a panicStubAgent) Subscriptions() []events.EventType { return nil }
func (a panicStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func registerReceiptTestAgent(t *testing.T, am *AgentManager, cfg runtimeactors.AgentConfig) {
	t.Helper()
	if err := am.lifecycle.registerExecution(testAuthorActivityContext(context.Background()), PersistedAgent{Config: cfg, Status: "active", HiredBy: "test"}, false, panicStubAgent{id: cfg.ID}, testManagerSubscriptionAdmission(t, cfg)); err != nil {
		t.Fatalf("register receipt test agent: %v", err)
	}
}

func TestMaybeTripAuthCircuitBreaker_PublishesFlowScopedAuthRequired(t *testing.T) {
	bus := &recordingReceiptBus{}
	pauseCalls := 0
	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeIngressSafetyPause: func(ctx context.Context, reason string, failure *runtimefailures.Envelope) error {
			pauseCalls++
			return bus.Publish(ctx, eventtest.RootIngress("", events.EventType("platform.paused"),
				"runtime", "", mustJSON(map[string]any{
					"reason":    reason,
					"paused_by": "runtime",
					"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
					"failure":   failure,
				}), 0, "", "", events.EventEnvelope{}, time.Now().UTC()))
		},
	})
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-a",
		EntityID:      "ent-123",
		FlowPath:      "review/inst-1",
	})

	inbound := eventtest.RootIngress("evt-1",
		events.EventType("work.requested"), "", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{})

	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(context.Background()), inbound)
	ctx = runtimecorrelation.WithRunID(ctx, inbound.RunID())
	am.maybeTripAuthCircuitBreaker(ctx, "agent-a", inbound.ID(), testAuthFailure())

	if len(bus.published) != 2 {
		t.Fatalf("published events = %d, want 2", len(bus.published))
	}
	if pauseCalls != 1 {
		t.Fatalf("runtime ingress safety pause calls = %d, want 1", pauseCalls)
	}
	for _, evt := range bus.published {
		if got := evt.ParentEventID(); got != inbound.ID() {
			t.Fatalf("%s parent_event_id = %q, want %q", evt.Type(), got, inbound.ID())
		}
	}
	var authEvt events.Event
	for _, evt := range bus.published {
		if evt.Type() == events.EventType("platform.auth_required") {
			authEvt = evt
		}
	}
	if authEvt.Type() != events.EventType("platform.auth_required") {
		t.Fatalf("published events = %#v, want platform.auth_required", bus.published)
	}
	if got := authEvt.ParentEventID(); got != inbound.ID() {
		t.Fatalf("auth event parent_event_id = %q, want %q", got, inbound.ID())
	}
	if got := authEvt.RunID(); got != inbound.RunID() {
		t.Fatalf("auth event run_id = %q, want %q", got, inbound.RunID())
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
	if err := json.Unmarshal(authEvt.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got := payload["flow_instance"]; got != "review/inst-1" {
		t.Fatalf("auth event flow_instance = %#v, want review/inst-1", got)
	}
	validateCurrentPlatformEventPayloadForManagerTest(t, string(authEvt.Type()), authEvt.Payload())
}

func TestMaybeTripAuthCircuitBreaker_PreservesCanceledEventLineage(t *testing.T) {
	bus := &recordingReceiptBus{}
	am := NewAgentManager(bus, nil)

	inbound := eventtest.RootIngress("evt-canceled",
		events.EventType("work.requested"), "", "", nil, 0, "run-canceled", "", events.EventEnvelope{}, time.Time{})

	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(context.Background()), inbound)
	ctx = runtimecorrelation.WithRunID(ctx, inbound.RunID())
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	am.maybeTripAuthCircuitBreaker(ctx, "agent-a", inbound.ID(), testAuthFailure())

	var authEvt events.Event
	for _, evt := range bus.published {
		if evt.Type() == events.EventType("platform.auth_required") {
			authEvt = evt
			break
		}
	}
	if authEvt.Type() != events.EventType("platform.auth_required") {
		t.Fatalf("published events = %#v, want platform.auth_required", bus.published)
	}
	if got := authEvt.ParentEventID(); got != inbound.ID() {
		t.Fatalf("auth event parent_event_id = %q, want %q", got, inbound.ID())
	}
	if got := authEvt.RunID(); got != inbound.RunID() {
		t.Fatalf("auth event run_id = %q, want %q", got, inbound.RunID())
	}
}

func validateCurrentPlatformEventPayloadForManagerTest(t testing.TB, eventType string, payload []byte) {
	t.Helper()
	source, err := yamlsource.LoadFile(runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	registry := runtimecontracts.EventSchemaRegistryFromBundle(&runtimecontracts.WorkflowContractBundle{Platform: spec})
	schema, ok := registry[eventType]
	if !ok {
		t.Fatalf("missing generated platform schema for %s", eventType)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal %s payload: %v", eventType, err)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema.Schema, decoded); err != nil {
		t.Fatalf("generated %s schema rejected producer payload %#v: %v", eventType, decoded, err)
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
			Failure:    testFailure("handler_failed"),
		},
	}
	am := NewAgentManager(bus, nil)
	am.store = store
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-a",
		EntityID:      "ent-123",
		FlowPath:      "review/inst-1",
	})

	for i := 0; i < deadLetterEscalationThreshold; i++ {
		am.maybeEscalateDeadLetter(testAuthorActivityContext(context.Background()), "evt-1", "agent-a")
	}

	if len(bus.published) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.published))
	}
	evt := bus.published[0]
	if evt.Type() != events.EventType("platform.dead_letter_escalation") {
		t.Fatalf("event type = %s, want platform.dead_letter_escalation", evt.Type())
	}
	if got := evt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("dead-letter escalation flow_instance = %q, want review/inst-1", got)
	}
	if got := evt.Scope(); got != events.EventScopeFlow {
		t.Fatalf("dead-letter escalation scope = %q, want %q", got, events.EventScopeFlow)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got := payload["flow_instance"]; got != "review/inst-1" {
		t.Fatalf("dead-letter escalation payload flow_instance = %#v, want review/inst-1", got)
	}
}

func TestHandleAgentLoopPanic_PublishesTypedFlowInstanceEnvelope(t *testing.T) {
	bus := &recordingReceiptBus{}
	am := NewAgentManager(bus, nil)
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-a",
		EntityID:      "ent-123",
		FlowPath:      "review/inst-1",
	})

	am.handleAgentLoopPanic(testAuthorActivityContext(context.Background()), panicStubAgent{id: "agent-a"}, 5, "scan.requested", "boom", "stack")

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
	if len(bus.runtimeLogs) != 1 || bus.runtimeLogs[0].Failure == nil {
		t.Fatalf("runtime logs = %#v, want one typed panic log", bus.runtimeLogs)
	}
	for _, evt := range bus.published {
		var payload struct {
			Failure runtimefailures.Envelope `json:"failure"`
		}
		if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
			t.Fatalf("unmarshal %s payload: %v", evt.Type(), err)
		}
		assertManagerFailureEqual(t, payload.Failure, *bus.runtimeLogs[0].Failure)
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
	evt := eventtest.RootIngress("evt-123",
		events.EventType("discovery/market_research.scan_assigned"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	evt = eventtest.ForDelivery(evt, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:agent-delivery"}})

	if err := am.processEvent(testAuthorActivityContext(context.Background()), agent, evt); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if agent.parent != "evt-123" {
		t.Fatalf("parent event = %q, want evt-123", agent.parent)
	}
	if agent.replyContextID != "reply-v1:agent-delivery" {
		t.Fatalf("agent reply context = %q", agent.replyContextID)
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
	evt := eventtest.RootIngress("evt-1", events.EventType("task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	agent := traceRecordingAgent{parent: ""}

	if err := am.processEvent(testAuthorActivityContext(context.Background()), &agent, evt); err != nil {
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

	result := am.processEventDetailed(testAuthorActivityContext(context.Background()), agent, eventtest.RootIngress(uuid.NewString(), events.EventType("task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
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
			receipt:       EventReceipt{EventID: "evt-1", AgentID: "agent-a", Status: ReceiptStatusError, RetryCount: 1, Failure: testFailure("handler_failed")},
			wantState:     "retrying",
			wantTerminal:  "",
			wantRetry:     1,
			wantReasonRaw: "handler_failure",
		},
		{
			name:          "exhausted",
			receipt:       EventReceipt{EventID: "evt-1", AgentID: "agent-a", Status: ReceiptStatusDeadLetter, RetryCount: 2, Failure: testFailure("handler_failed")},
			wantState:     "exhausted",
			wantTerminal:  "retry_exhausted",
			wantRetry:     2,
			wantReasonRaw: "retry_exhausted",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bus := &recordingReceiptBus{}
			store := &deliveryLifecycleStoreStub{}
			store.receipt = tc.receipt
			store.found = true
			am := NewAgentManager(bus, nil, store)

			am.writeReceipt(testAuthorActivityContext(context.Background()), "evt-1", "agent-a", ReceiptStatusError, testFailure("handler_failed"))

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
		Failure:    testFailure("handler_failed"),
	}
	store.found = true
	store.upsertErrs = []error{context.Canceled, nil}
	am := NewAgentManager(bus, nil, store)

	am.writeReceipt(testAuthorActivityContext(context.Background()), "evt-1", "agent-a", ReceiptStatusError, testFailure("handler_failed"))

	if store.upsertCalls != 2 {
		t.Fatalf("upsert calls = %d, want 2", store.upsertCalls)
	}
	if len(bus.runtimeLogs) != 1 {
		t.Fatalf("runtime logs = %d, want 1", len(bus.runtimeLogs))
	}
	detail := bus.runtimeLogs[0].Detail.(map[string]any)
	if detail["delivery_state"] != "retrying" || detail["delivery_reason"] != "handler_failure" {
		t.Fatalf("retry lifecycle detail = %#v", detail)
	}
}
