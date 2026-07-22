package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

type recordingReceiptBus struct {
	published   []events.Event
	runtimeLogs []runtimepipeline.RuntimeLogEntry
}

func receiptTestEvent(id string) events.Event {
	return eventtest.RunCreatingRootIngress(id, events.EventType("work.requested"), "source", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{})
}

func (b *recordingReceiptBus) Publish(_ context.Context, evt events.Event) error {
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
	completionErr          error
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
	am := newTestAgentManagerWithOptions(t, &recordingReceiptBus{}, nil, AgentManagerOptions{Budget: projectedEmergencyBudgetGuard{}})
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-a", EntityID: "entity-a"})
	work := eventtest.RunCreatingRootIngress("evt-work", events.EventType("work.requested"), "source", "", nil, 0, "", "", events.EventEnvelope{}, time.Now())
	if suppressed, reason := am.shouldSuppressForBudget("agent-a", work); !suppressed || reason != "suppressed by budget emergency guardrail" {
		t.Fatalf("projected emergency suppression=%v reason=%q", suppressed, reason)
	}
	threshold := eventtest.RunCreatingRootIngress("evt-budget", events.EventType("platform.budget_threshold_crossed"), "runtime", "", nil, 0, "", "", events.EventEnvelope{}, time.Now())
	if suppressed, reason := am.shouldSuppressForBudget("agent-a", threshold); suppressed || reason != "" {
		t.Fatalf("threshold event suppression=%v reason=%q, want exempt", suppressed, reason)
	}
}

func (b *recordingCompletionReceiptBus) ConvergeDeliveryRunCompletion(_ context.Context, evt events.Event) error {
	b.normalCompletionEvents = append(b.normalCompletionEvents, strings.TrimSpace(evt.ID()))
	return b.completionErr
}

type receiptReaderStub struct{}

func (*receiptReaderStub) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (*receiptReaderStub) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (*receiptReaderStub) EnsureEntitySchema(context.Context, string) error { return nil }

func TestDeliverySettlementConvergesRunCompletionWithExactEvent(t *testing.T) {
	bus := &recordingCompletionReceiptBus{}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{})
	am.convergeDeliveryRunCompletion(testAuthorActivityContext(context.Background()), receiptTestEvent("event-1"), "agent-1")
	if len(bus.normalCompletionEvents) != 1 || bus.normalCompletionEvents[0] != "event-1" {
		t.Fatalf("delivery completion events = %#v, want event-1", bus.normalCompletionEvents)
	}
}

func TestDeliveryRunCompletionFailureIsVisible(t *testing.T) {
	bus := &recordingCompletionReceiptBus{completionErr: errors.New("completion unavailable")}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{})
	am.convergeDeliveryRunCompletion(testAuthorActivityContext(context.Background()), receiptTestEvent("event-1"), "agent-1")
	if len(bus.normalCompletionEvents) != 1 || bus.normalCompletionEvents[0] != "event-1" {
		t.Fatalf("delivery completion events = %#v, want event-1", bus.normalCompletionEvents)
	}
	if len(bus.runtimeLogs) != 1 || bus.runtimeLogs[0].Action != "delivery_run_completion_failed" {
		t.Fatalf("runtime logs = %#v, want visible delivery completion failure", bus.runtimeLogs)
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

type partialOutputRetryStore struct {
	receiptReaderStub
	persisted map[string]bool
}

func (s *partialOutputRetryStore) EventExists(_ context.Context, eventID string) (bool, error) {
	return s.persisted[eventID], nil
}

type partialOutputRetryBus struct {
	store      *partialOutputRetryStore
	failSecond bool
	attempts   []string
	succeeded  []string
}

func (b *partialOutputRetryBus) Publish(_ context.Context, event events.Event) error {
	b.attempts = append(b.attempts, event.ID())
	if b.failSecond && len(b.attempts) == 2 {
		return errors.New("second output failed")
	}
	b.succeeded = append(b.succeeded, event.ID())
	b.store.persisted[event.ID()] = true
	return nil
}

func (*partialOutputRetryBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*partialOutputRetryBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*partialOutputRetryBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*partialOutputRetryBus) Unsubscribe(string)           {}
func (*partialOutputRetryBus) Store() runtimebus.EventStore { return runtimebus.InMemoryEventStore{} }
func (*partialOutputRetryBus) ResetInMemoryState() error    { return nil }
func (*partialOutputRetryBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}

type partialOutputRetryAgent struct{ id string }

func (a partialOutputRetryAgent) ID() string                      { return a.id }
func (partialOutputRetryAgent) Type() string                      { return "test" }
func (partialOutputRetryAgent) Subscriptions() []events.EventType { return nil }
func (a partialOutputRetryAgent) OnEvent(_ context.Context, inbound events.Event) ([]events.Event, error) {
	lineage := events.LineageFromEvent(inbound)
	build := func(eventType events.EventType) events.Event {
		return eventtest.ChildWithLineage(
			"",
			eventType,
			a.id,
			"",
			nil,
			0,
			lineage,
			events.EventEnvelope{},
			time.Time{},
		)
	}
	return []events.Event{build("output.first"), build("output.second")}, nil
}

func TestProcessEventDeterministicOutputIdentitySurvivesPartialSuccessRetry(t *testing.T) {
	store := &partialOutputRetryStore{persisted: map[string]bool{}}
	bus := &partialOutputRetryBus{store: store, failSecond: true}
	deliveryStore := newManagerDeliveryTestStore(t)
	manager := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore}, store)
	inbound := eventtest.RunCreatingRootIngress(uuid.NewString(), "input.received", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	agent := partialOutputRetryAgent{id: "agent-a"}
	ctx := managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID())

	first := manager.processEventDetailed(ctx, agent, inbound)
	if first.err == nil || len(bus.succeeded) != 1 {
		t.Fatalf("first attempt err=%v succeeded=%v", first.err, bus.succeeded)
	}
	firstOutputID := bus.succeeded[0]
	bus.failSecond = false
	deliveryStore.makeRetryEligible(t, inbound, agent.ID())
	second := manager.processEventDetailed(ctx, agent, inbound)
	if second.err != nil {
		t.Fatalf("retry error = %v", second.err)
	}
	if len(bus.succeeded) != 2 || bus.succeeded[0] != firstOutputID || bus.succeeded[1] == firstOutputID {
		t.Fatalf("successful outputs = %v, want one stable first output and one second output", bus.succeeded)
	}
	if len(bus.attempts) != 3 || bus.attempts[0] != firstOutputID || bus.attempts[1] != bus.attempts[2] || bus.attempts[1] == firstOutputID {
		t.Fatalf("publish attempts = %v", bus.attempts)
	}
}

func (a *outputRecordingAgent) ID() string                        { return "agent-a" }
func (a *outputRecordingAgent) Type() string                      { return "llm" }
func (a *outputRecordingAgent) Subscriptions() []events.EventType { return nil }
func (a *outputRecordingAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	a.calls++
	return []events.Event{
		eventtest.RunCreatingRootIngress("out-1", events.EventType("task.done"), "agent-a", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{
		RuntimeIngressSafetyPause: func(ctx context.Context, reason string, failure *runtimefailures.Envelope) error {
			pauseCalls++
			return bus.Publish(ctx, eventtest.RuntimeControl("", events.EventType("platform.paused"),
				"runtime", "", mustJSON(map[string]any{
					"reason":    reason,
					"paused_by": "runtime",
					"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
					"failure":   failure,
				}), 0, "run-1", "evt-1", events.EventEnvelope{}, time.Now().UTC()))
		},
	})
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-a",
		EntityID:      "ent-123",
		FlowPath:      "review/inst-1",
	})

	inbound := eventtest.RunCreatingRootIngress("evt-1",
		events.EventType("work.requested"), "", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{})

	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(context.Background()), inbound)
	ctx = runtimecorrelation.WithRunID(ctx, inbound.RunID())
	am.maybeTripAuthCircuitBreaker(ctx, "agent-a", inbound, testAuthFailure())

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
	am := newTestAgentManager(t, bus, nil)

	inbound := eventtest.RunCreatingRootIngress("evt-canceled",
		events.EventType("work.requested"), "", "", nil, 0, "run-canceled", "", events.EventEnvelope{}, time.Time{})

	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(context.Background()), inbound)
	ctx = runtimecorrelation.WithRunID(ctx, inbound.RunID())
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	am.maybeTripAuthCircuitBreaker(ctx, "agent-a", inbound, testAuthFailure())

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
	am := newTestAgentManager(t, nil, nil)
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
	store := &receiptReaderStub{}
	am := newTestAgentManager(t, bus, nil)
	am.store = store
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-a",
		EntityID:      "ent-123",
		FlowPath:      "review/inst-1",
	})

	evt := receiptTestEvent("evt-1")
	snapshot := runtimedelivery.Snapshot{
		DeliveryID: uuid.NewString(),
		EventID:    evt.ID(),
		Status:     runtimedelivery.StatusDeadLetter,
		RetryCount: 2,
		ReasonCode: "retry_exhausted",
		Failure:    testFailure("handler_failed"),
		SettledAt:  time.Now().UTC(),
		CreatedAt:  time.Now().Add(-time.Minute).UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	for i := 0; i < deadLetterEscalationThreshold; i++ {
		am.maybeEscalateDeadLetter(testAuthorActivityContext(context.Background()), evt, "agent-a", snapshot)
	}

	if len(bus.published) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.published))
	}
	publishedEvt := bus.published[0]
	if publishedEvt.Type() != events.EventType("platform.dead_letter_escalation") {
		t.Fatalf("event type = %s, want platform.dead_letter_escalation", publishedEvt.Type())
	}
	if got := publishedEvt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("dead-letter escalation flow_instance = %q, want review/inst-1", got)
	}
	if got := publishedEvt.Scope(); got != events.EventScopeFlow {
		t.Fatalf("dead-letter escalation scope = %q, want %q", got, events.EventScopeFlow)
	}
	var payload map[string]any
	if err := json.Unmarshal(publishedEvt.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got := payload["flow_instance"]; got != "review/inst-1" {
		t.Fatalf("dead-letter escalation payload flow_instance = %#v, want review/inst-1", got)
	}
}

func TestHandleAgentLoopPanic_PublishesTypedFlowInstanceEnvelope(t *testing.T) {
	bus := &recordingReceiptBus{}
	am := newTestAgentManager(t, bus, nil)
	registerReceiptTestAgent(t, am, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-a",
		EntityID:      "ent-123",
		FlowPath:      "review/inst-1",
	})

	runID, parentID := uuid.NewString(), uuid.NewString()
	inbound := eventtest.RunCreatingRootIngressWithMode(
		parentID,
		events.EventType("scan.requested"),
		"gateway",
		"task-panic",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EventEnvelope{EntityID: "ent-123", FlowInstance: "review/inst-1"},
		time.Now().UTC(),
		executionmode.Mock,
	)
	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(context.Background()), inbound)
	am.handleAgentLoopPanic(ctx, panicStubAgent{id: "agent-a"}, 5, "scan.requested", "boom", "stack")

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
		if evt.RunID() != runID || evt.ParentEventID() != parentID || evt.TaskID() != "task-panic" || evt.ExecutionMode() != executionmode.Mock {
			t.Fatalf("event %d lineage = run:%q parent:%q task:%q mode:%q", i, evt.RunID(), evt.ParentEventID(), evt.TaskID(), evt.ExecutionMode())
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
	am := newTestAgentManager(t, nil, nil)

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
	am := newTestAgentManager(t, nil, nil)
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("evt-123"),
		events.EventType("discovery/market_research.scan_assigned"), "", "", nil, 0, eventtest.UUID("trace-parent-run"), "", events.EventEnvelope{}, time.Time{})
	evt = eventtest.ForDelivery(evt, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:agent-delivery"}})

	if err := am.processEvent(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, evt); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if agent.parent != evt.ID() {
		t.Fatalf("parent event = %q, want %s", agent.parent, evt.ID())
	}
	if agent.replyContextID != "reply-v1:agent-delivery" {
		t.Fatalf("agent reply context = %q", agent.replyContextID)
	}
}

type deliveryLifecycleStoreStub struct {
	receiptReaderStub
	quiescenceChecks    int
	quiescedAfterChecks int
}

type renewalTrackingManagerDeliveryStore struct {
	runtimedelivery.Store
	renewals atomic.Int64
}

type shortLeaseManagerDeliveryStore struct {
	*managerDeliveryTestStore
	leaseTTL time.Duration
}

func (s *shortLeaseManagerDeliveryStore) ClaimAgentDelivery(ctx context.Context, evt events.Event, route events.DeliveryRoute) (claimed runtimedelivery.ClaimedObligation, err error) {
	if err := s.ensureDelivery(evt, route); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		claimed, err = s.adapter.ClaimExact(story, tx, evt, route, s.leaseTTL)
		return err
	})
	return claimed, err
}

func (s *shortLeaseManagerDeliveryStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.RenewClaim(story, tx, claim, s.leaseTTL)
		return err
	})
	return snapshot, err
}

type blockingOutputBus struct {
	recordingReceiptBus
	started sync.Once
	blocked chan struct{}
	release chan struct{}
}

func (b *blockingOutputBus) Publish(ctx context.Context, evt events.Event) error {
	b.started.Do(func() { close(b.blocked) })
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-b.release:
		b.published = append(b.published, evt)
		return nil
	}
}

type singleOutputAgent struct{ id string }

func (a singleOutputAgent) ID() string                      { return a.id }
func (singleOutputAgent) Type() string                      { return "test" }
func (singleOutputAgent) Subscriptions() []events.EventType { return nil }
func (a singleOutputAgent) OnEvent(_ context.Context, inbound events.Event) ([]events.Event, error) {
	return []events.Event{eventtest.ChildWithLineage("", "output.completed", a.id, "", nil, 0, events.LineageFromEvent(inbound), events.EventEnvelope{}, time.Time{})}, nil
}

type failingSettlementManagerDeliveryStore struct {
	runtimedelivery.Store
}

func (*failingSettlementManagerDeliveryStore) SettleSuccess(context.Context, runtimedelivery.Claim, []string, time.Duration) (runtimedelivery.Snapshot, error) {
	return runtimedelivery.Snapshot{}, errors.New("injected settlement failure")
}

type serializingAgent struct {
	id      string
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	active  int
	max     int
	order   []string
}

func (a *serializingAgent) ID() string                      { return a.id }
func (*serializingAgent) Type() string                      { return "test" }
func (*serializingAgent) Subscriptions() []events.EventType { return nil }
func (a *serializingAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.mu.Lock()
	a.active++
	if a.active > a.max {
		a.max = a.active
	}
	a.order = append(a.order, evt.ID())
	first := len(a.order) == 1
	a.mu.Unlock()
	if first {
		close(a.started)
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-a.release:
		}
	}
	a.mu.Lock()
	a.active--
	a.mu.Unlock()
	return nil, nil
}

func (s *renewalTrackingManagerDeliveryStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	s.renewals.Add(1)
	return s.Store.RenewClaim(ctx, claim)
}

func (s *deliveryLifecycleStoreStub) ActiveRunDeliveryQuiesced(context.Context, string, string, string) (string, bool, error) {
	s.quiescenceChecks++
	ok := s.quiescedAfterChecks > 0 && s.quiescenceChecks >= s.quiescedAfterChecks
	if !ok {
		return "", false, nil
	}
	return "runtime_nuke_cancelled", true, nil
}

func TestProcessEvent_RecordsCanonicalDeliveryLifecycleTransitions(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &deliveryLifecycleStoreStub{}
	deliveryStore := newManagerDeliveryTestStore(t)
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore}, store)
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("lifecycle-event"), events.EventType("task.started"), "", "", nil, 0, eventtest.UUID("lifecycle-run"), "", events.EventEnvelope{}, time.Time{})
	agent := traceRecordingAgent{parent: ""}

	if err := am.processEvent(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), &agent, evt); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if got := deliveryStore.activityTransitions(t); !reflect.DeepEqual(got, []string{"in_progress", "delivered"}) {
		t.Fatalf("delivery activity transitions = %#v, want [in_progress delivered]", got)
	}
}

func TestProcessEventRenewsExactClaimAroundAgentHandler(t *testing.T) {
	bus := &recordingReceiptBus{}
	baseStore := newManagerDeliveryTestStore(t)
	deliveryStore := &renewalTrackingManagerDeliveryStore{Store: baseStore}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("agent-claim-renewal"), events.EventType("task.started"), "", "", nil, 0, eventtest.UUID("agent-claim-renewal-run"), "", events.EventEnvelope{}, time.Time{})
	agent := &traceRecordingAgent{}
	result := am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, evt)
	if result.err != nil {
		t.Fatalf("process event: %v", result.err)
	}
	if got := deliveryStore.renewals.Load(); got < 2 {
		t.Fatalf("claim renewals = %d, want immediate and final handler renewal", got)
	}
	obligation, err := runtimedelivery.NewObligation(evt.ID(), evt.RunID(), managerAgentDeliveryRoute(agent.ID()))
	if err != nil {
		t.Fatalf("derive agent delivery obligation: %v", err)
	}
	snapshot, err := baseStore.Snapshot(context.Background(), obligation.DeliveryID())
	if err != nil {
		t.Fatalf("load renewed agent delivery: %v", err)
	}
	if snapshot.Status != runtimedelivery.StatusDelivered {
		t.Fatalf("renewed agent delivery status = %q, want delivered", snapshot.Status)
	}
}

func TestProcessEventHeartbeatCoversBlockedOutputAndPreventsReclaim(t *testing.T) {
	baseStore := newManagerDeliveryTestStore(t)
	// SQLite CURRENT_TIMESTAMP advances in whole seconds, so the proof lease
	// must span more than one tick while remaining short enough to exercise
	// several renewals in one test.
	shortStore := &shortLeaseManagerDeliveryStore{managerDeliveryTestStore: baseStore, leaseTTL: 1500 * time.Millisecond}
	bus := &blockingOutputBus{blocked: make(chan struct{}), release: make(chan struct{})}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: shortStore})
	agent := singleOutputAgent{id: "agent-a"}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "input.received", "gateway", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	resultCh := make(chan eventProcessResult, 1)
	go func() {
		resultCh <- am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, evt)
	}()
	select {
	case <-bus.blocked:
	case <-time.After(time.Second):
		t.Fatal("output publication did not block")
	}
	time.Sleep(3 * shortStore.leaseTTL)
	if _, err := baseStore.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, managerAgentDeliveryRoute(agent.ID())); !errors.Is(err, runtimedelivery.ErrIneligible) {
		t.Fatalf("second reclaimer error = %v, want current renewed claim", err)
	}
	close(bus.release)
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("process blocked output: %v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked output did not settle after release")
	}
	obligation, err := runtimedelivery.NewObligation(evt.ID(), evt.RunID(), managerAgentDeliveryRoute(agent.ID()))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := baseStore.Snapshot(context.Background(), obligation.DeliveryID())
	if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
		t.Fatalf("settled delivery = %#v err=%v, want delivered", snapshot, err)
	}
}

func TestProcessEventSettlementFailureIsNotReportedAsReplayed(t *testing.T) {
	baseStore := newManagerDeliveryTestStore(t)
	deliveryStore := &failingSettlementManagerDeliveryStore{Store: baseStore}
	am := newTestAgentManagerWithOptions(t, &recordingReceiptBus{}, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
	agent := &traceRecordingAgent{}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "input.received", "gateway", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	result := am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, evt)
	if result.err == nil || result.record.Outcome == startupManagerReplayOutcomeReplayed {
		t.Fatalf("settlement result = outcome:%q err:%v, want failed non-replayed result", result.record.Outcome, result.err)
	}
	obligation, err := runtimedelivery.NewObligation(evt.ID(), evt.RunID(), managerAgentDeliveryRoute(agent.ID()))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := baseStore.Snapshot(context.Background(), obligation.DeliveryID())
	if err != nil || snapshot.Status != runtimedelivery.StatusInProgress || snapshot.ClaimExpiresAt.IsZero() {
		t.Fatalf("failed settlement snapshot = %#v err=%v, want recoverable in-progress claim", snapshot, err)
	}
}

func TestClaimedAttemptExecutorSerializesLiveAndRecoveryForOneAgent(t *testing.T) {
	store := newManagerDeliveryTestStore(t)
	am := newTestAgentManagerWithOptions(t, &recordingReceiptBus{}, nil, AgentManagerOptions{DeliveryStore: store})
	agent := &serializingAgent{id: "agent-a", started: make(chan struct{}), release: make(chan struct{})}
	first := eventtest.RunCreatingRootIngress(uuid.NewString(), "input.first", "gateway", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	second := eventtest.RunCreatingRootIngress(uuid.NewString(), "input.second", "gateway", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	results := make(chan eventProcessResult, 2)
	go func() {
		results <- am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, first)
	}()
	select {
	case <-agent.started:
	case <-time.After(time.Second):
		t.Fatal("first claimed attempt did not start")
	}
	go func() {
		results <- am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, second)
	}()
	time.Sleep(50 * time.Millisecond)
	agent.mu.Lock()
	active, maxConcurrent, order := agent.active, agent.max, append([]string(nil), agent.order...)
	agent.mu.Unlock()
	if active != 1 || maxConcurrent != 1 || !reflect.DeepEqual(order, []string{first.ID()}) {
		t.Fatalf("blocked executor = active:%d max:%d order:%v", active, maxConcurrent, order)
	}
	close(agent.release)
	for range 2 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("claimed attempt execution: %v", result.err)
			}
		case <-time.After(time.Second):
			t.Fatal("claimed attempt execution did not complete")
		}
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if agent.max != 1 || !reflect.DeepEqual(agent.order, []string{first.ID(), second.ID()}) {
		t.Fatalf("executor result = max:%d order:%v", agent.max, agent.order)
	}
}

func TestProcessEvent_SkipsLateOutputAndReceiptAfterDestructiveResetQuiescence(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &deliveryLifecycleStoreStub{quiescedAfterChecks: 2}
	deliveryStore := newManagerDeliveryTestStore(t)
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore}, store)
	agent := &outputRecordingAgent{}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("task.started"), "", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Time{})
	result := am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, evt)
	if result.err != nil {
		t.Fatalf("processEventDetailed error = %v", result.err)
	}
	if agent.calls != 1 {
		t.Fatalf("agent calls = %d, want 1 before late quiescence guard", agent.calls)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none after quiescence", bus.published)
	}
	obligation, err := runtimedelivery.NewObligation(evt.ID(), evt.RunID(), managerAgentDeliveryRoute(agent.ID()))
	if err != nil {
		t.Fatalf("derive quiesced delivery obligation: %v", err)
	}
	snapshot, err := deliveryStore.Snapshot(context.Background(), obligation.DeliveryID())
	if err != nil {
		t.Fatalf("load quiesced delivery: %v", err)
	}
	if snapshot.Status != runtimedelivery.StatusInProgress {
		t.Fatalf("quiesced delivery status = %q, want in_progress for lease recovery", snapshot.Status)
	}
	if result.record.ReasonCode != "runtime_nuke_cancelled" {
		t.Fatalf("reason = %q, want runtime_nuke_cancelled", result.record.ReasonCode)
	}
}

func TestWriteReceipt_LogsRetryingAndExhaustedDeliveryLifecycleTransitions(t *testing.T) {
	cases := []struct {
		name          string
		wantState     string
		wantTerminal  string
		wantRetry     int
		wantReasonRaw string
		exhaust       bool
	}{
		{
			name:          "retrying",
			wantState:     "retrying",
			wantTerminal:  "",
			wantRetry:     1,
			wantReasonRaw: "handler_failure",
		},
		{
			name:          "exhausted",
			wantState:     "exhausted",
			wantTerminal:  "retry_exhausted",
			wantRetry:     1,
			wantReasonRaw: "retry_exhausted",
			exhaust:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bus := &recordingReceiptBus{}
			deliveryStore := newManagerDeliveryTestStore(t)
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
			evt := eventtest.RunCreatingRootIngress(eventtest.UUID("write-receipt-"+tc.name), events.EventType("work.requested"), "source", "", nil, 0, eventtest.UUID("write-receipt-run-"+tc.name), "", events.EventEnvelope{}, time.Time{})
			claim, err := deliveryStore.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, managerAgentDeliveryRoute("agent-a"))
			if err != nil {
				t.Fatalf("claim delivery: %v", err)
			}
			if tc.exhaust {
				am.writeReceipt(runtimedelivery.WithClaim(testAuthorActivityContext(context.Background()), claim.Claim), evt, "agent-a", ReceiptStatusError, testFailure("handler_failed"))
				deliveryStore.makeRetryEligible(t, evt, "agent-a")
				claim, err = deliveryStore.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, managerAgentDeliveryRoute("agent-a"))
				if err != nil {
					t.Fatalf("claim retry delivery: %v", err)
				}
				bus.runtimeLogs = nil
			}
			am.writeReceipt(runtimedelivery.WithClaim(testAuthorActivityContext(context.Background()), claim.Claim), evt, "agent-a", ReceiptStatusError, testFailure("handler_failed"))

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

func TestWriteReceiptUsesCanonicalHandlerRetryBase(t *testing.T) {
	for _, test := range []struct {
		name   string
		source semanticview.Source
		want   time.Duration
	}{
		{name: "default", want: time.Second},
		{
			name: "configured",
			source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
				"handler_retry_base_seconds": {Value: 37},
			}}}),
			want: 37 * time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deliveryStore := newManagerDeliveryTestStore(t)
			am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{DeliveryStore: deliveryStore, SemanticSource: test.source})
			evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("work.requested"), "source", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Time{})
			claimed, err := deliveryStore.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, managerAgentDeliveryRoute("agent-a"))
			if err != nil {
				t.Fatal(err)
			}
			settled, err := am.writeReceipt(runtimedelivery.WithClaim(testAuthorActivityContext(context.Background()), claimed.Claim), evt, "agent-a", ReceiptStatusError, testFailure("handler_failed"))
			if err != nil {
				t.Fatal(err)
			}
			if delay := settled.NextEligibleAt.Sub(settled.UpdatedAt); delay != test.want {
				t.Fatalf("agent retry delay = %s, want %s", delay, test.want)
			}
		})
	}
}

func TestWriteReceipt_ContextCancellationReturnsFailureAndLeavesClaimForLeaseRecovery(t *testing.T) {
	bus := &recordingReceiptBus{}
	deliveryStore := newManagerDeliveryTestStore(t)
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("cancelled-settlement"), events.EventType("work.requested"), "source", "", nil, 0, eventtest.UUID("cancelled-settlement-run"), "", events.EventEnvelope{}, time.Time{})
	claimed, err := deliveryStore.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, managerAgentDeliveryRoute("agent-a"))
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	cancel()
	if _, err := am.writeReceipt(runtimedelivery.WithClaim(ctx, claimed.Claim), evt, "agent-a", ReceiptStatusError, testFailure("handler_failed")); err == nil {
		t.Fatal("writeReceipt error = nil, want canceled settlement failure")
	}
	snapshot, err := deliveryStore.Snapshot(context.Background(), claimed.Claim.DeliveryID())
	if err != nil {
		t.Fatalf("load cancelled delivery: %v", err)
	}
	if snapshot.Status != runtimedelivery.StatusInProgress || snapshot.ClaimExpiresAt.IsZero() {
		t.Fatalf("cancelled delivery = status:%q claim_expires_at:%v, want leased in_progress work", snapshot.Status, snapshot.ClaimExpiresAt)
	}
}

func TestWriteReceiptLongRunningClaimUsesExactRenewalTime(t *testing.T) {
	bus := &recordingReceiptBus{}
	deliveryStore := newManagerDeliveryTestStore(t)
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("long-running-settlement"), events.EventType("work.requested"), "source", "", nil, 0, eventtest.UUID("long-running-settlement-run"), "", events.EventEnvelope{}, time.Time{})
	claimed, err := deliveryStore.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, managerAgentDeliveryRoute("agent-a"))
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	agedAt := time.Now().Add(-15 * time.Minute).UTC()
	if result, execErr := deliveryStore.db.ExecContext(context.Background(), `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'`, agedAt, agedAt, agedAt, claimed.Snapshot.DeliveryID); execErr != nil {
		t.Fatalf("age long-running delivery: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("age long-running delivery affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := deliveryStore.db.ExecContext(context.Background(), `UPDATE event_delivery_attempts SET started_at = ? WHERE delivery_id = ? AND claim_version = ? AND open_marker = TRUE`, agedAt, claimed.Snapshot.DeliveryID, claimed.Claim.Version()); execErr != nil {
		t.Fatalf("age long-running attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("age long-running attempt affected %d rows, err=%v", rows, rowsErr)
	}

	settled, err := am.writeReceipt(runtimedelivery.WithClaim(testAuthorActivityContext(context.Background()), claimed.Claim), evt, "agent-a", ReceiptStatusError, testFailure("handler_failed"))
	if err != nil {
		t.Fatalf("settle long-running claim: %v", err)
	}
	if settled.Status != runtimedelivery.StatusFailed || settled.RetryCount != 1 {
		t.Fatalf("settled long-running delivery = %#v, want first retry scheduled", settled)
	}
}
