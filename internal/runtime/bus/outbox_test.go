package bus_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/google/uuid"
)

type recordingEventStore struct {
	mu     sync.Mutex
	events []events.Event
}

type directRecipientTransactionalStore struct {
	mu            sync.Mutex
	descriptors   []runtimebus.ActiveAgentDescriptor
	events        []events.Event
	deliveries    map[string][]string
	routes        map[string][]events.DeliveryRoute
	deadLetterErr error
}

type directRecipientEventMutation struct {
	ctx   context.Context
	store *directRecipientTransactionalStore
}

func (s *recordingEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (*recordingEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (*recordingEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s *recordingEventStore) eventTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.events))
	for _, evt := range s.events {
		out = append(out, string(evt.Type()))
	}
	return out
}

func (s *directRecipientTransactionalStore) AppendEvent(context.Context, events.Event) error {
	return nil
}

func (s *directRecipientTransactionalStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliveries == nil {
		s.deliveries = map[string][]string{}
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

func (s *directRecipientTransactionalStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries[eventID]...), nil
}

func (s *directRecipientTransactionalStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.descriptors...), nil
}

func (*directRecipientTransactionalStore) BeginEventTx(context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (s *directRecipientTransactionalStore) EventMutationFromContext(ctx context.Context) (runtimebus.EventMutation, bool) {
	if _, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); !ok {
		return nil, false
	}
	mutation := &directRecipientEventMutation{store: s}
	mutation.ctx = runtimebus.WithEventMutationContext(ctx, mutation)
	return mutation, true
}

func (m *directRecipientEventMutation) Context() context.Context {
	if m == nil || m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *directRecipientEventMutation) AppendEvent(ctx context.Context, evt events.Event) error {
	return m.store.AppendEventTx(ctx, nil, evt)
}

func (m *directRecipientEventMutation) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return m.store.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (m *directRecipientEventMutation) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, _ map[string]events.RouteIdentity) error {
	return m.store.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (m *directRecipientEventMutation) InsertEventDeliveryRoutes(ctx context.Context, eventID string, routes []events.DeliveryRoute) error {
	return m.store.InsertEventDeliveryRoutesTx(ctx, nil, eventID, routes)
}

func (*directRecipientEventMutation) UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error {
	return nil
}

func (m *directRecipientEventMutation) RecordDeadLetter(context.Context, runtimedeadletters.Record) error {
	if m == nil || m.store == nil {
		return nil
	}
	return m.store.deadLetterErr
}

func (m *directRecipientEventMutation) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return m.store.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *directRecipientTransactionalStore) AppendEventTx(_ context.Context, _ *sql.Tx, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (s *directRecipientTransactionalStore) InsertEventDeliveriesTx(ctx context.Context, _ *sql.Tx, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (s *directRecipientTransactionalStore) InsertEventDeliveryRoutesTx(_ context.Context, _ *sql.Tx, eventID string, routes []events.DeliveryRoute) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routes == nil {
		s.routes = map[string][]events.DeliveryRoute{}
	}
	if s.deliveries == nil {
		s.deliveries = map[string][]string{}
	}
	s.routes[eventID] = events.NormalizeDeliveryRoutes(routes)
	for _, route := range s.routes[eventID] {
		if route.SubscriberType != "agent" {
			continue
		}
		s.deliveries[eventID] = append(s.deliveries[eventID], route.SubscriberID)
	}
	return nil
}

func (*directRecipientTransactionalStore) UpsertPipelineReceiptTx(context.Context, *sql.Tx, string, string, string) error {
	return nil
}

func (s *directRecipientTransactionalStore) deliveryRoutes(eventID string) []events.DeliveryRoute {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...)
}

func deliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == want.SubscriberType &&
			route.SubscriberID == want.SubscriberID &&
			route.Target.Normalized() == want.Target.Normalized() {
			return true
		}
	}
	return false
}

type interceptingTestHandler struct{}

func (interceptingTestHandler) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.Type() != events.EventType("custom.emitted") {
		return true, nil, nil
	}
	return false, []events.Event{eventtest.RootIngress(
		"",
		events.EventType("custom.followup"),
		"runtime",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
		time.Now().UTC(),
	)}, nil
}

func TestEngineDispatcherCollectsEmitIntentsWithChainDepth(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eventCollector := make([]events.Event, 0, 1)
	intentCollector := make([]runtimeengine.EmitIntent, 0, 1)
	ctx := runtimepipeline.WithPipelineEmitCollectors(context.Background(), &eventCollector, &intentCollector)

	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RootIngress("", events.EventType("custom.emitted"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}),
		ChainDepth: 3,
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if got := len(intentCollector); got != 1 {
		t.Fatalf("intent collector count = %d, want 1", got)
	}
	if got := intentCollector[0].ChainDepth; got != 3 {
		t.Fatalf("intent chain depth = %d, want 3", got)
	}
	if got := len(eventCollector); got != 0 {
		t.Fatalf("event collector count = %d, want 0", got)
	}
}

func TestEngineDispatcherQueuesWhenPipelineSQLTxActive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a", EntityID: "ent-1"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-a", events.EventType("custom.emitted"))
	defer eb.Unsubscribe("agent-a")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-post-commit-dispatch",
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),
	}
	postCommitActions := make([]func(), 0, 1)
	txctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)

	if err := eb.EngineOutbox().WriteOutbox(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommitActions) != 1 {
		_ = tx.Rollback()
		t.Fatalf("post-commit actions = %d, want 1", len(postCommitActions))
	}
	requireNoBusEvent(t, ch, "post-commit delivery before flush")

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	got := requireBusEvent(t, ch, "post-commit outbox dispatch")
	if got.ID() != intent.Event.ID() {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), intent.Event.ID())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherQueuesImmutableIntentSnapshotWhenPipelineSQLTxActive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	originalCh := eb.Subscribe("agent-original", events.EventType("custom.snapshot"))
	defer eb.Unsubscribe("agent-original")
	mutatedCh := eb.Subscribe("agent-mutated", events.EventType("custom.snapshot"))
	defer eb.Unsubscribe("agent-mutated")

	payload := []byte(`{"value":"original"}`)
	targetSet := []events.RouteIdentity{{FlowInstance: "flow-original", EntityID: "entity-original"}}
	recipients := []string{"agent-original"}
	intents := []runtimeengine.EmitIntent{{
		Event: eventtest.RootIngress("evt-queued-snapshot",
			events.EventType("custom.snapshot"), "", "", payload, 0, "", "", events.EventEnvelope{TargetSet: targetSet},
			time.Now().UTC()),

		Recipients: recipients,
	}}
	postCommitActions := make([]func(), 0, 1)
	txctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)

	if err := eb.EngineDispatcher().DispatchPostCommit(txctx, intents); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommitActions) != 1 {
		_ = tx.Rollback()
		t.Fatalf("post-commit actions = %d, want 1", len(postCommitActions))
	}
	copy(payload, []byte(`{"value":"mutated!"}`))
	targetSet[0] = events.RouteIdentity{FlowInstance: "flow-mutated", EntityID: "entity-mutated"}
	recipients[0] = "agent-mutated"
	intents[0].Event = eventtest.RootIngress(
		intents[0].Event.ID(),
		intents[0].Event.Type(),
		intents[0].Event.SourceAgent(),
		intents[0].Event.TaskID(),
		[]byte(`{"value":"reassigned"}`),
		intents[0].Event.ChainDepth(),
		intents[0].Event.RunID(),
		intents[0].Event.ParentEventID(),
		events.EventEnvelope{TargetSet: []events.RouteIdentity{{FlowInstance: "flow-reassigned", EntityID: "entity-reassigned"}}},
		intents[0].Event.CreatedAt())

	intents[0].Recipients = []string{"agent-reassigned"}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	got := requireBusEvent(t, originalCh, "immutable intent snapshot delivery")
	if string(got.Payload()) != `{"value":"original"}` {
		t.Fatalf("delivered payload = %s, want original snapshot", string(got.Payload()))
	}
	routes := got.TargetRoutes()
	if len(routes) != 1 || routes[0].FlowInstance != "flow-original" || routes[0].EntityID != "entity-original" {
		t.Fatalf("delivered target routes = %#v, want original snapshot", routes)
	}
	requireNoBusEvent(t, mutatedCh, "mutated recipient delivery")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherFailsClosedWithSQLTxAndNoPostCommitQueue(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	err = eb.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{
		Event: eventtest.RootIngress(
			"evt-no-post-commit-queue",
			events.EventType("custom.emitted"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),
	}})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected DispatchPostCommit to fail closed without post-commit queue")
	}
	if !strings.Contains(err.Error(), "post-commit dispatch requires pipeline post-commit actions") {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit error = %q, want post-commit queue failure", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxPersistsEventsAndDeliveriesInTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	entityID := uuid.NewString()
	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	recordingStore := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer", EntityID: entityID},
		},
	}
	eb, err := runtimebus.NewEventBus(recordingStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-1",
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"`+entityID+`"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Now().UTC(),
		),

		Recipients: []string{"reviewer"},
	}
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := len(recordingStore.events); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
	gotPersisted, err := recordingStore.ListEventDeliveryRecipients(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if strings.Join(gotPersisted, ",") != "reviewer" {
		t.Fatalf("persisted recipients = %v, want [reviewer]", gotPersisted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxSkipsEmptyNoopIntentBeforeAdmission(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	recordingStore := &directRecipientTransactionalStore{}
	eb, err := runtimebus.NewEventBus(recordingStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	intents := []runtimeengine.EmitIntent{{
		Event: eventtest.RootIngress("evt-empty-noop", "", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	}}
	if err := eb.EngineOutbox().WriteOutbox(ctx, intents); err != nil {
		t.Fatalf("WriteOutbox empty noop: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := len(recordingStore.events); got != 0 {
		t.Fatalf("persisted events = %d, want 0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherSkipsEmptyNoopIntentBeforeAdmission(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	intents := []runtimeengine.EmitIntent{{
		Event: eventtest.RootIngress("evt-empty-noop-dispatch", "", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	}}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), intents); err != nil {
		t.Fatalf("DispatchPostCommit empty noop: %v", err)
	}
}

func TestEngineOutboxSubscribedIntentConsumesCanonicalMaterializedRoutePlan(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "target-node",
		Target: events.RouteIdentity{
			FlowInstance: "review/inst-1",
		},
	}
	guardSawMaterializedRoute := false
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []events.DeliveryRoute{want}, nil
		},
		RecipientPlanGuard: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
				t.Fatalf("guard delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
			}
			guardSawMaterializedRoute = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress("evt-outbox-materialized-route",
			events.EventType("review/inst-1/task.started"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !guardSawMaterializedRoute {
		t.Fatal("recipient plan guard did not see materialized route")
	}
	if got := store.deliveryRoutes(intent.Event.ID()); !deliveryRoutesContain(got, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxAndDispatcher_UseCanonicalDirectRecipientManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
			{AgentID: "reviewer-ent-1", EntityID: "ent-1"},
			{AgentID: "reviewer-ent-2", EntityID: "ent-2"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := eb.Subscribe("control-plane")
	matchCh := eb.Subscribe("reviewer-ent-1")
	otherCh := eb.Subscribe("reviewer-ent-2")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-direct-intent",
			events.EventType("custom.direct"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),

		Recipients: []string{"control-plane", "reviewer-ent-1", "reviewer-ent-2", "missing-agent"},
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	wantPersisted := []string{"control-plane", "reviewer-ent-1"}
	if strings.Join(gotPersisted, ",") != strings.Join(wantPersisted, ",") {
		t.Fatalf("persisted recipients = %v, want %v", gotPersisted, wantPersisted)
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	_ = requireBusEvent(t, controlCh, "direct intent delivery to control-plane")
	evt := requireBusEvent(t, matchCh, "direct intent delivery to matching entity-scoped agent")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "direct intent delivery to filtered recipient")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutbox_TargetFailureDeadLetterErrorFailsClosed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	deadLetterErr := errors.New("dead letter recorder unavailable")
	store := &directRecipientTransactionalStore{deadLetterErr: deadLetterErr}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-outbox-target-failure",
			events.EventType("child/output.done"),
			"",
			"",
			[]byte(`{}`),
			0,
			"",
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "missing-entity", FlowInstance: "missing-flow"}),
			time.Now().UTC(),
		),
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	err = eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent})
	if !errors.Is(err, deadLetterErr) {
		t.Fatalf("WriteOutbox error = %v, want dead-letter persistence failure", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxAndDispatcher_DeliverInternalSubscribersOutsidePersistedManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	internalCh := eb.SubscribeInternal("workflow-runtime", events.EventType("custom.emitted"))
	agentCh := eb.Subscribe("agent-a", events.EventType("custom.emitted"))

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-internal-live",
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if strings.Join(gotPersisted, ",") != "agent-a" {
		t.Fatalf("persisted recipients = %v, want [agent-a]", gotPersisted)
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	evt := requireBusEvent(t, internalCh, "outbox event delivery to internal subscriber")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("internal event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, agentCh, "outbox event delivery to agent subscriber")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("agent event entity_id = %q, want ent-1", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherRunsInterceptorsForPersistedEmitIntents(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SetInterceptors(interceptingTestHandler{})

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-1",
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	got := store.eventTypes()
	if len(got) == 0 || got[0] != "custom.followup" {
		t.Fatalf("persisted event types = %v, want first event custom.followup", got)
	}
}

func TestEngineDispatcher_FailsClosedWithoutAuthoritativeRecipientManifestOnInMemoryBus(t *testing.T) {
	eb, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-missing-manifest",
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),
	}

	err = eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent})
	if err == nil {
		t.Fatal("expected DispatchPostCommit to fail without authoritative recipient manifest")
	}
	if got := err.Error(); !strings.Contains(got, "authoritative delivery recipient manifest is unavailable") {
		t.Fatalf("DispatchPostCommit error = %q, want missing authoritative manifest failure", got)
	}
}

func TestEngineDispatcher_DirectIntentUsesExplicitRecipientsWhenManifestWasNotPersisted(t *testing.T) {
	eb, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	recipientCh := eb.Subscribe("agent-a")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-direct-no-tx",
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),

		Recipients: []string{"agent-a"},
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}

	evt := requireBusEvent(t, recipientCh, "direct no-tx delivery to explicit recipient")
	if got := evt.EntityID(); got != "ent-1" {
		t.Fatalf("delivered event entity_id = %q, want ent-1", got)
	}
}

func TestEngineDispatcher_TransactionalDirectIntentHonorsEmptyPersistedManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer-ent-2", EntityID: "ent-2"},
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	filteredCh := eb.Subscribe("reviewer-ent-2")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RootIngress(
			"evt-empty-direct-manifest",
			events.EventType("custom.direct"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Now().UTC(),
		),

		Recipients: []string{"reviewer-ent-2"},
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if len(gotPersisted) != 0 {
		t.Fatalf("persisted recipients = %v, want empty authoritative manifest", gotPersisted)
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	requireNoBusEvent(t, filteredCh, "empty authoritative direct manifest delivery")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
