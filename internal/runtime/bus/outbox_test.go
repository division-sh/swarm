package bus_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeengine "swarm/internal/runtime/engine"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type recordingEventStore struct {
	mu     sync.Mutex
	events []events.Event
}

type directRecipientTransactionalStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	events      []events.Event
	deliveries  map[string][]string
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
		out = append(out, string(evt.Type))
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

func (s *directRecipientTransactionalStore) AppendEventTx(_ context.Context, _ *sql.Tx, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (s *directRecipientTransactionalStore) InsertEventDeliveriesTx(ctx context.Context, _ *sql.Tx, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (*directRecipientTransactionalStore) UpsertPipelineReceiptTx(context.Context, *sql.Tx, string, string, string) error {
	return nil
}

type interceptingTestHandler struct{}

func (interceptingTestHandler) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.Type != events.EventType("custom.emitted") {
		return true, nil, nil
	}
	return false, []events.Event{(events.Event{
		Type:        events.EventType("custom.followup"),
		SourceAgent: "runtime",
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(evt.EntityID())}, nil
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
		Event:      (events.Event{Type: events.EventType("custom.emitted")}).WithEntityID("ent-1"),
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
		Event: events.Event{
			ID:        "evt-1",
			Type:      events.EventType("custom.emitted"),
			Payload:   []byte(`{"entity_id":"` + entityID + `"}`),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID(entityID),
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
		Event: events.Event{
			ID:        "evt-direct-intent",
			Type:      events.EventType("custom.direct"),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("ent-1"),
		Recipients: []string{"control-plane", "reviewer-ent-1", "reviewer-ent-2", "missing-agent"},
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID)
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
	select {
	case <-controlCh:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control-plane did not receive direct intent")
	}
	select {
	case evt := <-matchCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("matched event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("matching entity-scoped agent did not receive direct intent")
	}
	select {
	case evt := <-otherCh:
		t.Fatalf("unexpected event delivered to filtered recipient: %#v", evt)
	case <-time.After(25 * time.Millisecond):
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
		Event: events.Event{
			ID:        "evt-1",
			Type:      events.EventType("custom.emitted"),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("ent-1"),
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
		Event: events.Event{
			ID:        "evt-missing-manifest",
			Type:      events.EventType("custom.emitted"),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("ent-1"),
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
		Event: events.Event{
			ID:        "evt-direct-no-tx",
			Type:      events.EventType("custom.emitted"),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("ent-1"),
		Recipients: []string{"agent-a"},
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}

	select {
	case evt := <-recipientCh:
		if got := evt.EntityID(); got != "ent-1" {
			t.Fatalf("delivered event entity_id = %q, want ent-1", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected explicit direct recipient to receive event")
	}
}
