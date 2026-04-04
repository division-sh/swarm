package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeengine "swarm/internal/runtime/engine"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/store"
)

type recordingEventStore struct {
	mu     sync.Mutex
	events []events.Event
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

func (s *recordingEventStore) eventTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.events))
	for _, evt := range s.events {
		out = append(out, string(evt.Type))
	}
	return out
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

	mock.ExpectBegin()
	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(
		sqlmock.NewRows([]string{"table_name", "column_name"}).
			AddRow("events", "event_id").
			AddRow("events", "event_name").
			AddRow("events", "entity_id").
			AddRow("events", "flow_instance").
			AddRow("events", "scope").
			AddRow("events", "payload").
			AddRow("events", "chain_depth").
			AddRow("events", "produced_by").
			AddRow("events", "produced_by_type").
			AddRow("events", "source_event_id").
			AddRow("events", "created_at").
			AddRow("event_deliveries", "event_id").
			AddRow("event_deliveries", "subscriber_type").
			AddRow("event_deliveries", "subscriber_id").
			AddRow("event_deliveries", "status").
			AddRow("event_deliveries", "retry_count").
			AddRow("event_deliveries", "reason_code").
			AddRow("event_deliveries", "last_error").
			AddRow("event_deliveries", "active_session_id").
			AddRow("event_deliveries", "started_at").
			AddRow("event_deliveries", "delivered_at").
			AddRow("event_deliveries", "created_at"),
	)
	mock.ExpectExec("INSERT INTO events").
		WithArgs("evt-1", "custom.emitted", "", "", "global", sqlmock.AnyArg(), 0, "", "platform", "", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO event_deliveries").
		WithArgs("evt-1", "reviewer").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	eb, err := runtimebus.NewEventBus(&store.PostgresStore{DB: db})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	intent := runtimeengine.EmitIntent{
		Event: events.Event{
			ID:        "evt-1",
			Type:      events.EventType("custom.emitted"),
			Payload:   []byte(`{"entity_id":"ent-1"}`),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("ent-1"),
		Recipients: []string{"reviewer"},
	}
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
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
