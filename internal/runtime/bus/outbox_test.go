package bus_test

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimeengine "empireai/internal/runtime/engine"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/store"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestEngineDispatcherCollectsEmitIntentsWithChainDepth(t *testing.T) {
	eb := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
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
	if got := len(eventCollector); got != 1 {
		t.Fatalf("event collector count = %d, want 1", got)
	}
}

func TestEngineOutboxPersistsEventsAndDeliveriesInTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").
		WithArgs("evt-1", "custom.emitted", "", "", "", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO event_deliveries").
		WithArgs("evt-1", "reviewer").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	eb := runtimebus.NewEventBus(&store.PostgresStore{DB: db})
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
