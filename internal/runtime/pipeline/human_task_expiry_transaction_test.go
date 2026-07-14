package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/uuid"
)

type transactionProbeHumanTaskExpiry struct {
	runID string
	event events.Event
}

func (e *transactionProbeHumanTaskExpiry) ExpireHumanTaskCardsInMutation(ctx context.Context, _ time.Time, _ int) ([]events.Event, error) {
	tx, ok := PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return nil, errors.New("pipeline transaction is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, e.runID, time.Now().UTC()); err != nil {
		return nil, err
	}
	return []events.Event{e.event}, nil
}

func TestHumanTaskExpiryPublishesInsideSelectedStoreMutation(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	runner := &recordingRuntimeMutationRunner{db: db}
	workflowStore := NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	expiry := &transactionProbeHumanTaskExpiry{
		runID: runID,
		event: events.NewRuntimeControlEvent(
			uuid.NewString(), events.EventType("mailbox.card_expired"), "platform", "", []byte(`{"card_id":"card-a"}`),
			0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
		),
	}
	bus := &recordingPipelineBus{publishErr: errors.New("injected event persistence failure")}
	coordinator := &PipelineCoordinator{bus: bus, workflowStore: workflowStore}

	if err := coordinator.expireHumanTaskCards(context.Background(), expiry, time.Now().UTC(), 10); err == nil {
		t.Fatal("expiry succeeded when transactional publication failed")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expiry state rows after publication rollback = %d, want 0", count)
	}

	bus.publishErr = nil
	if err := coordinator.expireHumanTaskCards(context.Background(), expiry, time.Now().UTC(), 10); err != nil {
		t.Fatalf("expiry with transactional publication: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("committed expiry state rows = %d, want 1", count)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].ID() != expiry.event.ID() {
		t.Fatalf("transactional expiry publishes = %#v", bus.publishes)
	}
}
