package pipeline_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type recoveryCapturePublisher struct {
	inner     runtimepipeline.Publisher
	published []events.Event
}

func (p *recoveryCapturePublisher) Publish(ctx context.Context, evt events.Event) error {
	p.published = append(p.published, evt)
	return p.inner.Publish(ctx, evt)
}

func TestRecoveryManager_ReplaysPersistedCorrelationEnvelope(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}

	runID := uuid.NewString()
	parentID := uuid.NewString()
	childID := uuid.NewString()

	parent := events.Event{
		ID:          parentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       runID,
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}
	child := events.Event{
		ID:            childID,
		Type:          events.EventType("system.recover"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         runID,
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-1 * time.Minute).UTC(),
	}

	if err := pg.AppendEvent(ctx, parent); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, child); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}

	var runsBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&runsBefore); err != nil {
		t.Fatalf("count runs before recovery: %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.published) != 1 {
		t.Fatalf("published events = %#v, want one replayed event", capture.published)
	}
	replayed := capture.published[0]
	if replayed.ID != childID {
		t.Fatalf("replayed event id = %q, want %q", replayed.ID, childID)
	}
	if replayed.RunID != runID {
		t.Fatalf("replayed run_id = %q, want %q", replayed.RunID, runID)
	}
	if replayed.ParentEventID != parentID {
		t.Fatalf("replayed parent_event_id = %q, want %q", replayed.ParentEventID, parentID)
	}

	var runsAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&runsAfter); err != nil {
		t.Fatalf("count runs after recovery: %v", err)
	}
	if runsAfter != runsBefore {
		t.Fatalf("run rows after recovery = %d, want %d", runsAfter, runsBefore)
	}

	var receiptStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, childID).Scan(&receiptStatus); err != nil {
		t.Fatalf("load child receipt: %v", err)
	}
	if receiptStatus != "success" {
		t.Fatalf("child receipt outcome = %q, want success", receiptStatus)
	}
}

func TestRecoveryManager_FailsClosedOnMissingPersistedRunID(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}

	badEventID := uuid.NewString()
	goodRunID := uuid.NewString()
	goodParentID := uuid.NewString()
	goodEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, 'system.recover', 'global', '{}'::jsonb, 'runtime', 'platform', now()
		)
	`, badEventID); err != nil {
		t.Fatalf("seed malformed event: %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          goodParentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       goodRunID,
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(good parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:            goodEventID,
		Type:          events.EventType("system.recover.good"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         goodRunID,
		ParentEventID: goodParentID,
		CreatedAt:     time.Now().Add(-1 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(good child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, goodParentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(good parent): %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	err = rm.Recover(ctx)
	if err == nil || !strings.Contains(err.Error(), "missing canonical run_id") {
		t.Fatalf("Recover error = %v, want missing canonical run_id", err)
	}
	if len(capture.published) != 1 {
		t.Fatalf("published events = %#v, want one valid replay", capture.published)
	}
	if capture.published[0].ID != goodEventID {
		t.Fatalf("published event id = %q, want %q", capture.published[0].ID, goodEventID)
	}
}
