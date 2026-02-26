package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_BeginEventTx_AppendAndDeliveriesTx(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	for _, id := range []string{"empire-coordinator", "spec-auditor"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
			VALUES ($1, 'stub', $1, 'holding', 'active', '{"system_prompt":"x"}'::jsonb, now(), now())
		`, id); err != nil {
			t.Fatalf("seed agent %s: %v", id, err)
		}
	}

	tx, err := pg.BeginEventTx(ctx)
	if err != nil {
		t.Fatalf("BeginEventTx: %v", err)
	}

	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   time.Now().UTC(),
	}
	if err := pg.AppendEventTx(ctx, tx, evt); err != nil {
		_ = tx.Rollback()
		t.Fatalf("AppendEventTx: %v", err)
	}
	if err := pg.InsertEventDeliveriesTx(ctx, tx, eventID, []string{"empire-coordinator", "spec-auditor"}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("InsertEventDeliveriesTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit event tx: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if nEvents != 1 || nDeliveries != 2 {
		t.Fatalf("expected event+2 deliveries persisted, got events=%d deliveries=%d", nEvents, nDeliveries)
	}
}

func TestPostgresStore_PersistEventWithDeliveries_SuccessAndRollbackOnFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{"system_prompt":"x"}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	eventID := uuid.NewString()
	if err := pg.PersistEventWithDeliveries(ctx, events.Event{
		ID:          eventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"SaaS in Argentina"}`),
		CreatedAt:   time.Now().UTC(),
	}, []string{" empire-coordinator ", "", "empire-coordinator"}); err != nil {
		t.Fatalf("PersistEventWithDeliveries success path: %v", err)
	}

	var nEvents, nDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = $1::uuid`, eventID).Scan(&nEvents); err != nil {
		t.Fatalf("count events success: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&nDeliveries); err != nil {
		t.Fatalf("count deliveries success: %v", err)
	}
	if nEvents != 1 || nDeliveries != 1 {
		t.Fatalf("expected deduped delivery insertion, got events=%d deliveries=%d", nEvents, nDeliveries)
	}

	// Delivery FK failure must roll back the entire tx (no event row should remain).
	failedEventID := uuid.NewString()
	err := pg.PersistEventWithDeliveries(ctx, events.Event{
		ID:          failedEventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"fail path"}`),
		CreatedAt:   time.Now().UTC(),
	}, []string{"missing-agent"})
	if err == nil {
		t.Fatal("expected error for unknown delivery agent")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "insert event delivery") {
		t.Fatalf("unexpected error: %v", err)
	}

	var rolledBackCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = $1::uuid`, failedEventID).Scan(&rolledBackCount); err != nil {
		t.Fatalf("count rolled back event: %v", err)
	}
	if rolledBackCount != 0 {
		t.Fatalf("expected failed tx to roll back event row, count=%d", rolledBackCount)
	}
}

func TestIsMissingPipelineReceiptsTable(t *testing.T) {
	if isMissingPipelineReceiptsTable(nil) {
		t.Fatal("nil error should not be treated as missing table")
	}
	if !isMissingPipelineReceiptsTable(assertErr("pq: relation \"pipeline_receipts\" does not exist")) {
		t.Fatal("expected missing table error to match")
	}
	if isMissingPipelineReceiptsTable(assertErr("some other db error")) {
		t.Fatal("unexpected positive match on unrelated error")
	}
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
