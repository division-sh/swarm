package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSQLiteRuntimeStore_RecordInboundEvent_DedupesWithCanonicalMarker(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	seedSQLiteInboundRunAndEntity(t, ctx, sqliteStore, runID, entityID, "customer-a")

	inserted, err := sqliteStore.RecordInboundEvent(ctx, "delivery-123", entityID, "github")
	if err != nil {
		t.Fatalf("RecordInboundEvent: %v", err)
	}
	if !inserted {
		t.Fatal("RecordInboundEvent inserted=false, want first insert")
	}
	duplicate, err := sqliteStore.RecordInboundEvent(ctx, "delivery-123", entityID, "github")
	if err != nil {
		t.Fatalf("RecordInboundEvent duplicate: %v", err)
	}
	if duplicate {
		t.Fatal("RecordInboundEvent duplicate inserted=true, want false")
	}

	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.inbound_recorded'
		  AND idempotency_key = ?
	`, inboundEventIdempotencyKey("delivery-123", entityID, "github")).Scan(&count); err != nil {
		t.Fatalf("count inbound marker: %v", err)
	}
	if count != 1 {
		t.Fatalf("inbound marker count = %d, want 1", count)
	}
}

func TestSQLiteRuntimeStore_DeleteInboundEventRemovesRollbackMarker(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	seedSQLiteInboundRunAndEntity(t, ctx, sqliteStore, runID, entityID, "customer-a")

	if inserted, err := sqliteStore.RecordInboundEvent(ctx, "delivery-123", entityID, "github"); err != nil || !inserted {
		t.Fatalf("RecordInboundEvent inserted=%v err=%v, want first insert", inserted, err)
	}
	if err := sqliteStore.DeleteInboundEvent(ctx, "delivery-123", entityID, "github"); err != nil {
		t.Fatalf("DeleteInboundEvent: %v", err)
	}
	if inserted, err := sqliteStore.RecordInboundEvent(ctx, "delivery-123", entityID, "github"); err != nil || !inserted {
		t.Fatalf("RecordInboundEvent after delete inserted=%v err=%v, want fresh insert", inserted, err)
	}
}

func seedSQLiteInboundRunAndEntity(t *testing.T, ctx context.Context, sqliteStore *SQLiteRuntimeStore, runID, entityID, slug string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, now); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('root', 'root', 'static', '{}', 'active', ?)
	`, now); err != nil {
		t.Fatalf("insert flow instance: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state, created_at, updated_at
		)
		VALUES (?, ?, 'root', 'default', ?, 'Customer A', 'active', ?, ?)
	`, runID, entityID, slug, now, now); err != nil {
		t.Fatalf("insert entity state: %v", err)
	}
}
