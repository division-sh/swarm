package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_AppendEvent_NormalizesInvalidOptionalUUIDs(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	eventID := uuid.NewString()
	err := pg.AppendEvent(ctx, events.Event{
		ID:          eventID,
		Type:        events.EventType("vertical.discovered"),
		SourceAgent: "discovery-coordinator",
		TaskID:      "legacy-task-key",
		VerticalID:  "pry_hc_telemedicine_001",
		Payload:     []byte(`{"name":"Telemedicine Platform"}`),
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("AppendEvent should not fail on non-UUID optional refs: %v", err)
	}

	var gotTaskID, gotVerticalID string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(task_id::text, ''), COALESCE(vertical_id::text, '')
		FROM events
		WHERE id = $1::uuid
	`, eventID).Scan(&gotTaskID, &gotVerticalID); err != nil {
		t.Fatalf("query event row: %v", err)
	}
	if gotTaskID != "" {
		t.Fatalf("expected normalized empty task_id, got %q", gotTaskID)
	}
	if gotVerticalID != "" {
		t.Fatalf("expected normalized empty vertical_id, got %q", gotVerticalID)
	}
}

func TestPostgresStore_PipelineReceipts_MissingEventsQuery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pipeline_receipts (
			event_id UUID PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
			status TEXT NOT NULL DEFAULT 'processed',
			error TEXT,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create pipeline_receipts: %v", err)
	}

	eventProcessed := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		CreatedAt:   time.Now().Add(-2 * time.Minute),
	}
	eventMissing := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive":"x"}`),
		CreatedAt:   time.Now().Add(-1 * time.Minute),
	}
	if err := pg.AppendEvent(ctx, eventProcessed); err != nil {
		t.Fatalf("append processed event: %v", err)
	}
	if err := pg.AppendEvent(ctx, eventMissing); err != nil {
		t.Fatalf("append missing event: %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, eventProcessed.ID, "processed", ""); err != nil {
		t.Fatalf("upsert processed receipt: %v", err)
	}

	missing, err := pg.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-1*time.Hour), 20)
	if err != nil {
		t.Fatalf("list missing pipeline receipts: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing event, got %d", len(missing))
	}
	if missing[0].ID != eventMissing.ID {
		t.Fatalf("expected missing event id=%s got=%s", eventMissing.ID, missing[0].ID)
	}
}
