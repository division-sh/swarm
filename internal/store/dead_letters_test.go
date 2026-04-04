package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	"swarm/internal/testutil"
)

func TestRecordDeadLetter_PersistsAndDedupes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        "deadletter.test",
		SourceAgent: "runtime",
		Payload:     []byte(`{"x":1}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID,
		FailureType:     "retry_exhausted",
		ErrorMessage:    "boom",
		RetryCount:      4,
		HandlerNode:     "agent-1",
	}
	if err := pg.RecordDeadLetter(ctx, rec); err != nil {
		t.Fatalf("RecordDeadLetter first: %v", err)
	}
	if err := pg.RecordDeadLetter(ctx, rec); err != nil {
		t.Fatalf("RecordDeadLetter duplicate: %v", err)
	}

	var (
		count      int
		eventName  string
		retryCount int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(original_event), ''), COALESCE(MAX(retry_count), 0)
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID).Scan(&count, &eventName, &retryCount); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if count != 1 {
		t.Fatalf("dead_letters count = %d, want 1", count)
	}
	if eventName != "deadletter.test" || retryCount != 4 {
		t.Fatalf("dead_letters row = event=%q retry=%d", eventName, retryCount)
	}
}

func TestRecordDeadLetter_AllowsNonUUIDEntityIDViaSourceEventPayload(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        "deadletter.test",
		SourceAgent: "runtime",
		Payload:     []byte(`{"entity_id":"ent-001","x":1}`),
		CreatedAt:   time.Now().UTC(),
	}
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID,
		EntityID:        "ent-001",
		FailureType:     "chain_depth_exceeded",
		ErrorMessage:    "too deep",
		HandlerNode:     "node-1",
	}
	if err := pg.RecordDeadLetter(ctx, rec); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	var (
		count           int
		hasStoredEntity sql.NullBool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), BOOL_OR(entity_id IS NOT NULL)
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID).Scan(&count, &hasStoredEntity); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if count != 1 {
		t.Fatalf("dead_letters count = %d, want 1", count)
	}
	if hasStoredEntity.Valid && hasStoredEntity.Bool {
		t.Fatalf("dead_letters entity_id present, want NULL for non-UUID entity id")
	}
}
