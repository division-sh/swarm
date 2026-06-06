package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRecordDeadLetter_PersistsAndDedupes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	entityID := uuid.NewString()
	evt := (events.NewProjectionEvent(uuid.NewString(),
		"deadletter.test",
		"runtime", "", []byte(`{"x":1}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID(),
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
	`, evt.ID()).Scan(&count, &eventName, &retryCount); err != nil {
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

	evt := events.NewProjectionEvent(uuid.NewString(),
		"deadletter.test",
		"runtime", "", []byte(`{"entity_id":"ent-001","x":1}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID(),
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
	`, evt.ID()).Scan(&count, &hasStoredEntity); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if count != 1 {
		t.Fatalf("dead_letters count = %d, want 1", count)
	}
	if hasStoredEntity.Valid && hasStoredEntity.Bool {
		t.Fatalf("dead_letters entity_id present, want NULL for non-UUID entity id")
	}
}

func TestRecordDeadLetter_PersistsTargetResolutionFailureContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	evt := (events.NewProjectionEvent(uuid.NewString(),
		"pin.output",
		"runtime", "", []byte(`{"x":1}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())).WithTargetRoute(events.RouteIdentity{EntityID: uuid.NewString(), FlowInstance: "flow/target"})
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID:     evt.ID(),
		FailureType:         "target_resolution_failed",
		TargetFailureReason: "target_not_subscribed",
		TargetContext:       []byte(`{"target":{"flow_instance":"flow/target"}}`),
		ErrorMessage:        "pin routing target delivery failed: target_not_subscribed",
		HandlerNode:         "pin_routing",
	}
	if err := pg.RecordDeadLetter(ctx, rec); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	var (
		failureType string
		reason      sql.NullString
		contextJSON string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT failure_type, target_failure_reason, target_context::text
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID()).Scan(&failureType, &reason, &contextJSON); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if failureType != "target_resolution_failed" {
		t.Fatalf("failure_type = %q, want target_resolution_failed", failureType)
	}
	if !reason.Valid || reason.String != "target_not_subscribed" {
		t.Fatalf("target_failure_reason = %#v, want target_not_subscribed", reason)
	}
	if contextJSON == "" || contextJSON == "{}" {
		t.Fatalf("target_context = %q, want populated JSON", contextJSON)
	}
}
