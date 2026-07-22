package store

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRecordDeadLetter_PersistsAndDedupes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	entityID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		"deadletter.test",
		"runtime",
		"",
		[]byte(`{"x":1}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)

	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID(),
		Failure:         testFailureEnvelope(runtimefailures.ClassConnectorFailure, "terminal_delivery_failure", nil),
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
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	evt := eventtest.RunCreatingRootIngress(uuid.NewString(),
		"deadletter.test",
		"runtime", "", []byte(`{"entity_id":"ent-001","x":1}`), 0, eventtest.UUID("persisted-projection-run"), "", events.EventEnvelope{}, time.Now().UTC())

	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID(),
		EntityID:        "ent-001",
		Failure:         testFailureEnvelope(runtimefailures.ClassChainDepthExceeded, "chain_depth_exceeded", nil),
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
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		"pin.output",
		"runtime",
		"",
		[]byte(`{"x":1}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: uuid.NewString(), FlowInstance: "flow/target"}),
		time.Now().UTC(),
	)

	if err := commitSemanticEventFixture(ctx, pg, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	rec := runtimedeadletters.Record{
		OriginalEventID: evt.ID(),
		Failure: testFailureEnvelope(runtimefailures.ClassTargetUnreachable, "target_not_subscribed", map[string]any{
			"target": map[string]any{"flow_instance": "flow/target"},
		}),
		HandlerNode: "pin_routing",
	}
	if err := pg.RecordDeadLetter(ctx, rec); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	var (
		failureJSON string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT failure::text
		FROM dead_letters
		WHERE original_event_id = $1::uuid
	`, evt.ID()).Scan(&failureJSON); err != nil {
		t.Fatalf("query dead_letters: %v", err)
	}
	if !strings.Contains(failureJSON, `"class": "platform.target_unreachable"`) || !strings.Contains(failureJSON, `"code": "target_not_subscribed"`) {
		t.Fatalf("failure = %s, want target_unreachable/target_not_subscribed", failureJSON)
	}
	if !strings.Contains(failureJSON, `"flow_instance": "flow/target"`) {
		t.Fatalf("failure = %q, want target context attribute", failureJSON)
	}
}
