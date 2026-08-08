package runtimepersistence

import (
	"context"
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

type exactDeadLetterStore interface {
	RecordDeadLetter(context.Context, runtimedeadletters.Record) error
}

func TestRecordDeadLetterRejectsEveryIncompleteOrNonCanonicalDimensionOnBothStores(t *testing.T) {
	base := runtimedeadletters.Record{
		OriginalEventID: uuid.NewString(), OriginalEvent: "deadletter.exact", OriginalPayload: []byte(`{"x":1}`),
		EntityID: uuid.NewString(), FlowInstance: "flow/exact",
		Failure:    testFailureEnvelope(runtimefailures.ClassRetryExhausted, "exact_failure", nil),
		RetryCount: 1, ChainDepth: 2, HandlerNode: "node-exact", Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	tests := []struct {
		name   string
		mutate func(*runtimedeadletters.Record)
	}{
		{name: "missing event id", mutate: func(r *runtimedeadletters.Record) { r.OriginalEventID = "" }},
		{name: "non uuid event id", mutate: func(r *runtimedeadletters.Record) { r.OriginalEventID = "event" }},
		{name: "non canonical event id", mutate: func(r *runtimedeadletters.Record) { r.OriginalEventID = " " + r.OriginalEventID }},
		{name: "delivery without claim", mutate: func(r *runtimedeadletters.Record) { r.DeliveryID = uuid.NewString() }},
		{name: "invalid delivery id", mutate: func(r *runtimedeadletters.Record) { r.DeliveryID, r.ClaimVersion = "delivery", 1 }},
		{name: "invalid entity id", mutate: func(r *runtimedeadletters.Record) { r.EntityID = "entity" }},
		{name: "missing event type", mutate: func(r *runtimedeadletters.Record) { r.OriginalEvent = "" }},
		{name: "missing flow instance", mutate: func(r *runtimedeadletters.Record) { r.FlowInstance = "" }},
		{name: "non canonical flow instance", mutate: func(r *runtimedeadletters.Record) { r.FlowInstance = "/flow/exact/" }},
		{name: "invalid failure", mutate: func(r *runtimedeadletters.Record) { r.Failure = runtimefailures.Envelope{} }},
		{name: "missing payload", mutate: func(r *runtimedeadletters.Record) { r.OriginalPayload = nil }},
		{name: "invalid payload", mutate: func(r *runtimedeadletters.Record) { r.OriginalPayload = []byte(`{`) }},
		{name: "negative retry", mutate: func(r *runtimedeadletters.Record) { r.RetryCount = -1 }},
		{name: "negative chain depth", mutate: func(r *runtimedeadletters.Record) { r.ChainDepth = -1 }},
		{name: "missing timestamp", mutate: func(r *runtimedeadletters.Record) { r.Timestamp = "" }},
		{name: "invalid timestamp", mutate: func(r *runtimedeadletters.Record) { r.Timestamp = "yesterday" }},
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(exactDeadLetterStore)
			ctx := testAuthorActivityContext()
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					record := base
					record.OriginalPayload = append([]byte(nil), base.OriginalPayload...)
					tc.mutate(&record)
					if err := selected.RecordDeadLetter(ctx, record); err == nil {
						t.Fatal("RecordDeadLetter accepted an incomplete or non-canonical record")
					}
				})
			}
			var count int
			query := `SELECT COUNT(*) FROM dead_letters`
			if err := fixture.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
				t.Fatalf("count dead letters: %v", err)
			}
			if count != 0 {
				t.Fatalf("dead letter rows = %d, want zero rejected rows", count)
			}
		})
	}
}

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
		OriginalEvent:   string(evt.Type()),
		OriginalPayload: evt.Payload(),
		EntityID:        evt.EntityID(),
		FlowInstance:    "runtime",
		Failure:         testFailureEnvelope(runtimefailures.ClassConnectorFailure, "terminal_delivery_failure", nil),
		RetryCount:      4,
		HandlerNode:     "agent-1",
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
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

func TestRecordDeadLetter_RejectsNonUUIDEntityIDWithoutRepair(t *testing.T) {
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
		OriginalEvent:   string(evt.Type()),
		OriginalPayload: evt.Payload(),
		EntityID:        "ent-001",
		FlowInstance:    "runtime",
		Failure:         testFailureEnvelope(runtimefailures.ClassChainDepthExceeded, "chain_depth_exceeded", nil),
		HandlerNode:     "node-1",
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := pg.RecordDeadLetter(ctx, rec); err == nil || !strings.Contains(err.Error(), "entity id must be a uuid") {
		t.Fatalf("RecordDeadLetter error = %v, want exact entity-id rejection", err)
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
	if count != 0 {
		t.Fatalf("dead_letters count = %d, want 0", count)
	}
	if hasStoredEntity.Valid {
		t.Fatalf("dead_letters aggregate = %#v, want no repaired row", hasStoredEntity)
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
		OriginalEvent:   string(evt.Type()),
		OriginalPayload: evt.Payload(),
		EntityID:        evt.EntityID(),
		FlowInstance:    "flow/target",
		Failure: testFailureEnvelope(runtimefailures.ClassTargetUnreachable, "target_not_subscribed", map[string]any{
			"target": map[string]any{"flow_instance": "flow/target"},
		}),
		HandlerNode: "pin_routing",
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
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
