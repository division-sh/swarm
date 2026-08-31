package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type exactDeadLetterStore interface {
	RecordDeadLetter(context.Context, runtimedeadletters.Record) error
}

type deadLetterRecorderFixtureStore interface {
	semanticEventFixtureStore
	runtimedeadletters.Recorder
}

func TestRecordDeadLetterExactOnceParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(deadLetterRecorderFixtureStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			entityID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.ExistingRunRootIngress(
				uuid.NewString(), "chain.e5", "node-5", "", []byte(`{}`), 5, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Date(2026, 8, 9, 4, 0, 0, 0, time.UTC),
			)
			if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
				t.Fatalf("commit chain source event: %v", err)
			}
			record := runtimedeadletters.Record{
				OriginalEventID: event.ID(), OriginalEvent: "chain.e6", OriginalPayload: []byte(`{}`),
				EntityID: entityID, FlowInstance: "runtime", RetryCount: 0, ChainDepth: 6,
				HandlerNode: "node-6:chain.e6", Timestamp: "2026-08-09T04:00:01Z",
				Failure: testFailureEnvelope(runtimefailures.ClassChainDepthExceeded, "chain_depth_exceeded", map[string]any{
					"chain_depth": 6, "event_type": "chain.e6",
				}),
			}
			for attempt := 1; attempt <= 2; attempt++ {
				if err := selected.RecordDeadLetter(ctx, record); err != nil {
					t.Fatalf("RecordDeadLetter attempt %d: %v", attempt, err)
				}
			}
			assertExactOnceDeadLetterRecord(t, ctx, fixture, event.ID())
		})
	}
}

func assertExactOnceDeadLetterRecord(t testing.TB, ctx context.Context, fixture authorActivityReceiptFixture, eventID string) {
	t.Helper()
	query := `
		SELECT
			(SELECT COUNT(*) FROM dead_letters WHERE original_event_id = ?),
			(SELECT COUNT(*)
			 FROM author_activity_occurrences aa
			 JOIN dead_letters dl ON aa.source_identity = CAST(dl.dead_letter_id AS TEXT)
			 WHERE dl.original_event_id = ?
			   AND aa.kind = 'dead_letter.recorded' AND aa.version = 2
			   AND aa.transition = 'recorded' AND aa.source_owner = 'dead_letters')`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `
			SELECT
				(SELECT COUNT(*) FROM dead_letters WHERE original_event_id = $1::uuid),
				(SELECT COUNT(*)
				 FROM author_activity_occurrences aa
				 JOIN dead_letters dl ON aa.source_identity = dl.dead_letter_id::text
				 WHERE dl.original_event_id = $1::uuid
				   AND aa.kind = 'dead_letter.recorded' AND aa.version = 2
				   AND aa.transition = 'recorded' AND aa.source_owner = 'dead_letters')`
	}
	args := []any{eventID, eventID}
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		args = []any{eventID}
	}
	var rows, occurrences int
	if err := fixture.db.QueryRowContext(ctx, query, args...).Scan(&rows, &occurrences); err != nil {
		t.Fatalf("query exact-once dead-letter facts: %v", err)
	}
	if rows != 1 || occurrences != 1 {
		t.Fatalf("dead-letter exact-once facts = rows:%d occurrences:%d, want 1/1", rows, occurrences)
	}
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

func TestRecordDeadLetterExactDuplicateAndConflictParity(t *testing.T) {
	type deadLetterDeliveryStore interface {
		exactDeadLetterStore
		deliveryFixtureStore
	}

	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected, ok := fixture.store.(deadLetterDeliveryStore)
			if !ok {
				t.Fatalf("fixture store %T does not expose dead-letter delivery operations", fixture.store)
			}
			ctx := testAuthorActivityContext()
			entityID := uuid.NewString()
			event := eventtest.RunCreatingRootIngress(
				uuid.NewString(), "deadletter.identity", "runtime", "", []byte(`{"text":"canonical","value":1}`), 2,
				uuid.NewString(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
				time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC),
			)
			route := testAgentDeliveryRoute(t, event.RunID(), "dead-letter-agent", "dead-letter/primary")
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit event and delivery: %v", err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatalf("claim delivery: %v", err)
			}
			failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "terminal_delivery_failure", nil)
			settled, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureDeadLetter,
				ReasonCode:  failure.Detail.Code,
				Failure:     &failure, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
			})
			if err != nil {
				t.Fatalf("settle delivery as dead letter: %v", err)
			}
			base := runtimedeadletters.Record{
				OriginalEventID: event.ID(),
				DeliveryID:      settled.DeliveryID,
				ClaimVersion:    claimed.Claim.Version(),
				OriginalEvent:   string(event.Type()),
				OriginalPayload: event.Payload(),
				EntityID:        event.EntityID(),
				FlowInstance:    "runtime",
				Failure:         failure,
				RetryCount:      settled.RetryCount,
				ChainDepth:      event.ChainDepth(),
				HandlerNode:     route.Recipient.ID(),
				Timestamp:       settled.SettledAt.UTC().Format(time.RFC3339Nano),
			}
			if err := selected.RecordDeadLetter(ctx, base); err != nil {
				t.Fatalf("record exact duplicate: %v", err)
			}

			assertCounts := func(deadLetters, occurrences int) {
				t.Helper()
				var gotDeadLetters, gotOccurrences int
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters`).Scan(&gotDeadLetters); err != nil {
					t.Fatalf("count dead letters: %v", err)
				}
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences WHERE source_owner = 'dead_letters'`).Scan(&gotOccurrences); err != nil {
					t.Fatalf("count dead-letter occurrences: %v", err)
				}
				if gotDeadLetters != deadLetters || gotOccurrences != occurrences {
					t.Fatalf("dead-letter state = rows:%d occurrences:%d, want %d/%d", gotDeadLetters, gotOccurrences, deadLetters, occurrences)
				}
			}
			assertCounts(1, 1)

			mutations := []struct {
				name  string
				field string
				apply func(*runtimedeadletters.Record)
			}{
				{name: "original event id", field: "original_event_id", apply: func(r *runtimedeadletters.Record) { r.OriginalEventID = uuid.NewString() }},
				{name: "original event type", field: "original_event", apply: func(r *runtimedeadletters.Record) { r.OriginalEvent = "deadletter.changed" }},
				{name: "original payload", field: "original_payload", apply: func(r *runtimedeadletters.Record) { r.OriginalPayload = []byte(`{"text":"changed"}`) }},
				{name: "entity id", field: "entity_id", apply: func(r *runtimedeadletters.Record) { r.EntityID = uuid.NewString() }},
				{name: "flow instance", field: "flow_instance", apply: func(r *runtimedeadletters.Record) { r.FlowInstance = "dead-letter/changed" }},
				{name: "failure", field: "failure", apply: func(r *runtimedeadletters.Record) {
					r.Failure = testFailureEnvelope(runtimefailures.ClassRetryExhausted, "changed_failure", nil)
				}},
				{name: "retry count", field: "retry_count", apply: func(r *runtimedeadletters.Record) { r.RetryCount++ }},
				{name: "chain depth", field: "chain_depth", apply: func(r *runtimedeadletters.Record) { r.ChainDepth++ }},
				{name: "handler node", field: "handler_node", apply: func(r *runtimedeadletters.Record) { r.HandlerNode = "changed-handler" }},
				{name: "timestamp", field: "timestamp", apply: func(r *runtimedeadletters.Record) {
					parsed, _ := time.Parse(time.RFC3339Nano, r.Timestamp)
					r.Timestamp = parsed.Add(time.Second).Format(time.RFC3339Nano)
				}},
			}
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					conflicting := base
					conflicting.OriginalPayload = append([]byte(nil), base.OriginalPayload...)
					mutation.apply(&conflicting)
					err := selected.RecordDeadLetter(ctx, conflicting)
					var conflict *runtimedeadletters.IdentityConflict
					if !errors.As(err, &conflict) {
						t.Fatalf("RecordDeadLetter error = %v, want typed identity conflict", err)
					}
					if !containsString(conflict.Fields, mutation.field) {
						t.Fatalf("conflict fields = %v, want %q", conflict.Fields, mutation.field)
					}
					assertCounts(1, 1)
				})
			}

			secondRoute := testAgentDeliveryRoute(t, event.RunID(), "dead-letter-agent-two", "dead-letter/secondary")
			if err := commitDeliveryObligationFixture(ctx, selected, event, secondRoute); err != nil {
				t.Fatalf("commit second delivery: %v", err)
			}
			secondClaim, err := claimDeliveryFixture(ctx, selected, event, secondRoute)
			if err != nil {
				t.Fatalf("claim second delivery: %v", err)
			}
			for _, tc := range []struct {
				name    string
				payload []byte
			}{
				{name: "whitespace", payload: []byte(`{ "text": "canonical", "value": 1 }`)},
				{name: "key_order", payload: []byte(`{"value":1,"text":"canonical"}`)},
				{name: "numeric_lexeme", payload: []byte(`{"text":"canonical","value":1.0}`)},
			} {
				t.Run("source_payload_"+tc.name, func(t *testing.T) {
					sourceConflict := base
					sourceConflict.DeliveryID = secondClaim.Snapshot.DeliveryID
					sourceConflict.ClaimVersion = secondClaim.Claim.Version()
					sourceConflict.HandlerNode = secondRoute.Recipient.ID()
					sourceConflict.OriginalPayload = tc.payload
					if err := selected.RecordDeadLetter(ctx, sourceConflict); err == nil || !strings.Contains(err.Error(), "source event facts conflict: original_payload") {
						t.Fatalf("RecordDeadLetter source mismatch error = %v", err)
					}
					assertCounts(1, 1)
				})
			}
		})
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
