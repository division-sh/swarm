package cataloge2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogEntityDeadLetterObservationPreservesPhysicalRelationBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			h := &runtimeHarness{t: t, ctx: ctx, backend: backend}
			var selected any
			var setup storetest.RunFixtureStore
			var deadLetters interface {
				RecordDeadLetter(context.Context, runtimedeadletters.Record) error
			}
			var closeOwner func() error
			if backend == catalogBackendPostgres {
				h.pg = storetest.StartPostgresRuntimeStore(t)
				selected, setup, deadLetters, closeOwner = h.pg, h.pg, h.pg, h.pg.Close
				registerTestAuthorActivityCatalog(t, h.pg, "fixture.deadletter")
			} else {
				h.sqlite = storetest.StartSQLiteRuntimeStore(t)
				selected, setup, deadLetters, closeOwner = h.sqlite, h.sqlite, h.sqlite, h.sqlite.Close
				registerTestAuthorActivityCatalog(t, h.sqlite, "fixture.deadletter")
			}
			storetest.RequireRun(t, ctx, setup, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID})
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			boundary := time.Now().UTC().Truncate(time.Microsecond)
			headerID, payloadID, unrelatedID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			failure := runtimefailures.FromError(errors.New("physical relation probe"), "cataloge2e", "deadletter_relation").Failure
			if assertEntityDeadLetterOutcome(t, reader, boundary, payloadID) {
				t.Fatal("an entity without a persisted dead-letter relation received credit")
			}
			var want []storetest.DeadLetterObservationRow
			for i, payload := range []string{`{"entity_id":"` + payloadID + `"}`, `{}`} {
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.deadletter", "", "", []byte(payload), 0,
					catalogRuntimeRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, headerID), boundary)
				storetest.CommitSemanticEvent(t, ctx, selected, event)
				if err := deadLetters.RecordDeadLetter(ctx, runtimedeadletters.Record{
					OriginalEventID: event.ID(), OriginalEvent: string(event.Type()), OriginalPayload: event.Payload(),
					EntityID: headerID, FlowInstance: "runtime", HandlerNode: "fixture-node", Failure: failure,
					Timestamp: boundary.Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano),
				}); err != nil {
					t.Fatal(err)
				}
				row := storetest.DeadLetterObservationRow{OriginalEventID: event.ID(), StoredEntityID: headerID, HandlerNode: "fixture-node"}
				if i == 0 {
					row.PayloadEntityID = payloadID
				}
				want = append(want, row)
			}
			for _, probe := range []struct {
				entity string
				since  time.Time
				count  int
			}{
				{payloadID, boundary, 1},
				{headerID, boundary, 1},
				{payloadID, boundary.Add(time.Microsecond), 0},
				{headerID, boundary.Add(2 * time.Microsecond), 0},
				{unrelatedID, boundary, 0},
			} {
				if count, err := storetest.CountDeadLetterEntityRelationsSince(ctx, reader, probe.since, probe.entity); err != nil || count != probe.count {
					t.Fatalf("entity=%s since=%s count=%d want=%d err=%v", probe.entity, probe.since, count, probe.count, err)
				}
			}
			assertDeadLetter(t, reader, boundary.In(time.FixedZone("fixture-zone", 3600)), " "+payloadID+" ", true)
			assertDeadLetter(t, reader, boundary, unrelatedID, false)
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			rows, err := storetest.ReadDeadLetterObservationRows(ctx, reader)
			if err != nil || len(rows) != len(want) {
				t.Fatalf("diagnostic rows=%+v err=%v", rows, err)
			}
			for i := range want {
				if rows[i] != want[i] {
					t.Fatalf("diagnostic row[%d]=%+v want=%+v", i, rows[i], want[i])
				}
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("diagnostic read escaped selected coordinator: %+v", proof)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if rows, err := storetest.ReadDeadLetterObservationRows(canceled, reader); !errors.Is(err, context.Canceled) || rows != nil {
				t.Fatalf("canceled diagnostic read yielded rows=%+v err=%v", rows, err)
			}
			if count, err := storetest.CountDeadLetterEntityRelationsSince(canceled, reader, boundary, headerID); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("canceled relation yielded count=%d err=%v", count, err)
			}
			if _, err := storetest.ReadDeadLetterObservationRows(ctx, struct{}{}); err == nil {
				t.Fatal("foreign diagnostic owner accepted")
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if count, err := storetest.CountDeadLetterEntityRelationsSince(ctx, reader, boundary, headerID); err == nil || count != 0 {
				t.Fatalf("closed relation yielded count=%d err=%v", count, err)
			}
			if rows, err := storetest.ReadDeadLetterObservationRows(ctx, reader); err == nil || rows != nil {
				t.Fatalf("closed diagnostic yielded rows=%+v err=%v", rows, err)
			}
		})
	}
}
