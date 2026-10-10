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

func TestCatalogPublishedDeadLetterCountUsesExactOriginalEventBothStores(t *testing.T) {
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
			entityID := uuid.NewString()
			first := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.deadletter", "", "", []byte(`{}`), 0,
				catalogRuntimeRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			second := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.deadletter", "", "", []byte(`{}`), 0,
				catalogRuntimeRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			for _, event := range []events.Event{first, second} {
				storetest.CommitSemanticEvent(t, ctx, selected, event)
			}
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			assertPublishedEventDeadLetter(t, reader, map[string]struct{}{first.ID(): {}}, false)
			failure := runtimefailures.FromError(errors.New("observed source failure"), "cataloge2e", "deadletter_count").Failure
			if err := deadLetters.RecordDeadLetter(ctx, runtimedeadletters.Record{
				OriginalEventID: first.ID(), OriginalEvent: string(first.Type()), OriginalPayload: first.Payload(),
				EntityID: entityID, FlowInstance: "runtime", Failure: failure, Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			if count, err := storetest.CountDeadLettersForOriginalEvent(ctx, reader, first.ID()); err != nil || count != 1 {
				t.Fatalf("original-event count=%d err=%v", count, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("dead-letter observation escaped native read ownership: %+v", proof)
			}
			assertPublishedEventDeadLetter(t, reader, map[string]struct{}{" " + first.ID() + " ": {}}, true)
			assertPublishedEventDeadLetter(t, reader, map[string]struct{}{second.ID(): {}}, false)
			assertPublishedEventDeadLetter(t, reader, map[string]struct{}{}, false)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if count, err := storetest.CountDeadLettersForOriginalEvent(canceled, reader, first.ID()); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("canceled read yielded count=%d err=%v", count, err)
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if count, err := storetest.CountDeadLettersForOriginalEvent(ctx, reader, first.ID()); err == nil || count != 0 {
				t.Fatalf("closed read owner yielded count=%d err=%v", count, err)
			}
		})
	}
}
