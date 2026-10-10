package cataloge2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogCausalObservationPreservesScopeOrderAndPayloadReferencesBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			at := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
			h := &runtimeHarness{t: t, ctx: ctx, backend: backend, startedAt: at,
				publishedIDs: map[string]struct{}{"ffffffff-ffff-4fff-8fff-ffffffffffff": {}}}
			var selected any
			var setup storetest.RunFixtureStore
			var closeOwner func() error
			if backend == catalogBackendPostgres {
				h.pg = storetest.StartPostgresRuntimeStore(t)
				selected, setup, closeOwner = h.pg, h.pg, h.pg.Close
				registerTestAuthorActivityCatalog(t, h.pg, "fixture.root", "fixture.child", "fixture.foreign", "fixture.history")
			} else {
				h.sqlite = storetest.StartSQLiteRuntimeStore(t)
				selected, setup, closeOwner = h.sqlite, h.sqlite, h.sqlite.Close
				registerTestAuthorActivityCatalog(t, h.sqlite, "fixture.root", "fixture.child", "fixture.foreign", "fixture.history")
			}
			foreignRun := uuid.NewString()
			for _, runID := range []string{catalogRuntimeRunID, foreignRun} {
				storetest.RequireRun(t, ctx, setup, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})
			}
			headerID, childHeaderID := uuid.NewString(), uuid.NewString()
			root := eventtest.ExistingRunRootIngress("ffffffff-ffff-4fff-8fff-ffffffffffff", "fixture.root", "", "",
				[]byte(`{"entity_id":" payload-root "}`), 0, catalogRuntimeRunID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, headerID), at)
			child := eventtest.ChildWithLineage("00000000-0000-4000-8000-000000000002", "fixture.child", "", "",
				[]byte(`{"entity_id":""}`), 0,
				events.EventLineage{RunID: catalogRuntimeRunID, ParentEventID: root.ID(), ExecutionMode: executionmode.Live},
				events.EnvelopeForEntityID(events.EventEnvelope{}, childHeaderID), at)
			foreign := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.foreign", "", "",
				[]byte(`{"entity_id":"foreign-payload"}`), 0, foreignRun,
				events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), at.Add(time.Second))
			history := eventtest.ExistingRunRootIngress(uuid.NewString(), "fixture.history", "", "", []byte(`{}`), 0, catalogRuntimeRunID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), at.Add(-time.Second))
			for _, event := range []events.Event{history, root, child, foreign} {
				storetest.CommitSemanticEvent(t, ctx, selected, event)
			}
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			rows := catalogEventsSince(t, reader, at)
			want := []catalogStoredEvent{
				{ID: child.ID(), Name: "fixture.child", SourceEventID: root.ID(), PayloadEntityID: childHeaderID},
				{ID: root.ID(), Name: "fixture.root", PayloadEntityID: "payload-root"},
				{ID: foreign.ID(), Name: "fixture.foreign", PayloadEntityID: "foreign-payload"},
			}
			if !reflect.DeepEqual(rows, want) {
				t.Fatalf("time cut, tie order, parent or payload/fallback projection changed: got=%+v want=%+v", rows, want)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("causal read escaped its original native coordinator: %+v", proof)
			}
			if got := catalogCausalEventIDs(t, reader, at, h.publishedIDs); !reflect.DeepEqual(got, map[string]struct{}{root.ID(): {}, child.ID(): {}}) {
				t.Fatalf("causal scope admitted an unrelated or historical event: %+v", got)
			}
			if got := catalogCausalEntityIDs(t, reader, at, h.publishedIDs, "anchor"); !reflect.DeepEqual(got, map[string]struct{}{"anchor": {}, "payload-root": {}, childHeaderID: {}}) {
				t.Fatalf("payload references were replaced by header authority or unrelated entities: %+v", got)
			}
			if !h.hasExpectedEmittedEvents(ctx, "anchor", []string{"fixture.child"}, "", nil) ||
				h.hasExpectedEmittedEvents(ctx, "anchor", []string{"fixture.child", "fixture.child"}, "", nil) ||
				h.hasExpectedEmittedEvents(ctx, "anchor", []string{"fixture.foreign"}, "", nil) {
				t.Fatal("emission wait lost multiplicity or sibling exclusion")
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if h.hasExpectedEmittedEvents(canceled, "anchor", []string{"fixture.child"}, "", nil) {
				t.Fatal("canceled emission wait received success evidence")
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if rows, err := storetest.ReadCausalEventStorageSince(ctx, reader, at); err == nil || rows != nil {
				t.Fatalf("closed read owner yielded causal evidence: %+v %v", rows, err)
			}
		})
	}
}

func TestCatalogCausalObservationRejectsForeignOwnership(t *testing.T) {
	for _, selected := range []any{nil, struct{}{}} {
		if rows, err := storetest.ReadCausalEventStorageSince(context.Background(), selected, time.Now().UTC()); err == nil || rows != nil {
			t.Fatalf("foreign owner %T yielded causal evidence: %+v %v", selected, rows, err)
		}
	}
}
