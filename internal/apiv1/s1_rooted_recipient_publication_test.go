package apiv1

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestS1RootedRecipientPreviewPublicCreationAndReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var selected interface {
				canonicalEventPublishProofStore
				LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			bundle := eventPublishTargetRouteTestBundle(t)
			rooted, err := bundle.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			bus, err := newScopedAPITestEventBus(t, selected, runStartTestEventBusOptions(source))
			if err != nil {
				t.Fatal(err)
			}
			handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
			body := eventPublishBody("", rooted.Artifact().BundleHash(), "bootstrap.requested", `{"topic":"first"}`, "", "s1-rooted-preview")
			published := rpcCall(t, handler, body)
			if published.Error != nil {
				t.Fatalf("initial public publication: %#v", published.Error)
			}
			result := asMap(t, published.Result)
			runID := stringValue(t, result["run_id"], "run_id")
			eventID := stringValue(t, result["event_id"], "event_id")
			if result["new_run_created"] != true || len(asSlice(t, result["deliveries"])) != 1 {
				t.Fatalf("new public publication lost its root delivery: %#v", result)
			}
			assertEventPublishDeliveriesContain(t, asSlice(t, result["deliveries"]), "node", identitytest.RootNode(t, "bootstrap-node").Key(), "pending", 1)
			for _, phase := range []string{"initial", "fresh-publisher-replay", "conflicting-replay"} {
				switch phase {
				case "fresh-publisher-replay":
					fresh, err := newScopedAPITestEventBus(t, selected, runStartTestEventBusOptions(source))
					if err != nil {
						t.Fatal(err)
					}
					handler = eventPublishTestHandlerWithStores(t, selected, selected, selected, fresh, source)
					replayed := rpcCall(t, handler, body)
					if replayed.Error != nil {
						t.Fatalf("fresh publisher exact replay: %#v", replayed.Error)
					}
					replay := asMap(t, replayed.Result)
					if replay["event_id"] != eventID || replay["run_id"] != runID || len(asSlice(t, replay["deliveries"])) != 1 {
						t.Fatalf("exact replay changed committed identities or deliveries: %#v", replay)
					}
				case "conflicting-replay":
					conflict := rpcCall(t, handler, eventPublishBody("", rooted.Artifact().BundleHash(), "bootstrap.requested", `{"topic":"changed"}`, "", "s1-rooted-preview"))
					if conflict.Error == nil || asMap(t, conflict.Error.Data)["code"] != IdempotencyConflictCode {
						t.Fatalf("changed replay gained publication authority: %#v", conflict)
					}
				}
				retained, err := selected.LoadRunRootedSource(ctx, runID)
				if err != nil || !retained.Admission().Equal(rooted.Admission()) || retained.Artifact().BundleHash() != rooted.Artifact().BundleHash() {
					t.Fatalf("%s changed the frozen run source: source=%+v err=%v", phase, retained, err)
				}
				cardinality, err := storetest.ReadAPIFlowPublicationRefusalStorage(ctx, selected)
				if err != nil || cardinality.Runs != 1 || cardinality.Events != 1 || cardinality.APICompletions != 1 {
					t.Fatalf("%s duplicated committed work: counts=%+v err=%v", phase, cardinality, err)
				}
				if deliveries := countAllEventDeliveries(t, selected); deliveries != 1 {
					t.Fatalf("%s duplicated committed delivery: %d", phase, deliveries)
				}
			}
		})
	}
}

func TestS1RootedRunStartRecipientPreviewPublicCreationAndReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var selected interface {
				canonicalEventPublishProofStore
				LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			bundle := eventPublishTargetRouteTestBundle(t)
			rooted, err := bundle.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			runID := uuid.NewString()
			body := runStartBody(runID, rooted.Artifact().BundleHash(), "bootstrap.requested", `{"topic":"first"}`, "s1-rooted-run-start")
			var initialEventID string
			for _, phase := range []string{"initial", "fresh-publisher-replay"} {
				bus, err := newScopedAPITestEventBus(t, selected, runStartTestEventBusOptions(source))
				if err != nil {
					t.Fatal(err)
				}
				handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
				response := rpcCall(t, handler, body)
				if response.Error != nil {
					t.Fatalf("%s run.start: %#v", phase, response.Error)
				}
				result := asMap(t, response.Result)
				if result["run_id"] != runID || result["status"] != "running" {
					t.Fatalf("%s lost the created run: %#v", phase, result)
				}
				retained, err := selected.LoadRunRootedSource(ctx, runID)
				if err != nil || !retained.Admission().Equal(rooted.Admission()) {
					t.Fatalf("%s changed the frozen run source: source=%+v err=%v", phase, retained, err)
				}
				eventID, err := storetest.ReadLatestNamedEventIdentityStorage(ctx, selected, "bootstrap.requested", "")
				if err != nil || eventID == "" || initialEventID != "" && initialEventID != eventID {
					t.Fatalf("%s lost or replaced the committed event: initial=%q got=%q err=%v", phase, initialEventID, eventID, err)
				}
				initialEventID = eventID
				cardinality, err := storetest.ReadAPIFlowPublicationRefusalStorage(ctx, selected)
				if err != nil || cardinality.Runs != 1 || cardinality.Events != 1 || cardinality.APICompletions != 1 || countAllEventDeliveries(t, selected) != 1 {
					t.Fatalf("%s duplicated committed work: counts=%+v err=%v", phase, cardinality, err)
				}
			}
		})
	}
}
