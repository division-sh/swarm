package apiv1

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestOperatorEventPublishAgentHeaderSelectorsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected canonicalEventPublishProofStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			root := canonicalrouting.CopyExplicitAgentSelectors(t)
			for _, flow := range []string{"review", "other"} {
				writeRunCompletionFixtureFile(t, filepath.Join(root, flow, "state.yaml"), "test_entity: {}\n")
			}
			bundle := loadEventPublicationConstructorFixture(t, root)
			source := semanticview.Wrap(bundle)
			opts := runStartTestEventBusOptions(source)
			bus, err := newScopedAPITestEventBus(t, selected, opts)
			if err != nil {
				t.Fatal(err)
			}
			handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
			hash := runStartTestBundleHashForSource(source)
			initial := rpcCall(t, handler, eventPublishBody("", hash, "work.requested", `{}`, "", "selector-initial"))
			if initial.Error != nil {
				t.Fatalf("construct genuine public run/tree: %#v", initial.Error)
			}
			created := asMap(t, initial.Result)
			runID := stringValue(t, created["run_id"], "run_id")
			creatingID := stringValue(t, created["event_id"], "event_id")
			ctx := correlation.WithRunID(testAuthorActivityContextForSource(context.Background(), opts.SourceArtifactFact), runID)
			index := selected.(pipeline.FlowInstanceIndexReader)
			for _, flow := range []string{"review", "other"} {
				t.Run(flow, func(t *testing.T) {
					path, local := flow, flow
					owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(flow, local, path))
					if err != nil {
						t.Fatal(err)
					}
					lookup, err := pipeline.NewExactFlowInstanceLookup(source, opts.SourceArtifactFact, owner)
					if err != nil {
						t.Fatal(err)
					}
					before, found, err := index.LookupFlowInstance(ctx, lookup)
					if err != nil || !found {
						t.Fatalf("read actually constructed selector: %t %v", found, err)
					}
					identity := before.Identity()
					body := eventPublishBodyWithTarget(runID, "", hash, "work.requested", `{}`, "", "selector-"+flow, path, identity.EntityID)
					published := rpcCall(t, handler, body)
					if published.Error != nil {
						t.Fatalf("public header selector refused: %#v", published.Error)
					}
					id := stringValue(t, asMap(t, published.Result)["event_id"], "selected event_id")
					readback, err := selected.LoadOperatorEvent(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					want := 1
					if flow == "review" {
						want = 2
					}
					if len(readback.Deliveries) != want {
						t.Fatalf("public selector included another receiver: %+v", readback.Deliveries)
					}
					for _, delivery := range readback.Deliveries {
						route := delivery.Route
						if route.Target.Route().FlowID != flow || route.Target.Route().FlowInstance != path {
							t.Fatalf("public readback crossed selected flow: %+v", route)
						}
						if route.Recipient.IsAgent() {
							if !route.Target.EntitylessReceiver() || route.Target.Route().EntityID != "" || route.Initialization.CreatingEventID() != creatingID || route.Initialization.ConstructionTarget().Route().EntityID != identity.EntityID {
								t.Fatalf("public selector donated header to actor or lost receipt: %+v", route)
							}
						} else if !route.Target.ExistingEntity() || route.Target.Route().EntityID != identity.EntityID {
							t.Fatalf("public mixed selector changed node ownership: %+v", route)
						}
					}
					duplicate := rpcCall(t, handler, body)
					if duplicate.Error != nil || !reflect.DeepEqual(duplicate.Result, published.Result) {
						t.Fatalf("public exact retry changed completion: %+v", duplicate)
					}
					after, found, err := index.LookupFlowInstance(ctx, lookup)
					if err != nil || !found || !reflect.DeepEqual(after.Identity(), before.Identity()) {
						t.Fatalf("public selector changed construction: %+v %v", after.Identity(), err)
					}
					hostile := rpcCall(t, handler, eventPublishBodyWithTarget(runID, "", hash, "work.requested", `{}`, "", "hostile-"+flow, path, uuid.NewString()))
					if hostile.Error == nil {
						t.Fatal("unrelated header entity authorized public delivery")
					}
					if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{readback.Deliveries[0].Route}); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
