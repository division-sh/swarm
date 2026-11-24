package cataloge2e

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

type c53ReforkStore interface {
	forkexecution.SelectedContractForkLifecycle
	forkexecution.SourceArtifactSelectedContractSourceStore
	storetest.RootedDataCatalogStore
	bus.PreparedPublishEventReader
	deliverylifecycle.Store
}

func TestWrappedC53ReforkControlBothStores(t *testing.T) {
	runWrappedC53ReforkBothStores(t, func(t *testing.T, h *runtimeHarness, ctx context.Context, _ c53ReforkStore, _ semanticview.Source, _ runfork.RunForkPlan, request forkexecution.SelectedContractExecutionRequest) (forkexecution.SelectedContractExecutionResult, error) {
		return executeCatalogLiveFork(t, h, ctx, request)
	})
}

func runWrappedC53ReforkBothStores(t *testing.T, refork func(*testing.T, *runtimeHarness, context.Context, c53ReforkStore, semanticview.Source, runfork.RunForkPlan, forkexecution.SelectedContractExecutionRequest) (forkexecution.SelectedContractExecutionResult, error)) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := wrappedC53ReforkFixture(t)
			h := newLiveForkCatalogHarness(t, root, backend)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			var selected c53ReforkStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			for _, envelope := range []string{"left", "right"} {
				for _, account := range []string{"7", "8"} {
					h.publishAndWait(catalogTriggerStep{Event: "group.opened", Payload: map[string]any{
						"envelope_id": envelope, "account_id": account, "ticket_id": "42",
					}}, catalogRuntimePublishTimeout)
				}
			}
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			h.publishAndWait(catalogTriggerStep{Event: "group.cross.requested", Payload: map[string]any{
				"envelope_id": "left", "source_account_id": "7", "ticket_id": "42", "target_account_id": "8",
			}}, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			view, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			sourceRun, cut := catalogRuntimeRunID, ""
			var target events.RouteIdentity
			for _, event := range view {
				if !strings.HasSuffix(event.EventName, "/ticket.cross") {
					continue
				}
				if cut != "" || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
					t.Fatalf("control did not settle its one original cross-account route: %+v", event)
				}
				cut, target = event.EventID, event.Deliveries[0].Route.Target.Route()
			}
			if cut == "" || target.FlowID != "envelope/account" {
				t.Fatalf("control lacks its native C53 publication: target=%+v", target)
			}
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, selected)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			origin := requireC53CreatingEvent(t, ctx, selected, loaded.Source, sourceRun, cut)
			for hop := 0; hop < 2; hop++ {
				plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: cut})
				if err != nil {
					t.Fatalf("plan hop=%d: %v", hop, err)
				}
				if plan.ForkPoint.RoutingSource.Route().FlowID != "envelope/account/ticket" || plan.ForkPoint.EventID != cut {
					t.Fatalf("control lacks its exact keyed producer: hop=%d cut=%+v", hop, plan.ForkPoint)
				}
				request := forkexecution.SelectedContractExecutionRequest{
					SourceRunID: sourceRun, At: cut, Owner: owner, SourceLoader: loader, ContractSelection: selection,
					AgentRuntime: options, ExpectedBundleHash: selection.BundleHash,
				}
				execute := executeCatalogLiveFork
				if hop == 0 {
					result, err := execute(t, h, ctx, request)
					requireC53ReforkResult(t, result, err, hop)
					sourceRun = result.Materialization.ForkRunID
				} else {
					result, err := refork(t, h, ctx, selected, loaded.Source, plan, request)
					requireC53ReforkResult(t, result, err, hop)
					sourceRun = result.Materialization.ForkRunID
				}
				cut = activityidentity.ForkLineageEventID(sourceRun, cut)
				origin = runfork.ProjectReplayEventID(sourceRun, origin)
				requireC53SettledDelivery(t, ctx, selected, cut, target)
				child, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: cut})
				if err != nil {
					t.Fatal(err)
				}
				ids, known := child.HistoricalEventIDs(child.ForkPoint.Revision)
				_, found, err := storetest.ReadCanonicalEventRecord(ctx, selected, origin)
				if !known || slices.Contains(ids, origin) || err != nil || found {
					t.Fatalf("child gained its ancestor's creating publication: hop=%d found=%t err=%v", hop, found, err)
				}
				producer := child.ForkPoint.RoutingSource.Route()
				_, state, found, err := runforkadmission.FixedConstructionForRoute(loaded.Source, child, flowidentity.StoredRoute(producer.FlowID, flowidentity.LogicalInstanceID(producer.FlowInstance), producer.FlowInstance))
				if err != nil || !found || state.MaterializationMetadata.HistoricalConstruction == nil || state.MaterializationMetadata.HistoricalConstruction.SourceRunID != plan.SourceRunID {
					t.Fatalf("child lost its inherited producer custody: hop=%d found=%t err=%v", hop, found, err)
				}
				for _, work := range child.PendingWork {
					if work.EventID == origin {
						t.Fatalf("child gained an ancestor delivery: hop=%d work=%+v", hop, work)
					}
				}
			}
		})
	}
}

func requireC53ReforkResult(t *testing.T, result forkexecution.SelectedContractExecutionResult, err error, hop int) {
	t.Helper()
	if err != nil || !result.Activation.Activated || result.ExecutedEventCount < 1 {
		t.Fatalf("refork control: hop=%d activated=%t materialized=%t executed=%d error=%v",
			hop, result.Activation.Activated, result.Materialization.ForkRunID != "", result.ExecutedEventCount, err)
	}
}

func requireC53CreatingEvent(t *testing.T, ctx context.Context, selected c53ReforkStore, source semanticview.Source, run, cut string) string {
	t.Helper()
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: run, At: cut})
	if err != nil {
		t.Fatal(err)
	}
	route := plan.ForkPoint.RoutingSource.Route()
	_, entity, found, err := runforkadmission.FixedConstructionForRoute(source, plan, flowidentity.StoredRoute(route.FlowID, flowidentity.LogicalInstanceID(route.FlowInstance), route.FlowInstance))
	if err != nil || !found {
		t.Fatalf("missing original ticket construction: found=%t error=%v", found, err)
	}
	meta := entity.MaterializationMetadata
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(meta.InitialMaterialization, run, entity.EntityID, meta.FlowInstance, meta.FlowTemplate)
	if err != nil || receipt.CreatingInput.Input != "opened" {
		t.Fatalf("original ticket lacks its native opening: %v", err)
	}
	id := receipt.CreatingInput.EventID
	_, found, err = plan.HistoricalRouteSelector(id)
	if err != nil || !found {
		t.Fatalf("original creating selector: found=%t error=%v", found, err)
	}
	for _, work := range plan.PendingWork {
		if work.EventID == id && work.DeliveryRoute.Target.Route() == route &&
			work.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
			return id
		}
	}
	t.Fatal("original ticket opening did not settle")
	return ""
}

func requireC53SettledDelivery(t *testing.T, ctx context.Context, selected c53ReforkStore, cut string, target events.RouteIdentity) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
	defer cancel()
	for {
		publication, found, err := selected.LoadPreparedPublishEvent(wait, cut)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if len(publication.DeliveryRoutes) != 1 || publication.DeliveryRoutes[0].Target.Route() != target {
				t.Fatalf("control changed its selected receiver: routes=%+v", publication.DeliveryRoutes)
			}
			id, err := deliverylifecycle.DeliveryID(cut, publication.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			row, err := selected.Snapshot(wait, id)
			if err != nil || row.Terminal() && row.Status != deliverylifecycle.StatusDelivered {
				t.Fatalf("control delivery did not settle: status=%s error=%v", row.Status, err)
			}
			if row.Status == deliverylifecycle.StatusDelivered {
				return
			}
		}
		select {
		case <-wait.Done():
			t.Fatalf("control delivery wait: %v", wait.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func wrappedC53ReforkFixture(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyWrappedLexicalAddressing(t, false)
	edits := []struct{ file, old, replacement string }{
		{"schema.yaml", "inputs: [opened, root.audit, cross.requested]", "instance: envelope_id\ninputs: [opened, cross.requested, group.opened, group.cross.requested]\noutputs: [opened, cross.requested]"},
		{"schema.yaml", "  - {event: root.audit, from: ., to: account, resolution: select}\n", ""},
		{"schema.yaml", "  - {event: root.audit, from: ., to: account/audit}\n", ""},
		{"events.yaml", "root.audit:\n  account_id: text\n", ""},
		{"account/schema.yaml", "opened, root.audit, cross.forwarded", "opened, cross.forwarded"},
		{"account/nodes.yaml", "  root.audit: {}\n", ""},
		{"account/audit/schema.yaml", "ticket.done, root.audit", "ticket.done"},
		{"account/audit/nodes.yaml", "  root.audit: {}\n", ""},
	}
	for _, edit := range edits {
		path := filepath.Join(root, "envelope", edit.file)
		raw, err := os.ReadFile(path)
		if err != nil || strings.Count(string(raw), edit.old) != 1 {
			t.Fatalf("wrapped control %s: expected its exact declaration, read error=%v", edit.file, err)
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(raw), edit.old, edit.replacement, 1)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for name, value := range map[string]string{
		"schema.yaml": `inputs: [group.opened, group.cross.requested]
connect:
  - {event: group.opened, from: ., to: envelope, resolution: select-or-create}
  - {event: group.cross.requested, from: ., to: envelope, resolution: select}
`,
		"events.yaml": `group.opened:
  envelope_id: text
  account_id: text
  ticket_id: text
group.cross.requested:
  envelope_id: text
  source_account_id: text
  ticket_id: text
  target_account_id: text
`,
		"envelope/state.yaml": "envelope_id: text\n",
		"envelope/nodes.yaml": `forward:
  group.opened:
    emit: {event: opened, fields: {account_id: payload.account_id, ticket_id: payload.ticket_id}}
  group.cross.requested:
    emit: {event: cross.requested, fields: {source_account_id: payload.source_account_id, ticket_id: payload.ticket_id, target_account_id: payload.target_account_id}}
`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
