package cataloge2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

const (
	ordinaryTicketFlow  = "envelope/account/ticket"
	ordinaryAccountFlow = "envelope/account"
	ordinaryAuditFlow   = "envelope/account/bridge/audit"
)

type ticketFirstPublicationReader struct{ bus.PreparedPublishEventReader }

func (r ticketFirstPublicationReader) LoadPreparedPublishEvent(ctx context.Context, id string) (bus.PreparedPublishEvent, bool, error) {
	publication, found, err := r.PreparedPublishEventReader.LoadPreparedPublishEvent(ctx, id)
	if err != nil || !found {
		return publication, found, err
	}
	event := publication.Event.Event()
	if event.RunID() != catalogRuntimeRunID || !strings.HasSuffix(string(event.Type()), "/cross.requested") || event.RoutingSource().Route().FlowID != "envelope" {
		return publication, found, nil
	}
	routes := slices.Clone(publication.DeliveryRoutes)
	if len(routes) != 2 || !slices.ContainsFunc(routes, func(r events.DeliveryRoute) bool { return r.Target.Route().FlowID == ordinaryTicketFlow }) ||
		!slices.ContainsFunc(routes, func(r events.DeliveryRoute) bool { return r.Target.Route().FlowID == ordinaryAccountFlow }) {
		return bus.PreparedPublishEvent{}, false, fmt.Errorf("original request lacks its two exact native routes")
	}
	if routes[0].Target.Route().FlowID != ordinaryTicketFlow {
		routes[0], routes[1] = routes[1], routes[0]
	}
	publication.DeliveryRoutes = routes
	return publication, true, nil
}

func TestOrdinarySelectedForkExcludesCompletedRecipientBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyBoundCrossInstanceConnection(t)
			h := newRuntimeHarnessWithTerminalProvider(t, root, backend, true, nil, nil, nil, runlifecycle.LiveRunScope{FlowPath: "."}, func(deps *runtimepkg.RuntimeDeps) {
				deps.EventBusDurable.PreparedEvents = ticketFirstPublicationReader{deps.EventBusDurable.PreparedEvents}
			})
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			var selected crossInstanceSelectedForkStore = h.pg
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
			upstream, output := holdOrdinaryForkSibling(t, h, ctx)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: output})
			if err != nil || plan.ForkPoint.EventID != output {
				t.Fatalf("read original output's fixed cut: %+v / %v", plan.ForkPoint, err)
			}
			receiver := requireOrdinaryForkCapture(t, plan, upstream, output)
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, selected)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			request := forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: output,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig()), ExpectedBundleHash: selection.BundleHash,
			}
			frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source, ContractSelection: selection})
			if err != nil || len(frontier.FrontierEvents) != 2 ||
				!slices.ContainsFunc(frontier.FrontierEvents, func(e runfork.RunForkContractFrontierEvent) bool { return e.SourceEventID == upstream }) ||
				!slices.ContainsFunc(frontier.FrontierEvents, func(e runfork.RunForkContractFrontierEvent) bool { return e.SourceEventID == output }) {
				t.Fatalf("ordinary fork must select the two original publications: %+v / %v", frontier, err)
			}
			result, err := executeCatalogLiveFork(t, h, ctx, request)
			if err != nil || !result.Activation.Activated {
				t.Fatalf("ordinary fork did not reach execution: %+v / %v", result, err)
			}
			proveOrdinaryForkReceiver(t, h, ctx, loaded.Source, plan, result.Materialization.ForkRunID, upstream, output, receiver)
			proveOrdinaryEnvelopeRefork(t, h, ctx, selected, loaded.Source, request, result.Materialization.ForkRunID, upstream, output)
		})
	}
}

func proveOrdinaryEnvelopeRefork(t *testing.T, h *runtimeHarness, ctx context.Context, selected crossInstanceSelectedForkStore, source semanticview.Source, request forkexecution.SelectedContractExecutionRequest, run, originalRequest, originalOutput string) {
	t.Helper()
	request.SourceRunID, request.At = run, activityidentity.ForkLineageEventID(run, originalOutput)
	upstream := activityidentity.ForkLineageEventID(run, originalRequest)
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: run, At: request.At})
	if err != nil {
		t.Fatal(err)
	}
	var producer events.RouteIdentity
	for _, work := range plan.PendingWork {
		if work.EventID == upstream && work.Classification == runfork.RunForkPendingClassificationPending && work.DeliveryRoute.Target.Route().FlowID == ordinaryAccountFlow {
			producer = work.RoutingSource.Route()
		}
	}
	if producer.FlowID != "envelope" {
		t.Fatalf("refork cut does not contain the pending envelope publication: %+v", plan.ForkPoint)
	}
	audit := requireOrdinaryReforkCut(t, plan, upstream, request.At)
	receiver := audit.DeliveryRoute.Target.Route()
	fixed, entity, found, err := runforkadmission.FixedConstructionForRoute(source, plan, flowidentity.StoredRoute(receiver.FlowID, flowidentity.LogicalInstanceID(receiver.FlowInstance), receiver.FlowInstance))
	if err != nil || !found || entity.Fields["cross_count"] != int64(0) {
		t.Fatalf("refork cut must retain the exact audit at zero: found=%t count=%v error=%v", found, entity.Fields["cross_count"], err)
	}
	selection := request.ContractSelection
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: selection})
	if err != nil {
		t.Fatal(err)
	}
	witness, found, err := plan.PendingProducerConstructionWitness(producer, frontier)
	marker, hasMarker := plan.InheritedConstructionWitness()
	if err != nil || !found || !hasMarker || witness.CreatingEventID == marker.CreatingEventID || witness.Ancestor.ForkPoint != marker.Ancestor.ForkPoint {
		t.Fatalf("refork lacks distinct envelope and ticket witnesses at the same origin: envelope=%t ticket=%t / %v", found, hasMarker, err)
	}
	topology := func(plan runfork.RunForkPlan) runfork.RunForkSelectedContractRouteTopology {
		t.Helper()
		history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, ContractSelection: selection, FrontierAdmission: frontier})
		if err != nil {
			t.Fatal(err)
		}
		proof, err := forkexecution.BuildSelectedContractRouteTopology(forkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: history})
		if err != nil {
			t.Fatal(err)
		}
		return proof
	}
	proof := topology(plan)
	if !proof.DynamicTopologySupported || !hasOrdinaryConstructorProof(proof, producer.FlowInstance, witness.CreatingEventID) ||
		!hasOrdinaryConstructorProof(proof, plan.ForkPoint.RoutingSource.Route().FlowInstance, marker.CreatingEventID) {
		t.Fatalf("refork did not bind both exact ancestor proofs: %+v", proof)
	}
	markerOnly := ordinaryReforkMarkerOnly(t, plan, marker)
	without := topology(markerOnly)
	if without.DynamicTopologySupported || !hasOrdinaryConstructorProof(without, plan.ForkPoint.RoutingSource.Route().FlowInstance, marker.CreatingEventID) {
		t.Fatalf("missing outer witness did not leave just that proof unproved: %+v", without)
	}
	operation := catalogStagedForkOperation(t, ctx, selected, selection, run, request.At)
	request.ForkOperation = &operation
	staged := stageCatalogSelectedContractFork(t, ctx, selected, request.Owner, request.SourceLoader, selection, request.AgentRuntime, run, request.At, operation)
	recovery, found, err := selected.LoadRunForkSelectedContractRouteRecovery(ctx, staged.ForkRunID)
	if err != nil || !found || recovery.DynamicTopologyProofCount != len(proof.DynamicTopologyProofs) {
		t.Fatalf("staging did not retain the complete topology: found=%t / %v", found, err)
	}
	result, err := executeCatalogLiveFork(t, h, ctx, request)
	if err != nil || !result.Activation.Activated || result.Materialization.ForkRunID != staged.ForkRunID {
		t.Fatalf("staged envelope refork did not recover its child: %+v / %v", result, err)
	}
	recovered, found, err := selected.LoadRunForkSelectedContractRouteRecovery(ctx, staged.ForkRunID)
	if err != nil || !found || recovered.RouteTopologyFingerprint != recovery.RouteTopologyFingerprint {
		t.Fatalf("recovery changed the original topology binding: found=%t / %v", found, err)
	}
	for _, original := range []string{witness.CreatingEventID, marker.CreatingEventID} {
		projected := runfork.ProjectReplayEventID(staged.ForkRunID, runfork.ProjectReplayEventID(run, original))
		_, found, err := storetest.ReadCanonicalEventRecord(ctx, selected, projected)
		if err != nil || found {
			t.Fatalf("ancestor proof became a child publication: %s found=%t / %v", projected, found, err)
		}
		deliveries, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, selected, projected)
		if err != nil || len(deliveries) != 0 {
			t.Fatalf("ancestor proof became a child's delivery: %s count=%d / %v", projected, len(deliveries), err)
		}
	}
	projected, err := runfork.ProjectCapturedConstructionIdentity(run, staged.ForkRunID, fixed)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			logSelectedReceiverCountFailure(t, ctx, selected, plan, 1, staged.ForkRunID, projected)
		}
	}()
	proveOrdinaryForkReceiver(t, h, ctx, source, plan, staged.ForkRunID, upstream, request.At, receiver)
}

func requireOrdinaryReforkCut(t *testing.T, plan runfork.RunForkPlan, upstream, output string) runfork.RunForkPendingWork {
	t.Helper()
	if plan.ForkPoint.EventID != output {
		t.Fatalf("refork did not pin the first child's audit publication: %+v", plan.ForkPoint)
	}
	var sibling, audit runfork.RunForkPendingWork
	for _, work := range plan.PendingWork {
		if work.DeliveryID == "" || (work.EventID != upstream && work.EventID != output) {
			continue
		}
		if work.Classification != runfork.RunForkPendingClassificationPending || work.ClaimVersion != 0 {
			t.Fatalf("refork cut must precede these delivery claims: event=%s delivery=%s state=%s claim=%d", work.EventID, work.DeliveryID, work.Classification, work.ClaimVersion)
		}
		switch flow := work.DeliveryRoute.Target.Route().FlowID; {
		case work.EventID == upstream && flow == ordinaryAccountFlow && sibling.DeliveryID == "":
			sibling = work
		case work.EventID == output && flow == ordinaryAuditFlow && audit.DeliveryID == "":
			audit = work
		default:
			t.Fatalf("unexpected physical delivery on the refork cut: event=%s delivery=%s target=%s", work.EventID, work.DeliveryID, flow)
		}
	}
	t.Logf("refork cut revision=%d upstream=%s sibling=%s/%s output=%s receiver=%s/%s", plan.ForkPoint.Revision, upstream,
		sibling.DeliveryID, sibling.Classification, output, audit.DeliveryID, audit.Classification)
	if sibling.DeliveryID == "" || audit.DeliveryID == "" {
		t.Fatal("refork cut requires one pending sibling and one pending audit, with no ticket reporter")
	}
	return audit
}

func hasOrdinaryConstructorProof(topology runfork.RunForkSelectedContractRouteTopology, path, id string) bool {
	return slices.ContainsFunc(topology.DynamicTopologyProofs, func(proof runfork.RunForkSelectedContractDynamicTopologyProof) bool {
		return proof.FlowInstance == path && slices.Equal(proof.SourceEventIDs, []string{id}) && proof.FixedCutEvidenceFingerprint != ""
	})
}

func ordinaryReforkMarkerOnly(t *testing.T, plan runfork.RunForkPlan, marker runfork.InheritedConstructionWitness) runfork.RunForkPlan {
	t.Helper()
	ids, _ := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	var selectors []events.Event
	for _, id := range ids {
		event, found, err := plan.HistoricalRouteSelector(id)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			selectors = append(selectors, event)
		}
	}
	without, err := plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, selectors)
	if err != nil {
		t.Fatal(err)
	}
	without, err = without.WithInheritedConstructionWitness(marker)
	if err != nil {
		t.Fatal(err)
	}
	return without
}

func holdOrdinaryForkSibling(t *testing.T, h *runtimeHarness, ctx context.Context) (string, string) {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration(ordinaryAccountFlow, "receiver")
	if err != nil {
		t.Fatal(err)
	}
	held, release := make(chan string, 1), make(chan struct{})
	h.rt.Pipeline.SetTestWorkflowNodeHandlerStartHook(func(ctx context.Context, nodeID string, event events.Event) error {
		if nodeID != node.Key() || event.RunID() != catalogRuntimeRunID || !strings.HasSuffix(string(event.Type()), "/cross.requested") {
			return nil
		}
		select {
		case held <- event.ID():
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	published := make(chan error, 1)
	go func() {
		published <- h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "group.cross.requested", Payload: map[string]any{
			"envelope_id": "left", "source_account_id": "7", "ticket_id": "42", "target_account_id": "8",
		}}, catalogRuntimePublishTimeout, false)
	}()
	defer func() {
		close(release)
		if err := <-published; err != nil {
			t.Errorf("join original publication: %v", err)
		}
		h.rt.Pipeline.SetTestWorkflowNodeHandlerStartHook(nil)
	}()
	wait, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
	defer cancel()
	var upstream string
	select {
	case upstream = <-held:
	case <-wait.Done():
		t.Fatalf("source account sibling did not reach the existing handler hold: %v", wait.Err())
	}
	view, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range view {
		if event.SourceEventID == upstream && strings.HasSuffix(event.EventName, "/ticket.cross") {
			return upstream, event.EventID
		}
	}
	t.Fatalf("ticket-first schedule did not commit the output before the source sibling: upstream=%s", upstream)
	return "", ""
}

func requireOrdinaryForkCapture(t *testing.T, plan runfork.RunForkPlan, upstream, output string) events.RouteIdentity {
	t.Helper()
	var ticket, sibling, audit runfork.RunForkPendingWork
	for _, work := range plan.PendingWork {
		switch flow := work.DeliveryRoute.Target.Route().FlowID; {
		case work.EventID == upstream && flow == ordinaryTicketFlow:
			ticket = work
		case work.EventID == upstream && flow == ordinaryAccountFlow:
			sibling = work
		case work.EventID == output && flow == ordinaryAuditFlow:
			audit = work
		}
	}
	t.Logf("source revision=%d upstream=%s ticket=%s/%s sibling=%s/%s output=%s receiver=%s/%s",
		plan.ForkPoint.Revision, upstream, ticket.DeliveryID, ticket.Classification, sibling.DeliveryID, sibling.Classification, output, audit.DeliveryID, audit.Classification)
	if ticket.DeliveryID == "" || ticket.Classification != runfork.RunForkPendingClassificationDeliveredCompleted ||
		sibling.DeliveryID == "" || sibling.Classification != runfork.RunForkPendingClassificationPending && sibling.Classification != runfork.RunForkPendingClassificationInProgress ||
		audit.DeliveryID == "" || audit.Classification != runfork.RunForkPendingClassificationPending {
		t.Fatal("the deliberate fixed cut lacks one completed producer beside the unfinished sibling and output delivery")
	}
	return audit.DeliveryRoute.Target.Route()
}

func proveOrdinaryForkReceiver(t *testing.T, h *runtimeHarness, ctx context.Context, source semanticview.Source, plan runfork.RunForkPlan, child, upstream, output string, receiver events.RouteIdentity) {
	t.Helper()
	upstreamChild, outputChild := activityidentity.ForkLineageEventID(child, upstream), activityidentity.ForkLineageEventID(child, output)
	view, err := catalogRunScopedOperatorEvents(h, child)
	if err != nil {
		t.Fatal(err)
	}
	var outputs []string
	var regenerated string
	for _, event := range view {
		if strings.HasSuffix(event.EventName, "/ticket.cross") {
			outputs = append(outputs, event.EventID)
			if event.SourceEventID == upstreamChild {
				regenerated = event.EventID
			}
		}
	}
	instances, err := h.workflow.ListWorkflowInstances(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	var count any
	var target events.RouteIdentity
	fixed, _, found, err := runforkadmission.FixedConstructionForRoute(source, plan, flowidentity.StoredRoute(receiver.FlowID, flowidentity.LogicalInstanceID(receiver.FlowInstance), receiver.FlowInstance))
	if err != nil || !found {
		t.Fatalf("read original captured audit constructor: found=%t error=%v", found, err)
	}
	projected, err := runfork.ProjectCapturedConstructionIdentity(plan.SourceRunID, child, fixed)
	if err != nil {
		t.Fatal(err)
	}
	for _, instance := range instances {
		if instance.EntityID == projected.EntityID && instance.StorageRef == projected.InstancePath {
			count = instance.Fields["cross_count"]
			target = events.RouteIdentity{FlowID: projected.TemplateID, FlowInstance: projected.InstancePath, EntityID: projected.EntityID}
		}
	}
	upstreamDeliveries, outputDeliveries := view[upstreamChild].Deliveries, view[outputChild].Deliveries
	t.Logf("joined child upstream=%s deliveries=%+v output=%s deliveries=%+v ticket outputs=%v regenerated=%s audit cross_count=%v",
		upstreamChild, upstreamDeliveries, outputChild, outputDeliveries, outputs, regenerated, count)
	if len(upstreamDeliveries) != 1 || upstreamDeliveries[0].Status != "delivered" || upstreamDeliveries[0].Route.Target.Route().FlowID != ordinaryAccountFlow ||
		len(outputDeliveries) != 1 || outputDeliveries[0].Status != "delivered" || outputDeliveries[0].Route.Target.Route() != target ||
		len(outputs) != 1 || outputs[0] != outputChild || regenerated != "" || count != int64(1) {
		t.Fatal("ordinary fork must execute only the unfinished sibling and settle the original output once")
	}
}
