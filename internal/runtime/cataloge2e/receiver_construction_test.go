package cataloge2e

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func requireExistingRootConstructionDependency(t *testing.T, h *runtimeHarness, event events.Event, route events.DeliveryRoute) {
	t.Helper()
	owner := flowidentity.RunScopedFlowInstance{RunID: event.RunID(), Route: flowidentity.StoredRoute(".", event.RunID(), event.RunID())}
	receipt, err := h.rt.Pipeline.LoadFlowConstructionPublication(catalogRunContext(h, event.RunID()), owner, event.RunID())
	if err != nil {
		t.Fatal(err)
	}
	want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: event.RunID(), EntityID: receipt.Identity.EntityID})
	if !events.SameDeliveryTargetOwnership(route.Initialization.ConstructionTarget(), want) ||
		route.Initialization.CreatingEventID() != receipt.CreatingInput.EventID ||
		!owner.MatchesAgentRoute(route.AgentIdentity) || route.Initialization.ReceiverRoute() != route.AgentIdentity.Route {
		t.Fatal("declared root receiver lost its exact original construction or borrowed child ownership")
	}
	if _, historical := route.Initialization.HistoricalConstruction(); historical {
		t.Fatal("native root construction was reinterpreted as historical")
	}
	if err := route.Initialization.ValidateEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := route.Initialization.ValidateRoute(route); err != nil {
		t.Fatal(err)
	}
}

func requireDeclaredAgentReceiverOwnership(t *testing.T, runID string, node, agent deliverylifecycle.Snapshot) {
	t.Helper()
	target := node.Route.Target.Route()
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(target.FlowID, flowidentity.LogicalInstanceID(target.FlowInstance), target.FlowInstance))
	if err != nil {
		t.Fatal(err)
	}
	static := events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: target.FlowID, FlowInstance: target.FlowInstance})
	if !node.Route.Target.ExistingEntity() || !events.SameDeliveryTargetOwnership(agent.Route.Target, static) || !owner.MatchesAgentRoute(agent.Route.AgentIdentity) {
		t.Fatalf("node and declaration-owned entityless agent disagree on exact flow: node=%s/%+v agent=%s/%+v identity=%+v", node.Route.Target.Code(), target, agent.Route.Target.Code(), agent.Route.Target.Route(), agent.Route.AgentIdentity)
	}
}

func requireReceiverConstructedBeforeDelivery(t *testing.T, h *runtimeHarness, runID string, snapshot deliverylifecycle.Snapshot) {
	t.Helper()
	if snapshot.Route.Target.Empty() || snapshot.StartedAt.IsZero() {
		t.Fatalf("delivery lacks an exact constructed receiver or execution timestamp: %+v", snapshot)
	}
	target := snapshot.Route.Target.Route()
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(target.FlowID, flowidentity.LogicalInstanceID(target.FlowInstance), target.FlowInstance))
	if err != nil {
		t.Fatal(err)
	}
	header, found, err := h.workflow.Load(catalogRunContext(h, runID), owner)
	if err != nil || !found {
		t.Fatalf("read exact constructed receiver: %v", err)
	}
	entityAgrees := header.EntityID == target.EntityID
	if snapshot.Route.Target.EntitylessReceiver() {
		entityAgrees = target.EntityID == "" && header.EntityID != ""
	}
	if !entityAgrees || header.WorkflowName != target.FlowID || header.CreatedAt.After(snapshot.StartedAt) {
		t.Fatalf("ordinary delivery preceded or contradicted receiver construction: target=%+v header=%s/%s/%s execution=%s", target, header.EntityID, header.WorkflowName, header.CreatedAt, snapshot.StartedAt)
	}
}
