package bus

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestSyntheticCarryProjectionIsRouteScopedForMixedDeliveries(t *testing.T) {
	evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	projection := mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "case-1"})
	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("validator"), PayloadProjection: projection})
	if err != nil {
		t.Fatalf("project synthetic route: %v", err)
	}
	unprojected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("auditor")})
	if err != nil {
		t.Fatalf("project ordinary route: %v", err)
	}
	if got := payloadStringField(t, projected.Event(), "validation_case_id"); got != "case-1" {
		t.Fatalf("projected validation_case_id = %q, want case-1", got)
	}
	if got := payloadStringField(t, unprojected.Event(), "validation_case_id"); got != "" {
		t.Fatalf("ordinary route leaked validation_case_id = %q", got)
	}
	if string(evt.Payload()) != `{"candidate":"acct-1"}` {
		t.Fatalf("journal payload mutated = %s", evt.Payload())
	}
}

func TestDeliveryRouteProjectionClearsJournalTargetForUntargetedLiveRecipient(t *testing.T) {
	want := events.RouteIdentity{FlowInstance: "validation/one", EntityID: "entity-1"}
	evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{want}), time.Now().UTC())

	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("validator")})
	if err != nil {
		t.Fatalf("project untargeted live route: %v", err)
	}
	if routes := projected.Event().TargetRoutes(); len(routes) != 0 {
		t.Fatalf("projected target routes = %#v, want targetless recipient view", routes)
	}
	if got := projected.JournalEvent().TargetRoutes(); len(got) != 1 || got[0] != want {
		t.Fatalf("journal target routes = %#v, want immutable projection %#v", got, want)
	}
}

func TestMixedRoutePlanUsesTargetSetForSingleTargetAndTargetlessDelivery(t *testing.T) {
	target := events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/one", EntityID: "entity-1"}
	evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan := newRoutePlan(evt)
	plan.AddDeliveryIntents(
		RoutePlanDeliveryIntent{Recipient: events.MustNodeDeliveryRecipient("worker"), TargetOwnership: events.MustExistingEntityTarget(target), Persist: true},
		RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient("observer"), AgentIdentity: testAgentRouteIdentity(t, "observer", ""), Persist: true},
	)

	projected, changed, err := resolveRoutePlanEventProjection(evt, plan)
	if err != nil {
		t.Fatalf("resolve mixed route projection: %v", err)
	}
	if !changed || !projected.TargetRoute().Empty() {
		t.Fatalf("mixed projection target = %#v changed=%t, want explicit target_set", projected.TargetRoute(), changed)
	}
	if routes := projected.TargetRoutes(); len(routes) != 1 || !events.SameRouteIdentity(routes[0], target) {
		t.Fatalf("mixed projection target_set = %#v, want %#v", routes, target)
	}
	untargeted, err := projectEventForDeliveryRoute(projected, plan.DeliveryRoutes()[1])
	if err != nil {
		t.Fatalf("project targetless mixed delivery: %v", err)
	}
	if untargeted.Event().HasTargetRoute() {
		t.Fatalf("targetless mixed delivery retained journal target: %#v", untargeted.Event().NormalizedEnvelope())
	}
}

func TestDeliveryRouteProjectionClearsJournalSingularTargetForTargetlessRoute(t *testing.T) {
	target := events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/one", EntityID: "entity-1"}
	evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())

	projected, err := projectEventForDeliveryRoute(evt, events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient("historical-agent"),
		AgentIdentity: testAgentRouteIdentity(t, "historical-agent", ""),
	})
	if err != nil {
		t.Fatalf("project historical targetless route: %v", err)
	}
	if projected.Event().HasTargetRoute() {
		t.Fatalf("targetless delivery projection = %#v, want no receiver target", projected.Event().NormalizedEnvelope())
	}
	if !events.SameRouteIdentity(projected.JournalEvent().TargetRoute(), target) {
		t.Fatalf("journal target = %#v, want immutable singular target %#v", projected.JournalEvent().TargetRoute(), target)
	}
}

func TestAllTargetlessRoutePlanClearsStaleEventTargets(t *testing.T) {
	targets := []events.RouteIdentity{
		{FlowID: "worker", FlowInstance: "worker/one", EntityID: "entity-1"},
		{FlowID: "worker", FlowInstance: "worker/two", EntityID: "entity-2"},
	}
	for _, test := range []struct {
		name     string
		envelope events.EventEnvelope
	}{
		{name: "singular target", envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, targets[0])},
		{name: "target set", envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, targets)},
	} {
		t.Run(test.name, func(t *testing.T) {
			evt := eventtest.RunCreatingRootIngress("projection-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", test.envelope, time.Now().UTC())
			plan := newRoutePlan(evt)
			plan.AddDeliveryIntents(RoutePlanDeliveryIntent{
				Recipient: events.MustAgentDeliveryRecipient("observer"), AgentIdentity: testAgentRouteIdentity(t, "observer", ""), Persist: true,
			})

			projected, changed, err := resolveRoutePlanEventProjection(evt, plan)
			if err != nil {
				t.Fatalf("resolve all-targetless route projection: %v", err)
			}
			if !changed || projected.HasTargetRoute() {
				t.Fatalf("all-targetless projection = %#v changed=%t, want cleared target facts", projected.NormalizedEnvelope(), changed)
			}
		})
	}
}

func TestRouteLessPlanClearsStaleEventTargets(t *testing.T) {
	target := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/one", EntityID: eventtest.UUID("route-less-stale-target"),
	}
	for _, test := range []struct {
		name     string
		envelope events.EventEnvelope
	}{
		{name: "singular", envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, target)},
		{name: "set", envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{target})},
	} {
		t.Run(test.name, func(t *testing.T) {
			evt := preparedProjectionEvent(test.name, test.envelope)
			projected, changed, err := resolveRoutePlanEventProjection(evt, newRoutePlan(evt))
			if err != nil {
				t.Fatalf("resolve route-less projection: %v", err)
			}
			if !changed || projected.HasTargetRoute() {
				t.Fatalf("route-less projection = %#v changed=%t, want absent target facts", projected.NormalizedEnvelope(), changed)
			}
		})
	}
}

func TestPreparedPublishEventValidateCanonicalTargetProjectionShapes(t *testing.T) {
	first := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/one", EntityID: eventtest.UUID("prepared-projection-first"),
	}.Normalized()
	second := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/two", EntityID: eventtest.UUID("prepared-projection-second"),
	}.Normalized()
	firstRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("worker-one"),
		Target:    events.MustExistingEntityTarget(first),
	}
	secondRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("worker-two"),
		Target:    events.MustExistingEntityTarget(second),
	}
	targetless := exactAgentDeliveryRoute(testAgentRouteIdentity(t, "observer", ""))

	for _, test := range []struct {
		name       string
		routes     []events.DeliveryRoute
		envelope   events.EventEnvelope
		noDelivery bool
	}{
		{name: "zero", noDelivery: true},
		{name: "all_targetless", routes: []events.DeliveryRoute{targetless}},
		{name: "singular", routes: []events.DeliveryRoute{firstRoute}, envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, first)},
		{name: "mixed_targetless", routes: []events.DeliveryRoute{firstRoute, targetless}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{first})},
		{name: "multiple", routes: []events.DeliveryRoute{firstRoute, secondRoute}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{first, second})},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared := preparedProjectionAggregate(t, test.name, test.envelope, test.routes, test.noDelivery)
			if err := prepared.Validate(); err != nil {
				t.Fatalf("validate canonical %s projection: %v", test.name, err)
			}
		})
	}
}

func TestPreparedPublishEventTargetSetProjectionIsDeterministicAcrossRouteOrder(t *testing.T) {
	first := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/a", EntityID: eventtest.UUID("prepared-order-first"),
	}.Normalized()
	second := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/z", EntityID: eventtest.UUID("prepared-order-second"),
	}.Normalized()
	firstRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("worker-a"),
		Target:    events.MustExistingEntityTarget(first),
	}
	secondRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("worker-z"),
		Target:    events.MustExistingEntityTarget(second),
	}

	var projections [][]events.RouteIdentity
	for _, routes := range [][]events.DeliveryRoute{
		{firstRoute, secondRoute},
		{secondRoute, firstRoute},
	} {
		projected, _, err := ResolvePreparedPublishEventTargetProjection(preparedProjectionEvent("deterministic", events.EventEnvelope{}), routes)
		if err != nil {
			t.Fatalf("resolve deterministic target set: %v", err)
		}
		projections = append(projections, projected.TargetRoutes())
	}
	for index, projection := range projections {
		if len(projection) != 2 || projection[0] != first || projection[1] != second {
			t.Fatalf("projection %d = %#v, want stable structured order [%#v %#v]", index, projection, first, second)
		}
	}
}

func TestPreparedPublishEventRejectsTargetProjectionShapeDrift(t *testing.T) {
	first := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/one", EntityID: eventtest.UUID("prepared-hostile-first"),
	}.Normalized()
	second := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/two", EntityID: eventtest.UUID("prepared-hostile-second"),
	}.Normalized()
	foreign := events.RouteIdentity{
		FlowID: "foreign", FlowInstance: "foreign/one", EntityID: eventtest.UUID("prepared-hostile-foreign"),
	}.Normalized()
	firstRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("worker-one"),
		Target:    events.MustExistingEntityTarget(first),
	}
	secondRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("worker-two"),
		Target:    events.MustExistingEntityTarget(second),
	}
	targetless := exactAgentDeliveryRoute(testAgentRouteIdentity(t, "observer", ""))

	for _, test := range []struct {
		name       string
		routes     []events.DeliveryRoute
		envelope   events.EventEnvelope
		noDelivery bool
	}{
		{name: "zero_with_singular", envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, first), noDelivery: true},
		{name: "all_targetless_with_set", routes: []events.DeliveryRoute{targetless}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{first})},
		{name: "singular_missing", routes: []events.DeliveryRoute{firstRoute}},
		{name: "singular_wrong_identity", routes: []events.DeliveryRoute{firstRoute}, envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, foreign)},
		{name: "singular_encoded_as_set", routes: []events.DeliveryRoute{firstRoute}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{first})},
		{name: "mixed_encoded_as_singular", routes: []events.DeliveryRoute{firstRoute, targetless}, envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, first)},
		{name: "multiple_wrong_cardinality", routes: []events.DeliveryRoute{firstRoute, secondRoute}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{first})},
		{name: "multiple_wrong_identity", routes: []events.DeliveryRoute{firstRoute, secondRoute}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{first, foreign})},
		{name: "multiple_noncanonical_order", routes: []events.DeliveryRoute{firstRoute, secondRoute}, envelope: events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{second, first})},
		{name: "multiple_encoded_as_singular", routes: []events.DeliveryRoute{firstRoute, secondRoute}, envelope: events.EnvelopeForTargetRoute(events.EventEnvelope{}, first)},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared := preparedProjectionAggregate(t, test.name, test.envelope, test.routes, test.noDelivery)
			err := prepared.Validate()
			if err == nil || !strings.Contains(err.Error(), "event target projection") {
				t.Fatalf("hostile projection error = %v, want aggregate target projection rejection", err)
			}
		})
	}
}

func TestPrepareSelectedForkPublishProjectsExactTargetedRoutes(t *testing.T) {
	const eventType = "worker/work.started"
	target := events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker/inst-1", EntityID: uuid.NewString(),
	}.Normalized()
	targetHandler := runtimepipeline.MustDeliveryTargetHandler("worker", "target-node")
	routeTable := newRouteTable(nil)
	routeTable.eventPath[eventType] = struct{}{}
	routeTable.routes[eventType] = []Subscriber{{
		Recipient:      events.MustNodeDeliveryRecipient("target-node"),
		Path:           "worker",
		LocalizedEvent: "work.started",
		handlerFlowID:  "worker",
		handlerNodeID:  "target-node",
		targetHandler:  targetHandler,
		routeSource:    subscriberRouteSourceSubscription,
	}}
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(target)
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(materializedTargetBundleWithHandler(
			"worker", "target-node", "work.started", existingOwnerHandlerFixture(),
		)),
		RouteTable: routeTable,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			return []DeliveryRouteBlueprint{{
				Recipient: events.MustNodeDeliveryRecipient("target-node"),
				Target:    target,
				Handler:   targetHandler.ForEvent("work.started"),
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("create selected-fork EventBus: %v", err)
	}
	lineage, err := events.NewSelectedForkLineage(
		uuid.NewString(), uuid.NewString(), uuid.NewString(), "selection:target-projection", "fork-task", "live",
	)
	if err != nil {
		t.Fatal(err)
	}
	evt := eventtest.SelectedForkReplay(
		uuid.NewString(), eventType, eventtest.Producer(events.EventProducerNode, "selected-node"), "fork-task",
		[]byte(`{"selected":true}`), 0, lineage, events.EventEnvelope{}, time.Now().UTC(),
	)
	prepared, err := eb.PrepareSelectedForkPublish(testAuthorActivityContext(context.Background()), evt)
	if err != nil {
		t.Fatalf("prepare selected-fork publication: %v", err)
	}
	t.Cleanup(func() { _ = eb.AbandonPreparedPublish(context.Background(), prepared) })

	request := prepared.CommitRequest()
	if got := request.Event.Event().TargetRoute().Normalized(); got != target {
		t.Fatalf("selected-fork event target = %#v, want %#v", got, target)
	}
	if err := request.ValidatePreparedEvent(); err != nil {
		t.Fatalf("validate selected-fork prepared aggregate: %v", err)
	}
}

func preparedProjectionAggregate(
	t testing.TB,
	label string,
	envelope events.EventEnvelope,
	routes []events.DeliveryRoute,
	noDelivery bool,
) PreparedPublishEvent {
	t.Helper()
	evt := preparedProjectionEvent(label, envelope)
	admitted, err := events.RevalidatePersistedEvent(evt)
	if err != nil {
		t.Fatalf("admit prepared projection event: %v", err)
	}
	settlement := exactSiblingDeliverySettlement(t)
	if noDelivery {
		ledger, err := events.NewConnectEvaluationLedger(nil)
		if err != nil {
			t.Fatalf("admit no-delivery ledger: %v", err)
		}
		settlement, err = events.NewNoDeliverySettlement(
			events.EventWriteNormalPublication,
			events.NoDeliveryDeclaredConsumerNoPlan,
			ledger,
		)
		if err != nil {
			t.Fatalf("admit no-delivery settlement: %v", err)
		}
	}
	return PreparedPublishEvent{Event: admitted, Settlement: settlement, DeliveryRoutes: routes}
}

func preparedProjectionEvent(label string, envelope events.EventEnvelope) events.Event {
	return eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType("prepared.projection."+strings.ReplaceAll(label, "_", ".")), "fixture", "", json.RawMessage(`{}`), 0,
		uuid.NewString(), "", envelope, time.Now().UTC(),
	)
}

func TestTargetlessReceiverViewClearsJournalTargetAcrossDispatchModes(t *testing.T) {
	for _, mode := range []string{"direct", "subscribed_post_commit"} {
		t.Run(mode, func(t *testing.T) {
			var store *targetRouteMemoryStore
			if mode == "subscribed_post_commit" {
				store = newTargetRouteMemoryStore()
			}
			bus, err := newScopedTestEventBus(store)
			if err != nil {
				t.Fatal(err)
			}
			token := testAgentLifecycleToken(t, "observer", "", 1, 1)
			deliveries := bus.ReplaceAgentRoute(
				token,
				testAgentSubscriptionAdmission(t, "observer", events.EventType("validation.requested")),
			)
			target := events.RouteIdentity{
				FlowID: "worker", FlowInstance: "worker/one", EntityID: uuid.NewString(),
			}
			evt := eventtest.RunCreatingRootIngress(
				uuid.NewString(), events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0,
				uuid.NewString(), "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC(),
			)
			route := exactAgentDeliveryRoute(token.Identity)
			dispatchEvent := evt

			switch mode {
			case "direct":
				err = bus.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"observer"}, []events.DeliveryRoute{route})
			case "subscribed_post_commit":
				dispatchEvent, err = events.ResolveEnvelope(evt, events.EnvelopeForBroadcast(evt.NormalizedEnvelope()))
				if err != nil {
					t.Fatalf("resolve canonical targetless journal event: %v", err)
				}
				store.events[evt.ID()] = dispatchEvent
				store.settlements[evt.ID()] = exactSiblingDeliverySettlement(t)
				store.routes[evt.ID()] = []events.DeliveryRoute{route}
				_, _, err = (engineDispatcher{bus: bus}).dispatchIntent(
					context.Background(), runtimeengine.EmitIntent{Event: dispatchEvent},
				)
			}
			if err != nil {
				t.Fatalf("%s delivery: %v", mode, err)
			}
			delivery := <-deliveries
			if delivery.Event().HasTargetRoute() {
				t.Fatalf("%s receiver target = %#v, want targetless route-local view", mode, delivery.Event().NormalizedEnvelope())
			}
			if err := delivery.Complete(); err != nil {
				t.Fatalf("complete %s delivery: %v", mode, err)
			}
			if mode == "direct" && !events.SameRouteIdentity(evt.TargetRoute(), target) {
				t.Fatalf("direct journal target = %#v, want immutable %#v", evt.TargetRoute(), target)
			}
			if mode == "subscribed_post_commit" && dispatchEvent.HasTargetRoute() {
				t.Fatalf("subscribed canonical journal target = %#v, want absent", dispatchEvent.NormalizedEnvelope())
			}
		})
	}
}

func TestRoutePlanTargetProjectionPreservesExplicitlyAbsentIngressSource(t *testing.T) {
	source, err := events.NewExternalIngressRoutingSource(
		"telegram-ingress", uuid.NewString(), events.RoutingSourceAuthorityProviderAdmissionPlan,
	)
	if err != nil {
		t.Fatalf("NewExternalIngressRoutingSource: %v", err)
	}
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		uuid.NewString(), events.EventType("inbound.telegram"), "telegram", "",
		json.RawMessage(`{"message":"hello"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, source, time.Now().UTC(),
	)
	target := events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver"}
	plan := newRoutePlan(evt)
	plan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		Recipient:       events.MustNodeDeliveryRecipient("receiver"),
		TargetOwnership: events.MustEntitylessReceiverTarget(target),
		Persist:         true,
	})

	projected, changed, err := resolveRoutePlanEventProjection(evt, plan)
	if err != nil {
		t.Fatalf("resolveRoutePlanEventProjection: %v", err)
	}
	if !changed || !events.SameRouteIdentity(projected.TargetRoute(), target) {
		t.Fatalf("projected target = %#v changed=%t, want %#v", projected.TargetRoute(), changed, target)
	}
	if !projected.SourceRoute().Empty() || projected.RoutingSource() != source {
		t.Fatalf("projected source = envelope:%#v typed:%#v, want absent envelope and immutable typed source %#v", projected.SourceRoute(), projected.RoutingSource(), source)
	}
}

func TestPayloadCarriesAreNotPersistedInDeliveryProjection(t *testing.T) {
	plan := mustInstanceKeyConnectRoutePlan(t, connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelect))
	projection, err := syntheticDeliveryPayloadProjection(plan, TemplateInstanceLifecycleDecision{
		Action:      templateInstanceLifecycleActionSelectedExisting,
		KeyMaterial: []runtimecontracts.TemplateInstanceKeyValue{{Field: mustBusTemplateInstanceField(t, "account_id"), Value: "acct-1"}},
	})
	if err != nil {
		t.Fatalf("syntheticDeliveryPayloadProjection: %v", err)
	}
	if !projection.Empty() {
		t.Fatalf("payload-owned select carry entered route projection: %#v", projection.Fields())
	}
}

func TestCreateSyntheticCarryFailsClosedOnDynamicPayloadCollisionBeforeHandler(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "validator")
	evt := eventtest.RunCreatingRootIngress("collision-event", events.EventType("validation.requested"), "", "", json.RawMessage(`{"validation_case_id":"producer-value"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{
		Recipient:         events.MustNodeDeliveryRecipient("validator"),
		Target:            events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "root"}),
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "synthetic-value"}),
	}
	err = eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"validator"}, []events.DeliveryRoute{route})
	if err == nil || !strings.Contains(err.Error(), "delivery payload projection conflicts with producer field") {
		t.Fatalf("delivery error = %v, want synthetic carry collision", err)
	}
	select {
	case delivered := <-ch:
		t.Fatalf("handler carrier received colliding event: %#v", delivered)
	default:
	}
}

func TestDeliveryRouteProjectionHasOneProductionOwner(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("validator"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "validation", FlowInstance: "validation/one", EntityID: "entity-1"}),
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": "case-1"}),
	}
	interceptor := &projectionCaptureInterceptor{}
	if _, _, _, err := eb.runNodeDeliveryRouteInterceptors(context.Background(), evt, []events.DeliveryRoute{route}, []DeliveryRouteInterceptor{interceptor}); err != nil {
		t.Fatalf("run route interceptor: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "validator")
	if err := eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"validator"}, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("deliver live route: %v", err)
	}
	live := <-ch
	if len(interceptor.events) != 1 || string(interceptor.events[0].Payload()) != string(live.Payload()) || interceptor.events[0].TargetRoute() != live.TargetRoute() {
		t.Fatalf("projector paths diverged: interceptor=%#v live=%#v", interceptor.events, live)
	}
}

type projectionCaptureInterceptor struct {
	events []events.Event
}

func (i *projectionCaptureInterceptor) InterceptDeliveryRoute(_ context.Context, evt events.DeliveryEvent, _ events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.events = append(i.events, evt.Event())
	return true, nil, runtimepipelineobligation.Continue(), nil
}

func payloadStringField(t *testing.T, evt events.Event, field string) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	value, _ := payload[field].(string)
	return value
}
