package bus

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type targetOwnerArcSourceKind uint8

const (
	targetOwnerArcRoot targetOwnerArcSourceKind = iota + 1
	targetOwnerArcStatic
	targetOwnerArcSingleton
	targetOwnerArcTemplate
	targetOwnerArcFlowOwnedControl
	targetOwnerArcSourceKindCount
)

type targetOwnerArcCase struct {
	name         string
	sourceKind   targetOwnerArcSourceKind
	sourcePath   string
	receiverMode string
	receiverPath string
	shared       bool
	existing     bool
	producerType events.EventProducerType
	sourceEntity string
}

func TestEventBusCrossFlowMaterializingTargetOwnershipMatrix(t *testing.T) {
	tests := []targetOwnerArcCase{
		{name: "root node to direct singleton", sourceKind: targetOwnerArcRoot, receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "child", producerType: events.EventProducerNode},
		{name: "static child node to nested singleton", sourceKind: targetOwnerArcStatic, sourcePath: "left/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "left/worker/result", producerType: events.EventProducerNode},
		{name: "singleton child node to nested singleton", sourceKind: targetOwnerArcSingleton, sourcePath: "left/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "left/worker/result", producerType: events.EventProducerNode},
		{name: "singleton child agent to nested singleton", sourceKind: targetOwnerArcSingleton, sourcePath: "left/agent", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "left/agent/result", producerType: events.EventProducerAgent},
		{name: "concrete template node to nested singleton", sourceKind: targetOwnerArcTemplate, sourcePath: "right/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "right/worker/result", producerType: events.EventProducerNode},
		{name: "concrete template agent to nested singleton", sourceKind: targetOwnerArcTemplate, sourcePath: "right/agent", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "right/agent/result", producerType: events.EventProducerAgent},
		{name: "flow owned control to nested singleton", sourceKind: targetOwnerArcFlowOwnedControl, sourcePath: "control/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "control/worker/result"},
	}
	covered := make(map[targetOwnerArcSourceKind]bool, len(tests))
	for _, test := range tests {
		covered[test.sourceKind] = true
	}
	if len(covered) != int(targetOwnerArcSourceKindCount-1) {
		t.Fatalf("source-kind matrix covers %d variants, want %d", len(covered), targetOwnerArcSourceKindCount-1)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTargetOwnerArc(t, test)
		})
	}
}

func TestEventBusNestedCrossFlowTargetOwnershipMatrix(t *testing.T) {
	for _, test := range []targetOwnerArcCase{
		{name: "static parent to nested singleton", sourceKind: targetOwnerArcStatic, sourcePath: "left/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "left/worker/result"},
		{name: "singleton parent to nested static shares exact owner", sourceKind: targetOwnerArcSingleton, sourcePath: "left/worker", receiverMode: runtimecontracts.FlowModeStatic, receiverPath: "left/worker/result", shared: true},
		{name: "singleton parent to nested singleton is distinct", sourceKind: targetOwnerArcSingleton, sourcePath: "left/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "left/worker/result"},
		{name: "existing singleton owner wins", sourceKind: targetOwnerArcStatic, sourcePath: "left/source", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "left/existing", existing: true},
		{name: "concrete template to nested singleton is distinct", sourceKind: targetOwnerArcTemplate, sourcePath: "right/worker", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "right/worker/result"},
		{name: "sibling repeated leaf uses full path", sourceKind: targetOwnerArcSingleton, sourcePath: "left/worker/result", receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "right/worker/result", sourceEntity: runtimeflowidentity.EntityID("unrelated/worker/result")},
		{name: "nested child to distinct static owner", sourceKind: targetOwnerArcSingleton, sourcePath: "left/worker/result", receiverMode: runtimecontracts.FlowModeStatic, receiverPath: "archive/worker/result", existing: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runTargetOwnerArc(t, test)
		})
	}
}

func TestNestedChildToConcreteTemplateReceiverUsesSelectedOwner(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelect)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("template selection source does not expose its semantic bundle")
	}
	producer, ok := bundle.FlowViewByID("producer")
	if !ok {
		t.Fatal("template selection source is missing producer flow")
	}
	producer.Path = "left/child/producer"
	source = semanticview.Wrap(bundle)
	selectedEntityID := eventtest.UUID("nested-template-selected-owner")
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID: "one", EntityID: selectedEntityID, FlowInstance: "account/one", FlowTemplate: "account",
				AddressFields: map[string]string{"entity.account_id": "acct-1"},
			}},
		},
	}
	interceptor := &connectRoutePlanNodeInterceptor{}
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source, TemplateInstanceActivator: store.Activate, Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("create EventBus: %v", err)
	}
	store.bus = eventBus
	if err := eventBus.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", "one")}); err != nil {
		t.Fatalf("add selected template route: %v", err)
	}
	runID := uuid.NewString()
	sourceRoute := events.RouteIdentity{
		FlowID: "producer", FlowInstance: "left/child/producer", EntityID: eventtest.UUID("nested-template-source-owner"),
	}.Normalized()
	routingSource, err := events.NewStaticFlowRoutingSource(sourceRoute)
	if err != nil {
		t.Fatalf("construct nested source: %v", err)
	}
	event := eventtest.ChildForProducerWithRoutingSource(
		uuid.NewString(), events.EventType("left/child/producer/account.ready"), eventtest.Producer(events.EventProducerNode, "producer-node"), "",
		[]byte(`{"account_id":"acct-1"}`), 0,
		events.EventLineage{RunID: runID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), routingSource, time.Now().UTC(),
	)
	want := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "account", FlowInstance: "account/one", EntityID: selectedEntityID,
	})
	plan, err := eventBus.CheckPublishRecipientPlan(context.Background(), event)
	if err != nil {
		t.Fatalf("preflight nested-to-template delivery: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
		t.Fatalf("nested-to-template preflight = failure:%q routes:%#v, want account/one selected owner", plan.TargetFailure, plan.DeliveryRoutes)
	}
	if sourceRoute.EntityID == want.Route().EntityID {
		t.Fatal("nested source and selected template owner must remain distinguishable")
	}
	if err := eventBus.Publish(context.Background(), event); err != nil {
		t.Fatalf("publish nested-to-template delivery: %v", err)
	}
	if routes := store.routes[event.ID()]; len(routes) != 1 || routes[0].Target != want || !routes[0].ConnectClaim.Equal(plan.DeliveryRoutes[0].ConnectClaim) {
		t.Fatalf("persisted nested-to-template route = %#v, want exact preflight target and claim", routes)
	}
	if interceptor.Count() != 1 || len(store.activations) != 0 {
		t.Fatalf("handler executions/activations = %d/%d, want selected execution without creation", interceptor.Count(), len(store.activations))
	}
}

func TestNestedChildToRootReceiverUsesSelectedOwner(t *testing.T) {
	source := connectRoutePlanSingletonProducerRootReceiverSource()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("root-return source does not expose its semantic bundle")
	}
	scout, ok := bundle.FlowViewByID("scout")
	if !ok {
		t.Fatal("root-return source is missing scout flow")
	}
	scout.Path = "left/child/scout"
	source = semanticview.Wrap(bundle)
	runID := uuid.NewString()
	rootEntityID := eventtest.UUID("nested-root-selected-owner")
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowInstance: runID, EntityID: rootEntityID})
	interceptor := &connectRoutePlanNodeInterceptor{}
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source, Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("create EventBus: %v", err)
	}
	sourceRoute := events.RouteIdentity{
		FlowID: "scout", FlowInstance: "left/child/scout", EntityID: eventtest.UUID("nested-root-source-owner"),
	}.Normalized()
	routingSource, err := events.NewStaticFlowRoutingSource(sourceRoute)
	if err != nil {
		t.Fatalf("construct nested root-return source: %v", err)
	}
	event := eventtest.ChildForProducerWithRoutingSource(
		uuid.NewString(), events.EventType("left/child/scout/scout.completed"), eventtest.Producer(events.EventProducerNode, "scout-worker"), "",
		[]byte(`{"proof":"nested-root-return"}`), 0,
		events.EventLineage{RunID: runID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), routingSource, time.Now().UTC(),
	)
	want := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: rootEntityID,
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
	if err != nil {
		t.Fatalf("preflight nested root return: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
		t.Fatalf("nested root-return preflight = failure:%q routes:%#v, want exact root owner", plan.TargetFailure, plan.DeliveryRoutes)
	}
	if sourceRoute.EntityID == rootEntityID {
		t.Fatal("nested source and root receiver owner must remain distinguishable")
	}
	if err := eventBus.Publish(ctx, event); err != nil {
		t.Fatalf("publish nested root return: %v", err)
	}
	if routes := store.routes[event.ID()]; len(routes) != 1 || routes[0].Target != want || !routes[0].ConnectClaim.Equal(plan.DeliveryRoutes[0].ConnectClaim) {
		t.Fatalf("persisted nested root-return route = %#v, want exact preflight target and claim", routes)
	}
	if interceptor.Count() != 1 {
		t.Fatalf("root receiver executions = %d, want 1", interceptor.Count())
	}
}

func runTargetOwnerArc(t *testing.T, test targetOwnerArcCase) {
	t.Helper()
	runID := uuid.NewString()
	source, eventType, sourceRoute, routingSource, currentOwner := targetOwnerArcFixture(t, test, runID)
	store := newTargetRouteMemoryStore()
	interceptor := &connectRoutePlanNodeInterceptor{}
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source,
		Interceptors:   []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("create EventBus: %v", err)
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("source-node"),
		Target:    currentOwner,
	})
	eventID := uuid.NewString()
	envelope := events.EventEnvelope{}
	if test.sourceKind != targetOwnerArcRoot {
		envelope = events.EnvelopeForSourceRoute(envelope, sourceRoute)
	}
	event := targetOwnerArcEvent(t, test, eventID, eventType, runID, envelope, routingSource)
	wantRoute := events.RouteIdentity{
		FlowID: "receiver", FlowInstance: test.receiverPath,
		EntityID: runtimeflowidentity.EntityID(test.receiverPath),
	}.Normalized()
	var wantOwner events.DeliveryTargetOwnership
	if test.existing {
		store.setTargetOwnerRoutes(wantRoute)
		wantOwner = events.MustExistingEntityTarget(wantRoute)
	} else if test.shared {
		wantRoute.EntityID = sourceRoute.EntityID
		wantOwner = events.MustExistingEntityTarget(wantRoute)
	} else {
		wantOwner = events.MustMaterializingEntityTarget(wantRoute)
	}
	if !test.shared && sourceRoute.EntityID == wantRoute.EntityID {
		t.Fatalf("source and receiver identities are not distinguishable: %#v", sourceRoute)
	}

	preflight, err := eventBus.CheckPublishRecipientPlan(ctx, event)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one exact receiver route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	assertTargetOwnerArcRoute(t, preflight.DeliveryRoutes[0], wantOwner)
	preflightRoute := preflight.DeliveryRoutes[0]

	if err := eventBus.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := interceptor.Count(); got != 1 {
		t.Fatalf("handler executions = %d, want 1", got)
	}
	persisted := store.routes[eventID]
	if len(persisted) != 1 {
		t.Fatalf("persisted routes = %#v, want one", persisted)
	}
	assertTargetOwnerArcRoute(t, persisted[0], wantOwner)
	if !persisted[0].ConnectClaim.Equal(preflightRoute.ConnectClaim) {
		t.Fatal("persisted connect claim differs from preflight claim")
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != wantRoute {
		t.Fatalf("persisted event target = %#v, want %#v", got, wantRoute)
	}
	settlement, ok := store.settlements[eventID]
	if !ok || !settlement.Delivered() || settlement.NoDelivery() {
		t.Fatalf("persisted settlement = %#v, want delivered route arm", settlement)
	}
	if err := settlement.Validate(persisted); err != nil {
		t.Fatalf("validate persisted settlement: %v", err)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("handler receipt = %q, want processed", got)
	}

	live, internal, replayRoutes, err := eventBus.replayRecipientsForCommittedEvent(ctx, event, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("load committed replay route: %v", err)
	}
	if !containsString(live, "arc-receiver") || !containsString(internal, "arc-receiver") || len(replayRoutes) != 1 {
		t.Fatalf("replay live/internal/routes = %#v/%#v/%#v", live, internal, replayRoutes)
	}
	assertTargetOwnerArcRoute(t, replayRoutes[0], wantOwner)
}

func targetOwnerArcEvent(
	t *testing.T,
	test targetOwnerArcCase,
	eventID string,
	eventType events.EventType,
	runID string,
	envelope events.EventEnvelope,
	routingSource events.RoutingSource,
) events.Event {
	t.Helper()
	payload := []byte(`{"proof":"target-owner-arc"}`)
	if test.sourceKind == targetOwnerArcFlowOwnedControl {
		return eventtest.RuntimeControlWithRoutingSource(
			eventID, eventType, "workflow-schedule", "", payload, 0, runID, uuid.NewString(), envelope, routingSource, time.Now().UTC(),
		)
	}
	producerType := test.producerType
	if producerType == "" {
		producerType = events.EventProducerNode
	}
	producerID := "source-node"
	if producerType == events.EventProducerAgent {
		producerID = "source-agent"
	}
	producer, err := events.NewProducerIdentity(producerType, producerID)
	if err != nil {
		t.Fatalf("construct %s producer: %v", producerType, err)
	}
	return eventtest.ChildForProducerWithRoutingSource(
		eventID, eventType, producer, "", payload, 0,
		events.EventLineage{RunID: runID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}, envelope, routingSource, time.Now().UTC(),
	)
}

func assertTargetOwnerArcRoute(t *testing.T, route events.DeliveryRoute, want events.DeliveryTargetOwnership) {
	t.Helper()
	if err := targetOwnerArcRouteIdentityError(route, want); err != nil {
		t.Fatal(err)
	}
	if route.ConnectClaim.Empty() {
		t.Fatal("delivery route is missing its exact connect execution claim")
	}
	flowID, nodeID, eventType, ok := route.ConnectClaim.NodeHandlerOwner("arc-receiver")
	if !ok || flowID != "receiver" || nodeID != "arc-receiver" || strings.TrimSpace(string(eventType)) == "" {
		t.Fatalf("connect claim handler owner = %q/%q/%q/%t, want receiver/arc-receiver/exact event", flowID, nodeID, eventType, ok)
	}
}

func targetOwnerArcRouteIdentityError(route events.DeliveryRoute, want events.DeliveryTargetOwnership) error {
	if route.Recipient.ID() != "arc-receiver" || route.Target != want {
		return fmt.Errorf("delivery route = %#v, want arc-receiver at %s %#v", route, want.Code(), want.Route())
	}
	return nil
}

func TestTargetOwnerArcProofRejectsRecipientOnlyMatch(t *testing.T) {
	want := events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID: "receiver", FlowInstance: "child", EntityID: eventtest.UUID("receiver-owned-target"),
	})
	wrong := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("arc-receiver"),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: "receiver", FlowInstance: "child", EntityID: eventtest.UUID("source-owned-target"),
		}),
	}
	if err := targetOwnerArcRouteIdentityError(wrong, want); err == nil {
		t.Fatal("recipient-only proof accepted a wrong destination ownership kind and entity")
	}
}

func targetOwnerArcFixture(
	t *testing.T,
	test targetOwnerArcCase,
	runID string,
) (semanticview.Source, events.EventType, events.RouteIdentity, events.RoutingSource, events.DeliveryTargetOwnership) {
	t.Helper()
	const localEvent = "work.ready"
	receiver := connectRoutePlanTestFlow{
		id: "receiver", path: test.receiverPath, mode: test.receiverMode,
		inputs: []runtimecontracts.FlowInputEventPin{{Name: "work_ready", Event: localEvent}},
		nodes: map[string]runtimecontracts.SystemNodeContract{
			"arc-receiver": {
				ID: "arc-receiver", SubscribesTo: []string{localEvent},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					localEvent: targetOwnerArcHandler(test.receiverMode),
				},
			},
		},
	}
	var (
		bundle        *runtimecontracts.WorkflowContractBundle
		eventType     events.EventType
		sourceRoute   events.RouteIdentity
		routingSource events.RoutingSource
		err           error
	)
	sourceEntityID := strings.TrimSpace(test.sourceEntity)
	if sourceEntityID == "" {
		sourceEntityID = eventtest.UUID("arc-source-" + test.name)
	}
	if test.sourceKind == targetOwnerArcRoot {
		bundle = connectRoutePlanTestBundle([]connectRoutePlanTestFlow{receiver}, []runtimecontracts.FlowPackageConnect{{
			From: ".work_ready", To: "receiver.work_ready",
		}})
		bundle.Semantics.Name = "root-workflow"
		bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{
			Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "work_ready", Event: localEvent}}},
		}}
		bundle.FlowTree.Root.Schema = *bundle.RootSchema
		bundle.Events = map[string]runtimecontracts.EventCatalogEntry{localEvent: {}}
		sourceRoute = events.RouteIdentity{FlowID: bundle.Semantics.Name, FlowInstance: runID, EntityID: sourceEntityID}.Normalized()
		routingSource, err = events.NewRootRoutingSource(sourceEntityID)
		eventType = localEvent
	} else {
		sourceMode := runtimecontracts.FlowModeStatic
		if test.sourceKind == targetOwnerArcSingleton {
			sourceMode = runtimecontracts.FlowModeSingleton
		} else if test.sourceKind == targetOwnerArcTemplate {
			sourceMode = runtimecontracts.FlowModeTemplate
		}
		producer := connectRoutePlanTestFlow{
			id: "producer", path: test.sourcePath, mode: sourceMode,
			outputs: []runtimecontracts.FlowOutputEventPin{{Name: "work_ready", Event: localEvent}},
		}
		bundle = connectRoutePlanTestBundle([]connectRoutePlanTestFlow{producer, receiver}, []runtimecontracts.FlowPackageConnect{{
			From: "producer.work_ready", To: "receiver.work_ready",
		}})
		bundle.Semantics.Name = "root-workflow"
		instancePath := test.sourcePath
		if test.sourceKind == targetOwnerArcTemplate {
			instancePath += "/instance-1"
		}
		sourceRoute = events.RouteIdentity{FlowID: "producer", FlowInstance: instancePath, EntityID: sourceEntityID}.Normalized()
		eventType = events.EventType(instancePath + "/" + localEvent)
		if test.sourceKind == targetOwnerArcTemplate {
			routingSource, err = events.NewConcreteTemplateInstanceRoutingSource(sourceRoute)
		} else if test.sourceKind == targetOwnerArcFlowOwnedControl {
			routingSource, err = events.NewFlowOwnedControlRoutingSource(sourceRoute)
		} else {
			routingSource, err = events.NewStaticFlowRoutingSource(sourceRoute)
		}
	}
	if err != nil {
		t.Fatalf("construct routing source: %v", err)
	}
	return semanticview.Wrap(bundle), eventType, sourceRoute, routingSource, events.MustExistingEntityTarget(sourceRoute)
}

func targetOwnerArcHandler(mode string) runtimecontracts.SystemNodeEventHandler {
	if strings.TrimSpace(mode) == runtimecontracts.FlowModeSingleton {
		return runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	}
	return existingOwnerHandlerFixture()
}

func TestEventBusCrossFlowTargetOwnerRejectsForeignSourceBeforePersistence(t *testing.T) {
	t.Run("root source and current owner disagree", TestEventBusPublish_RootConnectStructuralOwnerSourceDisagreementFailsBeforePersistence)
	t.Run("descendant has no compiled proof", TestEventBusPublish_DescendantWithoutConnectFailsBeforePersistence)
	t.Run("repeated leaf under wrong full path", testEventBusCrossFlowTargetOwnerRejectsWrongFullPathBeforePersistence)
}

func testEventBusCrossFlowTargetOwnerRejectsWrongFullPathBeforePersistence(t *testing.T) {
	test := targetOwnerArcCase{
		name: "wrong full path", sourceKind: targetOwnerArcSingleton, sourcePath: "left/worker/result",
		receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "right/worker/result", producerType: events.EventProducerNode,
	}
	runID := uuid.NewString()
	source, eventType, _, _, _ := targetOwnerArcFixture(t, test, runID)
	wrongRoute := events.RouteIdentity{
		FlowID: "producer", FlowInstance: "unrelated/worker/result", EntityID: eventtest.UUID("wrong-full-path-owner"),
	}.Normalized()
	wrongSource, err := events.NewStaticFlowRoutingSource(wrongRoute)
	if err != nil {
		t.Fatalf("construct wrong-path source: %v", err)
	}
	store := newTargetRouteMemoryStore()
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("create EventBus: %v", err)
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(wrongRoute)})
	eventID := uuid.NewString()
	event := targetOwnerArcEvent(
		t, test, eventID, eventType, runID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, wrongRoute), wrongSource,
	)
	plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
	if err != nil {
		t.Fatalf("preflight wrong full-path source: %v", err)
	}
	if len(plan.DeliveryRoutes) != 0 || len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 {
		t.Fatalf("wrong full-path source matched a same-leaf compiled edge: %#v", plan)
	}
	if _, ok := store.events[eventID]; ok || len(store.routes[eventID]) != 0 {
		t.Fatalf("wrong full-path source mutated event/routes: event=%t routes=%#v", ok, store.routes[eventID])
	}
	if _, ok := store.settlements[eventID]; ok {
		t.Fatalf("wrong full-path source mutated settlement: %#v", store.settlements[eventID])
	}
}

func TestEventBusCrossFlowTargetOwnerFailsClosedBeforeMutation(t *testing.T) {
	t.Run("scoped event without flow instance", TestEventBusPublish_NoTargetScopedRoutedNodeWithoutFlowInstanceFailsBeforePersistence)
	t.Run("mixed exact and wildcard cross flow", TestEventBusPublish_MixedExactAndWildcardCrossFlowRoutesFailBeforePersistence)
	t.Run("descendant without connect", TestEventBusPublish_DescendantWithoutConnectFailsBeforePersistence)
	t.Run("missing or ambiguous root owner", TestEventBusPublish_SingletonConnectToRootRejectsMissingOrAmbiguousSelectedOwnerBeforeMutation)
}
