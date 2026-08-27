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
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
		{name: "root node to direct singleton", sourceKind: targetOwnerArcRoot, receiverMode: runtimecontracts.FlowModeSingleton, receiverPath: "receiver", producerType: events.EventProducerNode},
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
	source := connectRoutePlanSingletonProducerRootReceiverSource(t)
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
		Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "source-node")),
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
		t.Fatalf("preflight failure/routes = %q/%#v, want one exact receiver route; connect plans=%#v issues=%#v", preflight.TargetFailure, preflight.DeliveryRoutes, runtimepinrouting.CompileConnectGraph(source).Plans(), runtimepinrouting.CompileConnectGraph(source).Issues())
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
	wantRecipientKey := persisted[0].Recipient.ID()
	if !containsString(live, wantRecipientKey) || !containsString(internal, wantRecipientKey) || len(replayRoutes) != 1 {
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
	node, eventType, ok := route.ConnectClaim.NodeHandlerOwner()
	if !ok || node.FlowID() != "receiver" || node.NodeID() != "arc-receiver" || strings.TrimSpace(string(eventType)) == "" {
		t.Fatalf("connect claim handler owner = %s/%q/%t, want receiver/arc-receiver/exact event", node.Key(), eventType, ok)
	}
}

func targetOwnerArcRouteIdentityError(route events.DeliveryRoute, want events.DeliveryTargetOwnership) error {
	if route.Recipient.LocalID() != "arc-receiver" || route.Target != want {
		return fmt.Errorf("delivery route = %#v, want arc-receiver at %s %#v", route, want.Code(), want.Route())
	}
	return nil
}

func TestTargetOwnerArcProofRejectsRecipientOnlyMatch(t *testing.T) {
	want := events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID: "receiver", FlowInstance: "child", EntityID: eventtest.UUID("receiver-owned-target"),
	})
	wrong := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "arc-receiver")),
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
		inputs: []runtimecontracts.FlowInputEventPin{{Event: localEvent}},
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
		repoRoot := canonicalrouting.RepoRoot(t)
		fixtureRoot := canonicalrouting.CopyRootOutputSingletonArc(t)
		bundle, err = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
		if err != nil {
			t.Fatalf("load root target-owner arc fixture: %v", err)
		}
		sourceRoute = events.RouteIdentity{FlowID: semanticview.Wrap(bundle).WorkflowName(), FlowInstance: runID, EntityID: sourceEntityID}.Normalized()
		routingSource, err = events.NewRootRoutingSource(sourceEntityID)
		eventType = "root.ready"
	} else {
		sourceMode := runtimecontracts.FlowModeStatic
		if test.sourceKind == targetOwnerArcSingleton {
			sourceMode = runtimecontracts.FlowModeSingleton
		} else if test.sourceKind == targetOwnerArcTemplate {
			sourceMode = runtimecontracts.FlowModeTemplate
		}
		producer := connectRoutePlanTestFlow{
			id: "producer", path: test.sourcePath, mode: sourceMode,
			outputs: []runtimecontracts.FlowOutputEventPin{{Event: localEvent}},
		}
		bundle = connectRoutePlanTestBundle([]connectRoutePlanTestFlow{producer, receiver}, []runtimecontracts.FlowPackageConnect{{
			Event: localEvent, From: "producer", To: "receiver",
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

func TestEventBusReentrantBoomerangPreservesExactOwnersWhilePriorDeliveriesRemainOpen(t *testing.T) {
	const (
		pingEvent = "work.ping"
		pongEvent = "work.pong"
	)
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyRootSingletonBoomerang(t))

	runID := uuid.NewString()
	rootRoute := events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("boomerang-root-owner"),
	}.Normalized()
	childRoute := events.RouteIdentity{
		FlowID: "boomerang", FlowInstance: "boomerang", EntityID: eventtest.UUID("boomerang-child-owner"),
	}.Normalized()
	if rootRoute.EntityID == childRoute.EntityID {
		t.Fatal("boomerang root and child owners must remain distinguishable")
	}
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(rootRoute, childRoute)
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("create boomerang EventBus: %v", err)
	}
	childDeliveries := subscribeInternalDeliveriesForTest(t, eventBus, testFlowNode(t, "boomerang", "boomerang-worker").Key(), events.EventType(pingEvent))
	rootDeliveries := subscribeInternalDeliveriesForTest(t, eventBus, testRootNode(t, "root-boomerang").Key(), events.EventType("boomerang/"+pongEvent))
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	rootCtx := runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "root-fan-out")), Target: events.MustExistingEntityTarget(rootRoute),
	})
	childCtx := runtimedelivery.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(childRoute)})
	rootSource, err := events.NewRootRoutingSource(rootRoute.EntityID)
	if err != nil {
		t.Fatalf("construct root routing source: %v", err)
	}
	childSource, err := events.NewStaticFlowRoutingSource(childRoute)
	if err != nil {
		t.Fatalf("construct child routing source: %v", err)
	}

	firstID := uuid.NewString()
	first := eventtest.RunCreatingRootIngressWithRoutingSource(
		firstID, events.EventType(pingEvent), "root-producer", "", []byte(`{"turn":1}`), 0, runID, "",
		events.EventEnvelope{}, rootSource, time.Now().UTC(),
	)
	if err := eventBus.Publish(rootCtx, first); err != nil {
		t.Fatalf("publish first root-to-child leg: %v", err)
	}
	firstChild := requireOpenTargetOwnerDelivery(t, childDeliveries, "first boomerang child leg")
	if got := firstChild.HandoffRoute().Target; got != events.MustExistingEntityTarget(childRoute) {
		t.Fatalf("first child target = %#v, want exact child owner %#v", got, childRoute)
	}

	pongID := uuid.NewString()
	pong := eventtest.ChildForProducerWithRoutingSource(
		pongID, events.EventType("boomerang/"+pongEvent), eventtest.Producer(events.EventProducerNode, "boomerang-worker"), "",
		[]byte(`{"turn":1}`), 0, events.EventLineage{RunID: runID, ParentEventID: firstID, ExecutionMode: executionmode.Live},
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, childRoute), childSource, time.Now().UTC(),
	)
	if err := eventBus.Publish(childCtx, pong); err != nil {
		t.Fatalf("publish child-to-root return leg: %v", err)
	}
	rootReturn := requireOpenTargetOwnerDelivery(t, rootDeliveries, "boomerang root return")
	if got := rootReturn.HandoffRoute().Target; got != events.MustExistingEntityTarget(rootRoute) {
		t.Fatalf("root return target = %#v, want exact root owner %#v", got, rootRoute)
	}

	secondID := uuid.NewString()
	second := eventtest.ChildForProducerWithRoutingSource(
		secondID, events.EventType(pingEvent), eventtest.Producer(events.EventProducerNode, "root-producer"), "",
		[]byte(`{"turn":2}`), 0, events.EventLineage{RunID: runID, ParentEventID: pongID, ExecutionMode: executionmode.Live},
		events.EventEnvelope{}, rootSource, time.Now().UTC(),
	)
	if err := eventBus.Publish(rootCtx, second); err != nil {
		t.Fatalf("publish reentrant root-to-child leg: %v", err)
	}
	secondChild := requireOpenTargetOwnerDelivery(t, childDeliveries, "reentrant boomerang child leg")
	if got := secondChild.HandoffRoute().Target; got != events.MustExistingEntityTarget(childRoute) {
		t.Fatalf("reentrant child target = %#v, want same exact child owner %#v", got, childRoute)
	}
	if firstChild.Event().ID() == secondChild.Event().ID() || firstChild.HandoffRoute().ConnectClaim.Empty() || secondChild.HandoffRoute().ConnectClaim.Empty() || rootReturn.HandoffRoute().ConnectClaim.Empty() {
		t.Fatalf("boomerang deliveries lost distinct event identity or exact connect claims: first=%#v root=%#v second=%#v", firstChild.HandoffRoute(), rootReturn.HandoffRoute(), secondChild.HandoffRoute())
	}
	for _, delivery := range []*LocalDelivery{secondChild, rootReturn, firstChild} {
		if err := delivery.Complete(); err != nil {
			t.Fatalf("complete boomerang delivery %s: %v", delivery.Event().ID(), err)
		}
	}
	for _, eventID := range []string{firstID, pongID, secondID} {
		routes := store.routes[eventID]
		settlement, ok := store.settlements[eventID]
		if len(routes) != 1 || !ok || !settlement.Delivered() || settlement.NoDelivery() {
			t.Fatalf("boomerang event %s routes/settlement = %#v/%#v, want one delivered exact route", eventID, routes, settlement)
		}
		if err := settlement.Validate(routes); err != nil {
			t.Fatalf("validate boomerang settlement %s: %v", eventID, err)
		}
	}
}

func TestEventBusPoisonedMixedOwnerFanOutFailsAtomicallyThenLegalOwnersAgree(t *testing.T) {
	const eventName = "work.ready"
	producer := connectRoutePlanTestFlow{
		id: "producer", mode: runtimecontracts.FlowModeStatic,
		outputs: []runtimecontracts.FlowOutputEventPin{{Event: eventName}},
	}
	receiver := func(id, mode string, handler runtimecontracts.SystemNodeEventHandler) connectRoutePlanTestFlow {
		return connectRoutePlanTestFlow{
			id: id, path: "fanout/" + id, mode: mode,
			inputs: []runtimecontracts.FlowInputEventPin{{Event: eventName}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				id + "-node": {ID: id + "-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventName: handler}},
			},
		}
	}
	existing := receiver("existing", runtimecontracts.FlowModeStatic, existingOwnerHandlerFixture())
	materializing := receiver("materializing", runtimecontracts.FlowModeSingleton, runtimecontracts.SystemNodeEventHandler{CreateEntity: true})
	entityless := receiver("entityless", runtimecontracts.FlowModeStatic, runtimecontracts.SystemNodeEventHandler{})
	poison := receiver("poison", runtimecontracts.FlowModeStatic, existingOwnerHandlerFixture())
	connect := func(id string) runtimecontracts.FlowPackageConnect {
		return runtimecontracts.FlowPackageConnect{Event: eventName, From: "producer", To: id}
	}
	sourceRoute := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("mixed-owner-source")}.Normalized()
	existingRoute := events.RouteIdentity{FlowID: "existing", FlowInstance: "fanout/existing", EntityID: eventtest.UUID("mixed-owner-existing")}.Normalized()
	poisonedStore := newTargetRouteMemoryStore()
	poisonedStore.setTargetOwnerRoutes(sourceRoute, existingRoute)
	poisonedBundle := connectRoutePlanTestBundle(
		[]connectRoutePlanTestFlow{producer, existing, materializing, entityless, poison},
		[]runtimecontracts.FlowPackageConnect{connect("existing"), connect("materializing"), connect("entityless"), connect("poison")},
	)
	poisonedBus, err := newScopedTestEventBus(poisonedStore, EventBusOptions{ContractBundle: semanticview.Wrap(poisonedBundle)})
	if err != nil {
		t.Fatalf("create poisoned fan-out EventBus: %v", err)
	}
	runID := uuid.NewString()
	ctx := runtimedelivery.WithRoute(runtimecorrelation.WithRunID(context.Background(), runID), events.DeliveryRoute{Target: events.MustExistingEntityTarget(sourceRoute)})
	eventID := uuid.NewString()
	event := connectRoutePlanStaticProducerEvent(
		eventID, events.EventType("producer/"+eventName), "", "", []byte(`{"proof":"mixed-owner"}`), 0, runID, "",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), time.Now().UTC(),
	)
	if _, err := poisonedBus.CheckPublishRecipientPlan(ctx, event); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
		t.Fatalf("poisoned fan-out preflight error = %v, want missing required owner", err)
	}
	if err := poisonedBus.Publish(ctx, event); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
		t.Fatalf("poisoned fan-out publish error = %v, want missing required owner", err)
	}
	if len(poisonedStore.events) != 0 || len(poisonedStore.routes) != 0 || len(poisonedStore.settlements) != 0 || len(poisonedStore.scopes) != 0 || len(poisonedStore.receipts) != 0 || len(poisonedStore.flowRoutes) != 0 {
		t.Fatalf("poisoned fan-out partially mutated durable state: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v",
			poisonedStore.events, poisonedStore.routes, poisonedStore.settlements, poisonedStore.scopes, poisonedStore.receipts, poisonedStore.flowRoutes)
	}

	legalStore := newTargetRouteMemoryStore()
	legalStore.setTargetOwnerRoutes(sourceRoute, existingRoute)
	legalBundle := connectRoutePlanTestBundle(
		[]connectRoutePlanTestFlow{producer, existing, materializing, entityless},
		[]runtimecontracts.FlowPackageConnect{connect("existing"), connect("materializing"), connect("entityless")},
	)
	interceptor := &connectRoutePlanNodeInterceptor{}
	legalBus, err := newScopedTestEventBus(legalStore, EventBusOptions{
		ContractBundle: semanticview.Wrap(legalBundle), Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("create legal mixed-owner EventBus: %v", err)
	}
	plan, err := legalBus.CheckPublishRecipientPlan(ctx, event)
	if err != nil {
		t.Fatalf("legal mixed-owner preflight: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 3 {
		t.Fatalf("legal mixed-owner preflight failure/routes = %q/%#v, want three exact owners", plan.TargetFailure, plan.DeliveryRoutes)
	}
	wantOwners := map[string]events.DeliveryTargetOwnership{
		"existing-node": events.MustExistingEntityTarget(existingRoute),
		"materializing-node": events.MustMaterializingEntityTarget(events.RouteIdentity{
			FlowID: "materializing", FlowInstance: "fanout/materializing", EntityID: runtimeflowidentity.EntityID("fanout/materializing"),
		}),
		"entityless-node": events.MustEntitylessReceiverTarget(events.RouteIdentity{
			FlowID: "entityless", FlowInstance: "fanout/entityless",
		}),
	}
	for _, route := range plan.DeliveryRoutes {
		want, ok := wantOwners[route.Recipient.LocalID()]
		if !ok || route.Target != want || route.ConnectClaim.Empty() {
			t.Fatalf("legal mixed-owner route = %#v, want exact classified owner from %#v", route, wantOwners)
		}
	}
	if err := legalBus.Publish(ctx, event); err != nil {
		t.Fatalf("publish legal mixed-owner fan-out: %v", err)
	}
	if interceptor.Count() != 3 {
		t.Fatalf("legal mixed-owner executions = %d, want 3", interceptor.Count())
	}
	if routes := legalStore.routes[eventID]; len(routes) != 3 {
		t.Fatalf("persisted legal mixed-owner routes = %#v, want 3", routes)
	} else {
		for _, route := range routes {
			if want := wantOwners[route.Recipient.LocalID()]; route.Target != want || route.ConnectClaim.Empty() {
				t.Fatalf("persisted legal mixed-owner route = %#v, want exact classified owner", route)
			}
		}
	}
	settlement, ok := legalStore.settlements[eventID]
	if !ok || !settlement.Delivered() || settlement.NoDelivery() {
		t.Fatalf("legal mixed-owner settlement = %#v, want delivered", settlement)
	}
	if err := settlement.Validate(legalStore.routes[eventID]); err != nil {
		t.Fatalf("validate legal mixed-owner settlement: %v", err)
	}
}

func TestEventBusTwoLevelFanOutDiamondKeepsNestedOwnersAndRootConvergenceExact(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, canonicalrouting.CopyTargetOwnerDiamond(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load two-level fan-out diamond fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	runID := uuid.NewString()
	rootRoute := events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("root-source-entity"),
	}.Normalized()
	leftRoute := events.RouteIdentity{
		FlowID: "branch", FlowInstance: "branch/left", EntityID: eventtest.UUID("diamond-left-parent"),
	}.Normalized()
	rightRoute := events.RouteIdentity{
		FlowID: "branch", FlowInstance: "branch/right", EntityID: eventtest.UUID("diamond-right-parent"),
	}.Normalized()
	hostileRoute := events.RouteIdentity{
		FlowID: "hostile", FlowInstance: "unrelated/worker/result", EntityID: eventtest.UUID("diamond-hostile-owner"),
	}.Normalized()
	store := &connectRoutePlanLifecycleStore{connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{
			{InstanceID: "left", EntityID: leftRoute.EntityID, FlowInstance: leftRoute.FlowInstance, FlowTemplate: "branch", AddressFields: map[string]string{"entity.branch_id": "left"}},
			{InstanceID: "right", EntityID: rightRoute.EntityID, FlowInstance: rightRoute.FlowInstance, FlowTemplate: "branch", AddressFields: map[string]string{"entity.branch_id": "right"}},
		},
	}}
	store.setTargetOwnerRoutes(rootRoute, leftRoute, rightRoute, hostileRoute)
	interceptor := &connectRoutePlanNodeInterceptor{}
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source, TemplateInstanceActivator: store.Activate, Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("create diamond EventBus: %v", err)
	}
	store.bus = eventBus
	for _, identity := range []runtimeflowidentity.Route{
		runtimeflowidentity.DeriveRoute("branch", "left"),
		runtimeflowidentity.DeriveRoute("branch", "right"),
	} {
		if err := eventBus.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
			t.Fatalf("materialize diamond branch route %s: %v", identity.InstancePath, err)
		}
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	rootCtx := runtimedelivery.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(rootRoute)})
	rootSource, err := events.NewRootRoutingSource(rootRoute.EntityID)
	if err != nil {
		t.Fatalf("construct diamond root source: %v", err)
	}
	parents := []struct {
		name  string
		route events.RouteIdentity
	}{
		{name: "left", route: leftRoute},
		{name: "right", route: rightRoute},
	}
	for _, parent := range parents {
		eventID := uuid.NewString()
		event := eventtest.RunCreatingRootIngressWithRoutingSource(
			eventID, events.EventType("branch.start"), "root-fan-out", "", []byte(fmt.Sprintf(`{"branch_id":%q}`, parent.name)),
			0, runID, "", events.EventEnvelope{}, rootSource, time.Now().UTC(),
		)
		plan, err := eventBus.CheckPublishRecipientPlan(rootCtx, event)
		if err != nil {
			t.Fatalf("preflight root-to-%s concrete branch: %v", parent.name, err)
		}
		want := events.MustExistingEntityTarget(parent.route)
		if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
			t.Fatalf("root-to-%s plan = failure:%q routes:%#v, want exact concrete branch %#v", parent.name, plan.TargetFailure, plan.DeliveryRoutes, want)
		}
		if err := eventBus.Publish(rootCtx, event); err != nil {
			t.Fatalf("publish root-to-%s concrete branch: %v", parent.name, err)
		}
		if routes := store.routes[eventID]; len(routes) != 1 || routes[0].Target != want || routes[0].ConnectClaim.Empty() {
			t.Fatalf("persisted root-to-%s route = %#v, want exact concrete branch", parent.name, routes)
		}
	}

	var branchDeliveryIDs []string
	branchWorker := testFlowNode(t, "branch", "branch-worker")
	for _, parent := range parents {
		parentSource, err := events.NewConcreteTemplateInstanceRoutingSource(parent.route)
		if err != nil {
			t.Fatalf("construct %s concrete branch source: %v", parent.name, err)
		}
		parentCtx := runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(branchWorker), Target: events.MustExistingEntityTarget(parent.route),
		})
		eventID := uuid.NewString()
		branchDeliveryIDs = append(branchDeliveryIDs, eventID)
		event := eventtest.ChildForProducerWithRoutingSource(
			eventID, events.EventType("branch/"+parent.name+"/work.ready"), eventtest.Producer(events.EventProducerNode, "branch-worker-"+parent.name), "",
			[]byte(fmt.Sprintf(`{"branch_id":%q}`, parent.name)), 0,
			events.EventLineage{RunID: runID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
			events.EnvelopeForSourceRoute(events.EventEnvelope{}, parent.route), parentSource, time.Now().UTC(),
		)
		plan, err := eventBus.CheckPublishRecipientPlan(parentCtx, event)
		if err != nil {
			t.Fatalf("preflight %s second-level fan-out: %v", parent.name, err)
		}
		if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 2 {
			t.Fatalf("%s second-level plan = failure:%q routes:%#v, want static plus singleton", parent.name, plan.TargetFailure, plan.DeliveryRoutes)
		}
		var staticSeen, singletonSeen bool
		for _, route := range plan.DeliveryRoutes {
			if route.ConnectClaim.Empty() {
				t.Fatalf("%s second-level route lacks exact connect claim: %#v", parent.name, route)
			}
			if strings.Contains(route.Recipient.LocalID(), "static-result") {
				staticSeen = true
				if route.Target.Code() != "existing_entity" || route.Target.Route().EntityID != parent.route.EntityID || route.Target.Route().FlowInstance != "branch/static-result" {
					t.Fatalf("%s nested static target = %s %#v, want parent-shared existing owner %#v", parent.name, route.Target.Code(), route.Target.Route(), parent.route)
				}
			} else if strings.Contains(route.Recipient.LocalID(), "singleton-result") {
				singletonSeen = true
				wantPath := "branch/singleton-result"
				if route.Target.Code() != "materializing_entity" || route.Target.Route().FlowInstance != wantPath || route.Target.Route().EntityID != runtimeflowidentity.EntityID(wantPath) {
					t.Fatalf("%s nested singleton target = %s %#v, want materializing %q", parent.name, route.Target.Code(), route.Target.Route(), wantPath)
				}
				if route.Target.Route().EntityID == parent.route.EntityID || route.Target.Route().EntityID == rootRoute.EntityID {
					t.Fatalf("%s singleton reused parent/root owner: %#v", parent.name, route.Target.Route())
				}
			} else {
				t.Fatalf("%s second-level fan-out reached unrelated same-leaf receiver: %#v", parent.name, route)
			}
		}
		if !staticSeen || !singletonSeen {
			t.Fatalf("%s second-level routes = %#v, want one static and one singleton", parent.name, plan.DeliveryRoutes)
		}
		if err := eventBus.Publish(parentCtx, event); err != nil {
			t.Fatalf("publish %s second-level fan-out: %v", parent.name, err)
		}
		if routes := store.routes[eventID]; len(routes) != 2 {
			t.Fatalf("persisted %s second-level routes = %#v, want 2", parent.name, routes)
		} else {
			for _, route := range routes {
				if route.ConnectClaim.Empty() || strings.Contains(route.Recipient.LocalID(), "hostile") {
					t.Fatalf("persisted %s route captured hostile receiver or lost claim: %#v", parent.name, route)
				}
			}
		}
	}

	for _, parent := range parents {
		parentSource, err := events.NewConcreteTemplateInstanceRoutingSource(parent.route)
		if err != nil {
			t.Fatalf("construct %s convergence source: %v", parent.name, err)
		}
		parentCtx := runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(branchWorker), Target: events.MustExistingEntityTarget(parent.route),
		})
		for branch := 0; branch < 2; branch++ {
			eventID := uuid.NewString()
			event := eventtest.ChildForProducerWithRoutingSource(
				eventID, events.EventType("branch/"+parent.name+"/branch.done"), eventtest.Producer(events.EventProducerNode, fmt.Sprintf("result-%d", branch)), "",
				[]byte(fmt.Sprintf(`{"branch_id":%q,"result":%d}`, parent.name, branch)), 0,
				events.EventLineage{RunID: runID, ParentEventID: branchDeliveryIDs[branch], ExecutionMode: executionmode.Live},
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, parent.route), parentSource, time.Now().UTC(),
			)
			plan, err := eventBus.CheckPublishRecipientPlan(parentCtx, event)
			if err != nil {
				t.Fatalf("preflight %s convergence branch %d: %v", parent.name, branch, err)
			}
			want := events.MustExistingEntityTarget(rootRoute)
			if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
				t.Fatalf("%s convergence branch %d plan = failure:%q routes:%#v, want exact root owner", parent.name, branch, plan.TargetFailure, plan.DeliveryRoutes)
			}
			if err := eventBus.Publish(parentCtx, event); err != nil {
				t.Fatalf("publish %s convergence branch %d: %v", parent.name, branch, err)
			}
		}
	}
	if got := interceptor.Count(); got != 10 {
		t.Fatalf("diamond executions = %d, want 2 parent + 4 nested + 4 root convergence with no hostile capture", got)
	}
	for eventID, routes := range store.routes {
		settlement, ok := store.settlements[eventID]
		if !ok || !settlement.Delivered() || settlement.NoDelivery() {
			t.Fatalf("diamond event %s settlement = %#v, want delivered", eventID, settlement)
		}
		if err := settlement.Validate(routes); err != nil {
			t.Fatalf("validate diamond settlement %s: %v", eventID, err)
		}
	}
}

func requireOpenTargetOwnerDelivery(t testing.TB, deliveries <-chan *LocalDelivery, label string) *LocalDelivery {
	t.Helper()
	select {
	case delivery := <-deliveries:
		if delivery == nil {
			t.Fatalf("%s: queued delivery is nil", label)
		}
		return delivery
	default:
		t.Fatalf("%s: expected queued delivery", label)
		return nil
	}
}
