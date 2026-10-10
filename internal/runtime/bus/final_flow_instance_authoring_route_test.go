package bus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/google/uuid"
)

type finalFlowInstanceAuthoringLifecycleStore struct {
	*targetRouteMemoryStore
	bus                         *EventBus
	flowInstances               []ActiveFlowInstanceDescriptor
	flowInstanceDescriptorCalls int
	activations                 []runtimepipeline.FlowInstanceActivationRequest
	sourceArtifactFact          runtimecorrelation.SourceArtifactFact
	workflowVersion             string
}

func (s *finalFlowInstanceAuthoringLifecycleStore) ListActiveFlowInstanceDescriptors(_ context.Context, runID string) ([]ActiveFlowInstanceDescriptor, error) {
	s.flowInstanceDescriptorCalls++
	return exactTestFlowInstanceDescriptors(s.flowInstances, s.workflowVersion, s.sourceArtifactFact, runID), nil
}

func (s *finalFlowInstanceAuthoringLifecycleStore) ListActiveFlowInstanceDescriptorsForScope(_ context.Context, runID string, templateIDs, instancePaths []string) ([]ActiveFlowInstanceDescriptor, error) {
	s.flowInstanceDescriptorCalls++
	return connectRoutePlanScopedDescriptors(
		exactTestFlowInstanceDescriptors(s.flowInstances, s.workflowVersion, s.sourceArtifactFact, runID),
		templateIDs, instancePaths,
	), nil
}

func (s *finalFlowInstanceAuthoringLifecycleStore) ListActiveFlowInstanceDescriptorsForKey(_ context.Context, runID, templateID, keyField, keyValue string) ([]ActiveFlowInstanceDescriptor, error) {
	s.flowInstanceDescriptorCalls++
	return connectRoutePlanKeyedDescriptors(
		exactTestFlowInstanceDescriptors(s.flowInstances, s.workflowVersion, s.sourceArtifactFact, runID),
		templateID, keyField, keyValue,
	), nil
}

func (s *finalFlowInstanceAuthoringLifecycleStore) ReplaceFlowInstanceRouteTopology(ctx context.Context, sets []FlowInstanceRouteRecordSet) (FlowInstanceRouteTopologyResult, error) {
	if err := s.targetRouteMemoryStore.ReplaceFlowInstanceRouteTopology(ctx, sets); err != nil {
		return FlowInstanceRouteTopologyResult{}, err
	}
	return FlowInstanceRouteTopologyResult{Acknowledged: true}, nil
}

func (s *finalFlowInstanceAuthoringLifecycleStore) setTestSemanticSource(fact runtimecorrelation.SourceArtifactFact, workflowVersion string) {
	s.sourceArtifactFact = fact
	s.workflowVersion = workflowVersion
}

func (s *finalFlowInstanceAuthoringLifecycleStore) Activate(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	s.activations = append(s.activations, req)
	accountID, _ := req.ResolvedKey.(string)
	s.flowInstances = append(s.flowInstances, ActiveFlowInstanceDescriptor{
		RunID:         req.TriggerEvent.RunID(),
		InstanceID:    req.Instance.InstanceID,
		EntityID:      req.Instance.EntityID,
		FlowInstance:  req.Instance.InstancePath,
		FlowTemplate:  req.Instance.TemplateID,
		AddressFields: map[string]string{"entity.account_id": accountID},
	})
	if s.bus == nil {
		return nil
	}
	return s.bus.AddFlowInstanceRouteContextFixture(ctx, FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRouteForRun(req.TriggerEvent.RunID(), req.Instance.Route()),
	})
}

func TestEventBusFinalFlowInstanceAuthoringFixture_RenamedConnectRoutePersistsReplayableTemplateTarget(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	evt := finalFlowInstanceAuthoringAccountReadyEvent(uuid.NewString(), "acct-42")
	store := &finalFlowInstanceAuthoringLifecycleStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:          source,
		TemplateInstancePlanner: newTestFlowInstanceActivationOwner(store.Activate),
		Durable:                 DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: evt.RunID()}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb

	installConnectionSourceConstructionForRun(t, eb, source, "producer", evt.RunID())
	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(store.activations) != 0 {
		t.Fatalf("preflight activations = %d, want none", len(store.activations))
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one deterministic template route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if !preflight.UsesCanonicalRouteAuthority() {
		t.Fatalf("preflight route authority is not canonical connect-route authority")
	}
	if !preflight.DeliveryRoutes[0].Target.MaterializingEntity() {
		t.Fatalf("preflight target ownership = %q, want materializing_entity", preflight.DeliveryRoutes[0].Target.Code())
	}
	preview := preflight.DeliveryRoutes[0].Target.Route()
	if preview.FlowID != finalflowinstanceauthoring.TemplateFlowID || preview.FlowInstance == "" || preview.EntityID == "" {
		t.Fatalf("preflight target = %#v, want %s template route", preview, finalflowinstanceauthoring.TemplateFlowID)
	}
	lookup, err := runtimepipeline.NewExactFlowInstanceLookup(source, eb.sourceArtifactFact, testRunScopedFlowRouteForRun(evt.RunID(), runtimeflowidentity.StoredRoute(finalflowinstanceauthoring.TemplateFlowID, runtimeflowidentity.LogicalInstanceID(preview.FlowInstance), preview.FlowInstance)))
	if err != nil {
		t.Fatal(err)
	}
	if observed, found, err := store.LookupFlowInstance(constructionIndexContext(t, source), lookup); err != nil || found || observed.Valid() {
		t.Fatalf("preflight published a native receiver: observed=%+v found=%v err=%v", observed, found, err)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish account ready: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	fields, err := testFlowActivationConstructorFields(activation)
	if err != nil {
		t.Fatal(err)
	}
	if activation.ResolvedKey != "acct-42" ||
		fields[finalflowinstanceauthoring.TemplateInstanceBy] != "acct-42" {
		t.Fatalf("activation key/constructor fields = %#v/%#v, want account_id from receiver carry", activation.ResolvedKey, fields)
	}
	if _, exists := fields["entity_type"]; exists {
		t.Fatalf("constructor fields retain typed entity_type: %#v", fields)
	}
	if _, exists := fields["instance_kind"]; exists {
		t.Fatalf("constructor fields retain typed instance_kind: %#v", fields)
	}
	if activation.Bookkeeping["last_source_event"] != evt.ID() {
		t.Fatalf("activation bookkeeping = %#v, want last_source_event", activation.Bookkeeping)
	}
	persistedRoutes := store.routes[evt.ID()]
	if len(persistedRoutes) != 1 || persistedRoutes[0].Recipient.LocalID() != finalflowinstanceauthoring.TemplateNodeID {
		t.Fatalf("persisted delivery routes = %#v, want one %s subscriber", persistedRoutes, finalflowinstanceauthoring.TemplateNodeID)
	}
	want := events.DeliveryRoute{Recipient: persistedRoutes[0].Recipient, Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID:       finalflowinstanceauthoring.TemplateFlowID,
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}),
	}
	want.Initialization, err = events.AdmitFlowReceiverInitialization(evt, want.Target)
	if err != nil {
		t.Fatal(err)
	}
	if !deliveryRoutesContain(persistedRoutes, want) {
		t.Fatalf("persisted delivery routes = %#v, want lifecycle-created template route %#v", persistedRoutes, want)
	}
	if got := store.scopes[evt.ID()]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("post-publish planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("post-publish route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}

	retryTarget := subscribeInternalDeliveriesForTest(t, eb, persistedRoutes[0].Recipient.ID())
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish same-event retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("same-event retry activations = %d, want committed replay without second activation", len(store.activations))
	}
	requireNoBusEvent(t, retryTarget, "same-event publish retry")

	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "drift",
		EntityID:      "ent-drift",
		FlowInstance:  finalflowinstanceauthoring.TemplateFlowID + "/drift",
		FlowTemplate:  finalflowinstanceauthoring.TemplateFlowID,
		AddressFields: map[string]string{"entity.account_id": "acct-42"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{
		Identity: testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute(finalflowinstanceauthoring.TemplateFlowID, "drift")),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(drift): %v", err)
	}
	store.flowInstanceDescriptorCalls = 0
	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, retryTarget, "persisted replay after descriptor drift")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("drift replay target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because persisted delivery target route is authoritative", got)
	}
}

func TestEventBusFinalFlowInstanceAuthoringFixture_FailsClosedForMissingAndAmbiguousKeys(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	tests := []struct {
		name          string
		payload       json.RawMessage
		flowInstances []ActiveFlowInstanceDescriptor
		wantFailure   string
	}{
		{
			name:        "missing renamed producer key",
			payload:     json.RawMessage(`{"score":"91","decision":"approved"}`),
			wantFailure: runtimepinrouting.ConnectFailureInstanceSourceValueMissing.Code(),
		},
		{
			name:    "ambiguous receiver key",
			payload: json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: "ent-1", FlowInstance: finalflowinstanceauthoring.TemplateFlowID + "/one", FlowTemplate: finalflowinstanceauthoring.TemplateFlowID, AddressFields: map[string]string{"entity.account_id": "acct-42"}},
				{InstanceID: "two", EntityID: "ent-2", FlowInstance: finalflowinstanceauthoring.TemplateFlowID + "/two", FlowTemplate: finalflowinstanceauthoring.TemplateFlowID, AddressFields: map[string]string{"entity.account_id": "acct-42"}},
			},
			wantFailure: runtimepinrouting.ConnectFailureTargetAmbiguous.Code(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &finalFlowInstanceAuthoringLifecycleStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
				flowInstances:          tc.flowInstances,
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source,
				TemplateInstancePlanner: newTestFlowInstanceActivationOwner(func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
					t.Fatal("fail-closed route must not activate a template instance")
					return nil
				}),
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := finalFlowInstanceAuthoringEvent(uuid.NewString(), tc.payload)
			installConnectionSourceConstructionForRun(t, eb, source, "producer", evt.RunID())
			for _, descriptor := range tc.flowInstances {
				instance := StoredFlowInstanceIdentityFixture(source, finalflowinstanceauthoring.TemplateFlowID, descriptor.InstanceID, evt.RunID(), descriptor.EntityID)
				if err := eb.AddFlowInstanceRouteFixture(FlowInstanceRouteMaterializationRequest{
					Identity: testRunScopedFlowRouteForRun(evt.RunID(), instance.Route()), Instance: instance,
				}); err != nil {
					t.Fatalf("install ambiguous receiver construction: %v", err)
				}
			}

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if plan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", plan.TargetFailure, tc.wantFailure)
			}
			if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
				len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("fail-closed route exposed executable plan: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
					plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish fail-closed event: %v", err)
			}
			if routes := store.routes[evt.ID()]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none", routes)
			}
		})
	}
}

func finalFlowInstanceAuthoringAccountReadyEvent(eventID, accountID string) events.Event {
	return finalFlowInstanceAuthoringEvent(eventID, json.RawMessage(`{"account_id":"`+accountID+`","score":"91","decision":"approved"}`))
}

func finalFlowInstanceAuthoringEvent(eventID string, payload json.RawMessage) events.Event {
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID:       "producer",
		FlowInstance: "producer",
		EntityID:     runtimeflowidentity.EntityID("producer"),
	})
	if err != nil {
		panic(err)
	}
	return eventtest.RunCreatingRootIngressWithRoutingSource(
		eventID,
		events.EventType("producer/account.ready"),
		"",
		"",
		payload,
		0,
		eventtest.UUID("run:"+eventID),
		"",
		events.EventEnvelope{},
		source,
		time.Now().UTC(),
	)
}
