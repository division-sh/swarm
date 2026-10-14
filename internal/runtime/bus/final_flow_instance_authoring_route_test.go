package bus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/google/uuid"
)

type finalFlowInstanceAuthoringLifecycleStore struct {
	*targetRouteMemoryStore
	activations []runtimepipeline.FlowInstanceActivationRequest
}

func (s *finalFlowInstanceAuthoringLifecycleStore) Activate(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	s.activations = append(s.activations, req)
	return nil
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
	observed, found, err := store.LookupFlowInstance(constructionIndexContext(t, source), lookup)
	if err != nil || !found || observed.Identity() != activation.Instance || observed.InstanceKey() != "acct-42" {
		t.Fatalf("committed receiver lost native construction: %+v found=%t err=%v", observed, found, err)
	}
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
	want.Initialization, err = events.AdmitFlowReceiverInitialization(evt, want.Target, flowReceiverRouteFixture(t, activation.Instance.Route()))
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

	drift := StoredFlowInstanceIdentityFixture(source, finalflowinstanceauthoring.TemplateFlowID, "drift", evt.RunID(), "ent-drift")
	store.installIndexObservation(constructionIndexObservation(t, source, evt.RunID(), drift, "acct-42"))
	store.refuseInstanceIndexReads(t)
	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, retryTarget, "persisted replay after native-index drift")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("drift replay target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
}

func TestEventBusFinalFlowInstanceAuthoringFixture_FailsClosedForMissingAndAmbiguousKeys(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	tests := []struct {
		name        string
		payload     json.RawMessage
		receivers   []struct{ instanceID, entityID string }
		wantFailure string
		corruption  bool
	}{
		{
			name:        "missing renamed producer key",
			payload:     json.RawMessage(`{"score":"91","decision":"approved"}`),
			wantFailure: runtimepinrouting.ConnectFailureInstanceSourceValueMissing.Code(),
		},
		{
			name:    "ambiguous receiver key",
			payload: json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
			receivers: []struct{ instanceID, entityID string }{
				{"one", "ent-1"}, {"two", "ent-2"},
			},
			corruption: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			evt := finalFlowInstanceAuthoringEvent(uuid.NewString(), tc.payload)
			store := &finalFlowInstanceAuthoringLifecycleStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source,
				Durable:        DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: evt.RunID()}},
				TemplateInstancePlanner: newTestFlowInstanceActivationOwner(func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
					t.Fatal("fail-closed route must not activate a template instance")
					return nil
				}),
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			installConnectionSourceConstructionForRun(t, eb, source, "producer", evt.RunID())
			for _, receiver := range tc.receivers {
				instance := StoredFlowInstanceIdentityFixture(source, finalflowinstanceauthoring.TemplateFlowID, receiver.instanceID, evt.RunID(), receiver.entityID)
				store.installIndexObservation(constructionIndexObservation(t, source, evt.RunID(), instance, "acct-42"))
				store.installConstructionReceipt(testRunScopedFlowRouteForRun(evt.RunID(), instance.Route()), runtimepipeline.FlowConstructionPublicationEvidence{Identity: instance})
			}

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			var corruption *runtimepipeline.FlowInstanceConstructionCorruption
			if tc.corruption {
				if !errors.As(err, &corruption) {
					t.Fatalf("duplicate durable selector did not report corruption: %v", err)
				}
			} else if err != nil {
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
			err = eb.Publish(context.Background(), evt)
			if tc.corruption {
				if !errors.As(err, &corruption) || len(store.events) != 0 || len(store.activations) != 0 {
					t.Fatalf("corrupt selector mutated publication/construction or lost its error: %v", err)
				}
			} else if err != nil {
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
