package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"time"
)

func TestDeliveryRouteResolver_SeparatesRouteResolutionAndDiagnostics(t *testing.T) {
	resolver := deliveryRouteResolver{
		resolveRoutedSubscribers: func(events.Event) []Subscriber {
			return []Subscriber{{
				ID:           "scan-orchestrator",
				Type:         "node",
				Path:         "discovery",
				MatchPattern: "producer/scan.requested",
				RouteSource:  "pin_auto_wire",
			}}
		},
		resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
			return []deliveryRecipientCandidate{
				{ID: "direct-agent", PersistAsDelivery: true},
				{ID: "scan-orchestrator", PersistAsDelivery: false},
				{ID: "direct-agent", PersistAsDelivery: true},
			}
		},
		describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
			return []PublishDiagnosticRecipient{{
				ID:             "scan-orchestrator",
				Type:           "node",
				Path:           "discovery",
				MatchedPattern: "producer/scan.requested",
				RouteSource:    "pin_auto_wire",
				LocalizedEvent: "scan.requested",
			}}
		},
	}

	result := resolver.Resolve(eventtest.RootIngress("", events.EventType("producer/scan.requested"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if got, want := len(result.RoutedRecipients), 1; got != want {
		t.Fatalf("routed recipients = %d, want %d", got, want)
	}
	if got, want := len(result.SubscribedRecipients), 2; got != want {
		t.Fatalf("subscription recipients = %d, want %d", got, want)
	}
	if got, want := len(result.Recipients), 2; got != want {
		t.Fatalf("candidate recipients = %d, want %d", got, want)
	}
	if got := result.Recipients[0].ID; got != "direct-agent" {
		t.Fatalf("first candidate recipient = %q, want direct-agent", got)
	}
	if got := result.Recipients[1].ID; got != "scan-orchestrator" {
		t.Fatalf("second candidate recipient = %q, want scan-orchestrator", got)
	}
	if got := result.ExtraDetail["routed_recipients_count"]; got != 1 {
		t.Fatalf("routed_recipients_count = %#v, want 1", got)
	}
	if got := result.ExtraDetail["subscription_recipients_count"]; got != 3 {
		t.Fatalf("subscription_recipients_count = %#v, want 3", got)
	}
	routed, _ := result.ExtraDetail["routed_recipients"].([]map[string]any)
	if len(routed) != 1 || routed[0]["id"] != "scan-orchestrator" {
		t.Fatalf("routed_recipients detail = %#v", result.ExtraDetail["routed_recipients"])
	}
}

func TestDeliveryRecipientPolicy_FiltersExplicitAgentScopeIntoManifest(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{
				"entity-agent": {AgentID: "entity-agent", EntityID: "ent-1"},
				"other-agent":  {AgentID: "other-agent", EntityID: "ent-2"},
				"shared-agent": {AgentID: "shared-agent"},
			}, true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}), agentDeliveryRecipientCandidates([]string{"entity-agent", "other-agent", "shared-agent"}))
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if got, want := len(manifest.Recipients), 2; got != want {
		t.Fatalf("recipient count = %d, want %d", got, want)
	}
	if manifest.Recipients[0] != "entity-agent" || manifest.Recipients[1] != "shared-agent" {
		t.Fatalf("recipients = %#v, want [entity-agent shared-agent]", manifest.Recipients)
	}
	if len(manifest.PersistedRecipients) != 2 || manifest.PersistedRecipients[0] != "entity-agent" || manifest.PersistedRecipients[1] != "shared-agent" {
		t.Fatalf("persisted recipients = %#v, want [entity-agent shared-agent]", manifest.PersistedRecipients)
	}
}

func TestDeliveryRecipientPolicy_KeepsInternalSubscribersLiveOnlyUnderDescriptorPlanning(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{
				"agent-a": {AgentID: "agent-a"},
			}, true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}), []deliveryRecipientCandidate{
		{ID: "workflow-runtime", PersistAsDelivery: false},
		{ID: "node:scan-orchestrator", PersistAsDelivery: false},
		{ID: "agent-a", PersistAsDelivery: true},
		{ID: "missing-agent", PersistAsDelivery: true},
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(manifest.Recipients) != 3 || manifest.Recipients[0] != "workflow-runtime" || manifest.Recipients[1] != "node:scan-orchestrator" || manifest.Recipients[2] != "agent-a" {
		t.Fatalf("recipients = %#v, want [workflow-runtime node:scan-orchestrator agent-a]", manifest.Recipients)
	}
	if len(manifest.PersistedRecipients) != 1 || manifest.PersistedRecipients[0] != "agent-a" {
		t.Fatalf("persisted recipients = %#v, want [agent-a]", manifest.PersistedRecipients)
	}
}

func TestDeliveryRecipientPolicy_TargetedEventFailsWhenTargetInstanceIsGone(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{
				"agent-a": {AgentID: "agent-a", EntityID: "ent-1", FlowInstance: "flow/active"},
			}, true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RootIngress(
		"",
		"task.completed",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-2", FlowInstance: "flow/missing"}),
		time.Time{},
	), agentDeliveryRecipientCandidates([]string{"agent-a"}))
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if manifest.TargetFailure != runtimepinrouting.FailureTargetUnreachableTerminated {
		t.Fatalf("target failure = %q, want %q", manifest.TargetFailure, runtimepinrouting.FailureTargetUnreachableTerminated)
	}
	if len(manifest.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none", manifest.PersistedRecipients)
	}
}

func TestDeliveryRecipientPolicy_TargetedEventFailsWhenTargetDoesNotSubscribe(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{
				"target-agent": {AgentID: "target-agent", EntityID: "ent-1", FlowInstance: "flow/target"},
			}, true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RootIngress(
		"",
		"task.completed",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1", FlowInstance: "flow/target"}),
		time.Time{},
	), agentDeliveryRecipientCandidates([]string{"other-agent"}))
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if manifest.TargetFailure != runtimepinrouting.FailureTargetNotSubscribed {
		t.Fatalf("target failure = %q, want %q", manifest.TargetFailure, runtimepinrouting.FailureTargetNotSubscribed)
	}
	if len(manifest.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none", manifest.PersistedRecipients)
	}
}

func TestDeliveryRecipientPolicy_TargetedFlowInstanceWithoutSubscriberIsNotSubscribed(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{}, true, nil
		},
		loadActiveTargetDescriptors: func(context.Context) ([]ActiveTargetDescriptor, bool, error) {
			return []ActiveTargetDescriptor{{
				FlowInstance: "component-scaffold/aaaaaaaa-1111-4111-8111-aaaaaaaa1111",
			}}, true, nil
		},
	}
	target := ActiveTargetDescriptor{FlowInstance: "component-scaffold/aaaaaaaa-1111-4111-8111-aaaaaaaa1111"}.Normalized()

	manifest, err := policy.Evaluate(
		context.Background(),
		eventtest.RootIngress(
			"",
			"component.service.completed",
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
				EntityID:     target.EntityID,
				FlowInstance: target.FlowInstance,
			}),
			time.Time{},
		),

		nil,
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if manifest.TargetFailure != runtimepinrouting.FailureTargetNotSubscribed {
		t.Fatalf("target failure = %q, want %q", manifest.TargetFailure, runtimepinrouting.FailureTargetNotSubscribed)
	}
}

func TestDeliveryRecipientPolicy_TargetedFlowInstanceMissingIsUnreachableTerminated(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
			return map[string]ActiveAgentDescriptor{}, true, nil
		},
		loadActiveTargetDescriptors: func(context.Context) ([]ActiveTargetDescriptor, bool, error) {
			return []ActiveTargetDescriptor{{
				FlowInstance: "component-scaffold/live",
			}}, true, nil
		},
	}

	manifest, err := policy.Evaluate(
		context.Background(),
		eventtest.RootIngress(
			"",
			"component.service.completed",
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
				EntityID:     ActiveTargetDescriptor{FlowInstance: "component-scaffold/missing"}.Normalized().EntityID,
				FlowInstance: "component-scaffold/missing",
			}),
			time.Time{},
		),

		nil,
	)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if manifest.TargetFailure != runtimepinrouting.FailureTargetUnreachableTerminated {
		t.Fatalf("target failure = %q, want %q", manifest.TargetFailure, runtimepinrouting.FailureTargetUnreachableTerminated)
	}
}

func TestDeliveryPlanner_ComposesRoutingPolicyAndManifest(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: "worker", Type: "node"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{
					{ID: "worker", PersistAsDelivery: false},
					{ID: "observer", PersistAsDelivery: true},
				}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "worker", Type: "node"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return nil, false, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := len(plan.RecipientIDs()), 2; got != want {
		t.Fatalf("plan recipients = %d, want %d", got, want)
	}
	if got, want := len(plan.PersistedRecipientIDs()), 1; got != want {
		t.Fatalf("persisted recipients = %d, want %d", got, want)
	}
	if got := plan.PersistedRecipientIDs()[0]; got != "observer" {
		t.Fatalf("persisted recipient = %q, want observer", got)
	}
	if got, want := len(plan.LiveRecipients), 2; got != want {
		t.Fatalf("route plan live recipients = %d, want %d", got, want)
	}
	if got, want := len(plan.DeliveryIntents), 2; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	var sawObserverAgent, sawWorkerNode bool
	for _, intent := range plan.DeliveryIntents {
		if intent.SubscriberType == "agent" && intent.SubscriberID == "observer" && intent.Producer == routeIntentProducerAgentPolicy {
			sawObserverAgent = true
		}
		if intent.SubscriberType == "node" && intent.SubscriberID == "worker" && intent.Producer == routeIntentProducerRootNodeRoute {
			sawWorkerNode = true
		}
	}
	if !sawObserverAgent || !sawWorkerNode {
		t.Fatalf("route plan delivery intents = %#v, want observer agent policy and worker node route intents", plan.DeliveryIntents)
	}
	if got := plan.ExtraDetail["routed_recipients_count"]; got != 1 {
		t.Fatalf("routed_recipients_count = %#v, want 1", got)
	}
}

func TestDeliveryPlanner_DoesNotDeadLetterTargetedWorkflowNodeSubscriber(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: "parent-listener", Type: "node"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "parent-listener", Type: "node"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"child/output.done",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1"}),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none for routed workflow node subscriber", plan.TargetFailure)
	}
	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "parent-listener",
		Target:         events.RouteIdentity{EntityID: "ent-1"},
	}
	if len(plan.DeliveryRoutes()) != 1 || !deliveryPlannerRoutesContain(plan.DeliveryRoutes(), wantRoute) {
		t.Fatalf("delivery routes = %#v, want semantic node route %#v", plan.DeliveryRoutes(), wantRoute)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerInternalTargetRoute {
		t.Fatalf("route plan delivery intent = %#v, want internal targeted route-table node authority", intent)
	}
}

func TestDeliveryPlanner_TargetedParentRoutePersistsSemanticNodeRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: "parent-collector", Type: "node", Path: "parent/inst-1"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "parent-collector", Type: "node", Path: "parent/inst-1"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	parentRoute := events.RouteIdentity{EntityID: "parent-entity", FlowInstance: "parent/inst-1"}
	plan, err := planner.Plan(context.Background(), eventtest.RootIngress("", "child/output.done", "", "", nil, 0, "", "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, parentRoute), time.Time{}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none for targeted ParentRoute node subscriber", plan.TargetFailure)
	}
	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "parent-collector",
		Target:         parentRoute,
	}
	if len(plan.DeliveryRoutes()) != 1 || !deliveryPlannerRoutesContain(plan.DeliveryRoutes(), wantRoute) {
		t.Fatalf("delivery routes = %#v, want ParentRoute semantic node route %#v", plan.DeliveryRoutes(), wantRoute)
	}
}

func TestDeliveryPlanner_PreservesTargetFailureWhenRoutedNodeDoesNotMatchTarget(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{ID: "unrelated-listener", Type: "node", Path: "other-flow"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "unrelated-listener", Type: "node"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"child/output.done",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1", FlowInstance: "target-flow"}),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TargetFailure != runtimepinrouting.FailureTargetUnreachableTerminated {
		t.Fatalf("target failure = %q, want %q", plan.TargetFailure, runtimepinrouting.FailureTargetUnreachableTerminated)
	}
}

func TestDeliveryPlanner_ExpandsTargetSetForInternalWorkflowRecipient(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{ID: "child-a-listener", Type: "node", Path: "child-a/inst-1"},
					{ID: "child-b-listener", Type: "node", Path: "child-b/inst-1"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"child/output.done",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
			{FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
		}),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none for target-routed workflow nodes", plan.TargetFailure)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want [workflow-runtime]", got)
	}
	if got := plan.DeliveryRoutes(); len(got) != 4 {
		t.Fatalf("delivery routes = %#v, want 4 target routes", got)
	}
	wantRoutes := []events.DeliveryRoute{
		{SubscriberType: "node", SubscriberID: "child-a-listener", Target: events.RouteIdentity{FlowInstance: "child-a/inst-1", EntityID: "ent-a"}},
		{SubscriberType: "node", SubscriberID: "child-b-listener", Target: events.RouteIdentity{FlowInstance: "child-b/inst-1", EntityID: "ent-b"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "child-a/inst-1", EntityID: "ent-a"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "child-b/inst-1", EntityID: "ent-b"}},
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryPlannerRoutesContain(plan.DeliveryRoutes(), wantRoute) {
			t.Fatalf("delivery routes = %#v, missing %#v", plan.DeliveryRoutes(), wantRoute)
		}
	}
	if got, want := len(plan.DeliveryIntents), 4; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	var semanticNodeRoutes, carrierRoutes int
	for _, intent := range plan.DeliveryIntents {
		if intent.Producer == routeIntentProducerInternalTargetRoute && (intent.SubscriberID == "child-a-listener" || intent.SubscriberID == "child-b-listener") {
			semanticNodeRoutes++
		}
		if intent.Producer == routeIntentProducerInternalTargetCarrier && intent.SubscriberID == "workflow-runtime" {
			carrierRoutes++
		}
	}
	if semanticNodeRoutes != 2 || carrierRoutes != 2 {
		t.Fatalf("route plan delivery intents = %#v, want 2 semantic node routes and 2 internal carrier routes", plan.DeliveryIntents)
	}
}

func TestDeliveryPlanner_ExpandsTargetSetForSameSemanticNode(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{ID: "task-handler", Type: "node", Path: "worker/w-001"},
					{ID: "task-handler", Type: "node", Path: "worker/w-002"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"worker/work.assign",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowInstance: "worker/w-001", EntityID: "worker/w-001"},
			{FlowInstance: "worker/w-002", EntityID: "worker/w-002"},
		}),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	wantRoutes := []events.DeliveryRoute{
		{SubscriberType: "node", SubscriberID: "task-handler", Target: events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}},
		{SubscriberType: "node", SubscriberID: "task-handler", Target: events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}},
	}
	if got := len(plan.DeliveryRoutes()); got != len(wantRoutes) {
		t.Fatalf("delivery routes = %#v, want %d same-node target routes", plan.DeliveryRoutes(), len(wantRoutes))
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryPlannerRoutesContain(plan.DeliveryRoutes(), wantRoute) {
			t.Fatalf("delivery routes = %#v, missing %#v", plan.DeliveryRoutes(), wantRoute)
		}
	}
}

func TestDeliveryPlanner_NoTargetConcreteRoutedNodePersistsSemanticNodeRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:   "lifecycle-orchestrator",
					Type: "node",
					Path: "operating/inst-1",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"operating/inst-1/opco.product_initialization_requested",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-operating"), "operating/inst-1"),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want [workflow-runtime]", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want lifecycle-orchestrator semantic node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "lifecycle-orchestrator" {
		t.Fatalf("delivery route = %#v, want node/lifecycle-orchestrator semantic authority", route)
	}
	if route.Target.FlowInstance != "operating/inst-1" || route.Target.EntityID != "ent-operating" {
		t.Fatalf("delivery target = %#v, want operating/inst-1 ent-operating", route.Target)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerConcreteNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want concrete route-table semantic node source", intent)
	}
}

func TestRoutedNodeInternalSubscriptionAliases_NestedSemanticScopeDoesNotLeakParentConcreteRoute(t *testing.T) {
	evt := eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/micro.started"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "child/grandchild/inst-1"),
		time.Time{},
	)

	aliases := routedNodeInternalSubscriptionAliases(evt, []Subscriber{{
		ID:   "grandchild-worker",
		Type: "node",
		Path: "child/grandchild",
	}})

	for _, alias := range aliases {
		if alias == "child/inst-1/micro.started" {
			t.Fatalf("aliases = %#v, leaked parent concrete route alias", aliases)
		}
	}
	want := map[string]struct{}{
		"child/grandchild/micro.started":        {},
		"child/grandchild/inst-1/micro.started": {},
	}
	if len(aliases) != len(want) {
		t.Fatalf("aliases = %#v, want exactly semantic and concrete route aliases", aliases)
	}
	for _, alias := range aliases {
		if _, ok := want[alias]; !ok {
			t.Fatalf("aliases = %#v, unexpected alias %q", aliases, alias)
		}
	}
}

func TestRoutedEventKeysForPlan_RuntimeCallbackLocalEventWithFlowInstanceDerivesConcreteKey(t *testing.T) {
	tests := []struct {
		name         string
		eventType    string
		flowInstance string
		want         []string
	}{
		{
			name:         "success callback",
			eventType:    "repo_scaffold.repo_commit_succeeded",
			flowInstance: "repo-scaffold/inst-1",
			want: []string{
				"repo_scaffold.repo_commit_succeeded",
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
			},
		},
		{
			name:         "failure callback",
			eventType:    "repo_scaffold.repo_commit_failed",
			flowInstance: "repo-scaffold/inst-1",
			want: []string{
				"repo_scaffold.repo_commit_failed",
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_failed",
			},
		},
		{
			name:         "semantic scoped event keeps existing concrete derivation",
			eventType:    "repo-scaffold/repo_scaffold.repo_commit_succeeded",
			flowInstance: "repo-scaffold/inst-1",
			want: []string{
				"repo-scaffold/repo_scaffold.repo_commit_succeeded",
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
			},
		},
		{
			name:         "root flow instance has no semantic scope",
			eventType:    "repo_scaffold.repo_commit_succeeded",
			flowInstance: "11111111-1111-4111-8111-111111111111",
			want: []string{
				"repo_scaffold.repo_commit_succeeded",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := routedEventKeysForPlan(eventtest.RootIngress(
				"",
				events.EventType(tc.eventType),
				"",
				"",
				nil,
				0,
				"",
				"",
				events.EnvelopeForFlowInstance(events.EventEnvelope{}, tc.flowInstance),
				time.Time{},
			))
			if len(got) != len(tc.want) {
				t.Fatalf("event keys = %#v, want %#v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("event keys = %#v, want %#v", got, tc.want)
				}
			}
		})
	}
}

func deliveryPlannerRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	want = want.Normalized()
	for _, got := range events.NormalizeDeliveryRoutes(routes) {
		if got == want {
			return true
		}
	}
	return false
}

func TestResolveInternalRecipientsForRoutedNodePlanning_DoesNotSelectParentConcreteRouteForNestedSemanticScope(t *testing.T) {
	eb, err := NewEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SubscribeInternal("parent-carrier", events.EventType("child/inst-1/micro.started"))
	defer eb.Unsubscribe("parent-carrier")

	evt := eventtest.RootIngress(
		"",
		events.EventType("child/grandchild/micro.started"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "child/grandchild/inst-1"),
		time.Time{},
	)
	got := eb.resolveInternalRecipientsForRoutedNodePlanning(evt, []Subscriber{{
		ID:   "grandchild-worker",
		Type: "node",
		Path: "child/grandchild",
	}})

	if len(got) != 0 {
		t.Fatalf("internal recipients = %#v, want none for parent concrete route", got)
	}
}

func TestDeliveryPlanner_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:           "portfolio-node",
					Type:         "node",
					MatchPattern: "opco.spinup_requested",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress("", "opco.spinup_requested", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"), time.Time{}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 0 {
		t.Fatalf("recipients = %#v, want none without an internal carrier", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal node", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want semantic root node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if !route.Target.Empty() {
		t.Fatalf("delivery target = %#v, want empty root target", route.Target)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
}

func TestDeliveryPlanner_NoTargetRootLocalEventWithFlowInstanceUsesRootNodeRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:           "test-node",
					Type:         "node",
					MatchPattern: "timer.check",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"timer.check",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"), "11111111-1111-4111-8111-111111111111"),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want test-node root node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "test-node" {
		t.Fatalf("delivery route = %#v, want node/test-node", route)
	}
	if !route.Target.Empty() {
		t.Fatalf("delivery target = %#v, want empty root target", route.Target)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerRootNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want root route-table node source", intent)
	}
}

func TestDeliveryPlanner_NoTargetScopedRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:           "child-intake",
					Type:         "node",
					Path:         "child",
					MatchPattern: "child/child.start",
					RouteSource:  "subscription",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress("", "child/child.start", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"), time.Time{}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want child-intake semantic node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "child-intake" {
		t.Fatalf("delivery route = %#v, want node/child-intake", route)
	}
	if route.Target.FlowInstance != "child" || route.Target.EntityID != "ent-child" {
		t.Fatalf("delivery target = %#v, want child ent-child", route.Target)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerScopedNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want scoped route-table node source", intent)
	}
}

func TestDeliveryPlanner_NoTargetScopedEventPreservesPathlessRoutedNodeRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{
						ID:           "project-observer",
						Type:         "node",
						MatchPattern: "child/child.start",
						RouteSource:  "subscription",
					},
					{
						ID:           "child-intake",
						Type:         "node",
						Path:         "child",
						MatchPattern: "child/child.start",
						RouteSource:  "subscription",
					},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress("", "child/child.start", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"), time.Time{}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if got := plan.DeliveryRoutes(); len(got) != 2 {
		t.Fatalf("delivery routes = %#v, want project-observer and child-intake semantic node routes", got)
	}
	routes := map[string]events.RouteIdentity{}
	for _, route := range plan.DeliveryRoutes() {
		if route.SubscriberType != "node" {
			t.Fatalf("delivery route = %#v, want node route", route)
		}
		routes[route.SubscriberID] = route.Target
	}
	if target, ok := routes["project-observer"]; !ok || !target.Empty() {
		t.Fatalf("project-observer target = %#v, ok=%v; want empty root target", target, ok)
	}
	if target, ok := routes["child-intake"]; !ok || target.FlowInstance != "child" || target.EntityID != "ent-child" {
		t.Fatalf("child-intake target = %#v, ok=%v; want child ent-child", target, ok)
	}
	if got, want := len(plan.DeliveryIntents), 2; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
}

func TestDeliveryPlanner_NoTargetCrossFlowStaticRoutedNodeUsesSubscriberScope(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:           "flow-a-node",
					Type:         "node",
					Path:         "flow-a",
					MatchPattern: "flow-b/order.completed",
					RouteSource:  "subscription",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"flow-b/order.completed",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-flow-b"), "flow-b/inst-1"),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want flow-a-node semantic node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "flow-a-node" {
		t.Fatalf("delivery route = %#v, want node/flow-a-node", route)
	}
	if route.Target.FlowInstance != "flow-a" || route.Target.EntityID != "ent-flow-b" {
		t.Fatalf("delivery target = %#v, want flow-a ent-flow-b", route.Target)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerScopedNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want scoped route-table node source", intent)
	}
}

func TestDeliveryPlanner_NoTargetWildcardStaticServiceRoutedNodeUsesSubscriberScope(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:           "repo-scaffold-node",
					Type:         "node",
					Path:         "repo-scaffold",
					MatchPattern: "component-scaffold/*/opco.repo_scaffold_requested",
					RouteSource:  "subscription",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"component-scaffold/component-a/opco.repo_scaffold_requested",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-component"), "component-scaffold/component-a"),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want repo-scaffold-node wildcard static-service node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "repo-scaffold-node" {
		t.Fatalf("delivery route = %#v, want node/repo-scaffold-node", route)
	}
	if route.Target.FlowInstance != "repo-scaffold" || route.Target.EntityID != "ent-component" {
		t.Fatalf("delivery target = %#v, want repo-scaffold ent-component", route.Target)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerScopedNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want scoped route-table node source", intent)
	}
}

func TestDeliveryPlanner_NoTargetDescendantScopedRoutedNodeUsesParentInstanceRoute(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					ID:           "grandchild-worker",
					Type:         "node",
					Path:         "child/grandchild",
					MatchPattern: "child/grandchild/micro.start",
					RouteSource:  "subscription",
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return map[string]ActiveAgentDescriptor{}, true, nil
			},
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RootIngress(
		"",
		"child/grandchild/micro.start",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"), "child/inst-1"),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
	}
	if len(plan.PersistedRecipientIDs()) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipientIDs())
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want grandchild-worker semantic node route", got)
	}
	route := plan.DeliveryRoutes()[0]
	if route.SubscriberType != "node" || route.SubscriberID != "grandchild-worker" {
		t.Fatalf("delivery route = %#v, want node/grandchild-worker", route)
	}
	if route.Target.FlowInstance != "child/inst-1/grandchild" || route.Target.EntityID != "ent-child" {
		t.Fatalf("delivery target = %#v, want child/inst-1/grandchild ent-child", route.Target)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerScopedNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want scoped route-table node source", intent)
	}
}

func TestDeliveryPlanner_FailsClosedOnPolicyError(t *testing.T) {
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber { return nil },
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "worker", PersistAsDelivery: true}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[string]ActiveAgentDescriptor, bool, error) {
				return nil, false, errors.New("descriptor store unavailable")
			},
		},
	)

	_, err := planner.Plan(context.Background(), eventtest.RootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err == nil || err.Error() != "descriptor store unavailable" {
		t.Fatalf("Plan err = %v, want descriptor store unavailable", err)
	}
}
