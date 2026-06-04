package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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

	result := resolver.Resolve(events.Event{Type: events.EventType("producer/scan.requested")})
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

	manifest, err := policy.Evaluate(context.Background(), (events.Event{
		Type: "task.completed",
	}).WithEntityID("ent-1"), agentDeliveryRecipientCandidates([]string{"entity-agent", "other-agent", "shared-agent"}))
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

	manifest, err := policy.Evaluate(context.Background(), events.Event{Type: "task.completed"}, []deliveryRecipientCandidate{
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

	manifest, err := policy.Evaluate(context.Background(), (events.Event{
		Type: "task.completed",
	}).WithTargetRoute(events.RouteIdentity{EntityID: "ent-2", FlowInstance: "flow/missing"}), agentDeliveryRecipientCandidates([]string{"agent-a"}))
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

	manifest, err := policy.Evaluate(context.Background(), (events.Event{
		Type: "task.completed",
	}).WithTargetRoute(events.RouteIdentity{EntityID: "ent-1", FlowInstance: "flow/target"}), agentDeliveryRecipientCandidates([]string{"other-agent"}))
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

	plan, err := planner.Plan(context.Background(), events.Event{Type: "task.completed"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := len(plan.Recipients), 2; got != want {
		t.Fatalf("plan recipients = %d, want %d", got, want)
	}
	if got, want := len(plan.PersistedRecipients), 1; got != want {
		t.Fatalf("persisted recipients = %d, want %d", got, want)
	}
	if got := plan.PersistedRecipients[0]; got != "observer" {
		t.Fatalf("persisted recipient = %q, want observer", got)
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

	plan, err := planner.Plan(context.Background(), (events.Event{
		Type: "child/output.done",
	}).WithTargetRoute(events.RouteIdentity{EntityID: "ent-1"}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none for routed workflow node subscriber", plan.TargetFailure)
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

	plan, err := planner.Plan(context.Background(), (events.Event{
		Type: "child/output.done",
	}).WithTargetRoute(events.RouteIdentity{EntityID: "ent-1", FlowInstance: "target-flow"}))
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

	plan, err := planner.Plan(context.Background(), (events.Event{
		Type: "child/output.done",
	}).WithTargetSet([]events.RouteIdentity{
		{FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
		{FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
	}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none for target-routed workflow nodes", plan.TargetFailure)
	}
	if got := plan.Recipients; len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want [workflow-runtime]", got)
	}
	if got := plan.DeliveryRoutes; len(got) != 2 {
		t.Fatalf("delivery routes = %#v, want 2 target routes", got)
	}
	for _, route := range plan.DeliveryRoutes {
		if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
			t.Fatalf("delivery route = %#v, want node/workflow-runtime", route)
		}
		if route.Target.Empty() {
			t.Fatalf("delivery route target is empty: %#v", route)
		}
	}
}

func TestDeliveryPlanner_NoTargetConcreteRoutedNodeUsesInternalCarrierAndNodeRoute(t *testing.T) {
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

	plan, err := planner.Plan(context.Background(), (events.Event{
		Type: "operating/inst-1/opco.product_initialization_requested",
	}).WithEntityID("ent-operating").WithFlowInstance("operating/inst-1"))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.Recipients; len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want [workflow-runtime]", got)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want workflow-runtime carrier route", got)
	}
	route := plan.DeliveryRoutes[0]
	if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
		t.Fatalf("delivery route = %#v, want node/workflow-runtime carrier", route)
	}
	if route.Target.FlowInstance != "operating/inst-1" || route.Target.EntityID != "ent-operating" {
		t.Fatalf("delivery target = %#v, want operating/inst-1 ent-operating", route.Target)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
}

func TestRoutedNodeInternalSubscriptionAliases_NestedSemanticScopeDoesNotLeakParentConcreteRoute(t *testing.T) {
	evt := (events.Event{
		Type: events.EventType("child/grandchild/micro.started"),
	}).WithFlowInstance("child/grandchild/inst-1")

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

func TestResolveInternalRecipientsForRoutedNodePlanning_DoesNotSelectParentConcreteRouteForNestedSemanticScope(t *testing.T) {
	eb, err := NewEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SubscribeInternal("parent-carrier", events.EventType("child/inst-1/micro.started"))
	defer eb.Unsubscribe("parent-carrier")

	evt := (events.Event{
		Type: events.EventType("child/grandchild/micro.started"),
	}).WithFlowInstance("child/grandchild/inst-1")
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

	plan, err := planner.Plan(context.Background(), (events.Event{
		Type: "opco.spinup_requested",
	}).WithEntityID("ent-root"))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := plan.Recipients; len(got) != 0 {
		t.Fatalf("recipients = %#v, want none without an internal carrier", got)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal node", plan.PersistedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want semantic root node route", got)
	}
	route := plan.DeliveryRoutes[0]
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

	_, err := planner.Plan(context.Background(), events.Event{Type: "task.completed"})
	if err == nil || err.Error() != "descriptor store unavailable" {
		t.Fatalf("Plan err = %v, want descriptor store unavailable", err)
	}
}
