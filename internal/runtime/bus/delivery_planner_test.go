package bus

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func testActiveAgentDescriptor(t testing.TB, agentID, entityID, flowInstance string) ActiveAgentDescriptor {
	t.Helper()
	identity := agentidentitytest.RootRuntime(t, agentID, "bus-test")
	if flowInstance != "" {
		scopeKey, instanceID := flowInstance, flowInstance
		if slash := strings.IndexByte(flowInstance, '/'); slash >= 0 {
			scopeKey, instanceID = flowInstance[:slash], flowInstance[slash+1:]
		}
		identity = agentidentitytest.Runtime(t, agentID, "bus-test", scopeKey, instanceID, flowInstance)
	}
	return ActiveAgentDescriptor{Identity: identity, EntityID: entityID}
}

func testActiveAgentDescriptors(descriptors ...ActiveAgentDescriptor) map[agentidentity.Identity]ActiveAgentDescriptor {
	out := make(map[agentidentity.Identity]ActiveAgentDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		out[descriptor.Identity] = descriptor
	}
	return out
}

func testSelectedOwnerPolicy(owners ...ActiveTargetDescriptor) deliveryRecipientPolicy {
	return deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return map[agentidentity.Identity]ActiveAgentDescriptor{}, true, nil
		},
		loadActiveTargetDescriptors: func(context.Context) ([]ActiveTargetDescriptor, bool, error) {
			return append([]ActiveTargetDescriptor(nil), owners...), true, nil
		},
		requireTargetOwners: true,
	}
}

func existingOwnerHandlerFixture() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{
		ID: "selected_owner", Check: `_entity.id != ""`,
	}}
}

type deliveryPlannerHandlerFixture struct {
	flowID string
	path   string
	event  string
}

var deliveryPlannerHandlerFixtures = map[string]deliveryPlannerHandlerFixture{
	"worker":                 {flowID: "root", event: "task.completed"},
	"parent-listener":        {flowID: "root", event: "output.done"},
	"parent-collector":       {flowID: "parent", path: "parent", event: "output.done"},
	"unrelated-listener":     {flowID: "other-flow", path: "other-flow", event: "output.done"},
	"child-a-listener":       {flowID: "child-a", path: "child-a", event: "output.done"},
	"child-b-listener":       {flowID: "child-b", path: "child-b", event: "output.done"},
	"task-handler":           {flowID: "worker", path: "worker", event: "work.assign"},
	"lifecycle-orchestrator": {flowID: "operating", path: "operating", event: "opco.product_initialization_requested"},
	"portfolio-node":         {flowID: "root", event: "opco.spinup_requested"},
	"test-node":              {flowID: "root", event: "timer.check"},
	"child-intake":           {flowID: "child", path: "child", event: "child.start"},
	"intake":                 {flowID: "worker", path: "worker", event: "task.assigned"},
	"project-observer":       {flowID: "root", event: "child.start"},
	"flow-a-node":            {flowID: "flow-a", path: "flow-a", event: "task.ready"},
	"repo-scaffold-node":     {flowID: "repo-scaffold", path: "repo-scaffold", event: "webhook.received"},
	"grandchild-worker":      {flowID: "child/grandchild", path: "child/grandchild", event: "micro.started"},
}

func newDeliveryPlannerWithHandlers(t testing.TB, resolver deliveryRouteResolver, policy deliveryRecipientPolicy, connectPlanners ...connectRoutePlanResolver) deliveryPlanner {
	t.Helper()
	source := deliveryPlannerHandlerSource(policy.requireTargetOwners)
	original := resolver.resolveRoutedSubscribers
	resolver.resolveRoutedSubscribers = func(evt events.Event) []Subscriber {
		routed := original(evt)
		for index := range routed {
			subscriber := &routed[index]
			if !subscriber.Recipient.IsNode() || !subscriber.targetHandler.Empty() {
				continue
			}
			fixture, ok := deliveryPlannerHandlerFixtures[subscriber.Recipient.LocalID()]
			if !ok {
				t.Fatalf("missing typed handler fixture for node %s", subscriber.Recipient.LocalID())
			}
			handlerNode := testRootNode(t, subscriber.Recipient.LocalID())
			if fixture.flowID != "root" {
				handlerNode = testFlowNode(t, fixture.flowID, subscriber.Recipient.LocalID())
			}
			handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, handlerNode)
			if err != nil {
				t.Fatalf("admit typed handler fixture for node %s: %v", subscriber.Recipient.LocalID(), err)
			}
			subscriber.handlerNode = handlerNode
			subscriber.Recipient = events.MustNodeDeliveryRecipient(handlerNode)
			subscriber.targetHandler = handler.ForEvent(events.EventType(fixture.event))
			if subscriber.LocalizedEvent == "" {
				subscriber.LocalizedEvent = fixture.event
			}
		}
		return routed
	}
	policy.semanticSource = source
	return newDeliveryPlanner(resolver, policy, connectPlanners...)
}

func deliveryPlannerHandlerSource(requireEntity bool) semanticview.Source {
	rootSchema := runtimecontracts.FlowSchemaDocument{Name: "root"}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Schema: rootSchema, Path: "."}
	byID := map[string]*runtimecontracts.FlowContractView{}
	byPath := map[string]*runtimecontracts.FlowContractView{}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &rootSchema,
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "root", Version: "1.0.0", NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{},
		},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{},
		Events: map[string]runtimecontracts.EventCatalogEntry{},
	}
	flows := map[string]*runtimecontracts.FlowContractView{}
	for nodeID, fixture := range deliveryPlannerHandlerFixtures {
		handler := runtimecontracts.SystemNodeEventHandler{}
		if requireEntity {
			handler = existingOwnerHandlerFixture()
		}
		node := runtimecontracts.SystemNodeContract{
			ID: nodeID, ExecutionType: "system_node", SubscribesTo: []string{fixture.event},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{fixture.event: handler},
		}
		if fixture.flowID == "root" {
			bundle.Nodes[nodeID] = node
			bundle.Events[fixture.event] = runtimecontracts.EventCatalogEntry{}
			bundle.Semantics.NodeHandlers[nodeID] = node.EventHandlers
			continue
		}
		flow := flows[fixture.flowID]
		if flow == nil {
			flow = &runtimecontracts.FlowContractView{
				Path: fixture.path, Paths: runtimecontracts.FlowContractPaths{FlowPath: fixture.path},
				Schema: runtimecontracts.FlowSchemaDocument{Mode: "static"},
				Nodes:  map[string]runtimecontracts.SystemNodeContract{}, Events: map[string]runtimecontracts.EventCatalogEntry{},
			}
			flows[fixture.flowID] = flow
		}
		flow.Nodes[nodeID] = node
		flow.Events[fixture.event] = runtimecontracts.EventCatalogEntry{}
	}
	for _, flow := range flows {
		root.Children = append(root.Children, *flow)
	}
	for index := range root.Children {
		flow := &root.Children[index]
		for flowID, candidate := range flows {
			if candidate == nil || strings.TrimSpace(candidate.Path) != strings.TrimSpace(flow.Path) {
				continue
			}
			byID[flowID] = flow
			break
		}
		byPath[flow.Paths.FlowPath] = flow
	}
	root.Nodes = bundle.Nodes
	root.Events = bundle.Events
	byID["."] = &root
	byPath["."] = &root
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{Root: &root, ByID: byID, ByPath: byPath}
	return semanticview.Wrap(bundle)
}

func TestDeliveryRouteResolver_SeparatesRouteResolutionAndDiagnostics(t *testing.T) {
	resolver := deliveryRouteResolver{
		resolveRoutedSubscribers: func(events.Event) []Subscriber {
			return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "scan-orchestrator")), Path: "discovery",
				MatchPattern: "producer/scan.requested",
				routeSource:  subscriberRouteSourcePinAutoWire,
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

	result := resolver.Resolve(eventtest.RunCreatingRootIngress("", events.EventType("producer/scan.requested"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
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
	if got := result.ExtraDetail["subscription_recipients_count"]; got != 2 {
		t.Fatalf("subscription_recipients_count = %#v, want 2 unique recipients", got)
	}
	routed, _ := result.ExtraDetail["routed_recipients"].([]map[string]any)
	if len(routed) != 1 || routed[0]["id"] != "scan-orchestrator" {
		t.Fatalf("routed_recipients detail = %#v", result.ExtraDetail["routed_recipients"])
	}
}

func TestDeliveryRouteResolver_ResolvesConcreteFlowInstanceSubscriptionKey(t *testing.T) {
	var resolvedKeys []string
	resolver := deliveryRouteResolver{
		resolveRoutedSubscribers: func(events.Event) []Subscriber { return nil },
		resolveSubscribedRecipients: func(eventKey string) []deliveryRecipientCandidate {
			resolvedKeys = append(resolvedKeys, eventKey)
			if eventKey != "support/instance-a/inbound.github.push" {
				return nil
			}
			return []deliveryRecipientCandidate{{
				ID:                "support-agent",
				AgentIdentity:     agentidentitytest.Runtime(t, "support-agent", "bus-test", "support", "instance-a", "support/instance-a"),
				PersistAsDelivery: true,
			}}
		},
		describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
	}
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		"",
		"inbound.github.push",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "support/instance-a"),
		eventtest.ConcreteTemplateRoutingSource("support", "support/instance-a", eventtest.UUID("support-source")),
		time.Time{},
	)

	result := resolver.Resolve(evt)
	if got, want := resolvedKeys, []string{"inbound.github.push", "support/instance-a/inbound.github.push"}; !slices.Equal(got, want) {
		t.Fatalf("resolved subscription keys = %#v, want %#v", got, want)
	}
	if len(result.Recipients) != 1 || result.Recipients[0].ID != "support-agent" {
		t.Fatalf("resolved recipients = %#v, want exact support-agent", result.Recipients)
	}
}

func TestDeliveryRecipientPolicy_FiltersExplicitAgentScopeIntoManifest(t *testing.T) {
	policy := deliveryRecipientPolicy{
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return testActiveAgentDescriptors(
				testActiveAgentDescriptor(t, "entity-agent", "ent-1", ""),
				testActiveAgentDescriptor(t, "other-agent", "ent-2", ""),
				testActiveAgentDescriptor(t, "shared-agent", "", ""),
			), true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Time{}), agentDeliveryRecipientCandidates([]string{"entity-agent", "other-agent", "shared-agent"}))
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
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return testActiveAgentDescriptors(testActiveAgentDescriptor(t, "agent-a", "", "")), true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}), []deliveryRecipientCandidate{
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
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return testActiveAgentDescriptors(testActiveAgentDescriptor(t, "agent-a", "ent-1", "flow/active")), true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RunCreatingRootIngress(
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
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return testActiveAgentDescriptors(testActiveAgentDescriptor(t, "target-agent", "ent-1", "flow/target")), true, nil
		},
	}

	manifest, err := policy.Evaluate(context.Background(), eventtest.RunCreatingRootIngress(
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
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return map[agentidentity.Identity]ActiveAgentDescriptor{}, true, nil
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
		eventtest.RunCreatingRootIngress(
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
		loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			return map[agentidentity.Identity]ActiveAgentDescriptor{}, true, nil
		},
		loadActiveTargetDescriptors: func(context.Context) ([]ActiveTargetDescriptor, bool, error) {
			return []ActiveTargetDescriptor{{
				FlowInstance: "component-scaffold/live",
			}}, true, nil
		},
	}

	manifest, err := policy.Evaluate(
		context.Background(),
		eventtest.RunCreatingRootIngress(
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

func TestDeliveryPlanner_DirectSameSlugUsesTargetRouteBeforeAmbiguity(t *testing.T) {
	descriptorA := testActiveAgentDescriptor(t, "requester", "requester-entity", "review/instance-a")
	descriptorB := testActiveAgentDescriptor(t, "requester", "requester-entity", "review/instance-b")
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return testActiveAgentDescriptors(descriptorA, descriptorB), true, nil
			},
		},
	)
	target := events.RouteIdentity{EntityID: "requester-entity", FlowInstance: "review/instance-b"}
	evt := eventtest.RunCreatingRootIngress(
		"",
		"human_task.approved",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
		time.Time{},
	)

	plan, err := planner.PlanDirect(context.Background(), evt, []string{"requester"})
	if err != nil {
		t.Fatalf("PlanDirect targeted requester: %v", err)
	}
	routes := plan.DeliveryRoutes()
	if len(routes) != 1 || routes[0].AgentIdentity != descriptorB.Identity || routes[0].Target.Route().Normalized() != target.Normalized() {
		t.Fatalf("targeted direct routes = %#v, want only requester instance B", routes)
	}

	untargeted := eventtest.RunCreatingRootIngress(
		"",
		"human_task.approved",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "requester-entity"),
		time.Time{},
	)
	if _, err := planner.PlanDirect(context.Background(), untargeted, []string{"requester"}); err == nil ||
		!strings.Contains(err.Error(), `direct recipient agent_id "requester" is ambiguous`) {
		t.Fatalf("untargeted same-slug PlanDirect error = %v, want ambiguity rejection", err)
	}
}

func TestDeliveryPlanner_ExactDirectRoutePreservesCommittedTargetWithoutDescriptorReinterpretation(t *testing.T) {
	descriptor := testActiveAgentDescriptor(t, "replay-agent", "", "")
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return testActiveAgentDescriptors(descriptor), true, nil
			},
		},
	)
	target := events.RouteIdentity{FlowID: "root-flow", FlowInstance: "run-1", EntityID: "run-1"}
	route := events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient("replay-agent"), AgentIdentity: descriptor.Identity,
		Target: events.MustExistingEntityTarget(target),
	}
	evt := eventtest.RunCreatingRootIngress(
		"", "task.completed", "", "", nil, 0, "", "",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Time{},
	)

	plan, err := planner.PlanExactDirect(context.Background(), evt, []events.DeliveryRoute{route})
	if err != nil {
		t.Fatalf("PlanExactDirect committed target: %v", err)
	}
	if got := plan.LiveRecipients; len(got) != 1 || got[0].AgentIdentity != descriptor.Identity {
		t.Fatalf("exact live recipients = %#v, want exact root agent identity", got)
	}
	if got := plan.DeliveryRoutes(); len(got) != 1 || got[0].Normalized() != route.Normalized() {
		t.Fatalf("exact delivery routes = %#v, want committed route %#v", got, route)
	}

	missing := testActiveAgentDescriptor(t, "replay-agent", "", "other/instance")
	missingRoute := route
	missingRoute.AgentIdentity = missing.Identity
	_, err = planner.PlanExactDirect(context.Background(), evt, []events.DeliveryRoute{missingRoute})
	if !errors.Is(err, ErrExactDirectRecipientUnavailable) {
		t.Fatalf("PlanExactDirect missing exact identity error = %v, want %v", err, ErrExactDirectRecipientUnavailable)
	}
}

func TestDeliveryPlanner_ComposesRoutingPolicyAndManifest(t *testing.T) {
	observerIdentity := agentidentitytest.RootRuntime(t, "observer", "delivery-planner-test")
	runID := uuid.NewString()
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "worker")), Path: "."}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{
					{ID: "worker", PersistAsDelivery: false},
					{ID: "observer", AgentIdentity: observerIdentity, PersistAsDelivery: true},
				}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "worker", Type: "node"}}
			},
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return map[agentidentity.Identity]ActiveAgentDescriptor{
					observerIdentity: {Identity: observerIdentity},
				}, true, nil
			},
			loadActiveTargetDescriptors: func(context.Context) ([]ActiveTargetDescriptor, bool, error) {
				return []ActiveTargetDescriptor{{ID: "root-owner", FlowInstance: runID, EntityID: eventtest.UUID("root-owner")}}, true, nil
			},
			requireTargetOwners: true,
		},
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}))
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
		if intent.Recipient.IsAgent() && intent.Recipient.LocalID() == "observer" && intent.Producer == routeIntentProducerAgentPolicy {
			sawObserverAgent = true
		}
		if intent.Recipient.IsNode() && intent.Recipient.LocalID() == "worker" && intent.Producer == routeIntentProducerRootNodeRoute {
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

func TestDeliveryPlanner_DoesNotDeadLetterExactlyTargetedRootWorkflowNodeSubscriber(t *testing.T) {
	rootRunID := uuid.NewString()
	rootTarget := events.RouteIdentity{FlowID: ".", FlowInstance: rootRunID, EntityID: "ent-1"}
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "parent-listener")), Path: "."}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "parent-listener", Type: "node"}}
			},
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{ID: "root-owner", FlowInstance: rootTarget.FlowInstance, EntityID: rootTarget.EntityID}),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		"child/output.done",
		"",
		"",
		nil,
		0,
		rootRunID,
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, rootTarget),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !plan.TargetFailure.Empty() {
		t.Fatalf("target failure = %q, want none for routed workflow node subscriber", plan.TargetFailure)
	}
	wantRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "parent-listener")),
		Target:    events.MustExistingEntityTarget(rootTarget),
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
	parentRoute := events.RouteIdentity{FlowID: "parent", EntityID: "parent-entity", FlowInstance: "parent/inst-1"}
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "parent", "parent-collector")), Path: "parent/inst-1"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "parent-collector", Type: "node", Path: "parent/inst-1"}}
			},
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{ID: "parent-owner", FlowInstance: parentRoute.FlowInstance, EntityID: parentRoute.EntityID}),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "child/output.done", "", "", nil, 0, "", "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, parentRoute), time.Time{}))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !plan.TargetFailure.Empty() {
		t.Fatalf("target failure = %q, want none for targeted ParentRoute node subscriber", plan.TargetFailure)
	}
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "parent", "parent-collector")), Target: events.MustExistingEntityTarget(parentRoute)}
	if len(plan.DeliveryRoutes()) != 1 || !deliveryPlannerRoutesContain(plan.DeliveryRoutes(), wantRoute) {
		t.Fatalf("delivery routes = %#v, want ParentRoute semantic node route %#v", plan.DeliveryRoutes(), wantRoute)
	}
}

func TestDeliveryPlanner_PreservesTargetFailureWhenRoutedNodeDoesNotMatchTarget(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "other-flow", "unrelated-listener")), Path: "other-flow"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "unrelated-listener", Type: "node"}}
			},
		},
		testSelectedOwnerPolicy(
			ActiveTargetDescriptor{ID: "child-a-owner", FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
			ActiveTargetDescriptor{ID: "child-b-owner", FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
		),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
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
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child-a", "child-a-listener")), Path: "child-a/inst-1"},
					{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child-b", "child-b-listener")), Path: "child-b/inst-1"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(
			ActiveTargetDescriptor{ID: "child-a-owner", FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
			ActiveTargetDescriptor{ID: "child-b-owner", FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
		),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		"child/output.done",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowID: "child-a", FlowInstance: "child-a/inst-1", EntityID: "ent-a"},
			{FlowID: "child-b", FlowInstance: "child-b/inst-1", EntityID: "ent-b"},
		}),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !plan.TargetFailure.Empty() {
		t.Fatalf("target failure = %q, want none for target-routed workflow nodes", plan.TargetFailure)
	}
	if got := plan.RecipientIDs(); len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("recipients = %#v, want [workflow-runtime]", got)
	}
	if got := plan.DeliveryRoutes(); len(got) != 2 {
		t.Fatalf("delivery routes = %#v, want 2 semantic target routes", got)
	}
	wantRoutes := []events.DeliveryRoute{
		{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child-a", "child-a-listener")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child-a", FlowInstance: "child-a/inst-1", EntityID: "ent-a"})},
		{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child-b", "child-b-listener")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child-b", FlowInstance: "child-b/inst-1", EntityID: "ent-b"})},
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryPlannerRoutesContain(plan.DeliveryRoutes(), wantRoute) {
			t.Fatalf("delivery routes = %#v, missing %#v", plan.DeliveryRoutes(), wantRoute)
		}
	}
	if got, want := len(plan.DeliveryIntents), 2; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	var semanticNodeRoutes int
	for _, intent := range plan.DeliveryIntents {
		if intent.Producer == routeIntentProducerInternalTargetRoute && (intent.Recipient.LocalID() == "child-a-listener" || intent.Recipient.LocalID() == "child-b-listener") {
			semanticNodeRoutes++
		}
	}
	if semanticNodeRoutes != 2 {
		t.Fatalf("route plan delivery intents = %#v, want 2 semantic node routes", plan.DeliveryIntents)
	}
}

func TestDeliveryPlanner_ExpandsTargetSetForSameSemanticNode(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "task-handler")), Path: "worker/w-001"},
					{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "task-handler")), Path: "worker/w-002"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(
			ActiveTargetDescriptor{ID: "worker-one", FlowInstance: "worker/w-001", EntityID: "worker/w-001"},
			ActiveTargetDescriptor{ID: "worker-two", FlowInstance: "worker/w-002", EntityID: "worker/w-002"},
		),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		"worker/work.assign",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: "worker/w-001"},
			{FlowID: "worker", FlowInstance: "worker/w-002", EntityID: "worker/w-002"},
		}),
		time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	wantRoutes := []events.DeliveryRoute{
		{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "task-handler")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: "worker/w-001"})},
		{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "task-handler")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-002", EntityID: "worker/w-002"})},
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
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "operating", "lifecycle-orchestrator")), Path: "operating/inst-1"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "operating-owner", FlowInstance: "operating/inst-1", EntityID: "ent-operating-owner",
		}),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngressWithRoutingSource(
		"",
		"operating/inst-1/opco.product_initialization_requested",
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-operating-source"), "operating/inst-1"),
		eventtest.ConcreteTemplateRoutingSource("operating", "operating/inst-1", "ent-operating-source"),
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
	if !route.Recipient.IsNode() || route.Recipient.LocalID() != "lifecycle-orchestrator" {
		t.Fatalf("delivery route = %#v, want node/lifecycle-orchestrator semantic authority", route)
	}
	if target := route.Target.Route(); target.FlowInstance != "operating/inst-1" || target.EntityID != "ent-operating-owner" {
		t.Fatalf("delivery target = %#v, want exact selected operating owner", route.Target)
	}
	if !plan.TargetFailure.Empty() {
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
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		"",
		events.EventType("child/grandchild/micro.started"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "child/grandchild/inst-1"),
		eventtest.ConcreteTemplateRoutingSource("grandchild", "child/grandchild/inst-1", eventtest.UUID("grandchild-source")),
		time.Time{},
	)

	aliases := routedNodeInternalSubscriptionAliases(evt, []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "grandchild", "grandchild-worker")), Path: "child/grandchild"}})

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
			source := eventtest.RootRoutingSource(eventtest.UUID("root-source"))
			if strings.Contains(tc.flowInstance, "/") {
				parts := strings.Split(tc.flowInstance, "/")
				source = eventtest.ConcreteTemplateRoutingSource(parts[len(parts)-2], tc.flowInstance, eventtest.UUID("concrete-source"))
			}
			got := routedEventKeysForPlan(eventtest.RunCreatingRootIngressWithRoutingSource(
				"",
				events.EventType(tc.eventType),
				"",
				"",
				nil,
				0,
				"",
				"",
				events.EnvelopeForFlowInstance(events.EventEnvelope{}, tc.flowInstance),
				source,
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
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	subscribeInternalDeliveriesForTest(t, eb, "parent-carrier", events.EventType("child/inst-1/micro.started"))
	defer unsubscribeTestAgent(eb, "parent-carrier")

	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		"",
		events.EventType("child/grandchild/micro.started"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "child/grandchild/inst-1"),
		eventtest.ConcreteTemplateRoutingSource("grandchild", "child/grandchild/inst-1", eventtest.UUID("grandchild-source")),
		time.Time{},
	)
	got := eb.resolveInternalRecipientsForRoutedNodePlanning(evt, []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "grandchild", "grandchild-worker")), Path: "child/grandchild"}})

	if len(got) != 0 {
		t.Fatalf("internal recipients = %#v, want none for parent concrete route", got)
	}
}

func TestDeliveryPlanner_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	runID := uuid.NewString()
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "portfolio-node")), Path: ".", MatchPattern: "opco.spinup_requested"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return nil
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "root-owner", FlowInstance: runID, EntityID: "ent-root",
		}),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "opco.spinup_requested", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{}))
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
	if !route.Recipient.IsNode() || route.Recipient.LocalID() != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if route.Target.Route() != (events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: "ent-root"}) {
		t.Fatalf("delivery target = %#v, want exact selected root owner", route.Target)
	}
	if !plan.TargetFailure.Empty() {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
}

func TestDeliveryPlanner_NoTargetRootLocalEventWithFlowInstanceUsesRootNodeRoute(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "test-node")), Path: ".", MatchPattern: "timer.check"}}
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
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "root-owner", FlowInstance: runID, EntityID: "ent-root",
		}),
	)

	plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
		"",
		"timer.check",
		"",
		"",
		nil,
		0,
		runID,
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, runID),
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
	if !route.Recipient.IsNode() || route.Recipient.LocalID() != "test-node" {
		t.Fatalf("delivery route = %#v, want node/test-node", route)
	}
	if route.Target.Route() != (events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: "ent-root"}) {
		t.Fatalf("delivery target = %#v, want exact selected root owner", route.Target)
	}
	if got, want := len(plan.DeliveryIntents), 1; got != want {
		t.Fatalf("route plan delivery intents = %d, want %d", got, want)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Producer != routeIntentProducerRootNodeRoute {
		t.Fatalf("route plan delivery intent = %#v, want root route-table node source", intent)
	}
}

func TestDeliveryPlanner_NoTargetScopedRoutedNodeWithoutFlowInstanceFailsClosed(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child", "child-intake")), Path: "child",
					MatchPattern: "child/child.start",
					routeSource:  subscriberRouteSourceSubscription,
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
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "child-owner", FlowInstance: "child", EntityID: "ent-child-owner",
		}),
	)

	if _, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "child/child.start", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"), time.Time{})); err == nil || !strings.Contains(err.Error(), "without exact same-instance, explicit-target, or compiled-connect authority") {
		t.Fatalf("Plan error = %v, want missing target-owner authority", err)
	}
}

func TestDeliveryPlanner_LiveCarrierDoesNotAuthorizeNoTargetScopedRoutedNode(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child", "child-intake")), Path: "child",
					MatchPattern: "child/child.start",
					routeSource:  subscriberRouteSourceSubscription,
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "child-intake", PersistAsDelivery: false}}
			},
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "child-owner", FlowInstance: "child", EntityID: "ent-child-owner",
		}),
	)

	if _, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "child/child.start", "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"), time.Time{})); err == nil || !strings.Contains(err.Error(), "without exact same-instance, explicit-target, or compiled-connect authority") {
		t.Fatalf("Plan error = %v, want live carrier to remain non-authoritative", err)
	}
}

func TestDeliveryPlanner_StaticRootSameInstanceRouteUsesSelectedRunOwner(t *testing.T) {
	runID := uuid.NewString()
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "test-node")), Path: ".",
					MatchPattern: "timer.check", routeSource: subscriberRouteSourceSubscription,
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "root-owner", FlowInstance: runID, EntityID: "ent-root",
		}),
	)

	plan, err := planner.Plan(context.Background(), eventtest.ExistingRunRootIngressWithRoutingSource(
		"", "timer.check", "", "", nil, 0, runID,
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, runID),
		eventtest.RootRoutingSource(eventtest.UUID("root-static-source")), time.Time{},
	))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	want := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: "ent-root"}
	if routes := plan.DeliveryRoutes(); len(routes) != 1 || routes[0].Recipient.LocalID() != "test-node" || routes[0].Target.Route() != want {
		t.Fatalf("delivery routes = %#v, want exact selected-run root owner %#v", routes, want)
	}
	if len(plan.DeliveryIntents) != 1 {
		t.Fatalf("delivery intents = %#v, want one exact concrete-node intent", plan.DeliveryIntents)
	}
	if got := plan.DeliveryIntents[0].Producer; got != routeIntentProducerRootNodeRoute {
		t.Fatalf("delivery intent producer = %s, want root_node_route", routeIntentProducerCode(got))
	}
}

func TestDeliveryPlanner_ExactSameInstanceTargetUsesCompiledReceiverMode(t *testing.T) {
	tests := []struct {
		name             string
		mode             string
		routingSource    events.RoutingSource
		wantFlowInstance string
	}{
		{
			name:             "template_preserves_concrete_instance",
			mode:             runtimecontracts.FlowModeTemplate,
			routingSource:    eventtest.ConcreteTemplateRoutingSource("validation", "validation/instance-a", eventtest.UUID("template-source")),
			wantFlowInstance: "validation/instance-a",
		},
		{
			name:             "static_uses_declared_receiver_scope",
			mode:             runtimecontracts.FlowModeStatic,
			routingSource:    eventtest.StaticFlowRoutingSource("validation", "validation/instance-a", eventtest.UUID("static-source")),
			wantFlowInstance: "validation",
		},
		{
			name:             "singleton_uses_declared_receiver_scope",
			mode:             runtimecontracts.FlowModeSingleton,
			routingSource:    eventtest.StaticFlowRoutingSource("validation", "validation/instance-a", eventtest.UUID("singleton-source")),
			wantFlowInstance: "validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flow := runtimecontracts.FlowContractView{
				Path:   "validation",
				Paths:  runtimecontracts.FlowContractPaths{FlowPath: "validation"},
				Schema: runtimecontracts.FlowSchemaDocument{Mode: tt.mode},
			}
			root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Path: ".", Children: []runtimecontracts.FlowContractView{flow}}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
					Root: &root,
					ByID: map[string]*runtimecontracts.FlowContractView{"validation": &root.Children[0]},
				},
			})
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(
				uuid.NewString(), "validation/thing.reviewed", "validator", "", nil, 0, uuid.NewString(),
				events.EnvelopeForFlowInstance(events.EventEnvelope{}, "validation/instance-a"),
				tt.routingSource, time.Now().UTC(),
			)
			subscriber := Subscriber{
				Recipient:    events.MustNodeDeliveryRecipient(testFlowNode(t, "validation", "entity-writer")),
				Path:         "validation",
				MatchPattern: "validation/thing.reviewed",
				routeSource:  subscriberRouteSourceSubscription,
			}

			intents := routedExactSameInstanceNoTargetNodeDeliveryIntents(source, evt, []Subscriber{subscriber})
			if len(intents) != 1 {
				t.Fatalf("delivery intents = %#v, want one", intents)
			}
			if got := intents[0].TargetBlueprint.FlowInstance; got != tt.wantFlowInstance {
				t.Fatalf("target flow instance = %q, want %q for compiled mode %q", got, tt.wantFlowInstance, tt.mode)
			}
		})
	}
}

func TestDeliveryPlanner_NoTargetMixedRootAndUnrelatedScopedNodeFailsClosed(t *testing.T) {
	runID := uuid.NewString()
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "project-observer")), Path: ".", MatchPattern: "child/child.start",
						routeSource: subscriberRouteSourceSubscription,
					},
					{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child", "child-intake")), Path: "child",
						MatchPattern: "child/child.start",
						routeSource:  subscriberRouteSourceSubscription,
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
		testSelectedOwnerPolicy(
			ActiveTargetDescriptor{ID: "root-owner", FlowInstance: runID, EntityID: "ent-root-owner"},
			ActiveTargetDescriptor{ID: "child-owner", FlowInstance: "child", EntityID: "ent-child-owner"},
		),
	)

	wantNode := testFlowNode(t, "child", "child-intake")
	if _, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "child/child.start", "", "", nil, 0, runID, "", events.EventEnvelope{}, time.Time{})); err == nil || !strings.Contains(err.Error(), `routed node "`+wantNode.Key()+`"`) {
		t.Fatalf("Plan error = %v, want unrelated child route rejection", err)
	}
}

func TestDeliveryPlanner_QualifiedRootInputFlowUsesTypedRouteAuthority(t *testing.T) {
	runID := uuid.NewString()
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					Recipient:      events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "intake")),
					Path:           "worker",
					MatchPattern:   "worker/task.assigned",
					LocalizedEvent: "task.assigned",
					routeSource:    subscriberRouteSourceRootInputFlow,
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "worker-owner", FlowInstance: "worker", EntityID: "ent-worker-owner",
		}),
	)

	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "worker/task.assigned", "operator", "", nil, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, runID), time.Now().UTC(),
	)
	plan, err := planner.Plan(context.Background(), evt)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	want := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "worker", "intake")),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: "worker", FlowInstance: "worker", EntityID: "ent-worker-owner",
		}),
	}
	if routes := plan.DeliveryRoutes(); len(routes) != 1 || !deliveryPlannerRoutesContain(routes, want) {
		t.Fatalf("delivery routes = %#v, want exact root-input owner %#v", routes, want)
	}
	if got := plan.DeliveryIntents[0].Producer; got != routeIntentProducerRootInputFlowNode {
		t.Fatalf("intent producer = %q, want typed root-input-flow authority", got)
	}
}

func TestDeliveryPlanner_NoTargetCrossFlowStaticRoutedNodeFailsClosed(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "flow-a", "flow-a-node")), Path: "flow-a",
					MatchPattern: "flow-b/order.completed",
					routeSource:  subscriberRouteSourceSubscription,
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
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "flow-a-owner", FlowInstance: "flow-a", EntityID: "ent-flow-a-owner",
		}),
	)

	_, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
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
	wantNode := testFlowNode(t, "flow-a", "flow-a-node")
	if err == nil || !strings.Contains(err.Error(), `routed node "`+wantNode.Key()+`"`) {
		t.Fatalf("Plan error = %v, want unrelated cross-flow route rejection", err)
	}
}

func TestDeliveryPlanner_NoTargetWildcardCrossFlowRoutedNodeFailsClosed(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "repo-scaffold", "repo-scaffold-node")), Path: "repo-scaffold",
					MatchPattern: "component-scaffold/*/opco.repo_scaffold_requested",
					routeSource:  subscriberRouteSourceSubscription,
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
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "repo-owner", FlowInstance: "repo-scaffold", EntityID: "ent-repo-owner",
		}),
	)

	_, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
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
	wantNode := testFlowNode(t, "repo-scaffold", "repo-scaffold-node")
	if err == nil || !strings.Contains(err.Error(), `routed node "`+wantNode.Key()+`"`) {
		t.Fatalf("Plan error = %v, want wildcard cross-flow route rejection", err)
	}
}

func TestDeliveryPlanner_NoTargetDescendantScopedRoutedNodeFailsClosed(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "child/grandchild", "grandchild-worker")), Path: "child/grandchild",
					MatchPattern: "child/grandchild/micro.start",
					routeSource:  subscriberRouteSourceSubscription,
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
		testSelectedOwnerPolicy(ActiveTargetDescriptor{
			ID: "grandchild-owner", FlowInstance: "child/inst-1/grandchild", EntityID: "ent-grandchild-owner",
		}),
	)

	_, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress(
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
	wantNode := testFlowNode(t, "child/grandchild", "grandchild-worker")
	if err == nil || !strings.Contains(err.Error(), `routed node "`+wantNode.Key()+`"`) {
		t.Fatalf("Plan error = %v, want descendant route rejection", err)
	}
}

func TestNoTargetNodeRouteFamiliesUseExactPersistedTargetOwner(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "concrete and no-recipient concrete", run: TestDeliveryPlanner_NoTargetConcreteRoutedNodePersistsSemanticNodeRoute},
		{name: "root", run: TestDeliveryPlanner_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute},
		{name: "root local event", run: TestDeliveryPlanner_NoTargetRootLocalEventWithFlowInstanceUsesRootNodeRoute},
		{name: "root input flow", run: TestEventBusPublish_RootInputFlowNodePersistsRouteBeforeDispatch},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

func TestRouteTargetOwnerIgnoresAbsentAndForeignSourceEntity(t *testing.T) {
	owner := events.RouteIdentity{
		FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: eventtest.UUID("selected-operating-owner"),
	}
	for _, test := range []struct {
		name     string
		sourceID string
	}{
		{name: "absent source"},
		{name: "foreign source", sourceID: eventtest.UUID("foreign-source-owner")},
	} {
		t.Run(test.name, func(t *testing.T) {
			planner := newDeliveryPlannerWithHandlers(t,
				deliveryRouteResolver{
					resolveRoutedSubscribers: func(events.Event) []Subscriber {
						return []Subscriber{{
							Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "operating", "lifecycle-orchestrator")),
							Path:      owner.FlowInstance,
						}}
					},
					resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
					resolveRoutedNodeInternalRecipients: func(events.Event, []Subscriber) []deliveryRecipientCandidate {
						return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
					},
					describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
				},
				testSelectedOwnerPolicy(ActiveTargetDescriptor{
					ID: "operating-owner", FlowInstance: owner.FlowInstance, EntityID: owner.EntityID,
				}),
			)
			envelope := events.EnvelopeForFlowInstance(events.EventEnvelope{}, owner.FlowInstance)
			if test.sourceID != "" {
				envelope = events.EnvelopeForEntityID(envelope, test.sourceID)
			}
			plan, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngressWithRoutingSource(
				"", events.EventType(owner.FlowInstance+"/opco.product_initialization_requested"),
				"", "", nil, 0, "", "", envelope,
				eventtest.ConcreteTemplateRoutingSource("operating", owner.FlowInstance, eventtest.UUID("typed-operating-source")), time.Time{},
			))
			if err != nil {
				t.Fatalf("plan no-target owner: %v", err)
			}
			routes := plan.DeliveryRoutes()
			if len(routes) != 1 || routes[0].Target.Route() != owner {
				t.Fatalf("delivery routes = %#v, want exact selected owner %#v", routes, owner)
			}
		})
	}
}

func TestDeliveryPlanner_FailsClosedOnPolicyError(t *testing.T) {
	planner := newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber { return nil },
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "worker", PersistAsDelivery: true}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient { return nil },
		},
		deliveryRecipientPolicy{
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return nil, false, errors.New("descriptor store unavailable")
			},
		},
	)

	_, err := planner.Plan(context.Background(), eventtest.RunCreatingRootIngress("", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err == nil || err.Error() != "descriptor store unavailable" {
		t.Fatalf("Plan err = %v, want descriptor store unavailable", err)
	}
}

func TestRoutedEventKeysForPlan_LocalizesStaticAndMaterializedFlowInstances(t *testing.T) {
	for _, tc := range []struct {
		name         string
		flowInstance string
		want         string
	}{
		{name: "static", flowInstance: "provider", want: "provider/mailbox.card_decided"},
		{name: "materialized template", flowInstance: "requester/account-a", want: "requester/account-a/mailbox.card_decided"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := eventtest.StaticFlowRoutingSource("provider", "provider", eventtest.UUID("provider-source"))
			if strings.Contains(tc.flowInstance, "/") {
				source = eventtest.ConcreteTemplateRoutingSource("requester", tc.flowInstance, eventtest.UUID("requester-source"))
			}
			evt := eventtest.RunCreatingRootIngressWithRoutingSource("", "mailbox.card_decided", "", "", nil, 0, "", "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, tc.flowInstance), source, time.Time{})
			keys := routedEventKeysForPlan(evt)
			found := false
			for _, key := range keys {
				if key == tc.want {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("routed keys = %#v, want %q", keys, tc.want)
			}
		})
	}
}

func TestRoutedRootNodeDeliveryIntentsRejectEmptyPathFromAnotherFlow(t *testing.T) {
	event := eventtest.RunCreatingRootIngress(
		"", "step.begin", "", "", nil, 0, "run-one", "", events.EventEnvelope{}, time.Time{},
	)
	rootNode := testRootNode(t, "root-handler")
	childNode := testFlowNode(t, "child-flow", "child-handler")
	root := Subscriber{
		Recipient:      events.MustNodeDeliveryRecipient(rootNode),
		Path:           ".",
		MatchPattern:   "step.begin",
		routeSource:    subscriberRouteSourceSubscription,
		handlerNode:    rootNode,
		targetHandler:  runtimepipeline.MustDeliveryTargetHandler(rootNode),
		LocalizedEvent: "step.begin",
	}
	child := Subscriber{
		Recipient:      events.MustNodeDeliveryRecipient(childNode),
		MatchPattern:   "step.begin",
		routeSource:    subscriberRouteSourceSubscription,
		handlerNode:    childNode,
		targetHandler:  runtimepipeline.MustDeliveryTargetHandler(childNode),
		LocalizedEvent: "step.begin",
	}
	intents := routedRootNodeDeliveryIntentsForNoTargetEvent(deliveryPlannerHandlerSource(false), event, []Subscriber{child, root})
	if len(intents) != 1 || intents[0].Recipient.LocalID() != "root-handler" || intents[0].TargetBlueprint.FlowID != "." {
		t.Fatalf("root intents = %#v, want only exact authored-root handler", intents)
	}
}
