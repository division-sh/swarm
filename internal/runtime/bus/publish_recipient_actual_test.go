package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPublishRecipientActualRetainsHandlerEventAndExactAgent(t *testing.T) {
	node := testFlowNode(t, "review", "target-node")
	owner := events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"})
	first := RoutePlanDeliveryIntent{
		Recipient: events.MustNodeDeliveryRecipient(node), TargetOwnership: owner,
		Handler: runtimepipeline.MustDeliveryTargetHandler(node).ForEvent("first.requested"),
		Persist: true, Producer: routeIntentProducerScopedNodeRoute,
	}
	second := first
	second.Handler = second.Handler.ForEvent("second.requested")
	identity := agentidentitytest.RootRuntime(t, "worker", "owner-a")
	other := agentidentitytest.RootRuntime(t, "worker", "owner-b")
	agent := RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: identity, Persist: true, Producer: routeIntentProducerAgentPolicy}
	otherAgent := agent
	otherAgent.AgentIdentity = other
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{first, second, agent, otherAgent}}
	actuals, err := plan.Normalized().recipientActuals()
	if err != nil {
		t.Fatal(err)
	}
	if len(actuals) != 4 {
		t.Fatalf("got %d actuals, want four distinct execution authorities", len(actuals))
	}
	seenEvents := map[events.EventType]bool{}
	seenAgents := map[agentidentity.Identity]bool{}
	for _, actual := range actuals {
		if actual.Route().Recipient.IsNode() {
			event, present := actual.Handler().EventOverride()
			if !present {
				t.Fatal("lost handler-local event")
			}
			seenEvents[event] = true
		} else {
			seenAgents[actual.Route().AgentIdentity] = true
		}
	}
	if !seenEvents["first.requested"] || !seenEvents["second.requested"] || !seenAgents[identity] || !seenAgents[other] {
		t.Fatal("exact authority lost in actual projection")
	}
}

func TestPublishRecipientActualRejectsMalformedIntentWithoutDroppingIt(t *testing.T) {
	node := testFlowNode(t, "review", "target-node")
	valid := RoutePlanDeliveryIntent{
		Recipient:       events.MustNodeDeliveryRecipient(node),
		TargetOwnership: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"}),
		Handler:         runtimepipeline.MustDeliveryTargetHandler(node).ForEvent("work.requested"),
		Persist:         true, Producer: routeIntentProducerScopedNodeRoute,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*RoutePlanDeliveryIntent)
	}{
		{"missing recipient", func(i *RoutePlanDeliveryIntent) { i.Recipient = events.DeliveryRecipient{} }},
		{"node with agent", func(i *RoutePlanDeliveryIntent) {
			i.AgentIdentity = agentidentitytest.RootRuntime(t, "worker", "owner")
		}},
		{"agent without identity", func(i *RoutePlanDeliveryIntent) { i.Recipient = events.MustAgentDeliveryRecipient("worker") }},
		{"foreign agent name", func(i *RoutePlanDeliveryIntent) {
			i.Recipient = events.MustAgentDeliveryRecipient("worker")
			i.AgentIdentity = agentidentitytest.RootRuntime(t, "other", "owner")
		}},
		{"missing handler", func(i *RoutePlanDeliveryIntent) { i.Handler = runtimepipeline.DeliveryTargetHandler{} }},
		{"missing producer", func(i *RoutePlanDeliveryIntent) { i.Producer = routeIntentProducerUnknown }},
		{"missing local event", func(i *RoutePlanDeliveryIntent) { i.Handler = runtimepipeline.MustDeliveryTargetHandler(node) }},
		{"connect without evidence", func(i *RoutePlanDeliveryIntent) { i.Producer = routeIntentProducerConnectRoutePlan }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := valid
			tc.mutate(&invalid)
			plan := (RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{valid, invalid}}).Normalized()
			if len(plan.DeliveryIntents) != 2 {
				t.Fatal("normalization hid the malformed intent")
			}
			if _, err := plan.recipientActuals(); err == nil {
				t.Fatal("malformed persistent intent admitted")
			}
		})
	}
	if _, err := (PublishRecipientPlan{DeliveryRoutes: []events.DeliveryRoute{{Recipient: valid.Recipient, Target: valid.TargetOwnership}}}).RecipientActuals(); err == nil {
		t.Fatal("delivery readback substituted for canonical intent evidence")
	}
}

func TestPublishRecipientActualRejectsCompetingProducerBeforeDedup(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyReceiverMixedAgent(t))
	graph := runtimepinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compile canonical connect source: %#v", issues)
	}
	var selected runtimepinrouting.ConnectRoutePlan
	for _, plan := range graph.Plans() {
		if plan.ReceiverEndpoint().Readback().ResolvedEvent == "sink/work.completed" {
			if selected.ReceiverLocalEvent() != "" {
				t.Fatal("canonical fixture has ambiguous receiver plan")
			}
			selected = plan
		}
	}
	if selected.ReceiverLocalEvent() == "" {
		t.Fatal("canonical fixture has no work.completed connect")
	}
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	var routes []runtimepinrouting.ConnectDeliveryRoute
	for _, subscriber := range table.Resolve("sink/work.completed") {
		if !subscriber.Recipient.IsNode() {
			continue
		}
		node, _ := subscriber.Recipient.Node()
		route := runtimepinrouting.ConnectDeliveryRoute{
			Recipient: subscriber.Recipient,
			Handler:   runtimepinrouting.MustConnectReceiverHandler(node),
			Target:    events.RouteIdentity{FlowID: "sink", FlowInstance: "sink"},
		}
		route.ConnectClaim, err = runtimepinrouting.ConnectExecutionClaim(selected, route)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, route)
	}
	if len(routes) != 1 {
		t.Fatalf("canonical receiver node routes = %d, want one", len(routes))
	}
	intents, err := connectRoutePlanDeliveryIntents("11111111-1111-4111-8111-111111111111", selected, routes, routes, false, nil)
	if err != nil || len(intents) != 1 {
		t.Fatalf("canonical compiled connect intents = %#v, err=%v", intents, err)
	}
	valid := intents[0]
	valid.TargetOwnership = events.MustEntitylessReceiverTarget(valid.TargetBlueprint)
	if valid.ConnectPlan.Empty() || valid.ConnectClaim.Empty() {
		t.Fatal("canonical fixture did not retain compiled plan and execution claim")
	}
	if actuals, err := (RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{valid}}).Normalized().recipientActuals(); err != nil || len(actuals) != 1 {
		t.Fatalf("valid compiled connect control rejected: actuals=%#v, err=%v", actuals, err)
	}
	invalid := valid
	invalid.Producer = routeIntentProducerScopedNodeRoute
	if _, err := (RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{invalid}}).recipientActuals(); err == nil {
		t.Fatal("standalone local producer with connect authority was accepted")
	}
	for _, tc := range []struct {
		name    string
		intents []RoutePlanDeliveryIntent
	}{
		{"valid_then_malformed", []RoutePlanDeliveryIntent{valid, invalid}},
		{"malformed_then_valid", []RoutePlanDeliveryIntent{invalid, valid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := (RoutePlan{DeliveryIntents: tc.intents}).Normalized()
			if len(plan.DeliveryIntents) != 2 {
				t.Errorf("normalization hid competing producer: retained %d intents, want 2", len(plan.DeliveryIntents))
			}
			if actuals, err := plan.recipientActuals(); err == nil || len(actuals) != 0 {
				t.Errorf("competing producer returned usable actuals: count=%d, err=%v", len(actuals), err)
			}
		})
	}
}
