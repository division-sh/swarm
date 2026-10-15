package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func selectedRecipientContextFixture(t *testing.T) (context.Context, events.Event, RoutePlan) {
	t.Helper()
	child := uuid.NewString()
	lineage, err := events.NewSelectedForkLineage(child, uuid.NewString(), uuid.NewString(), "selected-owner", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.SelectedForkReplay(uuid.NewString(), "work.ready", eventtest.Producer(events.EventProducerPlatform, "selected-owner"),
		"", []byte(`{}`), 0, lineage, events.EventEnvelope{}, time.Now().UTC())
	plan := RoutePlan{Event: event}
	var recipients []forkrecipient.Evidence
	for _, name := range []string{"first", "second"} {
		node := testFlowNode(t, ".", name)
		evidence, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustNodeDeliveryRecipient(node),
			Path: ".", HandlerNode: node, HandlerEvent: event.Type()})
		if err != nil {
			t.Fatal(err)
		}
		recipients = append(recipients, evidence)
		plan.DeliveryIntents = append(plan.DeliveryIntents, RoutePlanDeliveryIntent{Recipient: evidence.Recipient,
			TargetOwnership: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: child}),
			Handler:         pipeline.MustDeliveryTargetHandler(node).ForEvent(event.Type()), Producer: routeIntentProducerRootNodeRoute, Persist: true})
	}
	actor := agentidentitytest.RootDeclaredForRun(t, child, "worker", "reply-owner")
	actorPlan, err := actor.Plan()
	if err != nil {
		t.Fatal(err)
	}
	actorEvidence, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()),
		Path: ".", AgentPlan: actorPlan, HandlerEvent: event.Type()})
	if err != nil {
		t.Fatal(err)
	}
	recipients = append(recipients, actorEvidence)
	plan.DeliveryIntents = append(plan.DeliveryIntents, RoutePlanDeliveryIntent{Recipient: actorEvidence.Recipient,
		AgentIdentity: actor, Producer: routeIntentProducerAgentPolicy, Persist: true})
	ctx, err := WithSelectedForkRecipientPlan(context.Background(), event, recipients)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, event, plan.Normalized()
}

func TestSelectedForkRecipientContextsRestoreExactRoutesWithoutAddingRecipients(t *testing.T) {
	ctx, event, plan := selectedRecipientContextFixture(t)
	routes := plan.DeliveryRoutes()
	for i := range routes {
		if i != 0 {
			routes[i].Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: uuid.NewString()}}
		}
	}
	want := events.NormalizeDeliveryRoutes(routes)
	bound, err := WithSelectedForkRecipientContexts(ctx, event, routes)
	if err != nil {
		t.Fatal(err)
	}
	// Neither the caller's retained routes nor an ambient reply owns this output.
	routes[1].Context.Reply.ID = "mutated-input"
	for i := range plan.DeliveryIntents {
		plan.DeliveryIntents[i].Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "unrelated-input"}}
	}
	before := plan.Normalized()
	for repeat := 0; repeat < 2; repeat++ {
		got, err := restoreSelectedForkRecipientContexts(bound, event, plan)
		if err != nil || !reflect.DeepEqual(got.DeliveryRoutes(), want) || len(got.DeliveryIntents) != len(plan.DeliveryIntents) {
			t.Fatalf("exact node/agent contexts not restored: got=%+v want=%+v error=%v", got.DeliveryRoutes(), want, err)
		}
		if !reflect.DeepEqual(plan, before) {
			t.Fatal("context restoration changed the input plan")
		}
		for i := range got.DeliveryIntents {
			if got.DeliveryIntents[i].Context.Reply != nil {
				got.DeliveryIntents[i].Context.Reply.ID = "mutated-output"
			}
		}
	}
	if _, err := WithSelectedForkRecipientContexts(bound, event, want); err == nil {
		t.Fatal("an already-bound publication accepted replacement context")
	}
}

func TestSelectedForkRecipientContextsRefuseInexactRoutesAndRemainPublicationLocal(t *testing.T) {
	for _, fault := range []string{"missing", "extra", "wrong_owner", "wrong_actor_run", "contradictory_context"} {
		t.Run(fault, func(t *testing.T) {
			ctx, event, plan := selectedRecipientContextFixture(t)
			routes := plan.DeliveryRoutes()
			switch fault {
			case "missing":
				routes = routes[:len(routes)-1]
			case "extra":
				plan.DeliveryIntents = plan.DeliveryIntents[:len(plan.DeliveryIntents)-1]
			case "wrong_owner":
				for i := range routes {
					if routes[i].Recipient.IsNode() {
						routes[i].Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: uuid.NewString()})
						break
					}
				}
			case "wrong_actor_run":
				for i := range routes {
					if routes[i].Recipient.IsAgent() {
						routes[i].AgentIdentity.RunID = uuid.NewString()
					}
				}
			case "contradictory_context":
				other := routes[0]
				other.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: uuid.NewString()}}
				routes = append(routes, other)
			}
			before := plan.Normalized()
			bound, err := WithSelectedForkRecipientContexts(ctx, event, routes)
			if err == nil {
				_, err = restoreSelectedForkRecipientContexts(bound, event, plan)
			}
			if err == nil || !reflect.DeepEqual(plan, before) {
				t.Fatalf("inexact route was accepted or input changed: error=%v", err)
			}
		})
	}
	ctx, event, plan := selectedRecipientContextFixture(t)
	bound, err := WithSelectedForkRecipientContexts(ctx, event, plan.DeliveryRoutes())
	if err != nil {
		t.Fatal(err)
	}
	ordinary := eventtest.ExistingRunRootIngress(uuid.NewString(), event.Type(), "operator", "", event.Payload(), 0,
		event.RunID(), events.EventEnvelope{}, time.Now().UTC())
	if _, err := WithSelectedForkRecipientContexts(ctx, ordinary, plan.DeliveryRoutes()); err == nil {
		t.Fatal("ordinary publication acquired selected context")
	}
	lineage, _ := event.SelectedForkLineage()
	for _, other := range []events.Event{ordinary, eventtest.SelectedForkReplay(uuid.NewString(), event.Type(), event.Producer(), "", event.Payload(), 0,
		lineage, events.EventEnvelope{}, event.CreatedAt())} {
		got, err := restoreSelectedForkRecipientContexts(bound, other, plan)
		if err != nil || !reflect.DeepEqual(got, plan) {
			t.Fatalf("another publication inherited the context: error=%v", err)
		}
	}
	canceled, cancel := context.WithCancel(bound)
	cancel()
	if _, err := restoreSelectedForkRecipientContexts(canceled, event, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restoration error=%v", err)
	}
}

func TestSelectedForkRecipientContextsPreserveCompiledConnectAuthority(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyReceiverMixedAgent(t))
	graph := pinrouting.CompileConnectGraph(source)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	ctx, event, _ := selectedRecipientContextFixture(t)
	var intents []RoutePlanDeliveryIntent
	for _, plan := range graph.Plans() {
		if plan.ReceiverEndpoint().Readback().ResolvedEvent != "sink/work.completed" {
			continue
		}
		for _, registration := range table.ConnectDeclarationDefinitions("sink") {
			recipient, matched := registration.RecipientForPlan(plan)
			if !matched || recipient.Kind() != pinrouting.ConnectRecipientNode {
				continue
			}
			node := recipient.Handler().Node()
			route := pinrouting.ConnectDeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
				Handler: pinrouting.MustConnectReceiverHandler(node), Target: events.RouteIdentity{FlowID: "sink", FlowInstance: "sink"}}
			route.ConnectClaim, err = pinrouting.ConnectExecutionClaim(plan, route)
			if err != nil {
				t.Fatal(err)
			}
			intents, err = connectRoutePlanDeliveryIntents(event.RunID(), plan, []pinrouting.ConnectDeliveryRoute{route}, []pinrouting.ConnectDeliveryRoute{route}, false, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(intents) != 1 {
		t.Fatal("fixture lacks the exact compiled connection")
	}
	intents[0].TargetOwnership = events.MustEntitylessReceiverTarget(intents[0].TargetBlueprint)
	plan := (RoutePlan{Event: event, DeliveryIntents: intents}).Normalized()
	node, _ := intents[0].Recipient.Node()
	pin, _ := intents[0].ConnectClaim.ReceiverIdentity()
	localEvent, _ := intents[0].ConnectClaim.ReceiverEvent()
	recipient, err := forkrecipient.NewConnect(forkrecipient.Input{Recipient: intents[0].Recipient,
		Path: intents[0].TargetBlueprint.FlowInstance, HandlerNode: node, HandlerEvent: localEvent}, intents[0].ConnectPlan, pin)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = WithSelectedForkRecipientPlan(context.Background(), event, []forkrecipient.Evidence{recipient})
	if err != nil {
		t.Fatal(err)
	}
	routes := plan.DeliveryRoutes()
	routes[0].Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: uuid.NewString()}}
	bound, err := WithSelectedForkRecipientContexts(ctx, event, routes)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restoreSelectedForkRecipientContexts(bound, event, plan)
	if err != nil || !events.SameDeliveryRouteIdentity(got.DeliveryRoutes()[0], routes[0]) {
		t.Fatalf("compiled connection lost its exact context: error=%v", err)
	}
	if _, err := got.recipientActuals(); err != nil {
		t.Fatal(err)
	}
	plan.DeliveryIntents[0].ConnectClaim = events.ConnectExecutionClaim{}
	if _, err := restoreSelectedForkRecipientContexts(bound, event, plan); err == nil {
		t.Fatal("context restoration replaced missing compiled connect authority")
	}
}
