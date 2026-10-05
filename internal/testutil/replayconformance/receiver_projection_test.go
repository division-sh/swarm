package replayconformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestReceiverProjectionPreservesConstructionAndDeliverySemantics(t *testing.T) {
	for _, flow := range []bool{false, true} {
		first, routes := receiverProjectionFixture(t, "first", flow, "initializer", "receiver")
		second, replay := receiverProjectionFixture(t, "second", flow, "initializer", "receiver")
		before, _ := json.Marshal(routes)
		left, err := projectReceiverRoutes(first, routes, "generated:exact-causal-key")
		if err != nil {
			t.Fatal(err)
		}
		right, err := projectReceiverRoutes(second, replay, "generated:exact-causal-key")
		if err != nil || !reflect.DeepEqual(left, right) {
			t.Fatalf("flow=%v: generated publication UUID changed meaning: %v\n%s\n%s", flow, err, left, right)
		}
		after, _ := json.Marshal(routes)
		if !bytes.Equal(before, after) {
			t.Fatal("comparison mutated durable routes")
		}
		for _, change := range []string{"supplier", "target", "publication"} {
			if !flow && change == "supplier" {
				continue // No observer exists in the agent-only publication.
			}
			node, target := "initializer", "receiver"
			if change == "supplier" {
				node = "other-initializer"
			}
			if change == "target" {
				target = "other-receiver"
			}
			event, changed := receiverProjectionFixture(t, "third", flow, node, target)
			key := "generated:exact-causal-key"
			if change == "publication" {
				key = "generated:other-causal-key"
			}
			actual, err := projectReceiverRoutes(event, changed, key)
			if err != nil || reflect.DeepEqual(left, actual) {
				t.Fatalf("flow=%v: comparison lost %s: %v", flow, change, err)
			}
		}
		for _, corrupt := range []string{"event", "supplier", "target"} {
			copy := append([]events.DeliveryRoute(nil), routes...)
			event := first
			switch corrupt {
			case "event":
				event = second
			case "supplier":
				copy[1].Initialization = events.ReceiverInitialization{}
			case "target":
				copy[1].Target = events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: "wrong"})
			}
			if _, err := projectReceiverRoutes(event, copy, "generated:exact-causal-key"); err == nil {
				t.Fatalf("comparison normalized away %s corruption", corrupt)
			}
		}
		if _, err := projectReceiverRoutes(first, routes[1:], "generated:exact-causal-key"); err != nil {
			t.Fatalf("ordinary observer became a construction dependency: %v", err)
		}
	}
}

func receiverProjectionFixture(t *testing.T, eventName string, flow bool, nodeName, entity string) (events.Event, []events.DeliveryRoute) {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(eventtest.UUID(eventName), "item.received", "operator", "", json.RawMessage(`{}`), 0, eventtest.UUID("fixed-run"), events.EventEnvelope{}, time.Now())
	node := identitytest.FlowNode(t, "consumer", nodeName)
	target := events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: entity})
	owner := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: target}
	var err error
	owner.Initialization, err = events.AdmitFlowReceiverInitialization(event, target)
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256([]byte("receiver-pin"))
	owner.ConnectClaim, err = events.AdmitConnectExecutionClaim(sha256.Sum256([]byte("node-edge")), pin, owner.Recipient, node, "item.received")
	if err != nil {
		t.Fatal(err)
	}
	publication := []events.DeliveryRoute{owner}
	for _, label := range []string{"worker", "observer"} {
		name, err := agentidentity.DeclaredName(label, "consumer/agents.yaml")
		if err != nil {
			t.Fatal(err)
		}
		route, err := agentidentity.PresentRoute("consumer", "consumer", "consumer")
		if err != nil {
			t.Fatal(err)
		}
		actor, err := agentidentity.New(event.RunID(), name, route)
		if err != nil {
			t.Fatal(err)
		}
		agent := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(label), AgentIdentity: actor, Target: target, Initialization: owner.Initialization}
		agent.ConnectClaim, err = events.AdmitConnectExecutionClaim(sha256.Sum256([]byte(label)), pin, agent.Recipient, identity.ExecutableNode{}, "item.received")
		if err != nil {
			t.Fatal(err)
		}
		publication = append(publication, agent)
	}
	if !flow {
		publication = publication[1:]
	}
	return event, publication
}
