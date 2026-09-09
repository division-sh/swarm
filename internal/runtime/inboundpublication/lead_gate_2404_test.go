package inboundpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestLead2404CompleteManifestOrdering(t *testing.T) {
	node := identitytest.RootNode(t, "worker")
	direct := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"})}
	claimed := func(edge, pin, event string) events.DeliveryRoute {
		route := direct
		claim, err := events.AdmitConnectExecutionClaim(sha256.Sum256([]byte(edge)), sha256.Sum256([]byte(pin)), direct.Recipient, node, events.EventType(event))
		if err != nil {
			t.Fatal(err)
		}
		route.ConnectClaim = claim
		return route
	}
	agent := func(instance string) events.DeliveryRoute {
		name, err := agentidentity.DeclaredName("worker", "test://manifest")
		if err != nil {
			t.Fatal(err)
		}
		route, err := agentidentity.PresentRoute("flow", instance, "flow/"+instance)
		if err != nil {
			t.Fatal(err)
		}
		id, err := agentidentity.New("run-one", name, route)
		if err != nil {
			t.Fatal(err)
		}
		return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: id}
	}
	reply := direct
	reply.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-one"}}
	target := direct
	target.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/two", EntityID: "entity-2"})
	for _, tc := range []struct {
		name        string
		left, right events.DeliveryRoute
	}{
		{"direct_claim", direct, claimed("edge", "pin", "work.received")},
		{"edge_digest", claimed("edge-a", "pin", "work.received"), claimed("edge-b", "pin", "work.received")},
		{"pin_digest", claimed("edge", "pin-a", "work.received"), claimed("edge", "pin-b", "work.received")},
		{"handler_event", claimed("edge", "pin", "work.a"), claimed("edge", "pin", "work.b")},
		{"agent_identity", agent("one"), agent("two")},
		{"reply_control", direct, reply},
		{"target_control", direct, target},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes := []events.DeliveryRoute{tc.left, tc.right}
			if err := events.ValidateDeliveryRoutes(routes); err != nil {
				t.Fatalf("invalid probe pair: %v", err)
			}
			before, _ := json.Marshal(routes)
			first, hash1, count1, err := CanonicalRecipientManifest(routes)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(routes)
			if !bytes.Equal(before, after) {
				t.Fatal("input mutated")
			}
			second, hash2, count2, err := CanonicalRecipientManifest([]events.DeliveryRoute{tc.right, tc.left})
			if err != nil {
				t.Fatal(err)
			}
			if count1 != 2 || count2 != 2 || hash1 != hash2 || !bytes.Equal(first, second) {
				t.Errorf("valid route set is order-dependent: counts %d/%d hashes %s/%s", count1, count2, hash1, hash2)
			}
			duplicate, hash3, count3, err := CanonicalRecipientManifest([]events.DeliveryRoute{tc.left, tc.right, tc.left})
			if err != nil || count3 != 2 || hash1 != hash3 || !bytes.Equal(first, duplicate) {
				t.Errorf("exact duplicate changed identity: %d %s %v", count3, hash3, err)
			}
		})
	}
}

func TestLead2404ProjectionConflictStaysInvalid(t *testing.T) {
	node := identitytest.RootNode(t, "worker")
	left := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"})}
	right := left
	var err error
	left.PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"field": "one"})
	if err != nil {
		t.Fatal(err)
	}
	right.PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"field": "two"})
	if err != nil {
		t.Fatal(err)
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{left, right}); err == nil {
		t.Fatal("conflicting projections accepted")
	}
}
