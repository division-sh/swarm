package deliverylifecycle

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/google/uuid"
)

func TestSnapshotMatchesSettlementClaim(t *testing.T) {
	for _, class := range []SubscriberClass{SubscriberAgent, SubscriberNode} {
		t.Run(string(class), func(t *testing.T) {
			runID, eventID := uuid.NewString(), uuid.NewString()
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "receiver")), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"})}
			if class == SubscriberAgent {
				identity := agentidentitytest.RootRuntimeForRun(t, runID, "worker", "delivery-test")
				route = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			}
			routeID, err := route.Identity()
			if err != nil {
				t.Fatal(err)
			}
			deliveryID, err := DeliveryID(eventID, route)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := AdmitPersistedClaim(deliveryID, runID, events.EncodeDeliveryRouteIdentity(routeID), uuid.NewString(), 3, class, route.Recipient.ID())
			if err != nil {
				t.Fatal(err)
			}
			base := Snapshot{DeliveryID: deliveryID, RunID: runID, EventID: eventID, Route: route, RouteIdentity: routeID, SubscriberClass: class, SubscriberID: route.Recipient.ID(), ClaimVersion: 3, Status: StatusDelivered}
			for _, status := range []Status{StatusDelivered, StatusDeadLetter, StatusFailed, StatusPending, StatusInProgress, "unknown"} {
				t.Run(string(status), func(t *testing.T) {
					snapshot := base
					snapshot.Status = status
					want := status == StatusDelivered || status == StatusDeadLetter || status == StatusFailed
					if snapshot.MatchesSettlementClaim(claim) != want {
						t.Fatalf("status=%s match want=%t", status, want)
					}
				})
			}
			for name, mutate := range map[string]func(*Snapshot){
				"delivery":       func(s *Snapshot) { s.DeliveryID = uuid.NewString() },
				"run":            func(s *Snapshot) { s.RunID = uuid.NewString() },
				"event":          func(s *Snapshot) { s.EventID = uuid.NewString() },
				"version":        func(s *Snapshot) { s.ClaimVersion++ },
				"subscriber":     func(s *Snapshot) { s.SubscriberID = "foreign" },
				"class":          func(s *Snapshot) { s.SubscriberClass = "foreign" },
				"route":          func(s *Snapshot) { s.Route = events.DeliveryRoute{} },
				"route_identity": func(s *Snapshot) { s.RouteIdentity = events.DeliveryRouteIdentity{} },
			} {
				t.Run(name, func(t *testing.T) {
					snapshot := base
					mutate(&snapshot)
					if snapshot.MatchesSettlementClaim(claim) {
						t.Fatal("foreign settlement matched claim")
					}
				})
			}
			if base.MatchesSettlementClaim(Claim{}) || (Snapshot{}).MatchesSettlementClaim(claim) {
				t.Fatal("empty claim or snapshot matched")
			}
		})
	}
}
