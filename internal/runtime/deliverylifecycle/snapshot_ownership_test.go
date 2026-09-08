package deliverylifecycle_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestDecodeHistoricalSnapshotRejectsHashConsistentForeignAgentRun(t *testing.T) {
	const foreignRunID = "d279b452-b286-4989-9f51-309c4684d736"
	for _, fixture := range historicalRouteFixtures(t) {
		if !fixture.route.Recipient.IsAgent() {
			continue
		}
		t.Run(fixture.name, func(t *testing.T) {
			foreignRoute := fixture.route
			var err error
			foreignRoute.AgentIdentity, err = agentidentity.New(foreignRunID, fixture.route.AgentIdentity.Name, fixture.route.AgentIdentity.Route)
			if err != nil {
				t.Fatal(err)
			}
			foreignIdentity, err := foreignRoute.Identity()
			if err != nil {
				t.Fatalf("foreign route must be independently valid: %v", err)
			}
			for _, status := range []deliverylifecycle.Status{
				deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress,
				deliverylifecycle.StatusFailed, deliverylifecycle.StatusDelivered, deliverylifecycle.StatusDeadLetter,
			} {
				t.Run(string(status), func(t *testing.T) {
					// Recompute both route hash and delivery ID. Only the owning-run
					// relation is contradictory, not either identity's byte integrity.
					fact := historicalRouteFact(t, foreignRoute, status)
					if fact["route_identity"] != events.EncodeDeliveryRouteIdentity(foreignIdentity) || fact["run_id"] == foreignRunID {
						t.Fatal("fixture must have a consistent route hash and a distinct delivery owner")
					}
					_, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact))
					if !errors.Is(err, deliverylifecycle.ErrConflict) || !strings.Contains(err.Error(), "agent run does not match obligation run") {
						t.Fatalf("foreign nested owner: got %v, want owning-run conflict", err)
					}
				})
			}
		})
	}
}

func TestDecodeHistoricalSnapshotPreservesOwningRunWithoutCapability(t *testing.T) {
	const childRunID = "d279b452-b286-4989-9f51-309c4684d736"
	for _, fixture := range historicalRouteFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			for _, owner := range []string{"source", "child"} {
				t.Run(owner, func(t *testing.T) {
					route := fixture.route
					if owner == "child" && route.Recipient.IsAgent() {
						var err error
						route.AgentIdentity, err = agentidentity.New(childRunID, route.AgentIdentity.Name, route.AgentIdentity.Route)
						if err != nil {
							t.Fatal(err)
						}
					}
					fact := historicalRouteFact(t, route, deliverylifecycle.StatusInProgress)
					if owner == "child" {
						fact["run_id"] = childRunID
					}
					decoded, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact))
					if err != nil {
						t.Fatal(err)
					}
					assertHistoricalRoute(t, decoded, route)
					if decoded.RunID != fact["run_id"] || decoded.DeliveryID != fact["delivery_id"] || decoded.EventID != fact["event_id"] {
						t.Fatal("historical decoding changed owning coordinates")
					}
					if decoded.Authority.Validate() == nil {
						t.Fatal("historical ownership must not grant execution authority")
					}
					if decoded.ClaimVersion != 2 || decoded.ClaimExpiresAt.IsZero() {
						t.Fatal("historical lease evidence must remain data")
					}
				})
			}
		})
	}
}
