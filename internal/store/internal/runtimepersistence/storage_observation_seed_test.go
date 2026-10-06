package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
	"testing"
	"time"
)

func seedDeliveryRecoveryClaim(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context) runtimedelivery.ClaimedObligation {
	t.Helper()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "recovery.requested", "gateway", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: mustTestAgentIdentityForRun(runID, "agent-a", "recovery/instance-a")}
	selected := fixture.store.(deliveryFixtureStore)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, selected, event, route)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}
