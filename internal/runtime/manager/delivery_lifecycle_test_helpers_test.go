package manager

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func managerAgentDeliveryRoute(agentID string) events.DeliveryRoute {
	return managerAgentDeliveryRouteForRun(managerIdentityTestRunID, agentID)
}

func managerAgentDeliveryRouteForRun(runID, agentID string) events.DeliveryRoute {
	identity := managerAgentIdentity(agentID)
	identity.RunID = runID
	if err := identity.Validate(); err != nil {
		panic(err)
	}
	return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
}

func managerAgentIdentity(agentID string) runtimeagentidentity.Identity {
	name, err := runtimeagentidentity.RuntimeName(agentID, "manager.delivery_test")
	if err != nil {
		panic(err)
	}
	identity, err := runtimeagentidentity.New(managerIdentityTestRunID, name, runtimeagentidentity.RootRoute())
	if err != nil {
		panic(err)
	}
	return identity
}

func managerAgentDeliveryContext(ctx context.Context, runID, agentID string) context.Context {
	return runtimedelivery.WithRoute(ctx, managerAgentDeliveryRouteForRun(runID, agentID))
}

func managerAgentClaimContext(ctx context.Context, claim runtimedelivery.Claim, agentID string) context.Context {
	return runtimedelivery.WithClaim(managerAgentDeliveryContext(ctx, claim.RunID(), agentID), claim)
}

func managerClaimedDeliveryContext(
	t *testing.T,
	am *AgentManager,
	ctx context.Context,
	evt events.Event,
	agentID string,
) context.Context {
	t.Helper()
	ctx = managerAgentDeliveryContext(ctx, evt.RunID(), agentID)
	store, ok := am.deliveryStore.(interface {
		ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error)
		managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority
	})
	if !ok {
		t.Fatal("manager test delivery store does not expose typed claim authority")
	}
	authority := store.managerTestDeliveryAuthority()
	if admission, admitted := managedexecution.FromContext(ctx); admitted {
		var err error
		authority, err = runtimedelivery.NewExecutionAuthority(authority.SourceArtifact(), admission)
		if err != nil {
			t.Fatalf("construct managed delivery authority: %v", err)
		}
	}
	result, err := store.ClaimDelivery(ctx, authority, evt, managerAgentDeliveryRouteForRun(evt.RunID(), agentID))
	if err != nil {
		t.Fatalf("claim manager test delivery: %v", err)
	}
	claimed, ok := result.Acquired()
	if !ok {
		t.Fatalf("manager test delivery claim disposition = %s", result.Disposition)
	}
	return runtimedelivery.WithClaim(ctx, claimed.Claim)
}
