package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

const testMemoryRunID = "11111111-1111-1111-1111-111111111111"

func testMemory() agentmemory.Plan {
	return agentmemory.Authored(true)
}

func testMemoryIdentity(agentID, flowInstance string) agentmemory.Identity {
	scopeKey, instanceID, ok := strings.Cut(strings.Trim(flowInstance, "/"), "/")
	if !ok {
		panic("test memory flow instance must contain a scope and instance ID")
	}
	return agentmemory.Identity{
		RunID: testMemoryRunID,
		Agent: agentidentity.Identity{
			Name: agentidentity.Name{
				AgentID: agentID,
				Owner:   "test-fixture",
				Source:  agentidentity.NameSourceRuntimeCreated,
			},
			Route: agentidentity.Route{
				Presence:     agentidentity.RoutePresent,
				ScopeKey:     scopeKey,
				InstanceID:   instanceID,
				InstancePath: strings.Trim(flowInstance, "/"),
			},
		},
	}
}

func testAgentIdentity(agentID, flowInstance string) agentidentity.Identity {
	if strings.Trim(strings.TrimSpace(flowInstance), "/") != "" {
		return testMemoryIdentity(agentID, flowInstance).Agent
	}
	return agentidentity.Identity{
		Name:  agentidentity.Name{AgentID: agentID, Owner: "llm-test-fixture", Source: agentidentity.NameSourceRuntimeCreated},
		Route: agentidentity.RootRoute(),
	}
}

func withTestActorConcreteIdentity(ctx context.Context, identity agentidentity.Identity) context.Context {
	actor, ok := actors.ActorFromContext(ctx)
	if !ok {
		actor = actors.AgentConfig{}
	}
	actor.ID = identity.AgentID()
	actor.Identity = identity
	actor.FlowPath = actor.Identity.FlowInstance()
	return actors.WithActor(ctx, actor)
}

func withTestMemory(ctx context.Context, agentID, flowInstance string) context.Context {
	identity := testMemoryIdentity(agentID, flowInstance)
	ctx = withTestActorConcreteIdentity(ctx, identity.Agent)
	ctx = agentmemory.WithExecution(ctx, testMemory(), identity)
	return withTestOriginDeliveryClaim(ctx, identity.RunID, identity.AgentID())
}

func withTestStatelessMemory(t testing.TB, ctx context.Context, agentID, flowInstance string) context.Context {
	t.Helper()
	ctx = runtimecorrelation.WithRunID(ctx, testMemoryRunID)
	identity := agentidentitytest.RootRuntime(t, agentID, "llm-stateless-test")
	if strings.Trim(strings.TrimSpace(flowInstance), "/") != "" {
		identity = testMemoryIdentity(agentID, flowInstance).Agent
	}
	ctx = withTestActorConcreteIdentity(ctx, identity)
	ctx = agentmemory.WithExecution(ctx, agentmemory.Authored(false), agentmemory.Identity{
		RunID: testMemoryRunID,
		Agent: identity,
	})
	return withTestOriginDeliveryClaim(ctx, testMemoryRunID, identity.AgentID())
}

func withTestOriginDeliveryClaim(ctx context.Context, runID, agentID string) context.Context {
	claim, err := runtimedelivery.AdmitPersistedClaim(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		runID,
		"llm-test-origin:"+agentID,
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		1,
		runtimedelivery.SubscriberAgent,
		agentID,
	)
	if err != nil {
		panic(err)
	}
	return runtimedelivery.WithClaim(ctx, claim)
}

func setEffectHarnessAgent(t testing.TB, harness *effecttest.Harness, agentID, flowInstance string) {
	t.Helper()
	if harness == nil {
		t.Fatal("effect harness is nil")
	}
	identity := agentidentitytest.RootRuntime(t, agentID, "llm-effect-test")
	if strings.Trim(strings.TrimSpace(flowInstance), "/") != "" {
		identity = testMemoryIdentity(agentID, flowInstance).Agent
	}
	harness.Token.AgentID = agentID
	harness.Token.Identity = identity
}
