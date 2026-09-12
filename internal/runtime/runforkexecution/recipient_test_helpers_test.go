package runforkexecution

import (
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func testNodeFrontierRecipient(node runtimeidentity.ExecutableNode, localEvent events.EventType, path, source string) runfork.RunForkContractFrontierRecipient {
	evidence, err := forkrecipient.NewLocal(forkrecipient.Input{
		Recipient: events.MustNodeDeliveryRecipient(node), HandlerNode: node,
		HandlerEvent: localEvent, Path: path, RouteSource: source,
	})
	if err != nil {
		panic(err)
	}
	return evidence
}

func testAgentFrontierRecipient(plan agentidentity.Plan, localEvent events.EventType, path, source string) runfork.RunForkContractFrontierRecipient {
	evidence, err := forkrecipient.NewLocal(forkrecipient.Input{
		Recipient: events.MustAgentDeliveryRecipient(plan.AgentID()), AgentPlan: plan,
		HandlerEvent: localEvent, Path: path, RouteSource: source,
	})
	if err != nil {
		panic(err)
	}
	return evidence
}

func mustTestAgentPlan(identity agentidentity.Identity) agentidentity.Plan {
	plan, err := identity.Plan()
	if err != nil {
		panic(err)
	}
	return plan
}
