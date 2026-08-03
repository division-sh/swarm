package runforkexecution

import (
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func testNodeFrontierRecipient(id, path, source string) runfork.RunForkContractFrontierRecipient {
	return runfork.NewRunForkContractFrontierRecipient(
		events.MustNodeDeliveryRecipient(id), path, source, agentidentity.Identity{},
	)
}

func testAgentFrontierRecipient(id, path, source string, identity agentidentity.Identity) runfork.RunForkContractFrontierRecipient {
	return runfork.NewRunForkContractFrontierRecipient(
		events.MustAgentDeliveryRecipient(id), path, source, identity,
	)
}
