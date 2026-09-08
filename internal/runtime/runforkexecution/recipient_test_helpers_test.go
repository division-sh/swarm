package runforkexecution

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func testNodeFrontierRecipient(id, path, source string) runfork.RunForkContractFrontierRecipient {
	flowID := ""
	if parts := strings.Split(strings.Trim(strings.TrimSpace(path), "/"), "/"); len(parts) > 1 {
		flowID = parts[0]
	}
	node := mustRunForkRootNode(id)
	if flowID != "" {
		node = mustRunForkNode(flowID, id)
	}
	return runfork.NewRunForkContractFrontierRecipient(
		events.MustNodeDeliveryRecipient(node), path, source, agentidentity.Plan{},
	)
}

func testAgentFrontierRecipient(id, path, source string, plan agentidentity.Plan) runfork.RunForkContractFrontierRecipient {
	return runfork.NewRunForkContractFrontierRecipient(
		events.MustAgentDeliveryRecipient(id), path, source, plan,
	)
}

func mustTestAgentPlan(identity agentidentity.Identity) agentidentity.Plan {
	plan, err := identity.Plan()
	if err != nil {
		panic(err)
	}
	return plan
}
