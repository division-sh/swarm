package runtimepersistence

import (
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type forkFanOutConsumerModule struct {
	runForkGateWorkflowModule
	nodes []pipeline.WorkflowNode
}

func (m forkFanOutConsumerModule) WorkflowNodes() []pipeline.WorkflowNode {
	return append([]pipeline.WorkflowNode(nil), m.nodes...)
}
func (m forkFanOutConsumerModule) GuardRegistry() pipeline.GuardRegistry {
	return pipeline.NewContractGuardRegistry(m.source)
}
