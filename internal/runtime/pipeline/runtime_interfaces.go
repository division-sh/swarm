package pipeline

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const runtimeWorkflowID = "workflow-runtime"

type WorkflowRuntime interface {
	SemanticSource() semanticview.Source
	WorkflowNodes() []WorkflowNode
}

type BackgroundNode interface {
	Run(context.Context)
}

type systemNodeRuntimeLogger interface {
	LogRuntime(context.Context, RuntimeLogEntry) error
}

type SubscriptionReadyBackgroundNode interface {
	BackgroundNode
	AddSubscriptionReadyHook(func())
}

func (pc *PipelineCoordinator) SemanticSource() semanticview.Source {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.SemanticSource()
}

func (pc *PipelineCoordinator) WorkflowNodes() []WorkflowNode {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.WorkflowNodes()
}
