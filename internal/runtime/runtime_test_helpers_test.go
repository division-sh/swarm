package runtime

import (
	"context"

	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type semanticOnlyWorkflowRuntime struct {
	source semanticview.Source
}

func (s semanticOnlyWorkflowRuntime) SemanticSource() semanticview.Source { return s.source }
func (semanticOnlyWorkflowRuntime) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}
func (semanticOnlyWorkflowRuntime) WorkflowNodes() []runtimepipeline.WorkflowNode { return nil }
func (semanticOnlyWorkflowRuntime) TransitionEvaluator() runtimepipeline.TransitionEvaluator {
	return nil
}
func (semanticOnlyWorkflowRuntime) GuardRegistry() runtimepipeline.GuardRegistry { return nil }
func (semanticOnlyWorkflowRuntime) ActionRegistry() runtimepipeline.ActionRegistry {
	return nil
}

type noopLLMRuntime struct{}

func (noopLLMRuntime) StartSession(context.Context, string, string, []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{}, nil
}

func (noopLLMRuntime) ContinueSession(context.Context, *llm.Session, llm.Message) (*llm.Response, error) {
	return &llm.Response{}, nil
}
