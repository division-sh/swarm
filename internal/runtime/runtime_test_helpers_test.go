package runtime

import (
	"context"
	"errors"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
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

type noopLLMRuntime struct{ llm.NoopRuntime }

func (noopLLMRuntime) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
}

type claudeNoopLLMRuntime struct{ llm.NoopRuntime }

func (claudeNoopLLMRuntime) ProviderContract() llm.ProviderContract {
	return llm.ClaudeCLIProviderContract()
}

func (claudeNoopLLMRuntime) ProbeStartupVisibleToolSurface(context.Context, runtimeactors.AgentConfig, string, []llm.ToolDefinition) (*llm.Response, error) {
	return nil, errors.New("test Claude startup probe was unexpectedly reached")
}
