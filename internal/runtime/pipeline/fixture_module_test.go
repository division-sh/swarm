package pipeline

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type noopPipelineBus struct{}

func (noopPipelineBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (noopPipelineBus) Publish(context.Context, events.Event) error { return nil }
func (noopPipelineBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (noopPipelineBus) ResolveSubscribedRecipients(string) []string       { return nil }
func (noopPipelineBus) LogRuntime(context.Context, RuntimeLogEntry) error { return nil }
func (noopPipelineBus) EngineOutbox() runtimeengine.OutboxWriter          { return noOpEngineOutbox{} }
func (noopPipelineBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func newPipelineFixtureWorkflowModule(bundle *runtimecontracts.WorkflowContractBundle) (WorkflowModule, error) {
	source := semanticview.Wrap(bundle)
	workflow, err := LoadWorkflowDefinition(source)
	if err != nil {
		return nil, err
	}
	workflowNodes, err := LoadWorkflowNodes(source)
	if err != nil {
		return nil, err
	}
	return &pipelineFixtureWorkflowModule{
		source:         source,
		workflow:       workflow,
		workflowNodes:  workflowNodes,
		guardRegistry:  NewContractGuardRegistry(source),
		actionRegistry: NewContractActionRegistry(source),
	}, nil
}

type pipelineFixtureWorkflowModule struct {
	source         semanticview.Source
	workflow       *WorkflowDefinition
	workflowNodes  []WorkflowNode
	guardRegistry  GuardRegistry
	actionRegistry ActionRegistry
}

func (m *pipelineFixtureWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *pipelineFixtureWorkflowModule) WorkflowDefinition() *WorkflowDefinition {
	return m.workflow
}
func (m *pipelineFixtureWorkflowModule) WorkflowNodes() []WorkflowNode {
	return append([]WorkflowNode(nil), m.workflowNodes...)
}
func (m *pipelineFixtureWorkflowModule) GuardRegistry() GuardRegistry   { return m.guardRegistry }
func (m *pipelineFixtureWorkflowModule) ActionRegistry() ActionRegistry { return m.actionRegistry }
