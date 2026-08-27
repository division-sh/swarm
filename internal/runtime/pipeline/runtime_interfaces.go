package pipeline

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const runtimeWorkflowID = "workflow-runtime"

type WorkflowRuntime interface {
	SemanticSource() semanticview.Source
	WorkflowDefinition() *WorkflowDefinition
	WorkflowNodes() []WorkflowNode
	TransitionEvaluator() TransitionEvaluator
	GuardRegistry() GuardRegistry
	ActionRegistry() ActionRegistry
}

type WorkflowNodeExecutor interface {
	ExecutableNode() identity.ExecutableNode
	Subscriptions() []events.EventType
	InterceptPolicy(eventType string, evt events.Event) (consume bool, handled bool)
	Handle(ctx context.Context, evt events.Event) bool
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

type BackgroundWorkflowExecutorProvider interface {
	BackgroundWorkflowExecutor() WorkflowNodeExecutor
}

type TransitionEvaluator interface {
	Transition(state WorkflowState, to WorkflowStateID) (WorkflowTransition, bool)
	CanTransition(state WorkflowState, to WorkflowStateID) bool
}

type GuardRegistry interface {
	HasGuard(id identity.GuardKey) bool
	IsExecutable(id identity.GuardKey) bool
	GuardIDs() []string
	Guard(id identity.GuardKey) (runtimeregistry.GuardInstruction, bool)
}

type ActionRegistry interface {
	HasAction(id identity.ActionKey) bool
	IsExecutable(id identity.ActionKey) bool
	ActionIDs() []string
	Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool)
}

func (pc *PipelineCoordinator) SemanticSource() semanticview.Source {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.SemanticSource()
}

func (pc *PipelineCoordinator) WorkflowDefinition() *WorkflowDefinition {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.WorkflowDefinition()
}

func (pc *PipelineCoordinator) WorkflowNodes() []WorkflowNode {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.WorkflowNodes()
}

func (pc *PipelineCoordinator) TransitionEvaluator() TransitionEvaluator {
	return pc.WorkflowDefinition()
}

func (pc *PipelineCoordinator) GuardRegistry() GuardRegistry {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.GuardRegistry()
}

func (pc *PipelineCoordinator) ActionRegistry() ActionRegistry {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.ActionRegistry()
}
