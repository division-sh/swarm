package pipeline

import (
	"context"
	"strings"

	"swarm/internal/events"
	"swarm/internal/runtime/core/identity"
	runtimeregistry "swarm/internal/runtime/core/registry"
	"swarm/internal/runtime/semanticview"
)

const runtimeWorkflowID = "workflow-runtime"

func isRuntimeWorkflowSource(source string) bool {
	return strings.TrimSpace(source) == runtimeWorkflowID
}

type WorkflowRuntime interface {
	SemanticSource() semanticview.Source
	WorkflowDefinition() *WorkflowDefinition
	WorkflowNodes() []WorkflowNode
	WorkflowInstanceStore() WorkflowInstancePersistence
	TransitionEvaluator() TransitionEvaluator
	GuardRegistry() GuardRegistry
	ActionRegistry() ActionRegistry
}

type WorkflowNodeExecutor interface {
	NodeID() string
	Subscriptions() []events.EventType
	InterceptPolicy(eventType string, evt events.Event) (consume bool, handled bool)
	Handle(ctx context.Context, evt events.Event) bool
}

type BackgroundNode interface {
	Run(context.Context)
}

type BackgroundWorkflowExecutorProvider interface {
	BackgroundWorkflowExecutor() WorkflowNodeExecutor
}

type WorkflowInstancePersistence interface {
	Enabled() bool
	Load(ctx context.Context, instanceID string) (WorkflowInstance, bool, error)
	List(ctx context.Context) ([]WorkflowInstance, error)
	Upsert(ctx context.Context, instance WorkflowInstance) error
	Mutate(ctx context.Context, instanceID string, fn func(*WorkflowInstance)) error
	Delete(ctx context.Context, instanceID string) error
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

func (pc *PipelineCoordinator) WorkflowInstanceStore() WorkflowInstancePersistence {
	if pc == nil {
		return nil
	}
	return pc.workflowStore
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
