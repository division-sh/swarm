package pipeline

import (
	"context"

	"empireai/internal/events"
)

type WorkflowRuntime interface {
	WorkflowDefinition() *WorkflowDefinition
	WorkflowNodes() []WorkflowNode
	WorkflowStateStore() WorkflowStateStore
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

type WorkflowStateStore interface {
	Enabled(ctx context.Context, enabled bool) bool
	Load(ctx context.Context) PipelineStateSnapshot
	MarkProcessed(ctx context.Context, processed map[string]struct{}, eventID string) bool
	Persist(ctx context.Context, scans map[string]*scanAccumulator, pending map[string]pendingCandidate, validations map[string]*validationPipelineState)
	Clear(ctx context.Context, clearScoringDigest bool)
}

type TransitionEvaluator interface {
	Transition(state WorkflowState, to PipelineStage) (WorkflowTransition, bool)
	CanTransition(state WorkflowState, to PipelineStage) bool
}

type GuardRegistry interface {
	HasGuard(id string) bool
	GuardIDs() []string
}

type ActionRegistry interface {
	HasAction(id string) bool
	ActionIDs() []string
}

func (pc *FactoryPipelineCoordinator) WorkflowDefinition() *WorkflowDefinition {
	return EmpirePipelineWorkflow()
}

func (pc *FactoryPipelineCoordinator) WorkflowNodes() []WorkflowNode {
	return empirePipelineWorkflowNodes()
}

func (pc *FactoryPipelineCoordinator) WorkflowStateStore() WorkflowStateStore {
	if pc == nil {
		return nil
	}
	return pc.stateStore
}

func (pc *FactoryPipelineCoordinator) TransitionEvaluator() TransitionEvaluator {
	return EmpirePipelineWorkflow()
}

func (pc *FactoryPipelineCoordinator) GuardRegistry() GuardRegistry {
	return empireGuardRegistry()
}

func (pc *FactoryPipelineCoordinator) ActionRegistry() ActionRegistry {
	return empireActionRegistry()
}
