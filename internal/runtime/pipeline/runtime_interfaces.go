package pipeline

import (
	"context"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type WorkflowRuntime interface {
	ContractBundle() *runtimecontracts.WorkflowContractBundle
	WorkflowDefinition() *WorkflowDefinition
	WorkflowNodes() []WorkflowNode
	WorkflowStateStore() WorkflowStateStore
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

type WorkflowStateStore interface {
	Enabled(ctx context.Context, enabled bool) bool
	Load(ctx context.Context) PipelineStateSnapshot
	MarkProcessed(ctx context.Context, processed map[string]struct{}, eventID string) bool
	Persist(ctx context.Context, scans map[string]*scanAccumulator, pending map[string]pendingCandidate, validations map[string]*validationPipelineState)
	Clear(ctx context.Context, clearScoringDigest bool)
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
	Transition(state WorkflowState, to PipelineStage) (WorkflowTransition, bool)
	CanTransition(state WorkflowState, to PipelineStage) bool
}

type GuardRegistry interface {
	HasGuard(id string) bool
	IsExecutable(id string) bool
	GuardIDs() []string
	Guard(id string) (runtimecontracts.GuardActionEntry, bool)
}

type ActionRegistry interface {
	HasAction(id string) bool
	IsExecutable(id string) bool
	ActionIDs() []string
	Action(id string) (runtimecontracts.GuardActionEntry, bool)
}

func (pc *FactoryPipelineCoordinator) ContractBundle() *runtimecontracts.WorkflowContractBundle {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.ContractBundle()
}

func (pc *FactoryPipelineCoordinator) WorkflowDefinition() *WorkflowDefinition {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.WorkflowDefinition()
}

func (pc *FactoryPipelineCoordinator) WorkflowNodes() []WorkflowNode {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.WorkflowNodes()
}

func (pc *FactoryPipelineCoordinator) WorkflowStateStore() WorkflowStateStore {
	if pc == nil {
		return nil
	}
	return pc.stateStore
}

func (pc *FactoryPipelineCoordinator) WorkflowInstanceStore() WorkflowInstancePersistence {
	if pc == nil {
		return nil
	}
	return pc.workflowStore
}

func (pc *FactoryPipelineCoordinator) TransitionEvaluator() TransitionEvaluator {
	return pc.WorkflowDefinition()
}

func (pc *FactoryPipelineCoordinator) GuardRegistry() GuardRegistry {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.GuardRegistry()
}

func (pc *FactoryPipelineCoordinator) ActionRegistry() ActionRegistry {
	if pc == nil || pc.module == nil {
		return nil
	}
	return pc.module.ActionRegistry()
}
