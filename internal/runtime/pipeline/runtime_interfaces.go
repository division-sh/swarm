package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

type SubscriptionReadyBackgroundNode interface {
	BackgroundNode
	AddSubscriptionReadyHook(func())
}

type BackgroundWorkflowExecutorProvider interface {
	BackgroundWorkflowExecutor() WorkflowNodeExecutor
}

type WorkflowInstancePersistence interface {
	Enabled() bool
	Load(ctx context.Context, instanceID string) (WorkflowInstance, bool, error)
	List(ctx context.Context) ([]WorkflowInstance, error)
	SelectActiveByFields(ctx context.Context, scopeKey string, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error)
	Create(ctx context.Context, instance WorkflowInstance) error
	Upsert(ctx context.Context, instance WorkflowInstance) error
	MarkTerminated(ctx context.Context, instanceID string, terminatedAt time.Time) error
	Mutate(ctx context.Context, instanceID string, fn func(*WorkflowInstance)) error
	Delete(ctx context.Context, instanceID string) error
}

type SystemNodeReceiptPersistence interface {
	SystemNodeDeliveryAuthorized(ctx context.Context, nodeID, eventID string, retryLimit int) (bool, error)
	SystemNodeProcessed(ctx context.Context, nodeID, eventID string) (bool, error)
	SystemNodeDeliveryQuiesced(ctx context.Context, nodeID, eventID string) (bool, error)
	MarkSystemNodeDeliveryInProgress(ctx context.Context, nodeID, eventID string, retryLimit int) error
	MarkSystemNodeDeliveryFailed(ctx context.Context, nodeID, eventID, reasonCode string, failure *runtimefailures.Envelope, retryCount, retryLimit int) error
	MarkSystemNodeDeliveryDeadLetter(ctx context.Context, nodeID, eventID, reasonCode string, failure *runtimefailures.Envelope, retryCount int, sideEffects string) error
	MarkSystemNodeProcessedAndSettleDelivery(ctx context.Context, nodeID, eventID, sideEffects string) error
}

type SystemNodeTargetReceiptPersistence interface {
	SystemNodeDeliveryAuthorizedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, retryLimit int) (bool, error)
	SystemNodeProcessedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity) (bool, error)
	MarkSystemNodeDeliveryInProgressForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, retryLimit int) error
	MarkSystemNodeDeliveryFailedForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, reasonCode string, failure *runtimefailures.Envelope, retryCount, retryLimit int) error
	MarkSystemNodeDeliveryDeadLetterForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, reasonCode string, failure *runtimefailures.Envelope, retryCount int, sideEffects string) error
	MarkSystemNodeProcessedAndSettleDeliveryForTarget(ctx context.Context, nodeID, eventID string, target events.RouteIdentity, sideEffects string) error
}

type Store interface {
	WorkflowInstancePersistence
	SystemNodeReceiptPersistence
	RunPipelineMutation(ctx context.Context, fn func(context.Context) error) error
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
