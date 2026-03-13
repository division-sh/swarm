package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeengine "empireai/internal/runtime/engine"
	"empireai/internal/runtime/core/identity"
)

type Event = events.Event
type NodeExecutor = WorkflowNodeExecutor
type SystemNodeContract = runtimecontracts.SystemNodeContract
type SystemNodeEventHandler = runtimecontracts.SystemNodeEventHandler

type HandlerOutcome struct {
	Handled         bool
	ActionsExecuted []string
}

type HandlerExecutionEngine interface {
	ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event) (*HandlerOutcome, error)
}

type DeclarativeNode struct {
	nodeID   string
	contract SystemNodeContract
	engine   HandlerExecutionEngine
	hooks    *ProductHookRegistry
}

type ActionHandler func(ctx context.Context, evt Event, outcome *HandlerOutcome) (*HandlerOutcome, error)

type ProductHookRegistry struct {
	mu      sync.RWMutex
	actions map[string]ActionHandler
}

func NewNode(contract SystemNodeContract, engine HandlerExecutionEngine, hooks *ProductHookRegistry) NodeExecutor {
	return &DeclarativeNode{
		nodeID:   strings.TrimSpace(contract.ID),
		contract: contract,
		engine:   engine,
		hooks:    hooks,
	}
}

func (n *DeclarativeNode) NodeID() string {
	if n == nil {
		return ""
	}
	if nodeID := strings.TrimSpace(n.nodeID); nodeID != "" {
		return nodeID
	}
	return strings.TrimSpace(n.contract.ID)
}

func (n *DeclarativeNode) Subscriptions() []events.EventType {
	if n == nil {
		return nil
	}
	out := make([]events.EventType, 0, len(n.contract.SubscribesTo))
	for _, evt := range n.contract.SubscribesTo {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		out = append(out, events.EventType(evt))
	}
	return out
}

func (n *DeclarativeNode) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	if n == nil {
		return false, false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	policy, ok := workflowNodeEventPolicy(n.NodeID(), eventType)
	if !ok {
		return false, false
	}
	if policy.RequireVertical && workflowEventEntityID(evt) == "" {
		return false, false
	}
	return policy.Consume, true
}

func (n *DeclarativeNode) Handle(ctx context.Context, evt events.Event) bool {
	outcome, err := n.HandleEvent(ctx, evt)
	return err == nil && outcome != nil && outcome.Handled
}

func (n *DeclarativeNode) HandleEvent(ctx context.Context, evt Event) (*HandlerOutcome, error) {
	if n == nil {
		return nil, nil
	}
	handler, ok := n.contract.EventHandlers[strings.TrimSpace(string(evt.Type))]
	if !ok {
		return nil, nil
	}
	if n.engine == nil {
		return nil, fmt.Errorf("declarative node %s has no handler execution engine", n.NodeID())
	}
	outcome, err := n.engine.ExecuteHandlerSteps(ctx, handler, evt)
	if err != nil {
		return nil, err
	}
	return outcome, nil
}

func (r *ProductHookRegistry) Register(actionID string, handler ActionHandler) {
	if r == nil || handler == nil {
		return
	}
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.actions == nil {
		r.actions = make(map[string]ActionHandler)
	}
	r.actions[actionID] = handler
}

func (r *ProductHookRegistry) Get(actionID string) (ActionHandler, bool) {
	if r == nil {
		return nil, false
	}
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.actions[actionID]
	return handler, ok
}

type coordinatorHandlerExecutionEngine struct {
	nodeID      string
	coordinator *FactoryPipelineCoordinator
	executor    *runtimeengine.Executor
	node        *runtimeengine.DeclarativeNode
	err         error
}

func newCoordinatorHandlerExecutionEngine(pc *FactoryPipelineCoordinator, nodeID string) HandlerExecutionEngine {
	if pc == nil {
		return nil
	}
	engine := &coordinatorHandlerExecutionEngine{
		nodeID:      strings.TrimSpace(nodeID),
		coordinator: pc,
	}
	exec, err := runtimeengine.NewExecutor(coordinatorEngineDependencies(pc), newCoordinatorEngineEvaluator(pc))
	if err != nil {
		engine.err = err
		return engine
	}
	engine.executor = exec
	engine.node = runtimeengine.NewDeclarativeNode(strings.TrimSpace(nodeID), exec)
	return engine
}

func (e *coordinatorHandlerExecutionEngine) ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event) (*HandlerOutcome, error) {
	if e == nil || e.coordinator == nil {
		return nil, fmt.Errorf("handler execution engine is not configured")
	}
	if e.err != nil {
		return nil, e.err
	}
	if e.executor == nil || e.node == nil {
		return nil, fmt.Errorf("handler execution engine is not configured")
	}
	if e.nodeID == "" || strings.TrimSpace(string(evt.Type)) == "" {
		return &HandlerOutcome{Handled: false}, nil
	}
	entityID := workflowEventEntityID(evt)
	currentState := e.coordinator.currentWorkflowState(ctx, entityID)
	result, err := e.node.Handle(ctx, runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID(entityID),
		NodeID:   identity.NormalizeNodeID(e.nodeID),
		Event:    evt,
		Handler:  handler,
		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID(entityID),
			CurrentState: strings.TrimSpace(string(currentState.Stage)),
			Metadata:     cloneStringAnyMap(currentState.Metadata),
			StateBuckets: map[string]any{},
		},
	})
	if err != nil {
		return nil, err
	}
	if result.Status == runtimeengine.OutcomeUnknown {
		return &HandlerOutcome{Handled: false}, nil
	}
	return &HandlerOutcome{
		Handled:         result.Status != runtimeengine.OutcomeRejected && result.Status != runtimeengine.OutcomeDiscarded,
		ActionsExecuted: append([]string{}, result.ActionsExecuted...),
	}, nil
}
