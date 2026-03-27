package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	runtimeengine "swarm/internal/runtime/engine"
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

type declarativeWorkflowNode struct {
	nodeID      string
	coordinator *PipelineCoordinator
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

func (n *declarativeWorkflowNode) NodeID() string {
	if n == nil {
		return ""
	}
	return strings.TrimSpace(n.nodeID)
}

func (n *declarativeWorkflowNode) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *declarativeWorkflowNode) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
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
	if policy.RequireEntity && workflowEventEntityID(evt) == "" {
		return false, false
	}
	return policy.Consume, true
}

func (n *declarativeWorkflowNode) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil {
		return false
	}
	return n.coordinator.executeNodeHandlerPlan(ctx, n.NodeID(), evt)
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
	if policy.RequireEntity && workflowEventEntityID(evt) == "" {
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
	eventType := strings.TrimSpace(string(evt.Type))
	handler, ok := n.contract.EventHandlers[eventType]
	if !ok {
		for pattern, candidate := range n.contract.EventHandlers {
			if strings.TrimSpace(pattern) == eventType {
				continue
			}
			if runtimecontractsHandlerPatternMatches(pattern, eventType) {
				handler = candidate
				ok = true
				break
			}
		}
	}
	if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) && containsString(n.contract.SubscribesTo, eventType) {
		for _, candidate := range n.contract.EventHandlers {
			if candidate.Accumulate == nil {
				continue
			}
			if candidate.Accumulate.Completion.Mode != runtimecontracts.AccumulateModeTimeout && candidate.Accumulate.OnTimeout == nil {
				continue
			}
			handler = candidate
			ok = true
			break
		}
	}
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
	coordinator *PipelineCoordinator
	executor    *runtimeengine.Executor
	node        *runtimeengine.DeclarativeNode
	err         error
}

func newCoordinatorHandlerExecutionEngine(pc *PipelineCoordinator, nodeID string) HandlerExecutionEngine {
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
	flowID := workflowNodeFlowID(e.coordinator.SemanticSource(), e.nodeID)
	ctx = withPipelineFlowScope(ctx, flowID)
	currentState := e.coordinator.currentWorkflowState(ctx, entityID)
	exec := e.executor
	node := e.node
	var (
		parentEventCollector *[]events.Event
		collectedIntents     *[]runtimeengine.EmitIntent
		collectLocally       bool
	)
	ctx, parentEventCollector, collectedIntents, collectLocally = pipelineCollectorExecutionContext(ctx)
	if collectLocally {
		deps := coordinatorEngineDependencies(e.coordinator)
		deps.Outbox = noOpEngineOutbox{}
		tmpExec, err := runtimeengine.NewExecutor(deps, newCoordinatorEngineEvaluator(e.coordinator))
		if err != nil {
			return nil, err
		}
		exec = tmpExec
		node = runtimeengine.NewDeclarativeNode(strings.TrimSpace(e.nodeID), exec)
	}
	result, err := node.Handle(ctx, runtimeengine.ExecutionRequest{
		EntityID:   identity.NormalizeEntityID(entityID),
		NodeID:     identity.NormalizeNodeID(e.nodeID),
		FlowID:     identity.NormalizeFlowID(flowID),
		Event:      evt,
		ChainDepth: evt.ChainDepth,
		Handler:    handler,
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
	flushCollectedPipelineEmitIntents(parentEventCollector, collectedIntents)
	if result.Status == runtimeengine.OutcomeUnknown {
		return &HandlerOutcome{Handled: false}, nil
	}
	handled := result.Status != runtimeengine.OutcomeRejected && result.Status != runtimeengine.OutcomeDiscarded
	if handled {
		e.coordinator.reconcileWorkflowEventTimers(ctx, entityID, strings.TrimSpace(string(evt.Type)))
	}
	return &HandlerOutcome{
		Handled:         handled,
		ActionsExecuted: append([]string{}, result.ActionsExecuted...),
	}, nil
}
