package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
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
	if policy.RequireVertical && strings.TrimSpace(evt.VerticalID) == "" && strings.TrimSpace(asString(parsePayloadMap(evt.Payload)["vertical_id"])) == "" {
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
	if actionID := strings.TrimSpace(handler.Action); actionID != "" && n.hooks != nil {
		if hook, ok := n.hooks.Get(actionID); ok {
			return hook(ctx, evt, outcome)
		}
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
}

func newCoordinatorHandlerExecutionEngine(pc *FactoryPipelineCoordinator, nodeID string) HandlerExecutionEngine {
	if pc == nil {
		return nil
	}
	return &coordinatorHandlerExecutionEngine{
		nodeID:      strings.TrimSpace(nodeID),
		coordinator: pc,
	}
}

func (e *coordinatorHandlerExecutionEngine) ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event) (*HandlerOutcome, error) {
	if e == nil || e.coordinator == nil {
		return nil, fmt.Errorf("handler execution engine is not configured")
	}
	eventType := strings.TrimSpace(string(evt.Type))
	if e.nodeID == "" || eventType == "" {
		return &HandlerOutcome{Handled: false}, nil
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		verticalID = strings.TrimSpace(asString(parsePayloadMap(evt.Payload)["vertical_id"]))
	}
	triggerCtx := workflowTriggerContext{
		Event:           evt,
		State:           e.coordinator.currentWorkflowState(ctx, verticalID),
		ValidationState: e.coordinator.validationStateSnapshot(verticalID),
	}
	plan := handlerExecutionPlanFromNodeHandler(e.nodeID, eventType, handler)
	if !directHandlerExecutionPlanSupported(plan) {
		return &HandlerOutcome{Handled: false}, nil
	}
	if handlerPlanHasGuard(plan) {
		passed, _ := e.coordinator.evaluateWorkflowGuardSpec(triggerCtx, plan.GuardSpec)
		if !passed {
			return &HandlerOutcome{Handled: false}, nil
		}
	}
	actionsExecuted := e.coordinator.executeContractHandlerFirstPlan(ctx, triggerCtx, workflowTransitionFromHandlerPlan(triggerCtx.State, plan), plan)
	e.coordinator.reconcileWorkflowEventTimers(ctx, verticalID, eventType)
	return &HandlerOutcome{
		Handled:         true,
		ActionsExecuted: actionsExecuted,
	}, nil
}
