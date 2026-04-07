package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	"swarm/internal/runtime/core/timeridentity"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
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
	policies map[string]WorkflowEventPolicy
	engine   HandlerExecutionEngine
	hooks    *ProductHookRegistry
}

type ActionHandler func(ctx context.Context, evt Event, outcome *HandlerOutcome) (*HandlerOutcome, error)

type ProductHookRegistry struct {
	mu      sync.RWMutex
	actions map[string]ActionHandler
}

func NewNode(contract SystemNodeContract, source semanticview.Source, engine HandlerExecutionEngine, hooks *ProductHookRegistry) NodeExecutor {
	nodeID := strings.TrimSpace(contract.ID)
	subscriptions := make([]events.EventType, 0, len(contract.SubscribesTo))
	for _, evt := range contract.SubscribesTo {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		subscriptions = append(subscriptions, events.EventType(evt))
	}
	return &DeclarativeNode{
		nodeID:   nodeID,
		contract: contract,
		policies: buildWorkflowNodePolicies(source, nodeID, subscriptions),
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
	if n == nil || n.coordinator == nil {
		return nil
	}
	return workflowNodeSubscriptions(n.coordinator.WorkflowNodes(), n.NodeID())
}

func (n *declarativeWorkflowNode) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	if n == nil {
		return false, false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	policy, ok := workflowNodeEventPolicy(n.coordinator.WorkflowNodes(), n.NodeID(), eventType)
	if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) {
		if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload)); bucketOK && bucket.NodeID == n.NodeID() {
			policy, ok = workflowNodeEventPolicy(n.coordinator.WorkflowNodes(), n.NodeID(), bucket.EventType)
		}
	}
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
	policy, ok := n.policies[eventType]
	if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) {
		if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload)); bucketOK && bucket.NodeID == n.NodeID() {
			policy, ok = n.policies[bucket.EventType]
		}
	}
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
		if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload)); bucketOK && bucket.NodeID == n.NodeID() {
			for candidateEventType, candidate := range n.contract.EventHandlers {
				if strings.TrimSpace(candidateEventType) != bucket.EventType {
					continue
				}
				if !accumulationTimeoutHandler(candidate) {
					continue
				}
				handler = candidate
				ok = true
				break
			}
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
	entityID, evt = ensureHandlerEntityID(e.coordinator.SemanticSource(), handler, entityID, evt)
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
	stateSnapshot, err := handlerExecutionStateSnapshot(handler, entityID, currentState)
	if err != nil {
		return nil, err
	}
	result, err := node.Handle(ctx, runtimeengine.ExecutionRequest{
		EntityID:   identity.NormalizeEntityID(entityID),
		NodeID:     identity.NormalizeNodeID(e.nodeID),
		FlowID:     identity.NormalizeFlowID(flowID),
		Event:      evt,
		ChainDepth: evt.ChainDepth,
		Handler:    handler,
		State:      stateSnapshot,
	})
	if err != nil {
		return nil, err
	}
	e.coordinator.recordInterceptedEmitDeadLetters(ctx, evt, e.nodeID, &handlerExecutionOutcome{
		InterceptedEmits: append([]runtimeengine.EmitIntent(nil), result.DeadLetterIntents...),
	})
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

func ensureHandlerEntityID(source semanticview.Source, handler SystemNodeEventHandler, entityID string, evt Event) (string, Event) {
	if handler.CreateEntity {
		return uuid.NewString(), evt
	}
	entityID = strings.TrimSpace(firstNonEmptyString(entityID, evt.EntityID()))
	if entityID != "" {
		if strings.TrimSpace(evt.EntityID()) == "" {
			evt = evt.WithEntityID(entityID)
		}
		return entityID, evt
	}
	if !handlerMaterializesEntity(source, handler) {
		return "", evt
	}
	entityID = uuid.NewString()
	return entityID, evt.WithEntityID(entityID)
}

func handlerMaterializesEntity(source semanticview.Source, handler SystemNodeEventHandler) bool {
	if handler.CreateEntity {
		return true
	}
	allowedFields := workflowEntitySchemaFields(source)
	if len(allowedFields) == 0 {
		return false
	}
	if workflowDataWritesEntityFields(handler.DataAccumulation, allowedFields) {
		return true
	}
	if computeStoresEntityField(handler.Compute, allowedFields) {
		return true
	}
	if payloadTransformReferencesEntity(handler.PayloadTransform) {
		return true
	}
	if accumulateReferencesEntity(handler.Accumulate) {
		return true
	}
	for _, rule := range handler.Rules {
		if workflowDataWritesEntityFields(rule.DataAccumulation, allowedFields) {
			return true
		}
		if computeStoresEntityField(rule.Compute, allowedFields) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if workflowDataWritesEntityFields(rule.DataAccumulation, allowedFields) {
			return true
		}
		if computeStoresEntityField(rule.Compute, allowedFields) {
			return true
		}
	}
	return false
}

func handlerExecutionStateSnapshot(handler SystemNodeEventHandler, entityID string, state WorkflowState) (runtimeengine.StateSnapshot, error) {
	snapshot := runtimeengine.StateSnapshot{
		EntityID: identity.NormalizeEntityID(entityID),
		StateCarrier: runtimeengine.NewStateCarrier(
			nil,
			nil,
			map[string]map[string]any{},
		),
	}
	if handler.CreateEntity {
		snapshot.StateCarrier.Metadata = cloneStringAnyMap(state.Metadata)
		if snapshot.StateCarrier.Metadata == nil {
			snapshot.StateCarrier.Metadata = map[string]any{}
		}
		return snapshot, nil
	}
	snapshot.CurrentState = strings.TrimSpace(string(state.Stage))
	snapshot.StateCarrier.Metadata = cloneStringAnyMap(state.Metadata)
	carrier, err := runtimeengine.StateCarrierFromPersisted(snapshot.StateCarrier.Metadata, nil)
	if err != nil {
		return runtimeengine.StateSnapshot{}, fmt.Errorf("workflow state metadata.gates: %w", err)
	}
	snapshot.StateCarrier.Metadata = carrier.Metadata
	snapshot.StateCarrier.Gates = carrier.Gates
	return snapshot, nil
}

func workflowDataWritesEntityFields(spec runtimecontracts.WorkflowDataAccumulation, allowedFields map[string]struct{}) bool {
	for _, write := range spec.Writes {
		targetField := normalizeEntityWriteTarget(write.Target())
		if targetField == "" {
			continue
		}
		if _, ok := allowedFields[targetField]; ok {
			return true
		}
	}
	return false
}

func computeStoresEntityField(spec *runtimecontracts.ComputeSpec, allowedFields map[string]struct{}) bool {
	if spec == nil {
		return false
	}
	targetField := normalizeEntityWriteTarget(spec.StoreAs)
	if targetField == "" {
		return false
	}
	_, ok := allowedFields[targetField]
	return ok
}

func payloadTransformReferencesEntity(spec *runtimecontracts.PayloadTransformSpec) bool {
	if spec == nil {
		return false
	}
	for _, entry := range spec.TransformEntries() {
		if entry.Value.Kind == runtimecontracts.ExpressionKindCEL && strings.HasPrefix(strings.TrimSpace(entry.Value.CEL), "entity.") {
			return true
		}
	}
	return false
}

func accumulateReferencesEntity(spec *runtimecontracts.AccumulateSpec) bool {
	if spec == nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(spec.ExpectedFrom), "entity.")
}

func normalizeEntityWriteTarget(target string) string {
	target = strings.TrimSpace(target)
	switch {
	case strings.HasPrefix(target, "entity."):
		return strings.TrimSpace(strings.TrimPrefix(target, "entity."))
	case strings.HasPrefix(target, "metadata."):
		return strings.TrimSpace(strings.TrimPrefix(target, "metadata."))
	default:
		return target
	}
}
