package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
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
	ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event, handlerEventKey string) (*HandlerOutcome, error)
}

type declarativeWorkflowNode struct {
	nodeID      string
	coordinator *PipelineCoordinator
}

type DeclarativeNode struct {
	nodeID   string
	contract SystemNodeContract
	source   semanticview.Source
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
	effectiveSubscriptions := runtimecontracts.EffectiveSystemNodeSubscriptions(contract)
	subscriptions := make([]events.EventType, 0, len(effectiveSubscriptions))
	for _, evt := range effectiveSubscriptions {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		subscriptions = append(subscriptions, events.EventType(evt))
	}
	return &DeclarativeNode{
		nodeID:   nodeID,
		contract: contract,
		source:   source,
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
		eventType = strings.TrimSpace(string(evt.Type()))
	}
	policy, ok := workflowNodeEventPolicy(n.coordinator.WorkflowNodes(), n.NodeID(), eventType)
	if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) {
		if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload())); bucketOK && bucket.NodeID == n.NodeID() {
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
	effectiveSubscriptions := runtimecontracts.EffectiveSystemNodeSubscriptions(n.contract)
	out := make([]events.EventType, 0, len(effectiveSubscriptions))
	for _, evt := range effectiveSubscriptions {
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
		eventType = strings.TrimSpace(string(evt.Type()))
	}
	policy, ok := n.policies[eventType]
	if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) {
		if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload())); bucketOK && bucket.NodeID == n.NodeID() {
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
	eventType := strings.TrimSpace(string(evt.Type()))
	handlerEventKey := eventType
	handler, ok := n.resolvedHandlerForDelivery(evt)
	denyRawHandlerFallback := n.source != nil && semanticview.ImportBoundaryWildcardHandlerFallbackDenied(n.source, n.NodeID(), eventType)
	if !ok {
		if !denyRawHandlerFallback {
			handler, ok = n.contract.EventHandlers[eventType]
		}
	}
	if ok {
		handlerEventKey = workflowNodeHandlerEventKeyForExecution(n.source, n.NodeID(), evt)
	}
	if !ok && !denyRawHandlerFallback {
		for pattern, candidate := range n.contract.EventHandlers {
			if strings.TrimSpace(pattern) == eventType {
				continue
			}
			if runtimecontractsHandlerPatternMatches(pattern, eventType) {
				handler = candidate
				handlerEventKey = strings.TrimSpace(pattern)
				ok = true
				break
			}
		}
	}
	if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) && containsEventType(n.Subscriptions(), events.EventType(eventType)) {
		if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload())); bucketOK && bucket.NodeID == n.NodeID() {
			for candidateEventType, candidate := range n.contract.EventHandlers {
				if strings.TrimSpace(candidateEventType) != bucket.EventType {
					continue
				}
				if !accumulationTimeoutHandler(candidate) {
					continue
				}
				handler = candidate
				handlerEventKey = bucket.EventType
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
	outcome, err := n.engine.ExecuteHandlerSteps(ctx, handler, evt, handlerEventKey)
	if err != nil {
		return nil, err
	}
	return outcome, nil
}

func (n *DeclarativeNode) resolvedHandlerForDelivery(evt Event) (SystemNodeEventHandler, bool) {
	if n == nil || n.source == nil {
		return SystemNodeEventHandler{}, false
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(n.source, n.NodeID(), evt)
	return resolved.Handler, resolved.Matched
}

func containsEventType(values []events.EventType, want events.EventType) bool {
	want = events.EventType(strings.TrimSpace(string(want)))
	if want == "" {
		return false
	}
	for _, value := range values {
		if events.EventType(strings.TrimSpace(string(value))) == want {
			return true
		}
	}
	return false
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

func (e *coordinatorHandlerExecutionEngine) ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event, handlerEventKey string) (*HandlerOutcome, error) {
	if e == nil || e.coordinator == nil {
		return nil, fmt.Errorf("handler execution engine is not configured")
	}
	if e.err != nil {
		return nil, e.err
	}
	if e.executor == nil || e.node == nil {
		return nil, fmt.Errorf("handler execution engine is not configured")
	}
	if e.nodeID == "" || strings.TrimSpace(string(evt.Type())) == "" {
		return &HandlerOutcome{Handled: false}, nil
	}
	source := e.coordinator.SemanticSource()
	entityID := workflowEventEntityID(evt)
	flowID := workflowNodeFlowID(source, e.nodeID)
	selectedState := WorkflowState{}
	hasSelectedState := false
	if handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
		selected, err := e.coordinator.selectHandlerEntityForFlow(ctx, flowID, e.nodeID, handler, evt)
		if err != nil {
			return nil, err
		}
		entityID = selected.EntityID
		evt = selected.Event
		selectedState = selected.State
		hasSelectedState = true
	}
	if handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		selected, err := e.coordinator.selectOrCreateHandlerEntityForFlow(ctx, flowID, e.nodeID, handler, evt)
		if err != nil {
			return nil, err
		}
		entityID = selected.EntityID
		evt = selected.Event
		selectedState = selected.State
		hasSelectedState = true
	}
	entityID, evt = ensureHandlerEntityID(source, flowID, handler, entityID, evt)
	ctx = withPipelineFlowScope(ctx, flowID)
	currentState := e.coordinator.currentWorkflowState(ctx, entityID)
	if hasSelectedState && strings.TrimSpace(selectedState.EntityID) != "" && strings.TrimSpace(currentState.EntityID) == "" {
		currentState = selectedState
	}
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
	handlerEventKey = strings.TrimSpace(handlerEventKey)
	if handlerEventKey == "" {
		handlerEventKey = workflowNodeHandlerEventKeyForExecution(source, e.nodeID, evt)
	}
	workflowVersion := ""
	if source != nil {
		workflowVersion = source.WorkflowVersion()
	}
	stateSnapshot, err := handlerExecutionStateSnapshot(handler, entityID, currentState, flowID, workflowVersion)
	if err != nil {
		return nil, err
	}
	result, err := node.Handle(ctx, runtimeengine.ExecutionRequest{
		EntityID:        identity.NormalizeEntityID(entityID),
		NodeID:          identity.NormalizeNodeID(e.nodeID),
		FlowID:          identity.NormalizeFlowID(flowID),
		Event:           evt,
		HandlerEventKey: handlerEventKey,
		ChainDepth:      evt.ChainDepth(),
		Handler:         handler,
		State:           stateSnapshot,
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
		e.coordinator.reconcileWorkflowEventTimers(ctx, entityID, strings.TrimSpace(string(evt.Type())))
	}
	return &HandlerOutcome{
		Handled:         handled,
		ActionsExecuted: append([]string{}, result.ActionsExecuted...),
	}, nil
}

func ensureHandlerEntityID(source semanticview.Source, flowID string, handler SystemNodeEventHandler, entityID string, evt Event) (string, Event) {
	entityID = strings.TrimSpace(firstNonEmptyString(entityID, evt.EntityID()))
	if entityID != "" {
		if strings.TrimSpace(evt.EntityID()) == "" {
			evt = events.NewProjectionEvent(
				evt.ID(),
				evt.Type(),
				evt.SourceAgent(),
				evt.TaskID(),
				evt.Payload(),
				evt.ChainDepth(),
				evt.RunID(),
				evt.ParentEventID(),
				events.EnvelopeForEntityID(evt.NormalizedEnvelope(), entityID),
				evt.CreatedAt(),
			)
		}
		return entityID, evt
	}
	if !handlerMaterializesEntity(source, flowID, handler) {
		return "", evt
	}
	entityID = canonicalHandlerEntityID(source, flowID, evt)
	if entityID == "" {
		return "", evt
	}
	return entityID, events.NewProjectionEvent(
		evt.ID(),
		evt.Type(),
		evt.SourceAgent(),
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		evt.RunID(),
		evt.ParentEventID(),
		events.EnvelopeForEntityID(evt.NormalizedEnvelope(), entityID),
		evt.CreatedAt(),
	)
}

func canonicalHandlerEntityID(source semanticview.Source, flowID string, evt Event) string {
	if flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/"); flowInstance != "" {
		return FlowInstanceEntityID(flowInstance)
	}
	flowID = strings.TrimSpace(flowID)
	if flowID != "" {
		if source != nil {
			if flowPath := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"); flowPath != "" {
				return FlowInstanceEntityID(flowPath)
			}
		}
		return FlowInstanceEntityID(flowID)
	}
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		return FlowInstanceEntityID(runID)
	}
	return FlowInstanceEntityID("root")
}

func handlerMaterializesEntity(source semanticview.Source, flowID string, handler SystemNodeEventHandler) bool {
	if handler.CreateEntity {
		return true
	}
	if handlerActionMaterializesEntity(handler) {
		return true
	}
	if handlerMutatesEntityLifecycle(handler) {
		return true
	}
	if emitSitesReferenceEntity(handler) {
		return true
	}
	if accumulateReferencesEntity(handler.Accumulate) {
		return true
	}
	allowedFields := workflowEntitySchemaFields(source, flowID)
	if len(allowedFields) == 0 {
		return false
	}
	if workflowDataWritesEntityFields(handler.DataAccumulation, allowedFields) {
		return true
	}
	if computeStoresEntityField(handler.Compute, allowedFields) {
		return true
	}
	for _, rule := range handler.Rules {
		if ruleWritesEntityFields(rule, allowedFields) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if ruleWritesEntityFields(rule, allowedFields) {
			return true
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if ruleWritesEntityFields(rule, allowedFields) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil && ruleWritesEntityFields(*handler.Accumulate.OnTimeout, allowedFields) {
			return true
		}
	}
	return false
}

func handlerActionMaterializesEntity(handler SystemNodeEventHandler) bool {
	if actionMaterializesEntity(handler.Action) {
		return true
	}
	for _, rule := range handler.Rules {
		if actionMaterializesEntity(rule.Action) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if actionMaterializesEntity(rule.Action) {
			return true
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if actionMaterializesEntity(rule.Action) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil && actionMaterializesEntity(handler.Accumulate.OnTimeout.Action) {
			return true
		}
	}
	return false
}

func actionMaterializesEntity(action runtimecontracts.ActionSpec) bool {
	switch runtimecontracts.NormalizeHandlerActionID(action.ID) {
	case "record_evidence":
		return true
	default:
		return false
	}
}

func handlerMutatesEntityLifecycle(handler SystemNodeEventHandler) bool {
	if strings.TrimSpace(handler.AdvancesTo) != "" ||
		gateSpecName(handler.SetsGate) != "" ||
		len(handler.ClearGates) > 0 {
		return true
	}
	for _, rule := range handler.Rules {
		if strings.TrimSpace(rule.AdvancesTo) != "" {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if strings.TrimSpace(rule.AdvancesTo) != "" {
			return true
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if strings.TrimSpace(rule.AdvancesTo) != "" {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil && strings.TrimSpace(handler.Accumulate.OnTimeout.AdvancesTo) != "" {
			return true
		}
	}
	return false
}

func gateSpecName(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
}

func ruleWritesEntityFields(rule runtimecontracts.HandlerRuleEntry, allowedFields map[string]struct{}) bool {
	return workflowDataWritesEntityFields(rule.DataAccumulation, allowedFields) ||
		computeStoresEntityField(rule.Compute, allowedFields)
}

func handlerExecutionStateSnapshot(handler SystemNodeEventHandler, entityID string, state WorkflowState, workflowName string, workflowVersion string) (runtimeengine.StateSnapshot, error) {
	snapshot := runtimeengine.StateSnapshot{
		EntityID:        identity.NormalizeEntityID(entityID),
		WorkflowName:    strings.TrimSpace(workflowName),
		WorkflowVersion: strings.TrimSpace(workflowVersion),
		StateCarrier: runtimeengine.NewStateCarrier(
			nil,
			nil,
			map[string]map[string]any{},
		),
	}
	if handler.CreateEntity {
		snapshot.CurrentState = strings.TrimSpace(string(state.Stage))
		carrier, err := runtimeengine.StateCarrierFromPersisted(cloneStringAnyMap(state.Metadata), nil)
		if err != nil {
			return runtimeengine.StateSnapshot{}, fmt.Errorf("workflow state metadata.gates: %w", err)
		}
		snapshot.StateCarrier.Metadata = carrier.Metadata
		snapshot.StateCarrier.Gates = carrier.Gates
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

func emitSitesReferenceEntity(handler SystemNodeEventHandler) bool {
	if emitReferencesEntity(handler.Emit) {
		return true
	}
	if handler.FanOut != nil && emitReferencesEntity(handler.FanOut.Emit) {
		return true
	}
	for _, rule := range handler.Rules {
		if emitReferencesEntity(rule.Emit) {
			return true
		}
		if rule.FanOut != nil && emitReferencesEntity(rule.FanOut.Emit) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if emitReferencesEntity(rule.Emit) {
			return true
		}
		if rule.FanOut != nil && emitReferencesEntity(rule.FanOut.Emit) {
			return true
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if emitReferencesEntity(rule.Emit) {
				return true
			}
			if rule.FanOut != nil && emitReferencesEntity(rule.FanOut.Emit) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			rule := handler.Accumulate.OnTimeout
			if emitReferencesEntity(rule.Emit) {
				return true
			}
			if rule.FanOut != nil && emitReferencesEntity(rule.FanOut.Emit) {
				return true
			}
		}
	}
	return false
}

func emitReferencesEntity(spec runtimecontracts.EmitSpec) bool {
	if strings.TrimSpace(spec.From) == runtimecontracts.EmitFromEntity {
		return true
	}
	for _, value := range spec.Fields {
		if value.Kind == runtimecontracts.ExpressionKindCEL && workflowexpr.ExpressionReferencesEntity(value.CEL) {
			return true
		}
		if value.Kind == runtimecontracts.ExpressionKindCEL && strings.TrimSpace(value.CEL) == runtimecontracts.EmitFromEntity {
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
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return ""
	}
	field, _, _ := strings.Cut(path, ".")
	return strings.TrimSpace(field)
}
