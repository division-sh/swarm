package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	node        identity.ExecutableNode
	coordinator *PipelineCoordinator
}

type DeclarativeNode struct {
	node     identity.ExecutableNode
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

func NewNode(node identity.ExecutableNode, contract SystemNodeContract, source semanticview.Source, engine HandlerExecutionEngine, hooks *ProductHookRegistry) NodeExecutor {
	if !node.Valid() {
		return nil
	}
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
		node:     node,
		contract: contract,
		source:   source,
		policies: buildWorkflowNodePolicies(source, node, subscriptions),
		engine:   engine,
		hooks:    hooks,
	}
}

func (n *declarativeWorkflowNode) ExecutableNode() identity.ExecutableNode {
	if n == nil {
		return identity.ExecutableNode{}
	}
	return n.node
}

func (n *declarativeWorkflowNode) Subscriptions() []events.EventType {
	if n == nil || n.coordinator == nil {
		return nil
	}
	return workflowNodeSubscriptions(n.coordinator.WorkflowNodes(), n.ExecutableNode())
}

func (n *declarativeWorkflowNode) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	if n == nil {
		return false, false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type()))
	}
	policy, ok := workflowNodeEventPolicy(n.coordinator.WorkflowNodes(), n.ExecutableNode(), eventType)
	if !ok && isJoinLifecycleEvent(events.EventType(eventType)) {
		if resolution, refOK, err := resolveWorkflowJoinOccurrence(n.coordinator.SemanticSource(), evt); err == nil && refOK && resolution.Ref.Node().Equal(n.ExecutableNode()) {
			policy, ok = workflowNodeEventPolicy(n.coordinator.WorkflowNodes(), n.ExecutableNode(), resolution.Ref.HandlerEvent())
		}
	}
	if !ok {
		return false, false
	}
	return policy.Consume, true
}

func (n *declarativeWorkflowNode) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil {
		return false
	}
	return n.coordinator.executeNodeHandlerPlan(ctx, n.ExecutableNode(), evt)
}

func (n *DeclarativeNode) ExecutableNode() identity.ExecutableNode {
	if n == nil {
		return identity.ExecutableNode{}
	}
	return n.node
}

func (n *DeclarativeNode) Subscriptions() []events.EventType {
	if n == nil {
		return nil
	}
	effectiveSubscriptions := runtimecontracts.EffectiveSystemNodeSubscriptions(n.contract)
	out := make([]events.EventType, 0, len(effectiveSubscriptions))
	for _, evt := range effectiveSubscriptions {
		aliases, err := workflowNodeSubscriptionAliases(n.source, n.node, evt)
		if err != nil {
			continue
		}
		for _, alias := range aliases {
			if alias = strings.TrimSpace(alias); alias != "" {
				out = append(out, events.EventType(alias))
			}
		}
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
	if !ok && isJoinLifecycleEvent(events.EventType(eventType)) {
		if resolution, refOK, err := resolveWorkflowJoinOccurrence(n.source, evt); err == nil && refOK && resolution.Ref.Node().Equal(n.node) {
			policy, ok = n.policies[resolution.Ref.HandlerEvent()]
		}
	}
	if !ok {
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
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, n.source, n.node, evt)
	if resolved.Failure != "" {
		return nil, fmt.Errorf("resolve workflow handler for node %s: %s", n.node.Key(), resolved.Failure)
	}
	handler, ok := resolved.Handler, resolved.Matched
	if ok {
		handlerEventKey = workflowNodeHandlerEventKeyForExecution(ctx, n.source, n.node, evt)
	}
	if !ok && isJoinLifecycleEvent(events.EventType(eventType)) {
		resolution, refOK, err := resolveWorkflowJoinOccurrence(n.source, evt)
		if err != nil {
			return nil, err
		}
		if refOK && resolution.Ref.Node().Equal(n.node) {
			handler = resolution.Handler
			handlerEventKey = resolution.Ref.HandlerEvent()
			ok = true
		}
	}
	if !ok {
		return nil, nil
	}
	if n.engine == nil {
		return nil, fmt.Errorf("declarative node %s has no handler execution engine", n.node.Key())
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
	resolved := workflowNodeEventHandlerResolutionForDelivery(n.source, n.node, evt)
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
	nodeRef     identity.ExecutableNode
	coordinator *PipelineCoordinator
	executor    *runtimeengine.Executor
	node        *runtimeengine.DeclarativeNode
	err         error
}

func newCoordinatorHandlerExecutionEngine(pc *PipelineCoordinator, node identity.ExecutableNode) HandlerExecutionEngine {
	if pc == nil || !node.Valid() {
		return nil
	}
	engine := &coordinatorHandlerExecutionEngine{
		nodeRef:     node,
		coordinator: pc,
	}
	exec, err := runtimeengine.NewExecutor(coordinatorEngineDependencies(pc), newCoordinatorEngineEvaluator(pc))
	if err != nil {
		engine.err = err
		return engine
	}
	engine.executor = exec
	engine.node = runtimeengine.NewDeclarativeNode(node, exec)
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
	if !e.nodeRef.Valid() || strings.TrimSpace(string(evt.Type())) == "" {
		return &HandlerOutcome{Handled: false}, nil
	}
	source := e.coordinator.SemanticSource()
	entityID := workflowEventEntityID(evt)
	handlerFact := MustDeliveryTargetHandler(e.nodeRef)
	flowID := handlerFact.ExecutionFlowID(source)
	if handler.Join != nil {
		if executionFlowID := strings.TrimSpace(pipelineFlowScope(ctx)); executionFlowID != "" {
			flowID = executionFlowID
		}
	}
	selectedState := WorkflowState{}
	hasSelectedState := false
	stampedOwner, exactDelivery := stampedDeliveryTargetOwnership(ctx)
	if exactDelivery {
		if targetFlowID := strings.TrimSpace(stampedOwner.Route().FlowID); targetFlowID != "" {
			flowID = targetFlowID
		}
		entityID = stampedOwner.Route().EntityID
		if err := prepareStampedSelectOrCreateState(source, flowID, handler, evt, stampedOwner, &selectedState); err != nil {
			return nil, err
		}
		hasSelectedState = strings.TrimSpace(selectedState.EntityID) != ""
	}
	if !exactDelivery && handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
		selected, err := e.coordinator.selectHandlerEntityForFlow(ctx, flowID, e.nodeRef.Key(), handler, evt)
		if err != nil {
			return nil, err
		}
		entityID = selected.EntityID
		evt = selected.Event
		selectedState = selected.State
		hasSelectedState = true
	}
	if !exactDelivery && handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		selected, err := e.coordinator.selectOrCreateHandlerEntityForFlow(ctx, flowID, e.nodeRef.Key(), handler, evt)
		if err != nil {
			return nil, err
		}
		entityID = selected.EntityID
		evt = selected.Event
		selectedState = selected.State
		hasSelectedState = true
	}
	resolvedEntityID, resolvedEvent, err := ensureHandlerEntityID(source, flowID, handler, entityID, evt)
	if err != nil {
		return nil, err
	}
	entityID, evt = resolvedEntityID, resolvedEvent
	ctx = withPipelineFlowScope(ctx, flowID)
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	statePath := firstNonEmptyString(selectedState.Control.FlowPath, evt.FlowInstance())
	if exactDelivery {
		statePath = stampedOwner.Route().FlowInstance
	}
	var stateRoute runtimeflowidentity.Route
	if exactDelivery {
		stateRoute, err = workflowInstanceRouteForExecution(source, flowID, statePath)
	} else {
		stateRoute, err = canonicalHandlerRoute(
			source,
			flowID,
			statePath,
			evt,
		)
	}
	if err != nil {
		return nil, err
	}
	currentState := WorkflowState{Metadata: map[string]any{}}
	if !exactDelivery || !stampedOwner.EntitylessReceiver() {
		currentState, err = e.coordinator.currentWorkflowState(ctx, stateRoute, identity.NormalizeEntityID(entityID))
		if err != nil {
			return nil, err
		}
	}
	if hasSelectedState && strings.TrimSpace(selectedState.EntityID) != "" && strings.TrimSpace(currentState.EntityID) == "" {
		currentState = selectedState
	}
	if err := prepareHandlerMaterializationState(source, flowID, handler, stateRoute, entityID, &currentState); err != nil {
		return nil, err
	}
	node := e.node
	handlerEventKey = strings.TrimSpace(handlerEventKey)
	if handlerEventKey == "" {
		handlerEventKey = workflowNodeHandlerEventKeyForExecution(ctx, source, e.nodeRef, evt)
	}
	workflowVersion := ""
	if source != nil {
		workflowVersion = source.WorkflowVersion()
	}
	stateSnapshot, err := handlerExecutionStateSnapshot(handler, entityID, currentState, flowID, workflowVersion)
	if err != nil {
		return nil, err
	}
	producerSource, err := workflowNodeProducerSource(ctx, source, e.nodeRef, flowID, entityID, evt.RoutingSource())
	if err != nil {
		return nil, fmt.Errorf("admit workflow node producer source: %w", err)
	}
	joinDeclaration, err := workflowJoinDeclarationForExecution(source, evt, e.nodeRef, handlerEventKey, handler)
	if err != nil {
		return nil, err
	}
	result, err := node.Handle(ctx, runtimeengine.ExecutionRequest{
		EntityID:        identity.NormalizeEntityID(entityID),
		Node:            e.nodeRef,
		Route:           stateRoute,
		Event:           evt,
		ProducerSource:  producerSource,
		HandlerEventKey: handlerEventKey,
		JoinDeclaration: joinDeclaration,
		ChainDepth:      evt.ChainDepth(),
		Handler:         handler,
		State:           stateSnapshot,
	})
	logComputeModuleReplayEvidence(ctx, e.coordinator.bus, e.nodeRef.Key(), evt, result.ComputeModuleTraces)
	if err != nil {
		return nil, err
	}
	e.coordinator.recordInterceptedEmitDeadLetters(ctx, evt, e.nodeRef.Key(), &handlerExecutionOutcome{
		InterceptedEmits: append([]runtimeengine.EmitIntent(nil), result.DeadLetterIntents...),
	}, nil)
	return &HandlerOutcome{
		Handled:         runtimeengine.IsHandledOutcome(result.Status),
		ActionsExecuted: append([]string{}, result.ActionsExecuted...),
	}, nil
}

func canonicalHandlerMaterializationTarget(source semanticview.Source, flowID string, handler SystemNodeEventHandler, evt Event, blueprint events.RouteIdentity) (events.RouteIdentity, error) {
	blueprint = blueprint.Normalized()
	if blueprint.FlowInstance == "" {
		return events.RouteIdentity{}, fmt.Errorf("materializing handler requires an exact receiver flow instance")
	}
	want := blueprint
	want.FlowID = strings.TrimSpace(flowID)
	want.EntityID = runtimeflowidentity.EntityID(blueprint.FlowInstance)
	if handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		expected, err := selectOrCreateEntityExpectedValues(handler.SelectOrCreateEntity, evt)
		if err != nil {
			return events.RouteIdentity{}, fmt.Errorf("select_or_create_entity target: %w", err)
		}
		instanceID, err := selectOrCreateEntityInstanceID(source, flowID, expected)
		if err != nil {
			return events.RouteIdentity{}, fmt.Errorf("select_or_create_entity target: %w", err)
		}
		instance := deriveFlowInstanceIdentity(source, flowID, instanceID)
		if instance.InstancePath != blueprint.FlowInstance {
			return events.RouteIdentity{}, fmt.Errorf("select_or_create_entity target flow instance %q disagrees with canonical instance %q", blueprint.FlowInstance, instance.InstancePath)
		}
		want.EntityID = instance.EntityID
	}
	if blueprint.EntityID != "" && blueprint.EntityID != want.EntityID {
		return events.RouteIdentity{}, fmt.Errorf("materializing target entity %q disagrees with canonical future entity %q", blueprint.EntityID, want.EntityID)
	}
	return want.Normalized(), nil
}

func ensureHandlerEntityID(source semanticview.Source, flowID string, handler SystemNodeEventHandler, entityID string, evt Event) (string, Event, error) {
	entityID = strings.TrimSpace(firstNonEmptyString(entityID, evt.TargetRoute().EntityID))
	if entityID != "" {
		if strings.TrimSpace(evt.EntityID()) == "" {
			resolved, err := events.ResolveEnvelope(evt, events.EnvelopeForEntityID(evt.NormalizedEnvelope(), entityID))
			if err != nil {
				return "", evt, err
			}
			evt = resolved
		}
		return entityID, evt, nil
	}
	if !handlerExecutionEntityRequirement(source, flowID, handler).materializes() {
		return "", evt, nil
	}
	route, err := canonicalHandlerRoute(source, flowID, "", evt)
	if err != nil {
		return "", evt, err
	}
	entityID = runtimeflowidentity.EntityID(route.InstancePath)
	envelope := events.EnvelopeForFlowInstance(evt.NormalizedEnvelope(), route.InstancePath)
	envelope = events.EnvelopeForEntityID(envelope, entityID)
	resolved, err := events.ResolveEnvelope(evt, envelope)
	if err != nil {
		return "", evt, err
	}
	return entityID, resolved, nil
}

func canonicalHandlerRoute(source semanticview.Source, flowID, statePath string, evt Event) (runtimeflowidentity.Route, error) {
	flowID = strings.TrimSpace(flowID)
	statePath = strings.Trim(strings.TrimSpace(statePath), "/")
	if target := evt.TargetRoute().Normalized(); target.FlowInstance != "" && (target.FlowID == "" || target.FlowID == flowID) {
		return workflowInstanceRouteForExecution(source, flowID, target.FlowInstance)
	}
	if flowID != "" {
		if source != nil && flowID == strings.TrimSpace(source.WorkflowName()) {
			return workflowInstanceRouteForExecution(source, flowID, evt.RunID())
		}
		if statePath != "" {
			return workflowInstanceRouteForExecution(source, flowID, statePath)
		}
		if source != nil {
			if schema, ok := source.FlowSchemaByID(flowID); ok && strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
				return workflowInstanceRouteForExecution(source, flowID, evt.FlowInstance())
			}
		}
		return workflowInstanceRouteForExecution(source, flowID, "")
	}
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		return workflowInstanceRouteForPath(runID)
	}
	return runtimeflowidentity.Route{}, fmt.Errorf("materializing handler requires an exact workflow instance route")
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

func prepareHandlerMaterializationState(source semanticview.Source, flowID string, handler SystemNodeEventHandler, route runtimeflowidentity.Route, entityID string, state *WorkflowState) error {
	if state == nil || !handlerExecutionEntityRequirement(source, flowID, handler).materializes() {
		return nil
	}
	if !route.Valid() {
		return fmt.Errorf("materializing handler requires an exact workflow instance route")
	}
	state.Metadata = workflowMaterializeEntityFields(source, flowID, state.Metadata)
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	if existing := strings.Trim(strings.TrimSpace(state.Control.FlowPath), "/"); existing != "" && existing != route.InstancePath {
		return fmt.Errorf("materializing handler flow_path %q disagrees with exact value %q", existing, route.InstancePath)
	}
	if existing := strings.TrimSpace(state.Control.InstanceID); existing != "" && existing != route.InstanceID {
		return fmt.Errorf("materializing handler instance_id %q disagrees with exact value %q", existing, route.InstanceID)
	}
	state.Control.FlowPath = route.InstancePath
	state.Control.StorageRef = route.InstancePath
	state.Control.InstanceID = route.InstanceID
	state.EntityID = strings.TrimSpace(entityID)
	if strings.TrimSpace(string(state.Stage)) == "" {
		state.Stage = NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID))
	}
	return nil
}

func actionMaterializesEntity(action runtimecontracts.ActionSpec) bool {
	switch runtimecontracts.NormalizeHandlerActionID(action.ID) {
	case "record_evidence":
		return true
	default:
		return false
	}
}

func gateSpecName(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
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
	snapshot.StateCarrier.Control = state.Control
	snapshot.CurrentState = strings.TrimSpace(string(state.Stage))
	snapshot.StateCarrier.Fields = cloneStringAnyMap(state.Metadata)
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

func normalizeEntityWriteTarget(target string) string {
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return ""
	}
	field, _, _ := strings.Cut(path, ".")
	return strings.TrimSpace(field)
}
