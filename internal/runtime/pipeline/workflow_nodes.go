package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type WorkflowEventPolicy struct {
	Consume           bool
	VisibleDownstream bool
}

type ConsumerType string

const (
	ConsumerTypeUnknown         ConsumerType = ""
	ConsumerTypeSystemComponent ConsumerType = "system_component"
)

type workflowNodeExecutor = WorkflowNodeExecutor

type WorkflowNode struct {
	ID               string
	Subscriptions    []events.EventType
	Produces         []events.EventType
	OwnedTransitions []string
	Timers           []string
	ExecutionType    string
	Implementation   string
	StateTable       string
	IdempotencyTable string
	Policies         map[string]WorkflowEventPolicy
}

func workflowNodesSnapshot(nodes []WorkflowNode) []WorkflowNode {
	out := make([]WorkflowNode, 0, len(nodes))
	for _, node := range nodes {
		nodeCopy := node
		out = append(out, nodeCopy)
	}
	return out
}

func workflowNodeSubscriptions(nodes []WorkflowNode, nodeID string) []events.EventType {
	nodeID = strings.TrimSpace(nodeID)
	for _, node := range nodes {
		if strings.TrimSpace(node.ID) != nodeID {
			continue
		}
		return append([]events.EventType{}, node.Subscriptions...)
	}
	return nil
}

func workflowNodeEventPolicy(nodes []WorkflowNode, nodeID, eventType string) (WorkflowEventPolicy, bool) {
	eventType = strings.TrimSpace(eventType)
	nodeID = strings.TrimSpace(nodeID)
	for _, node := range nodes {
		if nodeID != "" && strings.TrimSpace(node.ID) != nodeID {
			continue
		}
		if policy, ok := workflowNodePolicyForEventType(node.Policies, eventType); ok {
			return policy, true
		}
	}
	return WorkflowEventPolicy{}, false
}

func workflowNodePolicyForDelivery(ctx context.Context, source semanticview.Source, node WorkflowNode, evt events.Event) (WorkflowEventPolicy, bool, error) {
	eventType := strings.TrimSpace(string(evt.Type()))
	if policy, ok := workflowNodePolicyForEventType(node.Policies, eventType); ok {
		return policy, true, nil
	}
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, strings.TrimSpace(node.ID), evt)
	if resolved.Failure != "" {
		return WorkflowEventPolicy{}, false, fmt.Errorf("resolve workflow handler for node %s: %s", strings.TrimSpace(node.ID), resolved.Failure)
	}
	if !resolved.Matched {
		return WorkflowEventPolicy{}, false, nil
	}
	candidates := []string{resolved.HandlerEventKey}
	if source != nil {
		candidates = append(candidates, workflowNodeExternalEventType(source, strings.TrimSpace(node.ID), resolved.HandlerEventKey))
	}
	for _, candidate := range candidates {
		if policy, ok := workflowNodePolicyForEventType(node.Policies, candidate); ok {
			return policy, true, nil
		}
	}
	return deriveWorkflowEventPolicy(source, resolved.HandlerEventKey), true, nil
}

func workflowNodePolicyForEventType(policies map[string]WorkflowEventPolicy, eventType string) (WorkflowEventPolicy, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" || policies == nil {
		return WorkflowEventPolicy{}, false
	}
	if policy, ok := policies[eventType]; ok {
		return policy, true
	}
	for pattern, policy := range policies {
		if strings.TrimSpace(pattern) == eventType {
			continue
		}
		if runtimecontractsHandlerPatternMatches(pattern, eventType) {
			return policy, true
		}
	}
	return WorkflowEventPolicy{}, false
}

func workflowNodeEventHandlerForDelivery(source semanticview.Source, nodeID string, evt events.Event) (runtimecontracts.SystemNodeEventHandler, bool) {
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, nodeID, evt)
	return resolved.Handler, resolved.Matched
}

type workflowNodeEventHandlerResolution struct {
	Handler         runtimecontracts.SystemNodeEventHandler
	HandlerEventKey string
	FlowID          string
	Matched         bool
	Failure         string
}

func workflowNodeEventHandlerResolutionForDelivery(source semanticview.Source, nodeID string, evt events.Event) workflowNodeEventHandlerResolution {
	return workflowNodeEventHandlerResolutionForDeliveryContext(nil, source, nodeID, evt)
}

func workflowNodeEventHandlerResolutionForDeliveryContext(ctx context.Context, source semanticview.Source, nodeID string, evt events.Event) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	rawEventType := eventidentity.Normalize(string(evt.Type()))
	if rawEventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	if isJoinLifecycleEvent(evt.Type()) {
		resolution, ok, err := resolveWorkflowJoinOccurrence(source, evt)
		if err != nil {
			return workflowNodeEventHandlerResolution{Failure: err.Error()}
		}
		if !ok || resolution.Ref.NodeID() != strings.TrimSpace(nodeID) {
			return workflowNodeEventHandlerResolution{}
		}
		handlerFlowID := resolution.Ref.FlowID()
		if handlerFlowID == "" {
			handlerFlowID = semanticview.RootExecutionFlowID(source)
		}
		return workflowNodeEventHandlerResolution{
			Handler: resolution.Handler, HandlerEventKey: resolution.Ref.HandlerEvent(),
			FlowID: handlerFlowID, Matched: true,
		}
	}
	route, deliveryRouted := workflowNodeDeliveryRoute(ctx)
	if deliveryRouted && !route.ConnectClaim.Empty() {
		handlerFlowID, handlerNodeID, handlerEvent, authorized := route.ConnectClaim.NodeHandlerOwner(nodeID)
		if !authorized {
			return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf("connect delivery claim does not authorize node %s", strings.TrimSpace(nodeID))}
		}
		handlerResolution := semanticview.ResolveFlowNodeSubscriptionHandler(source, handlerFlowID, handlerNodeID, string(handlerEvent))
		if !handlerResolution.Matched {
			return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf("connect delivery claim authorizes event %s but declaration node %s has no matching handler", handlerEvent, handlerNodeID)}
		}
		return workflowNodeEventHandlerResolution{
			Handler:         handlerResolution.Handler,
			HandlerEventKey: handlerResolution.HandlerEventKey,
			FlowID:          handlerFlowID,
			Matched:         true,
		}
	}
	if deliveryRouted {
		// Direct typed-pubsub delivery remains owned by the semantic source. Only
		// compiled-connect delivery requires the route-scoped connect claim above.
		if resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeID, rawEventType); resolved.Matched {
			return resolved
		}
		if resolved := workflowNodeDirectDeliveryHandlerResolution(source, nodeID, rawEventType, route.Target.Route()); resolved.Matched {
			return resolved
		}
		return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf("node delivery event %s has no semantic handler or stamped connect claim", rawEventType)}
	}
	if resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeID, rawEventType); resolved.Matched {
		return resolved
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeDirectDeliveryHandlerResolution(source semanticview.Source, nodeID, eventType string, target events.RouteIdentity) workflowNodeEventHandlerResolution {
	target = target.Normalized()
	if target.FlowID == "" {
		return workflowNodeEventHandlerResolution{}
	}
	eventType = eventidentity.Normalize(eventType)
	if resolved := semanticview.ResolveFlowNodeSubscriptionHandler(source, target.FlowID, nodeID, eventType); resolved.Matched {
		return workflowNodeEventHandlerResolution{
			Handler:         resolved.Handler,
			HandlerEventKey: resolved.HandlerEventKey,
			FlowID:          target.FlowID,
			Matched:         true,
		}
	}
	prefix := eventidentity.Normalize(target.FlowInstance) + "/"
	if prefix == "/" || !strings.HasPrefix(eventType, prefix) {
		return workflowNodeEventHandlerResolution{}
	}
	resolved := semanticview.ResolveFlowNodeSubscriptionHandler(source, target.FlowID, nodeID, strings.TrimPrefix(eventType, prefix))
	if !resolved.Matched {
		return workflowNodeEventHandlerResolution{}
	}
	return workflowNodeEventHandlerResolution{
		Handler:         resolved.Handler,
		HandlerEventKey: resolved.HandlerEventKey,
		FlowID:          target.FlowID,
		Matched:         true,
	}
}

func workflowNodeExactEventHandlerResolution(source semanticview.Source, nodeID, eventType string) workflowNodeEventHandlerResolution {
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	resolved := semanticview.ResolveNodeSubscriptionHandler(source, nodeID, eventType)
	if !resolved.Matched {
		return workflowNodeEventHandlerResolution{}
	}
	return workflowNodeEventHandlerResolution{
		Handler:         resolved.Handler,
		HandlerEventKey: resolved.HandlerEventKey,
		FlowID:          workflowNodeFlowID(source, nodeID),
		Matched:         true,
	}
}

func workflowNodeEventHandlerResolutionForEventType(source semanticview.Source, nodeID, eventType string) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	resolved := semanticview.ResolveNodeSubscriptionHandler(source, nodeID, eventType)
	if resolved.Matched {
		return workflowNodeEventHandlerResolution{
			Handler:         resolved.Handler,
			HandlerEventKey: resolved.HandlerEventKey,
			FlowID:          workflowNodeFlowID(source, nodeID),
			Matched:         true,
		}
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeHandlerEventKeyForExecution(ctx context.Context, source semanticview.Source, nodeID string, evt events.Event) string {
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, nodeID, evt)
	if resolved.Matched {
		return resolved.HandlerEventKey
	}
	return eventidentity.Normalize(string(evt.Type()))
}

func LoadWorkflowNodes(source semanticview.Source) ([]WorkflowNode, error) {
	if source == nil {
		return nil, ErrContractBundleNil
	}
	path := "workflow contract bundle"

	wantIDs := workflowRuntimeNodeIDs(source)
	nodes := source.NodeEntries()
	out := make([]WorkflowNode, 0, len(wantIDs))
	for _, nodeID := range wantIDs {
		entry, ok := nodes[nodeID]
		if !ok {
			return nil, fmt.Errorf("system node %q missing from %s", nodeID, path)
		}
		runtimeSubscriptions := source.NodeRuntimeSubscriptions(nodeID)
		subscriptions := make([]events.EventType, 0, len(runtimeSubscriptions))
		for _, evt := range runtimeSubscriptions {
			aliases, err := workflowNodeSubscriptionAliases(source, nodeID, evt)
			if err != nil {
				return nil, err
			}
			for _, resolved := range aliases {
				if resolved == "" {
					continue
				}
				subscriptions = append(subscriptions, events.EventType(resolved))
			}
		}
		effectiveProduces := semanticview.NodeEffectiveProduces(source, nodeID)
		produces := make([]events.EventType, 0, len(effectiveProduces))
		for _, evt := range effectiveProduces {
			evt = workflowNodeExternalEventType(source, nodeID, evt)
			if evt == "" {
				continue
			}
			produces = append(produces, events.EventType(evt))
		}
		out = append(out, WorkflowNode{
			ID:               nodeID,
			Subscriptions:    subscriptions,
			Produces:         produces,
			OwnedTransitions: append([]string{}, entry.OwnedTransitions...),
			Timers:           workflowNodeTimerIDs(entry.Timers),
			ExecutionType:    runtimecontracts.EffectiveSystemNodeExecutionType(entry),
			Implementation:   strings.TrimSpace(entry.Implementation),
			StateTable:       strings.TrimSpace(entry.StateTable),
			IdempotencyTable: strings.TrimSpace(entry.IdempotencyTable),
			Policies:         buildWorkflowNodePolicies(source, nodeID, subscriptions),
		})
	}
	return out, nil
}

func workflowNodeSubscriptionAliases(source semanticview.Source, nodeID, eventType string) ([]string, error) {
	nodeID = strings.TrimSpace(nodeID)
	eventType = eventidentity.Normalize(eventType)
	if nodeID == "" || eventType == "" || source == nil {
		if eventType == "" {
			return nil, nil
		}
		return []string{eventType}, nil
	}
	admission := semanticview.ClassifyNodeSubscription(source, nodeID, eventType)
	if !admission.Admitted() {
		return nil, fmt.Errorf("workflow node %s: %s", nodeID, admission.Message())
	}
	out := make([]string, 0, 2)
	appendAlias := func(value string) {
		value = strings.Trim(strings.TrimSpace(value), "/")
		if value == "" {
			return
		}
		for _, existing := range out {
			if existing == value {
				return
			}
		}
		out = append(out, value)
	}
	contractSource, _ := source.NodeContractSource(nodeID)
	flowID := strings.TrimSpace(contractSource.FlowID)
	if admission.Pattern() {
		for _, pattern := range admission.RoutePatterns() {
			appendAlias(pattern)
		}
		return out, nil
	}
	if source.FlowHasInputEvent(flowID, admission.LocalEvent()) {
		for _, pattern := range runtimepinrouting.ResolveFlowInputProducer(source, flowID, admission.LocalEvent()).AutoWireResolution().Patterns {
			appendAlias(pattern)
		}
	}
	for _, pattern := range admission.RoutePatterns() {
		appendAlias(pattern)
	}
	appendAlias(admission.LocalEvent())
	return out, nil
}

func workflowFlowInputProducerAliases(source semanticview.Source, targetFlowID, eventType string) []string {
	if source == nil {
		return nil
	}
	return append([]string{}, runtimepinrouting.ResolveFlowInputProducer(source, targetFlowID, eventType).AutoWireResolution().Patterns...)
}

func workflowFlowHasInputEvent(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	return source.FlowHasInputEvent(flowID, eventType)
}

func workflowRuntimeNodeIDs(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	nodes := source.NodeEntries()
	events := source.EventEntries()
	seen := make(map[string]struct{})
	out := make([]string, 0, len(nodes))
	for _, transition := range source.WorkflowTransitions() {
		nodeID := strings.TrimSpace(transition.Node)
		if nodeID == "" {
			continue
		}
		if _, ok := nodes[nodeID]; !ok {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	for _, entry := range events {
		nodeID := strings.TrimSpace(entry.OwningNode)
		if nodeID == "" {
			continue
		}
		if _, ok := nodes[nodeID]; !ok {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	for _, transition := range source.DerivedHandlerTransitions() {
		nodeID := strings.TrimSpace(transition.NodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := nodes[nodeID]; !ok {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		if strings.TrimSpace(transition.AdvancesTo) == "" {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	for nodeID, node := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		if len(source.NodeRuntimeSubscriptions(nodeID)) == 0 && len(node.OwnedTransitions) == 0 && len(node.Timers) == 0 {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	sort.Strings(out)
	return out
}

func buildWorkflowNodePolicies(source semanticview.Source, nodeID string, subscriptions []events.EventType) map[string]WorkflowEventPolicy {
	allowed := workflowNodeRuntimePolicyEvents(source, strings.TrimSpace(nodeID), subscriptions)
	if len(allowed) == 0 {
		return nil
	}
	subscribed := make(map[string]struct{}, len(subscriptions))
	for _, evt := range subscriptions {
		name := strings.TrimSpace(string(evt))
		if name != "" {
			subscribed[name] = struct{}{}
		}
	}
	policies := make(map[string]WorkflowEventPolicy, len(allowed))
	for eventType := range allowed {
		if _, ok := subscribed[eventType]; !ok {
			continue
		}
		policy := deriveWorkflowEventPolicy(source, eventType)
		policies[eventType] = policy
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
}

func workflowNodeTimerIDs(timers []runtimecontracts.WorkflowTimerContract) []string {
	if len(timers) == 0 {
		return nil
	}
	out := make([]string, 0, len(timers))
	for _, timer := range timers {
		if id := strings.TrimSpace(timer.ID); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func workflowNodeRuntimePolicyEvents(source semanticview.Source, nodeID string, subscriptions []events.EventType) map[string]struct{} {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || source == nil {
		return nil
	}
	out := make(map[string]struct{}, len(subscriptions)+8)
	for _, evt := range subscriptions {
		name := strings.TrimSpace(string(evt))
		if name != "" {
			out[name] = struct{}{}
		}
	}
	for eventType := range source.EventEntries() {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		for _, owner := range source.RuntimeEventOwners(eventType) {
			if strings.TrimSpace(owner) == nodeID {
				out[eventType] = struct{}{}
				break
			}
		}
	}
	for eventType := range source.NodeEventHandlers(nodeID) {
		eventType = workflowNodeExternalEventType(source, nodeID, eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, transition := range source.WorkflowTransitions() {
		if strings.TrimSpace(transition.Node) != nodeID {
			continue
		}
		trigger := strings.TrimSpace(transition.Trigger)
		if trigger != "" {
			out[trigger] = struct{}{}
		}
	}
	if contractSource, ok := source.NodeContractSource(nodeID); ok {
		flowID := strings.TrimSpace(contractSource.FlowID)
		if flowID != "" {
			for _, eventType := range source.FlowInputEvents(flowID) {
				eventType = strings.TrimSpace(eventType)
				if eventType != "" {
					out[eventType] = struct{}{}
				}
			}
			for _, eventType := range source.FlowOutputEvents(flowID) {
				eventType = strings.TrimSpace(eventType)
				if eventType != "" {
					out[eventType] = struct{}{}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowNodeExternalEventType(source semanticview.Source, nodeID, eventType string) string {
	if source == nil {
		return eventidentity.Normalize(eventType)
	}
	return source.ResolveNodeEventReference(nodeID, eventType)
}

func deriveWorkflowEventPolicy(source semanticview.Source, eventType string) WorkflowEventPolicy {
	eventType = strings.TrimSpace(eventType)
	entry, ok := source.EventEntry(eventType)
	if !ok {
		return WorkflowEventPolicy{}
	}
	consume, visible := deriveWorkflowEventDelivery(entry)
	return WorkflowEventPolicy{
		Consume:           consume,
		VisibleDownstream: visible,
	}
}

func (pc *PipelineCoordinator) BackgroundNodes() []BackgroundNode {
	return nil
}

func (pc *PipelineCoordinator) backgroundWorkflowExecutor(nodeID string) WorkflowNodeExecutor {
	if pc == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	for _, executor := range pc.workflowNodeExecutors() {
		if strings.TrimSpace(executor.NodeID()) != nodeID {
			continue
		}
		provider, ok := executor.(BackgroundWorkflowExecutorProvider)
		if !ok {
			return nil
		}
		return provider.BackgroundWorkflowExecutor()
	}
	return nil
}

func (pc *PipelineCoordinator) workflowNodeExecutors() []workflowNodeExecutor {
	if pc == nil {
		return nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return nil
	}
	nodes := pc.WorkflowNodes()
	out := make([]workflowNodeExecutor, 0, len(nodes))
	for _, node := range nodes {
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" {
			continue
		}
		contract, ok := source.NodeEntries()[nodeID]
		if !ok {
			continue
		}
		executor := NewNode(contract, pc.SemanticSource(), newCoordinatorHandlerExecutionEngine(pc, nodeID), nil)
		if executor == nil {
			continue
		}
		out = append(out, executor)
	}
	return out
}

func (pc *PipelineCoordinator) workflowNodeInterceptPolicy(ctx context.Context, eventType string, evt events.Event) (bool, bool, error) {
	eventType = strings.TrimSpace(eventType)
	source := pc.SemanticSource()
	for _, node := range pc.WorkflowNodes() {
		if !pc.workflowNodeDeliveryRouteMatches(ctx, strings.TrimSpace(node.ID), evt.TargetRoute()) {
			continue
		}
		var (
			policy WorkflowEventPolicy
			ok     bool
		)
		var err error
		policy, ok, err = workflowNodePolicyForDelivery(ctx, source, node, evt)
		if err != nil {
			applies, authorityErr := pc.workflowNodeConnectedInputFailureApplies(ctx, strings.TrimSpace(node.ID), evt)
			if authorityErr != nil {
				return false, true, authorityErr
			}
			if !applies {
				continue
			}
			return false, true, err
		}
		if !ok && isJoinLifecycleEvent(events.EventType(eventType)) {
			if resolution, refOK, resolveErr := resolveWorkflowJoinOccurrence(source, evt); resolveErr != nil {
				return false, true, resolveErr
			} else if refOK && resolution.Ref.NodeID() == strings.TrimSpace(node.ID) {
				if node.Policies != nil {
					policy, ok = workflowNodePolicyForEventType(node.Policies, resolution.Ref.HandlerEvent())
				}
			}
		}
		if ok {
			return policy.Consume, true, nil
		}
	}
	return false, false, nil
}

func (pc *PipelineCoordinator) workflowNodeConnectedInputFailureApplies(ctx context.Context, nodeID string, evt events.Event) (bool, error) {
	if _, ok := workflowNodeDeliveryRoute(ctx); ok {
		return true, nil
	}
	return false, nil
}

func (pc *PipelineCoordinator) dispatchWorkflowNodeEvent(ctx context.Context, evt events.Event) bool {
	handled, _ := pc.dispatchWorkflowNodeEventResult(ctx, evt)
	return handled
}

func (pc *PipelineCoordinator) dispatchWorkflowNodeEventResult(ctx context.Context, evt events.Event) (bool, error) {
	return pc.dispatchWorkflowNodeEventResultWithEmissionPlan(ctx, evt, nil)
}

func (pc *PipelineCoordinator) dispatchWorkflowNodeEventResultWithEmissionPlan(ctx context.Context, evt events.Event, emissions *pipelineEmissionPlan) (bool, error) {
	eventType := strings.TrimSpace(string(evt.Type()))
	if eventType == "" {
		return false, nil
	}
	handledAny := false
	for _, node := range pc.WorkflowNodes() {
		nodeID := strings.TrimSpace(node.ID)
		if !pc.workflowNodeDeliveryRouteMatches(ctx, nodeID, evt.TargetRoute()) {
			continue
		}
		handled, err := pc.executeNodeHandlerPlanResultWithEmissionPlan(ctx, nodeID, evt, emissions)
		if err != nil {
			return handledAny || handled, err
		}
		if handled {
			handledAny = true
		}
	}
	return handledAny, nil
}

func (pc *PipelineCoordinator) workflowNodeDeliveryRouteMatches(ctx context.Context, nodeID string, eventTarget events.RouteIdentity) bool {
	nodeID = strings.TrimSpace(nodeID)
	if route, ok := workflowNodeDeliveryRoute(ctx); ok {
		if route.Recipient.ID() != nodeID {
			return false
		}
		return pc.workflowNodeMatchesDeliveryTarget(nodeID, route.Target.Route())
	}
	return false
}

func (pc *PipelineCoordinator) workflowNodeMatchesDeliveryTarget(nodeID string, target events.RouteIdentity) bool {
	target = target.Normalized()
	if target.Empty() {
		return true
	}
	if target.FlowInstance == "" && target.FlowID == "" {
		return true
	}
	source := pc.SemanticSource()
	if source == nil {
		return false
	}
	flowID := strings.TrimSpace(workflowNodeFlowID(source, nodeID))
	if flowID == "" {
		rootScope := strings.Trim(strings.TrimSpace(source.WorkflowName()), "/")
		targetPath := strings.Trim(strings.TrimSpace(target.FlowInstance), "/")
		if rootScope == "" || targetPath != rootScope {
			return false
		}
		return target.FlowID == "" || strings.Trim(strings.TrimSpace(target.FlowID), "/") == rootScope
	}
	if target.FlowID != "" {
		return target.FlowID == flowID && pc.workflowNodeDeliveryTargetFlowInstanceMatches(source, flowID, target.FlowInstance)
	}
	flowPath := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	if flowPath == "" {
		flowPath = flowID
	}
	targetPath := strings.Trim(strings.TrimSpace(target.FlowInstance), "/")
	if workflowFlowMode(source, flowID) == runtimecontracts.FlowModeSingleton {
		return targetPath == flowPath || targetPath == flowID || pc.hasMaterializedFlowInstanceRoute(source, flowID, targetPath)
	}
	return workflowNodeDeliveryTargetPathMatches(flowPath, targetPath)
}

func (pc *PipelineCoordinator) workflowNodeDeliveryTargetFlowInstanceMatches(source semanticview.Source, flowID, flowInstance string) bool {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		return true
	}
	flowPath := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	if flowPath == "" {
		flowPath = strings.Trim(strings.TrimSpace(flowID), "/")
	}
	if workflowFlowMode(source, flowID) == runtimecontracts.FlowModeSingleton {
		return flowInstance == flowPath || flowInstance == strings.Trim(strings.TrimSpace(flowID), "/") || pc.hasMaterializedFlowInstanceRoute(source, flowID, flowInstance)
	}
	return true
}

type FlowInstanceRouteOwner interface {
	HasFlowInstanceRoute(runtimeflowidentity.Route) bool
	RemoveFlowInstanceRouteContext(context.Context, runtimeflowidentity.Route) error
	RetireCommittedFlowInstanceRoute(runtimeflowidentity.Route) error
}

func (pc *PipelineCoordinator) hasMaterializedFlowInstanceRoute(source semanticview.Source, flowID, instancePath string) bool {
	owner := pc.flowRoutes
	if owner == nil {
		return false
	}
	identity := runtimeflowidentity.StoredRoute(
		runtimeflowidentity.ScopeKey(source, flowID),
		runtimeflowidentity.LogicalInstanceID(instancePath),
		instancePath,
	)
	return identity.Valid() && owner.HasFlowInstanceRoute(identity)
}

func workflowNodeDeliveryTargetPathMatches(flowPath, targetPath string) bool {
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	targetPath = strings.Trim(strings.TrimSpace(targetPath), "/")
	if flowPath == "" || targetPath == "" {
		return false
	}
	if targetPath == flowPath || strings.HasPrefix(targetPath, flowPath+"/") {
		return true
	}
	head, _, ok := strings.Cut(flowPath, "/")
	if !ok || head == "" {
		return false
	}
	collapsed, ok := strings.CutPrefix(targetPath, head+"/")
	if !ok {
		return false
	}
	return collapsed == flowPath || strings.HasPrefix(collapsed, flowPath+"/")
}

func workflowFlowMode(source semanticview.Source, flowID string) string {
	if source == nil {
		return ""
	}
	if schema, ok := source.FlowSchemaByID(strings.TrimSpace(flowID)); ok {
		return strings.TrimSpace(schema.Mode)
	}
	return ""
}

func deriveWorkflowEventDelivery(entry runtimecontracts.EventCatalogEntry) (consume bool, visible bool) {
	switch strings.TrimSpace(entry.RuntimeHandling) {
	case "consuming":
		return true, false
	case "dual_delivery":
		return false, true
	case "passthrough":
		return false, true
	case "projection", "stage_projection":
		return false, true
	}
	consumerType := normalizeConsumerType(entry.ConsumerType)
	intercepted := truthyContractFlag(entry.Intercepted)
	passthrough := truthyContractFlag(entry.Passthrough)
	if consumerType == ConsumerTypeSystemComponent && intercepted && !passthrough {
		return true, false
	}
	return false, true
}

func normalizeConsumerType(value any) ConsumerType {
	return ConsumerType(strings.TrimSpace(asString(value)))
}

func truthyContractFlag(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "conditional" || s == "projection" || s == "consuming"
	default:
		return false
	}
}
