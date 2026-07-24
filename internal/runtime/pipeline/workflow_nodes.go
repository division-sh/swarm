package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type WorkflowEventPolicy struct {
	Consume           bool
	RequireEntity     bool
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

func workflowNodePolicyForDelivery(source semanticview.Source, node WorkflowNode, evt events.Event) (WorkflowEventPolicy, bool, error) {
	eventType := strings.TrimSpace(string(evt.Type()))
	if policy, ok := workflowNodePolicyForEventType(node.Policies, eventType); ok {
		return policy, true, nil
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, strings.TrimSpace(node.ID), evt)
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
	return deriveWorkflowEventPolicy(source, resolved.HandlerEventKey, strings.TrimSpace(resolved.Handler.AdvancesTo) != ""), true, nil
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
	Matched         bool
	Failure         string
}

func workflowNodeEventHandlerResolutionForDelivery(source semanticview.Source, nodeID string, evt events.Event) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	rawEventType := eventidentity.Normalize(string(evt.Type()))
	if rawEventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	if resolved := workflowNodeConnectedInputEventHandlerResolution(source, nodeID, evt); resolved.Matched || resolved.Failure != "" {
		return resolved
	}
	if resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeID, rawEventType); resolved.Matched {
		return resolved
	}
	localizedEventType := workflowNodeConcreteInstanceLocalEventType(source, nodeID, evt)
	if localizedEventType == "" || localizedEventType == rawEventType {
		return workflowNodeEventHandlerResolution{}
	}
	if resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeID, localizedEventType); resolved.Matched {
		return resolved
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeEventHandlerResolutionForEventType(source semanticview.Source, nodeID, eventType string) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	if handler, ok := source.NodeEventHandler(nodeID, eventType); ok {
		handlerEventKey := workflowNodeMatchedHandlerEventKey(source, nodeID, eventType)
		if bundle, bundleOK := semanticview.Bundle(source); bundleOK {
			resolved := bundle.ResolveNodeEventHandler(nodeID, eventType)
			if resolved.Matched && strings.TrimSpace(resolved.AuthoredEventType) != "" {
				handlerEventKey = strings.TrimSpace(resolved.AuthoredEventType)
			}
		}
		return workflowNodeEventHandlerResolution{
			Handler:         handler,
			HandlerEventKey: handlerEventKey,
			Matched:         true,
		}
	}
	if semanticview.ImportBoundaryWildcardHandlerFallbackDenied(source, nodeID, eventType) {
		return workflowNodeEventHandlerResolution{}
	}
	if bundle, ok := semanticview.Bundle(source); ok {
		resolved := bundle.ResolveNodeEventHandler(nodeID, eventType)
		if resolved.Matched {
			return workflowNodeEventHandlerResolution{
				Handler:         resolved.Handler,
				HandlerEventKey: strings.TrimSpace(resolved.AuthoredEventType),
				Matched:         true,
			}
		}
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeMatchedHandlerEventKey(source semanticview.Source, nodeID, eventType string) string {
	eventType = eventidentity.Normalize(eventType)
	if source == nil {
		return eventType
	}
	handlers := source.NodeEventHandlers(nodeID)
	if len(handlers) == 0 {
		return eventType
	}
	for key := range handlers {
		if eventidentity.Normalize(key) == eventType {
			return strings.TrimSpace(key)
		}
	}
	for key := range handlers {
		key = strings.TrimSpace(key)
		if key != "" && runtimecontractsHandlerPatternMatches(key, eventType) {
			return key
		}
	}
	return eventType
}

func workflowNodeConnectedInputEventHandlerResolution(source semanticview.Source, nodeID string, evt events.Event) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	contractSource, ok := source.NodeContractSource(nodeID)
	if !ok {
		return workflowNodeEventHandlerResolution{}
	}
	flowID := strings.TrimSpace(contractSource.FlowID)
	plans, _ := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	candidates := map[string]workflowNodeEventHandlerResolution{}
	matchedSourcePlan := false
	for _, inputPin := range source.FlowInputEventPins(flowID) {
		resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeID, inputPin.EventType())
		for _, plan := range plans {
			if strings.TrimSpace(plan.Receiver.FlowID) != flowID || strings.TrimSpace(plan.Receiver.Pin) != strings.TrimSpace(inputPin.PinName()) {
				continue
			}
			if runtimepinrouting.ConnectSourceEndpointMatchesEvent(plan.Source, evt) {
				matchedSourcePlan = true
				if resolved.Matched {
					localEvent := eventidentity.Normalize(plan.Receiver.Event)
					if localEvent == "" {
						localEvent = eventidentity.Normalize(inputPin.EventType())
					}
					candidates[localEvent] = resolved
				}
			}
		}
	}
	if len(candidates) == 1 {
		for _, resolved := range candidates {
			return resolved
		}
	}
	if len(candidates) > 1 {
		locals := make([]string, 0, len(candidates))
		for localEvent := range candidates {
			locals = append(locals, localEvent)
		}
		sort.Strings(locals)
		return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf(
			"event %s reaches node %s through multiple connected input events: %s",
			eventidentity.Normalize(string(evt.Type())),
			strings.TrimSpace(nodeID),
			strings.Join(locals, ", "),
		)}
	}
	if matchedSourcePlan {
		return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf(
			"event %s reaches node %s through a connected input with no matching handler",
			eventidentity.Normalize(string(evt.Type())),
			strings.TrimSpace(nodeID),
		)}
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeHandlerEventKeyForExecution(source semanticview.Source, nodeID string, evt events.Event) string {
	if isJoinLifecycleEvent(evt.Type()) {
		if ref, _, ok := timeridentity.ParseJoinRef(parsePayloadMap(evt.Payload())); ok && ref.NodeID == strings.TrimSpace(nodeID) {
			return ref.HandlerEvent
		}
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, nodeID, evt)
	if resolved.Matched {
		return resolved.HandlerEventKey
	}
	return eventidentity.Normalize(string(evt.Type()))
}

func workflowNodeConcreteInstanceLocalEventType(source semanticview.Source, nodeID string, evt events.Event) string {
	if source == nil {
		return ""
	}
	rawEventType := eventidentity.Normalize(string(evt.Type()))
	if rawEventType == "" || !strings.Contains(rawEventType, "/") {
		return ""
	}
	flowID := workflowNodeFlowID(source, nodeID)
	if flowID == "" {
		return ""
	}
	staticFlowPath := eventidentity.Normalize(source.FlowPath(flowID))
	if staticFlowPath == "" {
		staticFlowPath = flowID
	}
	handlerKeys := workflowNodeHandlerEventKeys(source, nodeID)
	if len(handlerKeys) == 0 {
		return ""
	}
	if localized := workflowNodeTargetRouteLocalEventType(flowID, staticFlowPath, handlerKeys, rawEventType, evt.TargetRoute()); localized != "" {
		return localized
	}
	if !eventTypeBelongsToNodeStaticFlow(rawEventType, staticFlowPath) {
		return ""
	}
	for _, flowInstance := range workflowNodeConcreteReceiverScopes(evt) {
		if flowInstance == "" || !strings.HasPrefix(flowInstance, staticFlowPath+"/") {
			continue
		}
		if !strings.HasPrefix(rawEventType, flowInstance+"/") {
			continue
		}
		localized := eventidentity.LocalizeForFlow(flowInstance, handlerKeys, rawEventType)
		if workflowNodeHasHandlerEventKey(handlerKeys, localized) {
			return localized
		}
	}
	remainder := strings.TrimPrefix(rawEventType, staticFlowPath+"/")
	if !strings.Contains(remainder, "/") {
		return ""
	}
	for _, key := range handlerKeys {
		key = eventidentity.Normalize(key)
		if key != "" && strings.HasSuffix(rawEventType, "/"+key) {
			return key
		}
	}
	return ""
}

func workflowNodeTargetRouteLocalEventType(flowID, staticFlowPath string, handlerKeys []string, rawEventType string, target events.RouteIdentity) string {
	target = target.Normalized()
	flowID = eventidentity.Normalize(flowID)
	staticFlowPath = eventidentity.Normalize(staticFlowPath)
	rawEventType = eventidentity.Normalize(rawEventType)
	if flowID == "" || staticFlowPath == "" || rawEventType == "" || len(handlerKeys) == 0 {
		return ""
	}
	targetFlowID := eventidentity.Normalize(target.FlowID)
	targetFlowInstance := eventidentity.Normalize(target.FlowInstance)
	if targetFlowID != flowID && (targetFlowInstance == "" || !strings.HasPrefix(targetFlowInstance, staticFlowPath+"/")) {
		return ""
	}
	for _, key := range handlerKeys {
		key = eventidentity.Normalize(key)
		if key != "" && strings.HasSuffix(rawEventType, "/"+key) {
			return key
		}
	}
	return ""
}

func eventTypeBelongsToNodeStaticFlow(eventType, staticFlowPath string) bool {
	eventType = eventidentity.Normalize(eventType)
	staticFlowPath = eventidentity.Normalize(staticFlowPath)
	if eventType == "" || staticFlowPath == "" {
		return false
	}
	return eventType == staticFlowPath || strings.HasPrefix(eventType, staticFlowPath+"/")
}

func workflowNodeConcreteReceiverScopes(evt events.Event) []string {
	out := make([]string, 0, 2)
	appendScope := func(value string) {
		value = eventidentity.Normalize(value)
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
	appendScope(evt.TargetRoute().FlowInstance)
	appendScope(evt.FlowInstance())
	return out
}

func workflowNodeHandlerEventKeys(source semanticview.Source, nodeID string) []string {
	if source == nil {
		return nil
	}
	handlers := source.NodeEventHandlers(nodeID)
	if len(handlers) == 0 {
		return nil
	}
	out := make([]string, 0, len(handlers))
	for key := range handlers {
		key = eventidentity.Normalize(key)
		if key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func workflowNodeHasHandlerEventKey(keys []string, eventType string) bool {
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	for _, key := range keys {
		if key == eventType {
			return true
		}
	}
	return false
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
			for _, resolved := range workflowNodeSubscriptionAliases(source, nodeID, evt) {
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

func workflowNodeSubscriptionAliases(source semanticview.Source, nodeID, eventType string) []string {
	nodeID = strings.TrimSpace(nodeID)
	eventType = eventidentity.Normalize(eventType)
	if nodeID == "" || eventType == "" || source == nil {
		if eventType == "" {
			return nil
		}
		return []string{eventType}
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
	contractSource, ok := source.NodeContractSource(nodeID)
	if !ok {
		appendAlias(source.ResolveNodeEventReference(nodeID, eventType))
		return out
	}
	flowID := strings.TrimSpace(contractSource.FlowID)
	if strings.Contains(eventType, "*") {
		if resolution := semanticview.ResolveImportBoundaryWildcardSubscriptionForNode(source, nodeID, eventType); resolution.Scoped {
			for _, pattern := range resolution.Patterns {
				appendAlias(pattern.EventPattern)
			}
			return out
		}
	}
	if source.FlowHasInputEvent(flowID, eventType) {
		for _, pattern := range source.FlowInputProducerPatterns(flowID, eventType) {
			appendAlias(pattern)
		}
	} else {
		appendAlias(source.ResolveNodeEventReference(nodeID, eventType))
		return out
	}
	appendAlias(source.ResolveNodeEventReference(nodeID, eventType))
	appendAlias(eventType)
	return out
}

func workflowFlowInputProducerAliases(source semanticview.Source, targetFlowID, eventType string) []string {
	if source == nil {
		return nil
	}
	return append([]string{}, source.ResolveFlowInputAutoWire(targetFlowID, eventType).Patterns...)
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
	transitionTriggers := workflowNodeTransitionTriggers(source, nodeID)
	policies := make(map[string]WorkflowEventPolicy, len(allowed))
	for eventType := range allowed {
		if _, ok := subscribed[eventType]; !ok {
			continue
		}
		policy := deriveWorkflowEventPolicy(source, eventType, transitionTriggers[eventType])
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

func workflowNodeTransitionTriggers(source semanticview.Source, nodeID string) map[string]bool {
	out := make(map[string]bool)
	if source == nil {
		return out
	}
	nodeID = strings.TrimSpace(nodeID)
	for _, transition := range source.WorkflowTransitions() {
		if strings.TrimSpace(transition.Node) != nodeID {
			continue
		}
		trigger := strings.TrimSpace(transition.Trigger)
		if trigger != "" {
			out[trigger] = true
		}
	}
	for _, transition := range source.DerivedHandlerTransitions() {
		if strings.TrimSpace(transition.NodeID) != nodeID {
			continue
		}
		if strings.TrimSpace(transition.AdvancesTo) == "" {
			continue
		}
		trigger := strings.TrimSpace(transition.EventType)
		if trigger != "" {
			out[trigger] = true
		}
	}
	return out
}

func deriveWorkflowEventPolicy(source semanticview.Source, eventType string, drivesTransition bool) WorkflowEventPolicy {
	eventType = strings.TrimSpace(eventType)
	entry, ok := source.EventEntry(eventType)
	if !ok {
		return WorkflowEventPolicy{RequireEntity: drivesTransition}
	}
	requireEntity := drivesTransition
	consume, visible := deriveWorkflowEventDelivery(entry)
	return WorkflowEventPolicy{
		Consume:           consume,
		RequireEntity:     requireEntity,
		VisibleDownstream: visible,
	}
}

func (pc *PipelineCoordinator) BackgroundNodes(_ any, _ *sql.DB) []BackgroundNode {
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
		policy, ok, err = workflowNodePolicyForDelivery(source, node, evt)
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
			if ref, _, refOK := timeridentity.ParseJoinRef(parsePayloadMap(evt.Payload())); refOK && ref.NodeID == strings.TrimSpace(node.ID) {
				if node.Policies != nil {
					policy, ok = workflowNodePolicyForEventType(node.Policies, ref.HandlerEvent)
				}
			}
		}
		if ok {
			if policy.RequireEntity && workflowEventEntityID(evt) == "" {
				continue
			}
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
		handled, err := pc.executeNodeHandlerPlanResult(ctx, nodeID, evt)
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
		if strings.TrimSpace(route.SubscriberID) != nodeID {
			return false
		}
		return pc.workflowNodeMatchesDeliveryTarget(nodeID, route.Target)
	}
	return pc.workflowNodeMatchesDeliveryTarget(nodeID, eventTarget)
}

func (pc *PipelineCoordinator) convergeWorkflowNodeNormalRunCompletion(ctx context.Context, nodeID string, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID())
	if eventID == "" {
		return
	}
	converger, ok := pc.bus.(systemNodeDeliveryRunCompletionConverger)
	if !ok || converger == nil {
		return
	}
	if err := converger.ConvergeDeliveryRunCompletion(ctx, evt); err != nil {
		if logger, ok := pc.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Converging normal run completion after workflow node receipt failed",
				Component: nodeID,
				Action:    "delivery_run_completion_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Failure:   pipelineDependencyFailure(err, "delivery_run_completion_failed", nodeID, "converge_delivery_run_completion"),
			})
		}
	}
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
		return false
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

type flowInstanceRouteOwner interface {
	HasFlowInstanceRoute(runtimeflowidentity.Route) bool
}

func (pc *PipelineCoordinator) hasMaterializedFlowInstanceRoute(source semanticview.Source, flowID, instancePath string) bool {
	owner, ok := pc.bus.(flowInstanceRouteOwner)
	if !ok || owner == nil {
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
