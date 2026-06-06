package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
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

func workflowSubscriptions(nodes []WorkflowNode) []events.EventType {
	seen := make(map[events.EventType]struct{})
	out := make([]events.EventType, 0, 32)
	for _, node := range nodes {
		for _, evt := range node.Subscriptions {
			if _, ok := seen[evt]; ok {
				continue
			}
			seen[evt] = struct{}{}
			out = append(out, evt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return strings.Compare(string(out[i]), string(out[j])) < 0 })
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

func workflowNodePolicyForDelivery(source semanticview.Source, node WorkflowNode, evt events.Event) (WorkflowEventPolicy, bool) {
	eventType := strings.TrimSpace(string(evt.Type()))
	if policy, ok := workflowNodePolicyForEventType(node.Policies, eventType); ok {
		return policy, true
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, strings.TrimSpace(node.ID), evt)
	if !resolved.Matched {
		return WorkflowEventPolicy{}, false
	}
	candidates := []string{resolved.HandlerEventKey}
	if source != nil {
		candidates = append(candidates, workflowNodeExternalEventType(source, strings.TrimSpace(node.ID), resolved.HandlerEventKey))
	}
	for _, candidate := range candidates {
		if policy, ok := workflowNodePolicyForEventType(node.Policies, candidate); ok {
			return policy, true
		}
	}
	return WorkflowEventPolicy{}, false
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
}

func workflowNodeEventHandlerResolutionForDelivery(source semanticview.Source, nodeID string, evt events.Event) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	rawEventType := eventidentity.Normalize(string(evt.Type()))
	if rawEventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	if resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeID, rawEventType); resolved.Matched {
		return resolved
	}
	localizedEventType := workflowNodeConcreteInstanceLocalEventType(source, nodeID, evt)
	if localizedEventType == "" || localizedEventType == rawEventType {
		return workflowNodeEventHandlerResolution{}
	}
	return workflowNodeEventHandlerResolutionForEventType(source, nodeID, localizedEventType)
}

func workflowNodeEventHandlerResolutionForEventType(source semanticview.Source, nodeID, eventType string) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	if bundle, ok := semanticview.Bundle(source); ok {
		resolved := bundle.ResolveNodeEventHandler(nodeID, eventType)
		if resolved.Matched {
			handler, ok := source.NodeEventHandler(nodeID, eventType)
			if !ok {
				handler = resolved.Handler
			}
			return workflowNodeEventHandlerResolution{
				Handler:         handler,
				HandlerEventKey: strings.TrimSpace(resolved.AuthoredEventType),
				Matched:         true,
			}
		}
	}
	handler, ok := source.NodeEventHandler(nodeID, eventType)
	if !ok {
		return workflowNodeEventHandlerResolution{}
	}
	return workflowNodeEventHandlerResolution{
		Handler:         handler,
		HandlerEventKey: workflowNodeMatchedHandlerEventKey(source, nodeID, eventType),
		Matched:         true,
	}
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

func workflowNodeHandlerEventKeyForExecution(source semanticview.Source, nodeID string, evt events.Event) string {
	if isAccumulationTimeoutEvent(evt.Type()) {
		if bucket, ok := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload())); ok && strings.TrimSpace(bucket.NodeID) == strings.TrimSpace(nodeID) {
			return bucket.EventType
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
	if !eventTypeBelongsToNodeStaticFlow(rawEventType, staticFlowPath) {
		return ""
	}
	handlerKeys := workflowNodeHandlerEventKeys(source, nodeID)
	if len(handlerKeys) == 0 {
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
		produces := make([]events.EventType, 0, len(entry.Produces))
		for _, evt := range entry.Produces {
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
			ExecutionType:    strings.TrimSpace(entry.ExecutionType),
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
	appendAlias(source.ResolveNodeEventReference(nodeID, eventType))
	contractSource, ok := source.NodeContractSource(nodeID)
	if !ok {
		return out
	}
	flowID := strings.TrimSpace(contractSource.FlowID)
	if flowID == "" {
		return out
	}
	if !source.FlowHasInputEvent(flowID, eventType) {
		return out
	}
	for _, pattern := range source.FlowInputProducerPatterns(flowID, eventType) {
		appendAlias(pattern)
	}
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
		if len(node.SubscribesTo) == 0 && len(node.OwnedTransitions) == 0 && len(node.Timers) == 0 {
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

func (pc *PipelineCoordinator) BackgroundNodes(bus systemNodeBus, db *sql.DB) []BackgroundNode {
	return pc.BackgroundNodesWithReceiptStore(bus, db, pc.workflowStore)
}

func (pc *PipelineCoordinator) BackgroundNodesWithReceiptStore(bus systemNodeBus, db *sql.DB, receiptStore SystemNodeReceiptPersistence) []BackgroundNode {
	if pc == nil || bus == nil {
		return nil
	}
	retryBase := workflowHandlerRetryBase(pc.SemanticSource())
	out := make([]BackgroundNode, 0, 1)
	for _, node := range pc.WorkflowNodes() {
		if strings.TrimSpace(node.ExecutionType) != runtimecontracts.SystemNodeExecutionType {
			continue
		}
		if executor := pc.backgroundWorkflowExecutor(strings.TrimSpace(node.ID)); executor != nil {
			if bg := newBackgroundWorkflowNodeWithReceiptStoreAndRetryBase(executor, bus, db, receiptStore, pc.eventReceiptsCapability, retryBase); bg != nil {
				bg.SetTestLifecycleProbe(pc.testLifecycleProbe)
				out = append(out, bg)
			}
		}
	}
	return out
}

func workflowHandlerRetryBase(source semanticview.Source) time.Duration {
	if source == nil {
		return time.Second
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "handler_retry_base_seconds")
	if !ok {
		return time.Second
	}
	switch typed := value.Value.(type) {
	case int:
		if typed > 0 {
			return time.Duration(typed) * time.Second
		}
	case int64:
		if typed > 0 {
			return time.Duration(typed) * time.Second
		}
	case float64:
		if typed > 0 {
			return time.Duration(typed * float64(time.Second))
		}
	default:
		if secs := strings.TrimSpace(asString(typed)); secs != "" {
			if parsed, err := time.ParseDuration(secs + "s"); err == nil && parsed > 0 {
				return parsed
			}
		}
	}
	return time.Second
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

func (pc *PipelineCoordinator) workflowNodeInterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	source := pc.SemanticSource()
	for _, node := range pc.WorkflowNodes() {
		if !pc.workflowNodeMatchesDeliveryTarget(strings.TrimSpace(node.ID), evt.TargetRoute()) {
			continue
		}
		var (
			policy WorkflowEventPolicy
			ok     bool
		)
		policy, ok = workflowNodePolicyForDelivery(source, node, evt)
		if !ok && isAccumulationTimeoutEvent(events.EventType(eventType)) {
			if bucket, bucketOK := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload())); bucketOK && bucket.NodeID == strings.TrimSpace(node.ID) {
				if node.Policies != nil {
					policy, ok = workflowNodePolicyForEventType(node.Policies, bucket.EventType)
				}
			}
		}
		if ok {
			if policy.RequireEntity && workflowEventEntityID(evt) == "" {
				continue
			}
			return policy.Consume, true
		}
	}
	return false, false
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
		if !pc.workflowNodeMatchesDeliveryTarget(nodeID, evt.TargetRoute()) {
			continue
		}
		handled, err := pc.executeNodeHandlerPlanResult(ctx, nodeID, evt)
		if err != nil {
			return handledAny || handled, err
		}
		if handled {
			pc.markWorkflowNodeProcessed(ctx, nodeID, evt)
			handledAny = true
		}
	}
	return handledAny, nil
}

func (pc *PipelineCoordinator) markWorkflowNodeProcessed(ctx context.Context, nodeID string, evt events.Event) {
	if pc == nil || pc.workflowStore == nil {
		return
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID := strings.TrimSpace(evt.ID())
	if nodeID == "" || eventID == "" {
		return
	}
	if !pc.eventReceiptsAvailable(ctx) {
		return
	}
	sideEffects := systemNodeProcessedReceiptSideEffects(nodeID, eventID)
	if err := pc.workflowStore.MarkSystemNodeProcessedAndSettleDelivery(ctx, nodeID, eventID, sideEffects); err != nil {
		if logger, ok := pc.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Marking the workflow node event as processed failed",
				Component: nodeID,
				Action:    "mark_processed_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
		return
	}
	pc.convergeWorkflowNodeNormalRunCompletion(ctx, nodeID, evt)
	pc.notifyTestLifecycleDeliveryStatus(ctx, nodeID, evt, "delivered")
}

func (pc *PipelineCoordinator) markWorkflowNodeDeliveryInProgress(ctx context.Context, nodeID string, evt events.Event) bool {
	if pc == nil || pc.workflowStore == nil {
		return false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID := strings.TrimSpace(evt.ID())
	if nodeID == "" || eventID == "" {
		return false
	}
	if !pc.eventReceiptsAvailable(ctx) {
		return false
	}
	if err := pc.workflowStore.MarkSystemNodeDeliveryInProgress(ctx, nodeID, eventID, DefaultSystemNodeRetryLimit); err != nil {
		pc.logWorkflowNodeDeliveryTransitionError(ctx, nodeID, evt, "mark_delivery_in_progress_failed", "Marking the workflow node delivery in progress failed", err)
		return false
	}
	pc.notifyTestLifecycleDeliveryStatus(ctx, nodeID, evt, "in_progress")
	return true
}

func (pc *PipelineCoordinator) markWorkflowNodeDeliveryDeadLetter(ctx context.Context, nodeID string, evt events.Event, reasonCode string, cause error, retryCount int) {
	if pc == nil || pc.workflowStore == nil {
		return
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID := strings.TrimSpace(evt.ID())
	if nodeID == "" || eventID == "" {
		return
	}
	if !pc.eventReceiptsAvailable(ctx) {
		return
	}
	errText := ""
	if cause != nil {
		errText = strings.TrimSpace(cause.Error())
	}
	sideEffects := systemNodeDeadLetterReceiptSideEffects(nodeID, eventID, reasonCode, errText, retryCount)
	if err := pc.workflowStore.MarkSystemNodeDeliveryDeadLetter(ctx, nodeID, eventID, reasonCode, errText, retryCount, sideEffects); err != nil {
		pc.logWorkflowNodeDeliveryTransitionError(ctx, nodeID, evt, "mark_delivery_dead_letter_failed", "Marking the workflow node delivery as dead_letter failed", err)
		return
	}
	pc.convergeWorkflowNodeNormalRunCompletion(ctx, nodeID, evt)
	pc.notifyTestLifecycleDeliveryStatus(ctx, nodeID, evt, "dead_letter")
}

func (pc *PipelineCoordinator) logWorkflowNodeDeliveryTransitionError(ctx context.Context, nodeID string, evt events.Event, action, message string, err error) {
	if pc == nil || err == nil {
		return
	}
	if logger, ok := pc.bus.(systemNodeRuntimeLogger); ok && logger != nil {
		logger.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   message,
			Component: nodeID,
			Action:    action,
			EventID:   strings.TrimSpace(evt.ID()),
			EventType: strings.TrimSpace(string(evt.Type())),
			EntityID:  workflowEventEntityID(evt),
			Error:     strings.TrimSpace(err.Error()),
		})
	}
}

func (pc *PipelineCoordinator) workflowNodeDeliveryAuthorized(ctx context.Context, nodeID string, evt events.Event) bool {
	if pc == nil {
		return false
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return false
	}
	if !pc.eventReceiptsAvailable(ctx) {
		return false
	}
	if pc.workflowStore == nil {
		return false
	}
	eventID := strings.TrimSpace(evt.ID())
	if eventID == "" {
		return false
	}
	ok, err := pc.workflowStore.SystemNodeDeliveryAuthorized(ctx, nodeID, eventID, DefaultSystemNodeRetryLimit)
	if err != nil {
		if logger, logOK := pc.bus.(systemNodeRuntimeLogger); logOK && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Checking workflow node delivery authority failed",
				Component: nodeID,
				Action:    "delivery_authority_check_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
		return false
	}
	if !ok {
		if logger, logOK := pc.bus.(systemNodeRuntimeLogger); logOK && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Workflow node delivery authority is missing; handler execution skipped",
				Component: nodeID,
				Action:    "delivery_authority_missing",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
			})
		}
	}
	return ok
}

func (pc *PipelineCoordinator) workflowNodeEventProcessed(ctx context.Context, nodeID string, evt events.Event) bool {
	if pc == nil || pc.workflowStore == nil {
		return false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID := strings.TrimSpace(evt.ID())
	if nodeID == "" || eventID == "" {
		return false
	}
	if !pc.eventReceiptsAvailable(ctx) {
		return false
	}
	ok, err := pc.workflowStore.SystemNodeProcessed(ctx, nodeID, eventID)
	return err == nil && ok
}

func (pc *PipelineCoordinator) eventReceiptsAvailable(ctx context.Context) bool {
	if pc == nil || pc.eventReceiptsCapability == nil {
		return false
	}
	ok, err := pc.eventReceiptsCapability(ctx)
	return err == nil && ok
}

func (pc *PipelineCoordinator) convergeWorkflowNodeNormalRunCompletion(ctx context.Context, nodeID string, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID())
	if eventID == "" {
		return
	}
	converger, ok := pc.bus.(systemNodeNormalRunCompletionConverger)
	if !ok || converger == nil {
		return
	}
	if err := converger.ConvergeNormalRunCompletionForEvent(ctx, eventID); err != nil {
		if logger, ok := pc.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Converging normal run completion after workflow node receipt failed",
				Component: nodeID,
				Action:    "normal_run_completion_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Error:     strings.TrimSpace(err.Error()),
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
		return target.FlowID == flowID
	}
	flowPath := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	if flowPath == "" {
		flowPath = flowID
	}
	targetPath := strings.Trim(strings.TrimSpace(target.FlowInstance), "/")
	return targetPath == flowPath || strings.HasPrefix(targetPath, flowPath+"/")
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
