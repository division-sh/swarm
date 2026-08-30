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
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
	Node             runtimeidentity.ExecutableNode
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

func workflowNodePolicyForDelivery(ctx context.Context, source semanticview.Source, node WorkflowNode, evt events.Event) (WorkflowEventPolicy, bool, error) {
	eventType := strings.TrimSpace(string(evt.Type()))
	if policy, ok := workflowNodePolicyForEventType(node.Policies, eventType); ok {
		return policy, true, nil
	}
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, node.Node, evt)
	if resolved.Failure != "" {
		return WorkflowEventPolicy{}, false, fmt.Errorf("resolve workflow handler for node %s: %s", node.Node.Key(), resolved.Failure)
	}
	if !resolved.Matched {
		return WorkflowEventPolicy{}, false, nil
	}
	candidates := []string{resolved.HandlerEventKey}
	if source != nil {
		candidates = append(candidates, workflowNodeExternalEventType(source, node.Node, resolved.HandlerEventKey))
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

type workflowNodeEventHandlerResolution struct {
	Handler         runtimecontracts.SystemNodeEventHandler
	HandlerEventKey string
	FlowID          string
	Matched         bool
	Failure         string
}

func workflowNodeEventHandlerResolutionForDelivery(source semanticview.Source, node runtimeidentity.ExecutableNode, evt events.Event) workflowNodeEventHandlerResolution {
	return workflowNodeEventHandlerResolutionForDeliveryContext(nil, source, node, evt)
}

func workflowNodeEventHandlerResolutionForDeliveryContext(ctx context.Context, source semanticview.Source, node runtimeidentity.ExecutableNode, evt events.Event) workflowNodeEventHandlerResolution {
	if source == nil || !node.Valid() {
		return workflowNodeEventHandlerResolution{}
	}
	executionFlowID := node.FlowPath()
	if executionFlowID == "" {
		executionFlowID = semanticview.RootExecutionFlowID(source)
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
		if !ok || !resolution.Ref.Node().Equal(node) {
			return workflowNodeEventHandlerResolution{}
		}
		return workflowNodeEventHandlerResolution{
			Handler: resolution.Handler, HandlerEventKey: resolution.Ref.HandlerEvent(),
			FlowID: executionFlowID, Matched: true,
		}
	}
	route, deliveryRouted := workflowNodeDeliveryRoute(ctx)
	if deliveryRouted && !route.ConnectClaim.Empty() {
		handlerNode, handlerEvent, authorized := route.ConnectClaim.NodeHandlerOwner()
		if !authorized || !handlerNode.Equal(node) {
			return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf("connect delivery claim does not authorize node %s", node.Key())}
		}
		handlerResolution := semanticview.ResolveExecutableNodeSubscriptionHandler(source, handlerNode, string(handlerEvent))
		if !handlerResolution.Matched {
			return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf("connect delivery claim authorizes event %s but declaration node %s has no matching handler", handlerEvent, handlerNode.Key())}
		}
		return workflowNodeEventHandlerResolution{
			Handler:         handlerResolution.Handler,
			HandlerEventKey: handlerResolution.HandlerEventKey,
			FlowID:          executionFlowID,
			Matched:         true,
		}
	}
	if deliveryRouted {
		// Direct typed-pubsub delivery remains owned by the semantic source. Only
		// compiled-connect delivery requires the route-scoped connect claim above.
		if resolved := workflowNodeEventHandlerResolutionForEventType(source, node, rawEventType); resolved.Matched {
			return resolved
		}
		if resolved := workflowNodeDirectDeliveryHandlerResolution(source, node, rawEventType, route.Target.Route()); resolved.Matched {
			return resolved
		}
		return workflowNodeEventHandlerResolution{Failure: fmt.Sprintf("node delivery event %s has no semantic handler or stamped connect claim", rawEventType)}
	}
	if resolved := workflowNodeEventHandlerResolutionForEventType(source, node, rawEventType); resolved.Matched {
		return resolved
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeDirectDeliveryHandlerResolution(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, target events.RouteIdentity) workflowNodeEventHandlerResolution {
	target = target.Normalized()
	if target.FlowID == "" {
		return workflowNodeEventHandlerResolution{}
	}
	executionFlowID := node.FlowPath()
	if executionFlowID == "" {
		executionFlowID = semanticview.RootExecutionFlowID(source)
	}
	eventType = eventidentity.Normalize(eventType)
	if target.FlowID != executionFlowID {
		return workflowNodeEventHandlerResolution{}
	}
	if resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, eventType); resolved.Matched {
		return workflowNodeEventHandlerResolution{
			Handler:         resolved.Handler,
			HandlerEventKey: resolved.HandlerEventKey,
			FlowID:          executionFlowID,
			Matched:         true,
		}
	}
	prefix := eventidentity.Normalize(target.FlowInstance) + "/"
	if prefix == "/" || !strings.HasPrefix(eventType, prefix) {
		return workflowNodeEventHandlerResolution{}
	}
	resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, strings.TrimPrefix(eventType, prefix))
	if !resolved.Matched {
		return workflowNodeEventHandlerResolution{}
	}
	return workflowNodeEventHandlerResolution{
		Handler:         resolved.Handler,
		HandlerEventKey: resolved.HandlerEventKey,
		FlowID:          executionFlowID,
		Matched:         true,
	}
}

func workflowNodeEventHandlerResolutionForEventType(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) workflowNodeEventHandlerResolution {
	if source == nil {
		return workflowNodeEventHandlerResolution{}
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return workflowNodeEventHandlerResolution{}
	}
	resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, eventType)
	if resolved.Matched {
		executionFlowID := node.FlowPath()
		if executionFlowID == "" {
			executionFlowID = semanticview.RootExecutionFlowID(source)
		}
		return workflowNodeEventHandlerResolution{
			Handler:         resolved.Handler,
			HandlerEventKey: resolved.HandlerEventKey,
			FlowID:          executionFlowID,
			Matched:         true,
		}
	}
	return workflowNodeEventHandlerResolution{}
}

func workflowNodeHandlerEventKeyForExecution(ctx context.Context, source semanticview.Source, node runtimeidentity.ExecutableNode, evt events.Event) string {
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, node, evt)
	if resolved.Matched {
		return resolved.HandlerEventKey
	}
	return eventidentity.Normalize(string(evt.Type()))
}

func LoadWorkflowNodes(source semanticview.Source) ([]WorkflowNode, error) {
	if source == nil {
		return nil, ErrContractBundleNil
	}
	records := source.ExecutableNodeRecords()
	out := make([]WorkflowNode, 0, len(records))
	for _, record := range records {
		node, err := record.Identity()
		if err != nil {
			return nil, fmt.Errorf("admit executable node %q: %w", record.LogicalID, err)
		}
		entry := record.Entry
		runtimeSubscriptions := source.ExecutableNodeRuntimeSubscriptions(node)
		subscriptions := make([]events.EventType, 0, len(runtimeSubscriptions))
		for _, evt := range runtimeSubscriptions {
			aliases, err := workflowNodeSubscriptionAliases(source, node, evt)
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
		effectiveProduces := semanticview.ExecutableNodeEffectiveProduces(source, node)
		produces := make([]events.EventType, 0, len(effectiveProduces))
		for _, evt := range effectiveProduces {
			evt = workflowNodeExternalEventType(source, node, evt)
			if evt == "" {
				continue
			}
			produces = append(produces, events.EventType(evt))
		}
		out = append(out, WorkflowNode{
			Node:             node,
			Subscriptions:    subscriptions,
			Produces:         produces,
			OwnedTransitions: append([]string{}, entry.OwnedTransitions...),
			Timers:           workflowNodeTimerIDs(entry.Timers),
			ExecutionType:    runtimecontracts.EffectiveSystemNodeExecutionType(entry),
			Implementation:   strings.TrimSpace(entry.Implementation),
			StateTable:       strings.TrimSpace(entry.StateTable),
			IdempotencyTable: strings.TrimSpace(entry.IdempotencyTable),
			Policies:         buildWorkflowNodePolicies(source, node, subscriptions),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node.Key() < out[j].Node.Key() })
	return out, nil
}

func workflowNodeSubscriptionAliases(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) ([]string, error) {
	eventType = eventidentity.Normalize(eventType)
	if !node.Valid() || eventType == "" || source == nil {
		if eventType == "" {
			return nil, nil
		}
		return []string{eventType}, nil
	}
	admission := semanticview.ClassifyExecutableNodeSubscription(source, node, eventType)
	if !admission.Admitted() {
		return nil, fmt.Errorf("workflow node %s: %s", node.Key(), admission.Message())
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
	flowID := node.FlowPath()
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

func buildWorkflowNodePolicies(source semanticview.Source, node runtimeidentity.ExecutableNode, subscriptions []events.EventType) map[string]WorkflowEventPolicy {
	allowed := workflowNodeRuntimePolicyEvents(source, node, subscriptions)
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

func workflowNodeRuntimePolicyEvents(source semanticview.Source, node runtimeidentity.ExecutableNode, subscriptions []events.EventType) map[string]struct{} {
	if !node.Valid() || source == nil {
		return nil
	}
	out := make(map[string]struct{}, len(subscriptions)+8)
	for _, evt := range subscriptions {
		name := strings.TrimSpace(string(evt))
		if name != "" {
			out[name] = struct{}{}
		}
	}
	for eventType := range source.ExecutableNodeEventHandlers(node) {
		eventType = workflowNodeExternalEventType(source, node, eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	flowID := node.FlowPath()
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
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowNodeExternalEventType(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) string {
	if source == nil {
		return eventidentity.Normalize(eventType)
	}
	return source.ResolveExecutableNodeEventReference(node, eventType)
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
		record, ok := source.ExecutableNode(node.Node)
		if !ok {
			continue
		}
		executor := NewNode(node.Node, record.Entry, pc.SemanticSource(), newCoordinatorHandlerExecutionEngine(pc, node.Node), nil)
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
	deliveryRoute, deliveryRouted := workflowNodeDeliveryRoute(ctx)
	deliveryNode, exactNodeRoute := deliveryRoute.Recipient.Node()
	nodeFound := false
	targetMatched := false
	for _, node := range pc.WorkflowNodes() {
		if exactNodeRoute && node.Node.Equal(deliveryNode) {
			nodeFound = true
			if pc.workflowNodeMatchesDeliveryTarget(node.Node, deliveryRoute.Target.Route()) {
				targetMatched = true
			}
		}
		if !pc.workflowNodeDeliveryRouteMatches(ctx, node.Node, evt.TargetRoute()) {
			continue
		}
		var (
			policy WorkflowEventPolicy
			ok     bool
		)
		var err error
		policy, ok, err = workflowNodePolicyForDelivery(ctx, source, node, evt)
		if err != nil {
			applies, authorityErr := pc.workflowNodeConnectedInputFailureApplies(ctx, node.Node, evt)
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
			} else if refOK && resolution.Ref.Node().Equal(node.Node) {
				if node.Policies != nil {
					policy, ok = workflowNodePolicyForEventType(node.Policies, resolution.Ref.HandlerEvent())
				}
			}
		}
		if ok {
			return policy.Consume, true, nil
		}
	}
	if deliveryRouted && exactNodeRoute {
		claimNode, claimEvent, claimed := deliveryRoute.ConnectClaim.NodeHandlerOwner()
		if !nodeFound {
			return false, true, fmt.Errorf("exact workflow node delivery recipient %s is not present in the runtime node census", deliveryNode.Key())
		}
		if !targetMatched {
			return false, true, fmt.Errorf("exact workflow node delivery recipient %s does not own target %#v", deliveryNode.Key(), deliveryRoute.Target.Route())
		}
		return false, true, fmt.Errorf("exact workflow node delivery recipient %s has no policy for event %s (connect_claim=%t handler=%s event=%s)", deliveryNode.Key(), eventType, claimed, claimNode.Key(), claimEvent)
	}
	return false, false, nil
}

func (pc *PipelineCoordinator) workflowNodeConnectedInputFailureApplies(ctx context.Context, _ runtimeidentity.ExecutableNode, evt events.Event) (bool, error) {
	if _, ok := workflowNodeDeliveryRoute(ctx); ok {
		return true, nil
	}
	return false, nil
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
		if !pc.workflowNodeDeliveryRouteMatches(ctx, node.Node, evt.TargetRoute()) {
			continue
		}
		handled, err := pc.executeNodeHandlerPlanResultWithEmissionPlan(ctx, node.Node, evt, emissions)
		if err != nil {
			return handledAny || handled, err
		}
		if handled {
			handledAny = true
		}
	}
	return handledAny, nil
}

func (pc *PipelineCoordinator) workflowNodeDeliveryRouteMatches(ctx context.Context, node runtimeidentity.ExecutableNode, eventTarget events.RouteIdentity) bool {
	if route, ok := workflowNodeDeliveryRoute(ctx); ok {
		recipient, exact := route.Recipient.Node()
		if !exact || !recipient.Equal(node) {
			return false
		}
		return pc.workflowNodeMatchesDeliveryTarget(node, route.Target.Route())
	}
	return false
}

func (pc *PipelineCoordinator) workflowNodeMatchesDeliveryTarget(node runtimeidentity.ExecutableNode, target events.RouteIdentity) bool {
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
	flowID := node.FlowPath()
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(source)
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

func workflowPersistedFlowMode(source semanticview.Source, flowID string) string {
	switch workflowFlowMode(source, flowID) {
	case runtimecontracts.FlowModeTemplate:
		return runtimecontracts.FlowModeTemplate
	case "", runtimecontracts.FlowModeStatic, runtimecontracts.FlowModeSingleton:
		return runtimecontracts.FlowModeStatic
	default:
		// Contract admission owns the authored mode vocabulary. Persistence stays fail-closed
		// to its static/template representation if an invalid source reaches this projection.
		return ""
	}
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
