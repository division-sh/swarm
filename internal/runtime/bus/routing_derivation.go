package bus

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Subscriber struct {
	Recipient      events.DeliveryRecipient
	Path           string
	MatchPattern   string
	routeSource    subscriberRouteSource
	LocalizedEvent string
	AgentPlan      agentidentity.Plan
	agentLifecycle agentLifecycleAdmission
	handlerNode    runtimeidentity.ExecutableNode
	connectHandler runtimepinrouting.ConnectReceiverHandler
	targetHandler  runtimepipeline.DeliveryTargetHandler
	subscription   semanticview.AuthoredSubscriptionAdmission
}

func (s Subscriber) RouteSourceCode() string { return s.routeSource.code() }

// HandlerForEvent exposes the same admitted handler projection used by live
// route planning. Callers must not localize the event independently.
func (s Subscriber) HandlerForEvent(eventType events.EventType) runtimepipeline.DeliveryTargetHandler {
	return routedSubscriberTargetHandler(s, eventType)
}

func independentPubsubSubscriber(s Subscriber) bool {
	return s.routeSource != subscriberRouteSourceConnectRoutePlan
}

type subscriberRouteSource uint8

const (
	subscriberRouteSourceSubscription subscriberRouteSource = iota + 1
	subscriberRouteSourcePinAutoWire
	subscriberRouteSourceConnectRoutePlan
)

func subscriberRouteSourceFromCode(code string) (subscriberRouteSource, bool) {
	switch strings.TrimSpace(code) {
	case "subscription":
		return subscriberRouteSourceSubscription, true
	case "pin_auto_wire":
		return subscriberRouteSourcePinAutoWire, true
	case "connect_route_plan":
		return subscriberRouteSourceConnectRoutePlan, true
	default:
		return 0, false
	}
}

func (s subscriberRouteSource) code() string {
	switch s {
	case subscriberRouteSourceSubscription:
		return "subscription"
	case subscriberRouteSourcePinAutoWire:
		return "pin_auto_wire"
	case subscriberRouteSourceConnectRoutePlan:
		return "connect_route_plan"
	default:
		return ""
	}
}

type RouteTable struct {
	mu                  sync.RWMutex
	source              semanticview.Source
	authoredEventPath   map[string]struct{}
	authoredScopes      map[string]struct{}
	templates           map[string]routeFlowTemplate
	connectDefinitions  map[string][]runtimepinrouting.ConnectRecipientRegistration
	staticAgentPlans    map[agentidentity.Plan]struct{}
	instanceOwners      map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.Instance
	connectGraph        runtimepinrouting.CompiledConnectGraph
	inputProducers      runtimepinrouting.FlowInputProducerResolver
	compiledSourceReady bool
}

type routeFlowTemplate struct {
	FlowID      string
	InputEvents []string
	LocalEvents map[string]struct{}
	Subscribers []routeSubscriberTemplate
}

type routeSubscriberTemplate struct {
	IDTemplate    string
	Kind          subscriberKind
	Patterns      []routeSubscriberTemplatePattern
	AgentNamePlan semanticview.AgentNamePlan
	HandlerNode   runtimeidentity.ExecutableNode
	TargetHandler runtimepipeline.DeliveryTargetHandler
}

type routeSubscriberTemplatePattern struct {
	raw        string
	admission  semanticview.AuthoredSubscriptionAdmission
	inputEvent bool
}

type subscriberKind uint8

const (
	subscriberNode subscriberKind = iota + 1
	subscriberAgent
)

func subscriberRecipient(kind subscriberKind, id string, node runtimeidentity.ExecutableNode) (events.DeliveryRecipient, error) {
	if kind == subscriberNode {
		return events.NewNodeDeliveryRecipient(node)
	}
	if kind == subscriberAgent {
		return events.NewAgentDeliveryRecipient(id)
	}
	return events.DeliveryRecipient{}, fmt.Errorf("subscriber kind is required")
}

type routeResolvedPattern struct {
	subscription   semanticview.AuthoredSubscriptionAdmission
	EventPattern   string
	MatchPattern   string
	routeSource    subscriberRouteSource
	LocalizedEvent string
	RoutePath      string
}

func DeriveRouteTable(source semanticview.Source) (*RouteTable, error) {
	graph, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	return deriveRouteTableWithInputProducers(source, graph, inputProducers)
}

func deriveRouteTableWithInputProducers(source semanticview.Source, graph runtimepinrouting.CompiledConnectGraph, inputProducers runtimepinrouting.FlowInputProducerResolver) (*RouteTable, error) {
	rt := newRouteTableWithGraph(source, graph)
	rt.inputProducers = inputProducers
	rt.compiledSourceReady = true
	if source == nil {
		return rt, nil
	}
	if _, err := semanticview.AgentNamePlans(source); err != nil {
		return nil, fmt.Errorf("compile declared agent names: %w", err)
	}
	for _, scope := range semanticview.FlowScopes(source) {
		agents, err := routeAgentDeclarationsForOwner(source, scope.ID)
		if err != nil {
			return nil, err
		}
		flowPath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		localEvents := routeFlowLocalEventSetWithInputProducers(scope, inputProducers)
		subscribers, err := routeSubscriberTemplates(source, scope, agents, localEvents)
		if err != nil {
			return nil, err
		}
		rt.templates[runtimeflowidentity.ScopeKey(source, scope.ID)] = routeFlowTemplate{
			FlowID:      scope.ID,
			InputEvents: append([]string{}, scope.InputEvents...),
			LocalEvents: cloneStringSet(localEvents),
			Subscribers: subscribers,
		}
		definitions, staticPlans, err := compileRouteConnectDefinitions(graph, scope, subscribers)
		if err != nil {
			return nil, err
		}
		if rt.connectDefinitions == nil {
			rt.connectDefinitions = make(map[string][]runtimepinrouting.ConnectRecipientRegistration)
		}
		rt.connectDefinitions[scope.ID] = definitions
		for _, plan := range staticPlans {
			rt.staticAgentPlans[plan.Normalize()] = struct{}{}
		}
		if strings.EqualFold(scope.Mode, "template") {
			continue
		}
		if flowPath != "" {
			rt.authoredScopes[flowPath] = struct{}{}
		}
		eventScope := routeEventIdentityScope(flowPath, localEvents, nil)
		for event := range localEvents {
			if absolute := eventScope.ResolveEvent(event, nil); absolute != "" && !strings.Contains(absolute, "*") {
				rt.authoredEventPath[absolute] = struct{}{}
			}
		}
	}

	return rt, nil
}

func compileRouteConnectDefinitions(graph runtimepinrouting.CompiledConnectGraph, scope semanticview.FlowScope, subscribers []routeSubscriberTemplate) ([]runtimepinrouting.ConnectRecipientRegistration, []agentidentity.Plan, error) {
	var definitions []runtimepinrouting.ConnectRecipientRegistration
	var staticPlans []agentidentity.Plan
	path := scope.Path
	if scope.ID == "." {
		path = "."
	}
	for _, subscriber := range subscribers {
		recipient, err := connectDeclarationRecipient(path, subscriber)
		if err != nil {
			return nil, nil, err
		}
		if !strings.EqualFold(scope.Mode, "template") && subscriber.Kind == subscriberAgent {
			staticPlans = append(staticPlans, recipient.AgentPlan())
		}
		for _, pattern := range subscriber.Patterns {
			eventsForPattern := []string{pattern.raw}
			if pattern.admission.Pattern() {
				eventsForPattern = nil
				for _, input := range scope.InputEvents {
					if eventidentity.MatchPattern(pattern.raw, input) {
						eventsForPattern = append(eventsForPattern, input)
					}
				}
			}
			for _, event := range eventsForPattern {
				definitions = append(definitions, graph.AdmitReceiverRecipient(scope.ID, events.EventType(event), recipient)...)
			}
		}
	}
	return definitions, staticPlans, nil
}

func connectDeclarationRecipient(path string, subscriber routeSubscriberTemplate) (runtimepinrouting.ConnectRecipient, error) {
	if subscriber.Kind == subscriberNode {
		return runtimepinrouting.NewConnectNodeRecipient(subscriber.HandlerNode, path)
	}
	name, err := subscriber.AgentNamePlan.Materialize()
	if err != nil {
		return runtimepinrouting.ConnectRecipient{}, err
	}
	route, err := runtimeflowidentity.StoredRoute("", "", path).AgentIdentityRoute()
	if err != nil {
		return runtimepinrouting.ConnectRecipient{}, err
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		return runtimepinrouting.ConnectRecipient{}, err
	}
	return runtimepinrouting.NewConnectAgentRecipient(name.AgentID, path, plan)
}

// ConnectDeclarationDefinitions is source-only descriptive data. It cannot
// establish a constructed receiver, attachment, or execution permission.
func (rt *RouteTable) ConnectDeclarationDefinitions(flowID string) []runtimepinrouting.ConnectRecipientRegistration {
	return append([]runtimepinrouting.ConnectRecipientRegistration(nil), rt.connectDefinitions[flowID]...)
}

// ConnectReceiverDefinitions binds immutable compiled definitions to data
// admitted by the caller's native or fixed-revision owner. It cannot establish
// instance existence, readiness, or execution authority.
func (rt *RouteTable) ConnectReceiverDefinitions(runID string, instance runtimeflowidentity.Instance) ([]runtimepinrouting.ConnectRecipientRegistration, error) {
	if rt == nil {
		return nil, fmt.Errorf("receiver binding requires its compiled source table")
	}
	if err := instance.ValidateConstruction(rt.source, runID); err != nil {
		return nil, err
	}
	var bound []runtimepinrouting.ConnectRecipientRegistration
	for _, definition := range rt.connectDefinitions[instance.TemplateID] {
		registration, err := rt.connectGraph.BindReceiverInstance(definition, instance)
		if err != nil {
			return nil, err
		}
		bound = append(bound, registration)
	}
	return bound, nil
}

func (rt *RouteTable) staticAgentDeclarationPlans() map[agentidentity.Plan]struct{} {
	plans := make(map[agentidentity.Plan]struct{})
	if rt == nil {
		return plans
	}
	for plan := range rt.staticAgentPlans {
		plans[plan] = struct{}{}
	}
	return plans
}

func projectSubscriberEvents(subscribers []Subscriber, eventType string) []Subscriber {
	if len(subscribers) == 0 {
		return nil
	}
	out := make([]Subscriber, 0, len(subscribers))
	for _, subscriber := range subscribers {
		if subscriber.Recipient.IsNode() && subscriber.subscription.Admitted() && subscriber.subscription.Pattern() {
			local, matched := subscriber.subscription.LocalEventAt(subscriber.Path, eventType)
			if !matched {
				continue
			}
			subscriber.LocalizedEvent = local
		}
		out = appendUniqueSubscriber(out, subscriber)
	}
	return out
}

func connectRecipientSubscribers(evaluation runtimepinrouting.ConnectRecipientEvaluation) []Subscriber {
	recipients := evaluation.Recipients()
	out := make([]Subscriber, 0, len(recipients))
	for _, recipient := range recipients {
		handlerNode := recipient.Handler().Node()
		typedRecipient, err := subscriberRecipient(subscriberNode, "", handlerNode)
		if recipient.Kind() == runtimepinrouting.ConnectRecipientAgent {
			handlerNode = runtimeidentity.ExecutableNode{}
			typedRecipient, err = subscriberRecipient(subscriberAgent, recipient.ID(), handlerNode)
		}
		if err != nil {
			continue
		}
		out = append(out, Subscriber{
			Recipient: typedRecipient, Path: recipient.Path(),
			LocalizedEvent: string(recipient.HandlerEvent()), AgentPlan: recipient.AgentPlan(),
			handlerNode:    handlerNode,
			connectHandler: recipient.Handler(),
			routeSource:    subscriberRouteSourceConnectRoutePlan,
		})
	}
	return dedupeSubscribers(out)
}

func (rt *RouteTable) AddFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	req = req.Normalized()
	identity, replay, err := rt.admitFlowInstanceRouteIdentityLocked(req)
	if err != nil || replay {
		return err
	}
	if _, ok := rt.templates[identity.Route.ScopeKey]; !ok {
		return fmt.Errorf("route template %q not found", identity.Route.ScopeKey)
	}
	if !rt.compiledSourceReady {
		return fmt.Errorf("flow-instance route materialization requires paired compiled source")
	}
	rt.instanceOwners[identity] = req.Instance
	return nil
}

func (rt *RouteTable) HasFlowInstanceRoute(identity runtimeflowidentity.RunScopedFlowInstance) bool {
	if rt == nil {
		return false
	}
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	owner, exists := rt.instanceOwners[identity]
	return exists && owner.Route() == identity.Route
}

func (rt *RouteTable) RemoveFlowInstanceRoute(identity runtimeflowidentity.RunScopedFlowInstance) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}
	return rt.removeFlowInstanceRoute(identity)
}

func (rt *RouteTable) removeFlowInstanceRoute(identity runtimeflowidentity.RunScopedFlowInstance) error {
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	owner, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	delete(rt.instanceOwners, owner)
	return nil
}

func newRouteTable(source semanticview.Source) *RouteTable {
	graph, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	rt := newRouteTableWithGraph(source, graph)
	rt.inputProducers = inputProducers
	rt.compiledSourceReady = true
	return rt
}

func newRouteTableWithGraph(source semanticview.Source, graph runtimepinrouting.CompiledConnectGraph) *RouteTable {
	return &RouteTable{
		source:            source,
		authoredEventPath: make(map[string]struct{}),
		authoredScopes:    make(map[string]struct{}),
		templates:         make(map[string]routeFlowTemplate),
		staticAgentPlans:  make(map[agentidentity.Plan]struct{}),
		instanceOwners:    make(map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.Instance),
		connectGraph:      graph,
	}
}

func (rt *RouteTable) admitFlowInstanceRouteIdentityLocked(req FlowInstanceRouteMaterializationRequest) (runtimeflowidentity.RunScopedFlowInstance, bool, error) {
	identity, err := normalizeFlowInstanceRouteIdentity(req.Identity)
	if err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, err
	}
	if err := req.Instance.ValidateConstruction(rt.source, identity.RunID); err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, err
	}
	if req.Instance.Route() != identity.Route {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf("route request differs from its exact construction identity")
	}
	_, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
	if err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, err
	}
	if exists {
		if rt.instanceOwners[identity] != req.Instance {
			return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf("route replay changed its construction parent")
		}
		return identity, true, nil
	}
	if collision := rt.flowInstanceRouteCollisionLocked(identity.Route.ScopeKey, identity.Route.InstancePath); collision != "" {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf("flow-instance route %q collides with authored canonical identity %q", identity.Route.InstancePath, collision)
	}
	return identity, false, nil
}

func normalizeFlowInstanceRouteIdentity(raw runtimeflowidentity.RunScopedFlowInstance) (runtimeflowidentity.RunScopedFlowInstance, error) {
	identity := raw.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, fmt.Errorf("flow-instance route identity requires run_id, scope_key, instance_id, and instance_path")
	}
	return identity, nil
}

func (rt *RouteTable) matchFlowInstanceRouteOwnerLocked(identity runtimeflowidentity.RunScopedFlowInstance) (runtimeflowidentity.RunScopedFlowInstance, bool, error) {
	owner, exists := rt.instanceOwners[identity]
	if exists {
		if owner.Route() != identity.Route {
			return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf(
				"flow-instance path %q is owned by scope %q instance %q, not scope %q instance %q",
				identity.Route.InstancePath,
				owner.ScopeKey,
				owner.InstanceID,
				identity.Route.ScopeKey,
				identity.Route.InstanceID,
			)
		}
		return identity, true, nil
	}
	for admitted, construction := range rt.instanceOwners {
		if admitted.RunID == identity.RunID && admitted.Route.InstancePath == identity.Route.InstancePath {
			return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf("flow-instance path %q is owned by scope %q instance %q", identity.Route.InstancePath, construction.ScopeKey, construction.InstanceID)
		}
	}
	return runtimeflowidentity.RunScopedFlowInstance{}, false, nil
}

func (rt *RouteTable) flowInstanceRouteCollisionLocked(templateScope, instancePath string) string {
	templateScope = eventidentity.Normalize(templateScope)
	instancePath = eventidentity.Normalize(instancePath)
	if templateScope == "" || instancePath == "" {
		return ""
	}
	if templateScope == instancePath {
		if template, found := rt.templates[templateScope]; found {
			if schema, found := rt.source.FlowSchemaByID(template.FlowID); found && schema.Instance.Empty() {
				return ""
			}
		}
	}
	for _, scopePath := range sortedStringKeys(rt.authoredScopes) {
		scopePath = eventidentity.Normalize(scopePath)
		switch {
		case instancePath == scopePath:
			return scopePath
		case strings.HasPrefix(scopePath, instancePath+"/"):
			return scopePath
		case strings.HasPrefix(instancePath, scopePath+"/") && templateScope != scopePath && !strings.HasPrefix(templateScope, scopePath+"/"):
			return scopePath
		}
	}
	for _, eventPath := range sortedStringKeys(rt.authoredEventPath) {
		if routeCanonicalPathsOverlap(instancePath, eventPath) {
			return eventPath
		}
	}
	return ""
}

func routeCanonicalPathsOverlap(left, right string) bool {
	left = eventidentity.Normalize(left)
	right = eventidentity.Normalize(right)
	if left == "" || right == "" {
		return false
	}
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

type routeAgentDeclaration struct {
	Declaration semanticview.AgentDeclaration
	NamePlan    semanticview.AgentNamePlan
}

func routeAgentDeclarationsForOwner(source semanticview.Source, ownerFlowID string) ([]routeAgentDeclaration, error) {
	return routeAgentDeclarations(source, semanticview.AgentDeclarationsForOwner(source, ownerFlowID))
}

func routeAgentDeclarations(source semanticview.Source, declarations []semanticview.AgentDeclaration) ([]routeAgentDeclaration, error) {
	out := make([]routeAgentDeclaration, 0, len(declarations))
	for _, declaration := range declarations {
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			return nil, fmt.Errorf("route subscriber %s declaration name: %w", declaration.Label(true), err)
		}
		out = append(out, routeAgentDeclaration{Declaration: declaration, NamePlan: plan})
	}
	return out, nil
}

func routeApplyResolvedPattern(subscriber Subscriber, resolved routeResolvedPattern) Subscriber {
	subscriber.subscription = resolved.subscription
	subscriber.routeSource = resolved.routeSource
	subscriber.LocalizedEvent = eventidentity.Normalize(resolved.LocalizedEvent)
	if matchPattern := eventidentity.Normalize(resolved.MatchPattern); matchPattern != "" {
		subscriber.MatchPattern = matchPattern
	}
	if subscriber.Path == "" {
		if routePath := eventidentity.Normalize(resolved.RoutePath); routePath != "" {
			subscriber.Path = routePath
		}
	}
	return subscriber
}

func routeFlowLocalEventSet(source semanticview.Source, scope semanticview.FlowScope) map[string]struct{} {
	_, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	return routeFlowLocalEventSetWithInputProducers(scope, inputProducers)
}

func routeFlowLocalEventSetWithInputProducers(scope semanticview.FlowScope, inputProducers runtimepinrouting.FlowInputProducerResolver) map[string]struct{} {
	out := routeEventKeys(scope.Events)
	for _, eventType := range scope.OutputEvents {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out[eventType] = struct{}{}
	}
	for _, eventType := range scope.InputEvents {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" || routeFlowInputProducerIsExternal(inputProducers.Resolve(scope.ID, eventType)) {
			continue
		}
		out[eventType] = struct{}{}
	}
	if autoEmit := strings.TrimSpace(scope.AutoEmitEvent); autoEmit != "" {
		out[autoEmit] = struct{}{}
	}
	return out
}

func routeFlowInputHasExternalProducer(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	resolution := runtimepinrouting.ResolveFlowInputProducer(source, flowID, eventType)
	return routeFlowInputProducerIsExternal(resolution)
}

func routeFlowInputProducerIsExternal(resolution runtimecontracts.FlowInputProducerResolution) bool {
	switch {
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress):
		return true
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress):
		return true
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect):
		return true
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerPlatformSource):
		return true
	default:
		return false
	}
}

func routeEventKeys(events map[string]runtimecontracts.EventCatalogEntry) map[string]struct{} {
	out := make(map[string]struct{}, len(events))
	for _, eventType := range sortedStringKeys(events) {
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	return out
}

func routeSubscriberTemplates(source semanticview.Source, scope semanticview.FlowScope, agents []routeAgentDeclaration, localEvents map[string]struct{}) ([]routeSubscriberTemplate, error) {
	out := make([]routeSubscriberTemplate, 0, len(agents)+len(scope.Nodes))
	for _, agent := range agents {
		declaration := agent.Declaration
		key := strings.TrimSpace(declaration.LocalID)
		entry := declaration.Entry
		patterns := normalizeStringList(entry.Subscriptions)
		if len(patterns) == 0 {
			continue
		}
		compiled := make([]routeSubscriberTemplatePattern, 0, len(patterns))
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberAgent, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber agent %s in flow %s: %s", key, scope.ID, admission.Message())
			}
			compiled = append(compiled, routeSubscriberTemplatePattern{
				raw: pattern, admission: admission,
				inputEvent: !admission.Pattern() && scope.ID != "" && source.FlowHasInputEvent(scope.ID, admission.LocalEvent()),
			})
		}
		out = append(out, routeSubscriberTemplate{
			Kind:          subscriberAgent,
			Patterns:      compiled,
			AgentNamePlan: agent.NamePlan,
		})
	}
	nodes, err := routeExecutableNodeDeclarations(source, scope.ID, scope.Nodes)
	if err != nil {
		return nil, err
	}
	for _, declaration := range nodes {
		entry := declaration.Entry
		handlerNode := declaration.Node
		semanticNodeID := handlerNode.NodeID()
		patterns := runtimecontracts.EffectiveSystemNodeSubscriptions(entry)
		if source != nil {
			patterns = source.ExecutableNodeRuntimeSubscriptions(handlerNode)
		}
		if len(patterns) == 0 {
			continue
		}
		targetHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, handlerNode)
		if err != nil {
			return nil, fmt.Errorf("route subscriber node %s in flow %s: %w", semanticNodeID, scope.ID, err)
		}
		compiled := make([]routeSubscriberTemplatePattern, 0, len(patterns))
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberNode, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber node %s in flow %s: %s", semanticNodeID, scope.ID, admission.Message())
			}
			compiled = append(compiled, routeSubscriberTemplatePattern{
				raw: pattern, admission: admission,
				inputEvent: !admission.Pattern() && scope.ID != "" && source.FlowHasInputEvent(scope.ID, admission.LocalEvent()),
			})
		}
		out = append(out, routeSubscriberTemplate{
			Kind:          subscriberNode,
			Patterns:      compiled,
			HandlerNode:   handlerNode,
			TargetHandler: targetHandler,
		})
	}
	return out, nil
}

type routeExecutableNodeDeclaration struct {
	Node  runtimeidentity.ExecutableNode
	Entry runtimecontracts.SystemNodeContract
}

func routeExecutableNodeDeclarations(source semanticview.Source, flowPath string, expected map[string]runtimecontracts.SystemNodeContract) ([]routeExecutableNodeDeclaration, error) {
	flowPath = strings.TrimSpace(flowPath)
	found := make(map[string]struct{}, len(expected))
	out := make([]routeExecutableNodeDeclaration, 0, len(expected))
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			return nil, fmt.Errorf("admit route subscriber node %s identity: %w", record.LogicalID, err)
		}
		if node.FlowPath() != flowPath {
			continue
		}
		if _, ok := expected[record.LogicalID]; !ok {
			continue
		}
		if _, duplicate := found[record.LogicalID]; duplicate {
			return nil, fmt.Errorf("route subscriber node %q has multiple exact declaration owners in flow %q", record.LogicalID, flowPath)
		}
		found[record.LogicalID] = struct{}{}
		out = append(out, routeExecutableNodeDeclaration{Node: node, Entry: record.Entry})
	}
	for nodeID := range expected {
		if _, ok := found[nodeID]; !ok {
			return nil, fmt.Errorf("route subscriber node %q has no exact declaration owner in flow %q", nodeID, flowPath)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node.Key() < out[j].Node.Key() })
	return out, nil
}

func routeResolveSubscriberPatterns(source semanticview.Source, kind subscriberKind, flowID string, inputEvents []string, authorityPath, routePath string, localEvents map[string]struct{}, raw string) ([]routeResolvedPattern, error) {
	_, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	return routeResolveSubscriberPatternsWithInputProducers(source, kind, flowID, inputEvents, authorityPath, routePath, localEvents, raw, inputProducers)
}

func routeResolveSubscriberPatternsWithInputProducers(source semanticview.Source, kind subscriberKind, flowID string, inputEvents []string, authorityPath, routePath string, localEvents map[string]struct{}, raw string, inputProducers runtimepinrouting.FlowInputProducerResolver) ([]routeResolvedPattern, error) {
	raw = eventidentity.Normalize(raw)
	flowID = strings.TrimSpace(flowID)
	if raw == "" {
		return nil, nil
	}
	admission := routeClassifyAuthoredSubscription(source, kind, flowID, inputEvents, authorityPath, localEvents, raw)
	if !admission.Admitted() {
		return nil, fmt.Errorf("route subscriber in flow %s: %s", flowID, admission.Message())
	}
	inputEvent := !admission.Pattern() && flowID != "" && source != nil && source.FlowHasInputEvent(flowID, admission.LocalEvent())
	return routeProjectAdmittedSubscriberPatterns(admission, flowID, routePath, inputEvent, inputProducers), nil
}

func routeProjectAdmittedSubscriberPatterns(admission semanticview.AuthoredSubscriptionAdmission, flowID, routePath string, inputEvent bool, inputProducers runtimepinrouting.FlowInputProducerResolver) []routeResolvedPattern {
	if inputEvent {
		patterns := routeInputProducerPatterns(inputProducers.Resolve(flowID, admission.LocalEvent()).AutoWireResolution())
		if len(patterns) > 0 {
			return patterns
		}
	}
	out := make([]routeResolvedPattern, 0, len(admission.RoutePatterns()))
	for _, pattern := range admission.RoutePatternsAt(routePath) {
		out = append(out, routeResolvedPattern{
			subscription:   admission,
			EventPattern:   pattern,
			routeSource:    subscriberRouteSourceSubscription,
			LocalizedEvent: admission.LocalEvent(),
		})
	}
	return out
}

func routeClassifyAuthoredSubscription(source semanticview.Source, kind subscriberKind, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, raw string) semanticview.AuthoredSubscriptionAdmission {
	consumerKind := semanticview.AuthoredSubscriptionConsumerNode
	if kind == subscriberAgent {
		consumerKind = semanticview.AuthoredSubscriptionConsumerAgent
	}
	return semanticview.ClassifyAuthoredSubscription(source, semanticview.AuthoredSubscriptionRequest{
		ConsumerKind: consumerKind,
		FlowID:       flowID,
		FlowPath:     basePath,
		LocalEvents:  localEvents,
		InputEvents:  inputEvents,
		Authored:     raw,
	})
}

func routeInputProducerPatterns(resolution runtimecontracts.FlowInputAutoWireResolution) []routeResolvedPattern {
	out := make([]routeResolvedPattern, 0, len(resolution.Patterns))
	for _, pattern := range resolution.Patterns {
		pattern = eventidentity.Normalize(pattern)
		if pattern == "" {
			continue
		}
		out = append(out, routeResolvedPattern{
			EventPattern: pattern,
			routeSource:  subscriberRouteSourcePinAutoWire,
		})
	}
	return out
}

func routeEventIdentityScope(basePath string, localEvents map[string]struct{}, inputEvents []string) eventidentity.Scope {
	return eventidentity.Scope{
		Path:        strings.Trim(strings.TrimSpace(basePath), "/"),
		LocalEvents: sortedStringKeys(localEvents),
		InputEvents: append([]string{}, inputEvents...),
	}
}

type resolvedSubscriberRoleIdentity struct {
	recipient      events.DeliveryRecipient
	path           string
	agentPlan      agentidentity.Plan
	agentLifecycle agentLifecycleAdmission
	routeSource    subscriberRouteSource
	localizedEvent string
	handlerNode    runtimeidentity.ExecutableNode
	connectHandler runtimepinrouting.ConnectReceiverHandler
	targetHandler  runtimepipeline.DeliveryTargetHandler
}

func resolvedSubscriberRoleKey(subscriber Subscriber) resolvedSubscriberRoleIdentity {
	return resolvedSubscriberRoleIdentity{
		recipient:      subscriber.Recipient,
		path:           eventidentity.Normalize(subscriber.Path),
		agentPlan:      subscriber.AgentPlan.Normalize(),
		agentLifecycle: subscriber.agentLifecycle,
		routeSource:    subscriber.routeSource,
		localizedEvent: eventidentity.Normalize(subscriber.LocalizedEvent),
		handlerNode:    subscriber.handlerNode,
		connectHandler: subscriber.connectHandler,
		targetHandler:  subscriber.targetHandler,
	}
}

func appendUniqueSubscriber(in []Subscriber, subscriber Subscriber) []Subscriber {
	key := resolvedSubscriberRoleKey(subscriber)
	for idx := range in {
		if resolvedSubscriberRoleKey(in[idx]) != key {
			continue
		}
		in[idx].MatchPattern = strongestSubscriberMatchEvidence(in[idx].MatchPattern, subscriber.MatchPattern)
		return in
	}
	return append(in, subscriber)
}

func strongestSubscriberMatchEvidence(left, right string) string {
	left = eventidentity.Normalize(left)
	right = eventidentity.Normalize(right)
	leftRank := subscriberMatchEvidenceRank(left)
	rightRank := subscriberMatchEvidenceRank(right)
	switch {
	case rightRank > leftRank:
		return right
	case leftRank > rightRank:
		return left
	case left == "":
		return right
	case right == "":
		return left
	case right < left:
		return right
	default:
		return left
	}
}

func subscriberMatchEvidenceRank(pattern string) int {
	if pattern == "" {
		return 0
	}
	if strings.Contains(pattern, "*") {
		return 1
	}
	return 2
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}

func sortedStringKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for key := range m {
		key = strings.TrimSpace(key)
		if key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func normalizeStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}
