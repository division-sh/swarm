package bus

import (
	"context"
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
	AgentIdentity  agentidentity.Identity
	handlerNode    runtimeidentity.ExecutableNode
	connectHandler runtimepinrouting.ConnectReceiverHandler
	targetHandler  runtimepipeline.DeliveryTargetHandler
}

func (s Subscriber) RouteSourceCode() string { return s.routeSource.code() }

type subscriberRouteSource uint8

const (
	subscriberRouteSourceSubscription subscriberRouteSource = iota + 1
	subscriberRouteSourcePinAutoWire
	subscriberRouteSourceRootInputFlow
	subscriberRouteSourceConnectRoutePlan
)

func subscriberRouteSourceFromCode(code string) (subscriberRouteSource, bool) {
	switch strings.TrimSpace(code) {
	case "subscription":
		return subscriberRouteSourceSubscription, true
	case "pin_auto_wire":
		return subscriberRouteSourcePinAutoWire, true
	case "root_input_flow":
		return subscriberRouteSourceRootInputFlow, true
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
	case subscriberRouteSourceRootInputFlow:
		return "root_input_flow"
	case subscriberRouteSourceConnectRoutePlan:
		return "connect_route_plan"
	default:
		return ""
	}
}

type RouteTable struct {
	mu                sync.RWMutex
	generationMu      sync.RWMutex
	generation        uint64
	source            semanticview.Source
	routes            map[string][]Subscriber
	rootInputRoutes   map[string][]Subscriber
	patterns          []routePattern
	eventPath         map[string]struct{}
	authoredEventPath map[string]struct{}
	authoredScopes    map[string]struct{}
	templates         map[string]routeFlowTemplate
	instanceOwners    map[string]runtimeflowidentity.Route
	instanceEventPath map[string][]string
	templateObservers map[string][]routeTemplateSourceObserver
	connectGraph      runtimepinrouting.CompiledConnectGraph
	connectRecipients []routeConnectRecipientRegistration
}

type routeConnectRecipientRegistration struct {
	registration runtimepinrouting.ConnectRecipientRegistration
	instancePath string
}

type routePattern struct {
	EventPattern       string
	Subscriber         Subscriber
	InstancePath       string
	SourceInstancePath string
}

type routeTemplateSourceObserver struct {
	SourceTemplatePath     string
	SourceLocalEvent       string
	Subscriber             Subscriber
	SubscriberInstancePath string
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
	RawPatterns   []string
	AgentNamePlan semanticview.AgentNamePlan
	HandlerNode   runtimeidentity.ExecutableNode
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
	EventPattern       string
	MatchPattern       string
	routeSource        subscriberRouteSource
	LocalizedEvent     string
	RoutePath          string
	SourceTemplatePath string
	SourceLocalEvent   string
}

type TypedPubSubAuthorizationError struct {
	Issues []semanticview.TypedPubSubConsumerIssue
}

func (e *TypedPubSubAuthorizationError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "typed pub/sub authorization failed"
	}
	messages := make([]string, 0, len(e.Issues))
	for _, issue := range e.Issues {
		messages = append(messages, fmt.Sprintf("%s: %s", issue.Failure, issue.Message()))
	}
	return strings.Join(messages, "; ")
}

func validateTypedPubSubAuthorizations(source semanticview.Source) error {
	if source == nil {
		return nil
	}
	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	if len(relations.Issues) == 0 {
		return nil
	}
	return &TypedPubSubAuthorizationError{Issues: relations.Issues}
}

func DeriveRouteTable(source semanticview.Source) (*RouteTable, error) {
	rt := newRouteTable(source)
	if source == nil {
		return rt, nil
	}
	if _, err := semanticview.AgentNamePlans(source); err != nil {
		return nil, fmt.Errorf("compile declared agent names: %w", err)
	}
	if err := validateTypedPubSubAuthorizations(source); err != nil {
		return nil, err
	}

	for _, scope := range semanticview.FlowScopes(source) {
		agents, err := routeAgentDeclarationsForOwner(source, scope.ID)
		if err != nil {
			return nil, err
		}
		flowPath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		localEvents := routeFlowLocalEventSet(source, scope)
		if strings.EqualFold(scope.Mode, "template") || routeFlowStanding(source, scope.ID) {
			subscribers, err := routeSubscriberTemplates(source, scope, agents)
			if err != nil {
				return nil, err
			}
			rt.templates[flowPath] = routeFlowTemplate{
				FlowID:      scope.ID,
				InputEvents: append([]string{}, scope.InputEvents...),
				LocalEvents: cloneStringSet(localEvents),
				Subscribers: subscribers,
			}
			continue
		}
		if flowPath != "" {
			rt.authoredScopes[flowPath] = struct{}{}
		}
		rt.addAuthoredEventPathsLocked(flowPath, localEvents)
		if err := rt.addAgentPatternsLocked(source, scope.ID, scope.InputEvents, flowPath, localEvents, agents); err != nil {
			return nil, err
		}
		nodes, err := routeExecutableNodeDeclarations(source, scope.ID, scope.Nodes)
		if err != nil {
			return nil, err
		}
		if err := rt.addNodePatternsLocked(source, scope.ID, scope.ID, scope.InputEvents, flowPath, localEvents, nodes); err != nil {
			return nil, err
		}
	}

	if err := rt.addRootInputFlowNodeRoutesLocked(source); err != nil {
		return nil, err
	}
	rt.rebuildLocked()
	return rt, nil
}

func routeFlowStanding(source semanticview.Source, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return false
	}
	schema, ok := source.FlowSchemaByID(flowID)
	return ok && strings.TrimSpace(schema.Activation) == runtimecontracts.FlowActivationStanding
}

func (rt *RouteTable) Resolve(eventType string) []Subscriber {
	if rt == nil {
		return nil
	}
	eventType = strings.Trim(strings.TrimSpace(eventType), "/")
	if eventType == "" {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := cloneSubscribers(rt.routes[eventType])
	for _, subscriber := range rt.rootInputRoutes[eventType] {
		out = appendUniqueRootInputSubscriber(out, subscriber)
	}
	if _, active := rt.eventPath[eventType]; !active {
		return out
	}
	for _, pattern := range rt.patterns {
		eventPattern := strings.Trim(strings.TrimSpace(pattern.EventPattern), "/")
		if eventPattern == "" || !strings.Contains(eventPattern, "*") {
			continue
		}
		if !RouteMatches(eventPattern, eventType) {
			continue
		}
		subscriber := pattern.Subscriber
		subscriber.MatchPattern = eventPattern
		out = appendUniqueSubscriber(out, subscriber)
	}
	return out
}

func (rt *RouteTable) EvaluateConnectSource(sourceEvent runtimepinrouting.SourceEvent) runtimepinrouting.ConnectRecipientEvaluation {
	if rt == nil {
		return runtimepinrouting.ConnectRecipientEvaluation{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.connectGraph.EvaluateSourceRecipients(sourceEvent, rt.connectRecipientAdmissionsLocked())
}

func (rt *RouteTable) evaluateConnectPlan(plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) runtimepinrouting.ConnectRecipientEvaluation {
	if rt == nil {
		return runtimepinrouting.ConnectRecipientEvaluation{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.connectGraph.EvaluateMaterializedRecipients(plan, targets, rt.connectRecipientAdmissionsLocked())
}

func (rt *RouteTable) connectRecipientAdmissionsLocked() []runtimepinrouting.ConnectRecipientRegistration {
	out := make([]runtimepinrouting.ConnectRecipientRegistration, 0, len(rt.connectRecipients))
	for _, registration := range rt.connectRecipients {
		out = append(out, registration.registration)
	}
	return out
}

func (rt *RouteTable) connectRecipientAdmissions() []runtimepinrouting.ConnectRecipientRegistration {
	if rt == nil {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.connectRecipientAdmissionsLocked()
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
			LocalizedEvent: string(recipient.HandlerEvent()), AgentIdentity: recipient.AgentIdentity(),
			handlerNode:    handlerNode,
			connectHandler: recipient.Handler(),
			routeSource:    subscriberRouteSourceConnectRoutePlan,
		})
	}
	return dedupeSubscribers(out)
}

func (rt *RouteTable) AddFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	return rt.addFlowInstanceRoute(req)
}

func (rt *RouteTable) addFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {

	req = req.Normalized()

	rt.mu.Lock()
	defer rt.mu.Unlock()

	identity, replay, err := rt.admitFlowInstanceRouteIdentityLocked(req.Identity)
	if err != nil {
		return err
	}
	if replay {
		return nil
	}
	req.Identity = identity
	instancePath := identity.InstancePath
	templateScope := identity.ScopeKey

	templateDef, ok := rt.templates[templateScope]
	if !ok {
		return fmt.Errorf("route template %q not found", templateScope)
	}
	rt.instanceOwners[instancePath] = identity
	rt.instanceEventPath[instancePath] = rt.addEventPathsLocked(instancePath, templateDef.LocalEvents)
	rt.materializeTemplateSourceObserversLocked(templateScope, instancePath)
	for _, subscriberTemplate := range templateDef.Subscribers {
		subscriberID := ""
		var name agentidentity.Name
		var err error
		if subscriberTemplate.Kind == subscriberAgent {
			name, err = subscriberTemplate.AgentNamePlan.Materialize()
			if err != nil {
				return fmt.Errorf("materialize route subscriber agent name: %w", err)
			}
			subscriberID = name.AgentID
		}
		recipient, err := subscriberRecipient(subscriberTemplate.Kind, subscriberID, subscriberTemplate.HandlerNode)
		if err != nil {
			return fmt.Errorf("materialize route subscriber: %w", err)
		}
		subscriber := Subscriber{
			Recipient: recipient, Path: instancePath,
			handlerNode: subscriberTemplate.HandlerNode,
		}
		if subscriber.Recipient.IsAgent() {
			agentRoute, err := identity.AgentIdentityRoute()
			if err != nil {
				return fmt.Errorf("materialize route subscriber flow identity: %w", err)
			}
			subscriber.AgentIdentity, err = agentidentity.New(name, agentRoute)
			if err != nil {
				return fmt.Errorf("materialize route subscriber concrete identity: %w", err)
			}
		}
		for _, rawPattern := range subscriberTemplate.RawPatterns {
			admittedSubscriber := subscriber
			if admittedSubscriber.Recipient.IsNode() {
				admittedSubscriber.targetHandler, err = runtimepipeline.AdmitDeliveryTargetHandler(
					rt.source, subscriber.handlerNode,
				)
				if err != nil {
					return fmt.Errorf("materialize route subscriber target handler: %w", err)
				}
			}
			if err := rt.addConnectRecipientLocked(templateDef.FlowID, templateDef.InputEvents, rawPattern, admittedSubscriber, instancePath); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatterns(rt.source, subscriberTemplate.Kind, templateDef.FlowID, templateDef.InputEvents, templateScope, instancePath, templateDef.LocalEvents, rawPattern)
			if err != nil {
				return err
			}
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(admittedSubscriber, resolved, instancePath)
			}
		}
	}
	rt.rebuildLocked()
	rt.generation++
	return nil
}

func (rt *RouteTable) HasFlowInstanceRoute(identity runtimeflowidentity.Route) bool {
	if rt == nil {
		return false
	}
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	owner, exists := rt.instanceOwners[identity.InstancePath]
	return exists && flowInstanceRouteIdentityEqual(owner, identity)
}

func (rt *RouteTable) flowInstanceTemplateID(identity runtimeflowidentity.Route) (string, bool) {
	if rt == nil {
		return "", false
	}
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return "", false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	template, exists := rt.templates[identity.ScopeKey]
	return strings.TrimSpace(template.FlowID), exists
}

func (rt *RouteTable) flowInstanceRouteRemovalOwner(identity runtimeflowidentity.Route) (runtimeflowidentity.Route, bool, error) {
	if rt == nil {
		return runtimeflowidentity.Route{}, false, fmt.Errorf("route table is required")
	}
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return runtimeflowidentity.Route{}, false, err
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.matchFlowInstanceRouteOwnerLocked(identity)
}

func (rt *RouteTable) RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	return rt.removeFlowInstanceRoute(identity)
}

func (rt *RouteTable) removeFlowInstanceRoute(identity runtimeflowidentity.Route) error {
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
	instancePath := owner.InstancePath
	delete(rt.instanceOwners, instancePath)
	for _, eventType := range rt.instanceEventPath[instancePath] {
		delete(rt.eventPath, eventType)
	}
	delete(rt.instanceEventPath, instancePath)
	filtered := rt.patterns[:0]
	for _, pattern := range rt.patterns {
		if pattern.InstancePath == instancePath || pattern.SourceInstancePath == instancePath {
			continue
		}
		filtered = append(filtered, pattern)
	}
	rt.patterns = filtered
	filteredConnect := rt.connectRecipients[:0]
	for _, registration := range rt.connectRecipients {
		if registration.instancePath == instancePath {
			continue
		}
		filteredConnect = append(filteredConnect, registration)
	}
	rt.connectRecipients = filteredConnect
	for sourceTemplatePath, observers := range rt.templateObservers {
		filteredObservers := observers[:0]
		for _, observer := range observers {
			if observer.SubscriberInstancePath == instancePath {
				continue
			}
			filteredObservers = append(filteredObservers, observer)
		}
		if len(filteredObservers) == 0 {
			delete(rt.templateObservers, sourceTemplatePath)
			continue
		}
		rt.templateObservers[sourceTemplatePath] = filteredObservers
	}
	rt.rebuildLocked()
	rt.generation++
	return nil
}

func (rt *RouteTable) MaterializedRoutes(identity runtimeflowidentity.Route) []FlowInstanceRouteRecord {
	if rt == nil {
		return nil
	}
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return nil
	}
	instancePath := identity.InstancePath
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	owner, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
	if err != nil || !exists || !flowInstanceRouteIdentityEqual(owner, identity) {
		return nil
	}

	type materializedRouteIdentity struct {
		instancePath string
		eventPattern string
		recipient    events.DeliveryRecipient
	}
	seen := make(map[materializedRouteIdentity]struct{})
	out := make([]FlowInstanceRouteRecord, 0, 8)
	for _, pattern := range rt.patterns {
		if strings.Trim(strings.TrimSpace(pattern.InstancePath), "/") != instancePath {
			continue
		}
		record := FlowInstanceRouteRecord{
			Identity:       identity,
			EventPattern:   strings.TrimSpace(pattern.EventPattern),
			SubscriberType: pattern.Subscriber.Recipient.Code(),
			SubscriberID:   pattern.Subscriber.Recipient.ID(),
			SourceFlow:     identity.ScopeKey,
		}
		key := materializedRouteIdentity{
			instancePath: record.Identity.InstancePath,
			eventPattern: record.EventPattern,
			recipient:    pattern.Subscriber.Recipient,
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventPattern != out[j].EventPattern {
			return out[i].EventPattern < out[j].EventPattern
		}
		if out[i].SubscriberType != out[j].SubscriberType {
			return out[i].SubscriberType < out[j].SubscriberType
		}
		return out[i].SubscriberID < out[j].SubscriberID
	})
	return out
}

func newRouteTable(source semanticview.Source) *RouteTable {
	return &RouteTable{
		generation:        1,
		source:            source,
		routes:            make(map[string][]Subscriber),
		rootInputRoutes:   make(map[string][]Subscriber),
		eventPath:         make(map[string]struct{}),
		authoredEventPath: make(map[string]struct{}),
		authoredScopes:    make(map[string]struct{}),
		templates:         make(map[string]routeFlowTemplate),
		instanceOwners:    make(map[string]runtimeflowidentity.Route),
		instanceEventPath: make(map[string][]string),
		templateObservers: make(map[string][]routeTemplateSourceObserver),
		connectGraph:      runtimepinrouting.CompileConnectGraph(source),
	}
}

type routeTableSnapshotGeneration struct {
	value uint64
}

type routeTableGenerationLeaseKey struct{}

type routeTableGenerationLease struct {
	table *RouteTable
}

func (rt *RouteTable) snapshotGeneration() routeTableSnapshotGeneration {
	if rt == nil {
		return routeTableSnapshotGeneration{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return routeTableSnapshotGeneration{value: rt.generation}
}

func (rt *RouteTable) snapshotGenerationCurrent(snapshot routeTableSnapshotGeneration) bool {
	if rt == nil {
		return snapshot.value == 0
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return snapshot.value != 0 && rt.generation == snapshot.value
}

func (rt *RouteTable) addFlowInstanceRouteForContext(ctx context.Context, req FlowInstanceRouteMaterializationRequest) error {
	if ctx != nil {
		if lease, _ := ctx.Value(routeTableGenerationLeaseKey{}).(routeTableGenerationLease); lease.table == rt {
			return rt.addFlowInstanceRoute(req)
		}
	}
	return rt.AddFlowInstanceRoute(req)
}

func (rt *RouteTable) removeFlowInstanceRouteForContext(ctx context.Context, identity runtimeflowidentity.Route) error {
	if ctx != nil {
		if lease, _ := ctx.Value(routeTableGenerationLeaseKey{}).(routeTableGenerationLease); lease.table == rt {
			return rt.removeFlowInstanceRoute(identity)
		}
	}
	return rt.RemoveFlowInstanceRoute(identity)
}

func rootInputFlowOwnsNodeRoute(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) bool {
	for _, scope := range source.FlowScopes() {
		if strings.EqualFold(scope.Mode, "template") || !normalizedStringListContains(scope.InputEvents, eventType) {
			continue
		}
		if strings.TrimSpace(routeFlowPath(source, scope.ID)) == node.FlowPath() {
			if _, declared := scope.Nodes[node.NodeID()]; declared {
				return true
			}
		}
	}
	return false
}

func (rt *RouteTable) addRootInputFlowNodeRoutesLocked(source semanticview.Source) error {
	if rt == nil || source == nil {
		return nil
	}
	rootInputs := routeRootInputEventSet(source)
	for _, scope := range source.FlowScopes() {
		if strings.EqualFold(scope.Mode, "template") {
			continue
		}
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "." {
			continue
		}
		flowPath := strings.Trim(strings.TrimSpace(routeFlowPath(source, flowID)), "/")
		if flowID == "" || flowPath == "" {
			continue
		}
		admittedInputs := routeAdmittedFlowIngressEventSet(source, scope, rootInputs)
		if len(admittedInputs) == 0 {
			continue
		}
		identityScope := routeEventIdentityScope(flowPath, routeFlowLocalEventSet(source, scope), scope.InputEvents)
		for _, eventType := range sortedStringKeys(admittedInputs) {
			localEvent := eventidentity.Normalize(identityScope.LocalizeInput(eventType))
			if localEvent == "" || !normalizedStringListContains(scope.InputEvents, localEvent) {
				continue
			}
			nodes, err := routeExecutableNodeDeclarations(source, flowPath, scope.Nodes)
			if err != nil {
				return err
			}
			for _, declaration := range nodes {
				handlerNode := declaration.Node
				semanticNodeID := handlerNode.NodeID()
				if !routeNodeSubscribesToLocalExact(source, handlerNode, localEvent) {
					continue
				}
				subscriber := Subscriber{
					Recipient:      events.MustNodeDeliveryRecipient(handlerNode),
					Path:           flowPath,
					MatchPattern:   eventType,
					routeSource:    subscriberRouteSourceRootInputFlow,
					LocalizedEvent: localEvent,
					handlerNode:    handlerNode,
				}
				subscriber.targetHandler, err = runtimepipeline.AdmitDeliveryTargetHandler(
					source, handlerNode,
				)
				if err != nil {
					return fmt.Errorf("admit root-input flow target handler %s for %s: %w", semanticNodeID, eventType, err)
				}
				rt.rootInputRoutes[eventType] = appendUniqueSubscriber(rt.rootInputRoutes[eventType], subscriber)
			}
		}
	}
	return nil
}

func routeAdmittedFlowIngressEventSet(source semanticview.Source, scope semanticview.FlowScope, rootInputs map[string]struct{}) map[string]struct{} {
	out := cloneStringSet(rootInputs)
	flowID := strings.TrimSpace(scope.ID)
	for _, localEvent := range scope.InputEvents {
		resolution := semanticview.ResolveNonConnectFlowInputProducer(source, flowID, localEvent)
		if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) {
			continue
		}
		eventType := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, localEvent))
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	return out
}

func routeRootInputEventSet(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	out := make(map[string]struct{})
	for _, eventType := range normalizeStringList(source.FlowInputEvents(".")) {
		out[eventType] = struct{}{}
	}
	return out
}

func routeNodeSubscribesToLocalExact(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) bool {
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return false
	}
	for _, authored := range source.ExecutableNodeRuntimeSubscriptions(node) {
		admission := semanticview.ClassifyExecutableNodeSubscription(source, node, authored)
		if admission.Admitted() && !admission.Pattern() && admission.LocalEvent() == eventType {
			return true
		}
	}
	return false
}

func (rt *RouteTable) addEventPathsLocked(basePath string, localEvents map[string]struct{}) []string {
	added := make([]string, 0, len(localEvents))
	scope := routeEventIdentityScope(basePath, localEvents, nil)
	for _, eventType := range sortedStringKeys(localEvents) {
		absolute := scope.ResolveEvent(eventType, nil)
		if absolute == "" || strings.Contains(absolute, "*") {
			continue
		}
		rt.eventPath[absolute] = struct{}{}
		added = append(added, absolute)
	}
	return added
}

func (rt *RouteTable) addAuthoredEventPathsLocked(basePath string, localEvents map[string]struct{}) []string {
	added := rt.addEventPathsLocked(basePath, localEvents)
	for _, eventType := range added {
		rt.authoredEventPath[eventType] = struct{}{}
	}
	return added
}

func (rt *RouteTable) admitFlowInstanceRouteIdentityLocked(raw runtimeflowidentity.Route) (runtimeflowidentity.Route, bool, error) {
	identity, err := normalizeFlowInstanceRouteIdentity(raw)
	if err != nil {
		return runtimeflowidentity.Route{}, false, err
	}
	_, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
	if err != nil {
		return runtimeflowidentity.Route{}, false, err
	}
	if exists {
		return identity, true, nil
	}
	if collision := rt.flowInstanceRouteCollisionLocked(identity.ScopeKey, identity.InstancePath); collision != "" {
		return runtimeflowidentity.Route{}, false, fmt.Errorf("flow-instance route %q collides with authored canonical identity %q", identity.InstancePath, collision)
	}
	return identity, false, nil
}

func normalizeFlowInstanceRouteIdentity(raw runtimeflowidentity.Route) (runtimeflowidentity.Route, error) {
	identity := runtimeflowidentity.StoredRoute(raw.ScopeKey, raw.InstanceID, raw.InstancePath)
	if !identity.Valid() {
		return runtimeflowidentity.Route{}, fmt.Errorf("flow-instance route identity requires scope_key, instance_id, and instance_path")
	}
	return identity, nil
}

func (rt *RouteTable) matchFlowInstanceRouteOwnerLocked(identity runtimeflowidentity.Route) (runtimeflowidentity.Route, bool, error) {
	owner, exists := rt.instanceOwners[identity.InstancePath]
	if exists {
		if !flowInstanceRouteIdentityEqual(owner, identity) {
			return runtimeflowidentity.Route{}, false, fmt.Errorf(
				"flow-instance path %q is owned by scope %q instance %q, not scope %q instance %q",
				identity.InstancePath,
				owner.ScopeKey,
				owner.InstanceID,
				identity.ScopeKey,
				identity.InstanceID,
			)
		}
		return owner, true, nil
	}
	expected := runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, "")
	singleton := identity.InstancePath == identity.ScopeKey &&
		identity.InstanceID == runtimeflowidentity.LogicalInstanceID(identity.ScopeKey)
	if !singleton && expected.InstancePath != identity.InstancePath {
		return runtimeflowidentity.Route{}, false, fmt.Errorf(
			"flow-instance route identity is inconsistent: scope %q and instance %q derive path %q, not %q",
			identity.ScopeKey,
			identity.InstanceID,
			expected.InstancePath,
			identity.InstancePath,
		)
	}
	return runtimeflowidentity.Route{}, false, nil
}

func flowInstanceRouteIdentityEqual(left, right runtimeflowidentity.Route) bool {
	return left.ScopeKey == right.ScopeKey && left.InstanceID == right.InstanceID && left.InstancePath == right.InstancePath
}

func (rt *RouteTable) flowInstanceRouteCollisionLocked(templateScope, instancePath string) string {
	templateScope = eventidentity.Normalize(templateScope)
	instancePath = eventidentity.Normalize(instancePath)
	if templateScope == "" || instancePath == "" {
		return ""
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

func (rt *RouteTable) addAgentPatternsLocked(
	source semanticview.Source,
	agentFlowID string,
	inputEvents []string,
	agentPath string,
	localEvents map[string]struct{},
	agents []routeAgentDeclaration,
) error {
	for _, agent := range agents {
		declaration := agent.Declaration
		key := strings.TrimSpace(declaration.LocalID)
		entry := declaration.Entry
		namePlan := agent.NamePlan
		name, err := namePlan.Materialize()
		if err != nil {
			return fmt.Errorf("route subscriber agent %s declaration identity: %w", key, err)
		}
		subscriber := Subscriber{
			Recipient: events.MustAgentDeliveryRecipient(name.AgentID),
			Path:      strings.Trim(strings.TrimSpace(agentPath), "/"),
		}
		if strings.TrimSpace(agentFlowID) == "." {
			subscriber.Path = "."
		}
		route := agentidentity.RootRoute()
		if subscriber.Path != "" && subscriber.Path != "." {
			route, err = runtimeflowidentity.StoredRoute("", "", subscriber.Path).AgentIdentityRoute()
			if err != nil {
				return fmt.Errorf("route subscriber agent %s flow identity: %w", key, err)
			}
		}
		subscriber.AgentIdentity, err = agentidentity.New(name, route)
		if err != nil {
			return fmt.Errorf("route subscriber agent %s concrete identity: %w", key, err)
		}
		for _, rawPattern := range normalizeStringList(entry.Subscriptions) {
			if err := rt.addConnectRecipientLocked(agentFlowID, inputEvents, rawPattern, subscriber, ""); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatterns(source, subscriberAgent, agentFlowID, inputEvents, agentPath, agentPath, localEvents, rawPattern)
			if err != nil {
				return err
			}
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(subscriber, resolved, "")
			}
		}
	}
	return nil
}

func (rt *RouteTable) addNodePatternsLocked(source semanticview.Source, routingFlowID, connectFlowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, nodes []routeExecutableNodeDeclaration) error {
	for _, declaration := range nodes {
		entry := declaration.Entry
		handlerNode := declaration.Node
		semanticNodeID := handlerNode.NodeID()
		subscriber := Subscriber{
			Recipient:   events.MustNodeDeliveryRecipient(handlerNode),
			Path:        strings.Trim(strings.TrimSpace(basePath), "/"),
			handlerNode: handlerNode,
		}
		if strings.TrimSpace(connectFlowID) == "." {
			subscriber.Path = "."
		}
		patterns := runtimecontracts.EffectiveSystemNodeSubscriptions(entry)
		if source != nil {
			patterns = source.ExecutableNodeRuntimeSubscriptions(handlerNode)
		}
		for _, rawPattern := range patterns {
			admittedSubscriber := subscriber
			targetHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(
				source, handlerNode,
			)
			if err != nil {
				return fmt.Errorf("admit route subscriber target handler %s for %s: %w", semanticNodeID, rawPattern, err)
			}
			admittedSubscriber.targetHandler = targetHandler
			if err := rt.addConnectRecipientLocked(connectFlowID, inputEvents, rawPattern, admittedSubscriber, ""); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatterns(source, subscriberNode, routingFlowID, inputEvents, basePath, basePath, localEvents, rawPattern)
			if err != nil {
				return err
			}
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(admittedSubscriber, resolved, "")
			}
		}
	}
	return nil
}

func (rt *RouteTable) addConnectRecipientLocked(flowID string, inputEvents []string, eventPattern string, subscriber Subscriber, instancePath string) error {
	var recipient runtimepinrouting.ConnectRecipient
	var err error
	switch {
	case subscriber.Recipient.IsNode():
		recipient, err = runtimepinrouting.NewConnectNodeRecipient(
			subscriber.handlerNode, subscriber.Path,
		)
	case subscriber.Recipient.IsAgent():
		recipient, err = runtimepinrouting.NewConnectAgentRecipient(subscriber.Recipient.ID(), subscriber.Path, subscriber.AgentIdentity)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	eventPattern = eventidentity.Normalize(eventPattern)
	eventTypes := []string{eventPattern}
	if strings.Contains(eventPattern, "*") {
		eventTypes = nil
		for _, eventType := range normalizeStringList(inputEvents) {
			if eventidentity.MatchPattern(eventPattern, eventType) {
				eventTypes = append(eventTypes, eventType)
			}
		}
	}
	for _, eventType := range eventTypes {
		for _, registration := range rt.connectGraph.AdmitReceiverRecipient(strings.TrimSpace(flowID), events.EventType(eventType), recipient) {
			rt.connectRecipients = append(rt.connectRecipients, routeConnectRecipientRegistration{
				registration: registration,
				instancePath: strings.Trim(strings.TrimSpace(instancePath), "/"),
			})
		}
	}
	return nil
}

func (rt *RouteTable) addResolvedPatternLocked(subscriber Subscriber, resolved routeResolvedPattern, subscriberInstancePath string) {
	resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
	sourceTemplatePath := eventidentity.Normalize(resolved.SourceTemplatePath)
	sourceLocalEvent := eventidentity.Normalize(resolved.SourceLocalEvent)
	if sourceTemplatePath != "" || sourceLocalEvent != "" {
		if sourceTemplatePath == "" || sourceLocalEvent == "" {
			return
		}
		rt.addTemplateSourceObserverLocked(routeTemplateSourceObserver{
			SourceTemplatePath:     sourceTemplatePath,
			SourceLocalEvent:       sourceLocalEvent,
			Subscriber:             resolvedSubscriber,
			SubscriberInstancePath: strings.Trim(strings.TrimSpace(subscriberInstancePath), "/"),
		})
		return
	}
	rt.patterns = append(rt.patterns, routePattern{
		EventPattern: resolved.EventPattern,
		Subscriber:   resolvedSubscriber,
		InstancePath: strings.Trim(strings.TrimSpace(subscriberInstancePath), "/"),
	})
}

func (rt *RouteTable) addTemplateSourceObserverLocked(observer routeTemplateSourceObserver) {
	observer.SourceTemplatePath = eventidentity.Normalize(observer.SourceTemplatePath)
	observer.SourceLocalEvent = eventidentity.Normalize(observer.SourceLocalEvent)
	observer.SubscriberInstancePath = strings.Trim(strings.TrimSpace(observer.SubscriberInstancePath), "/")
	if observer.SourceTemplatePath == "" || observer.SourceLocalEvent == "" {
		return
	}
	key := routeTemplateSourceObserverKey(observer)
	for _, existing := range rt.templateObservers[observer.SourceTemplatePath] {
		if routeTemplateSourceObserverKey(existing) == key {
			return
		}
	}
	rt.templateObservers[observer.SourceTemplatePath] = append(rt.templateObservers[observer.SourceTemplatePath], observer)
	for _, instancePath := range sortedStringKeys(rt.instanceOwners) {
		if rt.instanceOwners[instancePath].ScopeKey == observer.SourceTemplatePath {
			rt.materializeTemplateSourceObserverLocked(observer, instancePath)
		}
	}
}

func (rt *RouteTable) materializeTemplateSourceObserversLocked(templateScope, instancePath string) {
	templateScope = eventidentity.Normalize(templateScope)
	instancePath = eventidentity.Normalize(instancePath)
	for _, observer := range rt.templateObservers[templateScope] {
		rt.materializeTemplateSourceObserverLocked(observer, instancePath)
	}
}

func (rt *RouteTable) materializeTemplateSourceObserverLocked(observer routeTemplateSourceObserver, instancePath string) {
	instancePath = eventidentity.Normalize(instancePath)
	eventPattern := eventidentity.Normalize(instancePath + "/" + observer.SourceLocalEvent)
	if instancePath == "" || eventPattern == "" {
		return
	}
	if _, active := rt.eventPath[eventPattern]; !active {
		return
	}
	subscriber := observer.Subscriber
	subscriber.MatchPattern = eventPattern
	candidate := routePattern{
		EventPattern:       eventPattern,
		Subscriber:         subscriber,
		InstancePath:       observer.SubscriberInstancePath,
		SourceInstancePath: instancePath,
	}
	key := routePatternIdentity(candidate)
	for _, existing := range rt.patterns {
		if routePatternIdentity(existing) == key {
			return
		}
	}
	rt.patterns = append(rt.patterns, candidate)
}

type routeTemplateSourceObserverIdentity struct {
	sourceTemplatePath     string
	sourceLocalEvent       string
	subscriberRole         resolvedSubscriberRoleIdentity
	subscriberInstancePath string
}

func routeTemplateSourceObserverKey(observer routeTemplateSourceObserver) routeTemplateSourceObserverIdentity {
	return routeTemplateSourceObserverIdentity{
		sourceTemplatePath:     eventidentity.Normalize(observer.SourceTemplatePath),
		sourceLocalEvent:       eventidentity.Normalize(observer.SourceLocalEvent),
		subscriberRole:         resolvedSubscriberRoleKey(observer.Subscriber),
		subscriberInstancePath: strings.Trim(strings.TrimSpace(observer.SubscriberInstancePath), "/"),
	}
}

type routePatternIdentityKey struct {
	eventPattern       string
	subscriberRole     resolvedSubscriberRoleIdentity
	instancePath       string
	sourceInstancePath string
}

func routePatternIdentity(pattern routePattern) routePatternIdentityKey {
	return routePatternIdentityKey{
		eventPattern:       eventidentity.Normalize(pattern.EventPattern),
		subscriberRole:     resolvedSubscriberRoleKey(pattern.Subscriber),
		instancePath:       strings.Trim(strings.TrimSpace(pattern.InstancePath), "/"),
		sourceInstancePath: strings.Trim(strings.TrimSpace(pattern.SourceInstancePath), "/"),
	}
}

func routeApplyResolvedPattern(subscriber Subscriber, resolved routeResolvedPattern) Subscriber {
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

func (rt *RouteTable) rebuildLocked() {
	rt.routes = make(map[string][]Subscriber)
	eventTypes := sortedStringKeys(rt.eventPath)
	for _, pattern := range rt.patterns {
		if strings.Contains(pattern.EventPattern, "*") {
			for _, eventType := range eventTypes {
				if RouteMatches(pattern.EventPattern, eventType) {
					subscriber := pattern.Subscriber
					if strings.TrimSpace(subscriber.MatchPattern) == "" {
						subscriber.MatchPattern = pattern.EventPattern
					}
					rt.routes[eventType] = appendUniqueSubscriber(rt.routes[eventType], subscriber)
				}
			}
			continue
		}
		subscriber := pattern.Subscriber
		if strings.TrimSpace(subscriber.MatchPattern) == "" {
			subscriber.MatchPattern = pattern.EventPattern
		}
		rt.routes[pattern.EventPattern] = appendUniqueSubscriber(rt.routes[pattern.EventPattern], subscriber)
	}
}

func routeFlowLocalEventSet(source semanticview.Source, scope semanticview.FlowScope) map[string]struct{} {
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
		if eventType == "" || routeFlowInputHasExternalProducer(source, scope.ID, eventType) {
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
	switch {
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress):
		return true
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress):
		return true
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect):
		return true
	case resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryHarnessInjection):
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

func routeSubscriberTemplates(source semanticview.Source, scope semanticview.FlowScope, agents []routeAgentDeclaration) ([]routeSubscriberTemplate, error) {
	out := make([]routeSubscriberTemplate, 0, len(agents)+len(scope.Nodes))
	localEvents := routeFlowLocalEventSet(source, scope)
	for _, agent := range agents {
		declaration := agent.Declaration
		key := strings.TrimSpace(declaration.LocalID)
		entry := declaration.Entry
		patterns := normalizeStringList(entry.Subscriptions)
		if len(patterns) == 0 {
			continue
		}
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberAgent, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber agent %s: %s", key, admission.Message())
			}
		}
		out = append(out, routeSubscriberTemplate{
			Kind:          subscriberAgent,
			RawPatterns:   append([]string{}, patterns...),
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
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberNode, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber node %s: %s", semanticNodeID, admission.Message())
			}
		}
		out = append(out, routeSubscriberTemplate{
			Kind:        subscriberNode,
			RawPatterns: append([]string{}, patterns...),
			HandlerNode: handlerNode,
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

func routeFlowPath(source semanticview.Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	if flowID == "." {
		return "."
	}
	if source != nil {
		if path := source.FlowPath(flowID); path != "" {
			return path
		}
	}
	return flowID
}

func routeFlowIDForPath(source semanticview.Source, flowPath string) string {
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if source == nil || flowPath == "" {
		return ""
	}
	for _, scope := range source.FlowScopes() {
		if strings.Trim(strings.TrimSpace(scope.Path), "/") == flowPath {
			return strings.TrimSpace(scope.ID)
		}
	}
	scopePath := strings.TrimSpace(runtimeflowidentity.SemanticScopeFromInstancePath(flowPath))
	if scopePath == "" {
		return ""
	}
	for _, scope := range source.FlowScopes() {
		if strings.Trim(strings.TrimSpace(scope.Path), "/") == scopePath {
			return strings.TrimSpace(scope.ID)
		}
	}
	return ""
}

func routeResolveSubscriberPatterns(source semanticview.Source, kind subscriberKind, flowID string, inputEvents []string, authorityPath, routePath string, localEvents map[string]struct{}, raw string) ([]routeResolvedPattern, error) {
	raw = eventidentity.Normalize(raw)
	flowID = strings.TrimSpace(flowID)
	if raw == "" {
		return nil, nil
	}
	admission := routeClassifyAuthoredSubscription(source, kind, flowID, inputEvents, authorityPath, localEvents, raw)
	if !admission.Admitted() {
		return nil, fmt.Errorf("route subscriber %s", admission.Message())
	}
	localEvent := admission.LocalEvent()
	if !admission.Pattern() && flowID != "" && source != nil && source.FlowHasInputEvent(flowID, localEvent) {
		patterns := routeInputProducerPatterns(runtimepinrouting.ResolveFlowInputProducer(source, flowID, localEvent).AutoWireResolution())
		if len(patterns) > 0 {
			return patterns, nil
		}
	}
	out := make([]routeResolvedPattern, 0, len(admission.RoutePatterns()))
	for _, pattern := range admission.RoutePatterns() {
		out = append(out, routeResolvedPattern{
			EventPattern:   rebaseAdmittedSubscriptionPattern(pattern, authorityPath, routePath),
			routeSource:    subscriberRouteSourceSubscription,
			LocalizedEvent: admission.LocalEvent(),
		})
	}
	return out, nil
}

func rebaseAdmittedSubscriptionPattern(pattern, authorityPath, routePath string) string {
	pattern = eventidentity.Normalize(pattern)
	authorityPath = eventidentity.Normalize(authorityPath)
	routePath = eventidentity.Normalize(routePath)
	if pattern == "" || authorityPath == routePath || authorityPath == "" {
		return pattern
	}
	if pattern == authorityPath {
		return routePath
	}
	if strings.HasPrefix(pattern, authorityPath+"/") {
		return eventidentity.Normalize(routePath + strings.TrimPrefix(pattern, authorityPath))
	}
	return pattern
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

func normalizedStringListContains(values []string, needle string) bool {
	needle = eventidentity.Normalize(needle)
	if needle == "" {
		return false
	}
	for _, value := range values {
		if eventidentity.Normalize(value) == needle {
			return true
		}
	}
	return false
}

type resolvedSubscriberRoleIdentity struct {
	recipient      events.DeliveryRecipient
	path           string
	agentIdentity  agentidentity.Identity
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
		agentIdentity:  subscriber.AgentIdentity.Normalize(),
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

func appendUniqueRootInputSubscriber(in []Subscriber, subscriber Subscriber) []Subscriber {
	return appendUniqueSubscriber(in, subscriber)
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

func cloneSubscribers(in []Subscriber) []Subscriber {
	if len(in) == 0 {
		return nil
	}
	out := make([]Subscriber, len(in))
	copy(out, in)
	return out
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
