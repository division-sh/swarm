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

func (rt *RouteTable) ResolveIndependentPubsubForRun(runID, eventType string) []Subscriber {
	resolved := rt.ResolveForRun(runID, eventType)
	out := make([]Subscriber, 0, len(resolved))
	for _, subscriber := range resolved {
		if independentPubsubSubscriber(subscriber) {
			out = append(out, subscriber)
		}
	}
	return out
}

func (rt *RouteTable) ResolveIndependentPubsubFromSource(runID string, eventType events.EventType, source events.RoutingSource) []Subscriber {
	var out []Subscriber
	for _, key := range SourceEventRouteKeys(eventType, source) {
		for _, subscriber := range rt.ResolveIndependentPubsubForRun(runID, key) {
			out = appendUniqueSubscriber(out, subscriber)
		}
	}
	return out
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
	mu                          sync.RWMutex
	generationMu                sync.RWMutex
	generation                  uint64
	source                      semanticview.Source
	routes                      map[routeResolutionKey][]Subscriber
	patterns                    []routePattern
	exactPatternIndexes         map[string][]int
	wildcardPatternIndexes      []int
	resolutionIndexDirty        bool
	eventPath                   map[string]struct{}
	authoredEventPath           map[string]struct{}
	authoredScopes              map[string]struct{}
	templates                   map[string]routeFlowTemplate
	instanceOwners              map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.RunScopedFlowInstance
	publications                map[runtimeflowidentity.RunScopedFlowInstance]flowRoutePublicationRecord
	nextPublication             uint64
	instanceEventPath           map[runtimeflowidentity.RunScopedFlowInstance][]string
	templateObservers           map[string][]routeTemplateSourceObserver
	connectGraph                runtimepinrouting.CompiledConnectGraph
	inputProducers              runtimepinrouting.FlowInputProducerResolver
	compiledSourceReady         bool
	connectRecipients           []routeConnectRecipientRegistration
	connectRecipientsByInstance map[routeConnectRecipientKey][]routeConnectRecipientRegistration
	nextConnectRecipientOrdinal uint64
}

type routeConnectRecipientKey struct {
	runID        string
	instancePath string
}

type routeConnectRecipientRegistration struct {
	registration runtimepinrouting.ConnectRecipientRegistration
	runID        string
	instancePath string
	ordinal      uint64
}

type routeResolutionKey struct {
	runID     string
	eventType string
}

type routePattern struct {
	RunID              string
	EventPattern       string
	Subscriber         Subscriber
	InstancePath       string
	SourceInstancePath string
}

type routeTemplateSourceObserver struct {
	RunID                  string
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

func (rt *RouteTable) compiledRouteOwnerDependencies(inputProducers runtimepinrouting.FlowInputProducerResolver) []runtimepinrouting.RouteOwnerDependency {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	dependencies := make([]runtimepinrouting.RouteOwnerDependency, 0)
	for receiverPath, template := range rt.templates {
		for _, subscriber := range template.Subscribers {
			for _, pattern := range subscriber.Patterns {
				for _, resolved := range routeProjectAdmittedSubscriberPatterns(pattern.admission, template.FlowID, receiverPath, pattern.inputEvent, inputProducers) {
					if resolved.SourceTemplatePath == "" {
						continue
					}
					dependencies = append(dependencies, runtimepinrouting.RouteOwnerDependency{
						SourceFlowPath: resolved.SourceTemplatePath, ReceiverFlowPath: receiverPath,
					})
				}
			}
		}
	}
	for sourcePath, observers := range rt.templateObservers {
		for _, observer := range observers {
			if observer.SubscriberInstancePath == "" {
				continue
			}
			dependencies = append(dependencies, runtimepinrouting.RouteOwnerDependency{
				SourceFlowPath:   sourcePath,
				ReceiverFlowPath: runtimeflowidentity.SemanticScopeFromInstancePath(observer.SubscriberInstancePath),
			})
		}
	}
	return dependencies
}

func (rt *RouteTable) activeTemplateIDsForFlowPaths(paths []string) []string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if template, ok := rt.templates[path]; ok {
			seen[template.FlowID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
	subscription       semanticview.AuthoredSubscriptionAdmission
	EventPattern       string
	MatchPattern       string
	routeSource        subscriberRouteSource
	LocalizedEvent     string
	RoutePath          string
	SourceTemplatePath string
	SourceLocalEvent   string
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
		if strings.EqualFold(scope.Mode, "template") || routeFlowStanding(source, scope.ID) {
			subscribers, err := routeSubscriberTemplates(source, scope, agents, localEvents)
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
		if err := rt.addAgentPatternsLocked(source, scope.ID, scope.InputEvents, flowPath, localEvents, agents, inputProducers); err != nil {
			return nil, err
		}
		nodes, err := routeExecutableNodeDeclarations(source, scope.ID, scope.Nodes)
		if err != nil {
			return nil, err
		}
		if err := rt.addNodePatternsLocked(source, scope.ID, scope.ID, scope.InputEvents, flowPath, localEvents, nodes, inputProducers); err != nil {
			return nil, err
		}
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
	return rt.ResolveForRun("", eventType)
}

func (rt *RouteTable) staticAgentDeclarationPlans() map[agentidentity.Plan]struct{} {
	plans := make(map[agentidentity.Plan]struct{})
	if rt == nil {
		return plans
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	for _, pattern := range rt.patterns {
		subscriber := pattern.Subscriber
		if pattern.RunID == "" && subscriber.agentLifecycle == agentLifecycleAdmissionStaticDeclaration {
			plans[subscriber.AgentPlan.Normalize()] = struct{}{}
		}
	}
	return plans
}

func (rt *RouteTable) ResolveForRun(runID, eventType string) []Subscriber {
	if rt == nil {
		return nil
	}
	runID = strings.TrimSpace(runID)
	eventType = strings.Trim(strings.TrimSpace(eventType), "/")
	if eventType == "" {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := cloneSubscribers(rt.routes[routeResolutionKey{runID: runID, eventType: eventType}])
	if runID != "" {
		for _, subscriber := range rt.routes[routeResolutionKey{eventType: eventType}] {
			out = appendUniqueSubscriber(out, subscriber)
		}
	}
	if _, active := rt.eventPath[eventType]; !active {
		return projectSubscriberEvents(out, eventType)
	}
	indexes := rt.wildcardPatternIndexes
	if rt.resolutionIndexDirty {
		indexes = make([]int, len(rt.patterns))
		for index := range rt.patterns {
			indexes[index] = index
		}
	}
	for _, index := range indexes {
		pattern := rt.patterns[index]
		if pattern.RunID != "" && pattern.RunID != runID {
			continue
		}
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
	return projectSubscriberEvents(out, eventType)
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

func (rt *RouteTable) EvaluateConnectSource(runID string, sourceEvent runtimepinrouting.SourceEvent) runtimepinrouting.ConnectRecipientEvaluation {
	if rt == nil || strings.TrimSpace(runID) == "" {
		return runtimepinrouting.ConnectRecipientEvaluation{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.connectGraph.EvaluateSourceRecipients(sourceEvent, rt.connectRecipientAdmissionsForRunLocked(runID))
}

// EvaluateConnectPlan uses the same run-scoped registrations and evaluator as
// live delivery planning; targets must come from canonical materialization.
func (rt *RouteTable) EvaluateConnectPlan(runID string, plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) runtimepinrouting.ConnectRecipientEvaluation {
	return rt.evaluateConnectPlan(runID, plan, targets)
}

func (rt *RouteTable) evaluateConnectPlan(runID string, plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) runtimepinrouting.ConnectRecipientEvaluation {
	if rt == nil || strings.TrimSpace(runID) == "" {
		return runtimepinrouting.ConnectRecipientEvaluation{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.connectGraph.EvaluateMaterializedRecipients(plan, targets, rt.connectRecipientAdmissionsForRunLocked(runID))
}

func (rt *RouteTable) connectRecipientAdmissionsForRunLocked(runID string) []runtimepinrouting.ConnectRecipientRegistration {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil
	}
	out := make([]runtimepinrouting.ConnectRecipientRegistration, 0, len(rt.connectRecipients))
	for _, registration := range rt.connectRecipients {
		if registration.runID != "" && registration.runID != runID {
			continue
		}
		out = append(out, registration.registration)
	}
	return out
}

func (rt *RouteTable) connectRecipientAdmissionsForRun(runID string) []runtimepinrouting.ConnectRecipientRegistration {
	if rt == nil || strings.TrimSpace(runID) == "" {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.connectRecipientAdmissionsForRunLocked(runID)
}

func (rt *RouteTable) connectRecipientAdmissionsForTargets(runID string, plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) []runtimepinrouting.ConnectRecipientRegistration {
	if rt == nil || plan.FanIn() != nil || len(targets) == 0 {
		return rt.connectRecipientAdmissionsForRun(runID)
	}
	paths := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		if target.Empty() || target.FlowInstance == "" {
			return rt.connectRecipientAdmissionsForRun(runID)
		}
		path := target.FlowInstance
		if target.FlowID == "." {
			path = "."
		}
		paths[path] = struct{}{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	var selected []routeConnectRecipientRegistration
	selected = append(selected, rt.connectRecipientsByInstance[routeConnectRecipientKey{}]...)
	if runID != "" {
		selected = append(selected, rt.connectRecipientsByInstance[routeConnectRecipientKey{runID: runID}]...)
	}
	for path := range paths {
		if path == "" {
			continue
		}
		selected = append(selected, rt.connectRecipientsByInstance[routeConnectRecipientKey{instancePath: path}]...)
		if runID != "" {
			selected = append(selected, rt.connectRecipientsByInstance[routeConnectRecipientKey{runID: runID, instancePath: path}]...)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].ordinal < selected[j].ordinal })
	out := make([]runtimepinrouting.ConnectRecipientRegistration, 0, len(selected))
	for _, item := range selected {
		out = append(out, item.registration)
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
	return rt.addFlowInstanceRouteForContextWithInputProducers(nil, req, nil)
}

func (rt *RouteTable) addFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest, inputProducers *runtimepinrouting.FlowInputProducerResolver) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	priorPatterns := len(rt.patterns)
	changed, newEventPaths, err := rt.addFlowInstanceRouteLocked(req, inputProducers)
	if err != nil {
		return err
	}
	if changed {
		if rt.resolutionIndexDirty {
			rt.rebuildLocked()
		} else {
			for _, eventType := range newEventPaths {
				rt.resolveNewEventPathLocked(eventType)
			}
			var eventTypes []string
			for index := priorPatterns; index < len(rt.patterns); index++ {
				if eventTypes == nil && strings.Contains(rt.patterns[index].EventPattern, "*") {
					eventTypes = sortedStringKeys(rt.eventPath)
				}
				rt.indexPatternLocked(index, eventTypes)
			}
		}
		rt.generation++
	}
	return nil
}

// A private staged topology can defer its resolution index rebuild while
// retaining the ordinary request order and observer materialization.
func (rt *RouteTable) addFlowInstanceRouteForTopology(req FlowInstanceRouteMaterializationRequest, inputProducers *runtimepinrouting.FlowInputProducerResolver) (bool, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	added, _, err := rt.addFlowInstanceRouteLocked(req, inputProducers)
	if added {
		rt.resolutionIndexDirty = true
		rt.generation++
	}
	return added, err
}

func (rt *RouteTable) rebuildStagedFlowInstanceRoutes() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.rebuildLocked()
}

func (rt *RouteTable) addFlowInstanceRouteLocked(req FlowInstanceRouteMaterializationRequest, inputProducers *runtimepinrouting.FlowInputProducerResolver) (bool, []string, error) {
	req = req.Normalized()

	identity, replay, err := rt.admitFlowInstanceRouteIdentityLocked(req.Identity)
	if err != nil {
		return false, nil, err
	}
	if replay {
		return false, nil, nil
	}
	req.Identity = identity
	instancePath := identity.Route.InstancePath
	templateScope := identity.Route.ScopeKey

	templateDef, ok := rt.templates[templateScope]
	if !ok {
		return false, nil, fmt.Errorf("route template %q not found", templateScope)
	}
	if inputProducers == nil {
		if !rt.compiledSourceReady {
			return false, nil, fmt.Errorf("flow-instance route materialization requires paired compiled source")
		}
		prepared := rt.inputProducers
		inputProducers = &prepared
	}
	rt.instanceOwners[identity] = identity
	allEventPaths, newEventPaths := rt.addEventPathsLocked(instancePath, templateDef.LocalEvents)
	rt.instanceEventPath[identity] = allEventPaths
	rt.materializeTemplateSourceObserversLocked(identity)
	for _, subscriberTemplate := range templateDef.Subscribers {
		subscriberID := ""
		var name agentidentity.Name
		var err error
		if subscriberTemplate.Kind == subscriberAgent {
			name, err = subscriberTemplate.AgentNamePlan.Materialize()
			if err != nil {
				return false, nil, fmt.Errorf("materialize route subscriber agent name: %w", err)
			}
			subscriberID = name.AgentID
		}
		recipient, err := subscriberRecipient(subscriberTemplate.Kind, subscriberID, subscriberTemplate.HandlerNode)
		if err != nil {
			return false, nil, fmt.Errorf("materialize route subscriber: %w", err)
		}
		subscriber := Subscriber{
			Recipient: recipient, Path: instancePath,
			handlerNode: subscriberTemplate.HandlerNode,
		}
		if subscriber.Recipient.IsAgent() {
			agentRoute, err := identity.Route.AgentIdentityRoute()
			if err != nil {
				return false, nil, fmt.Errorf("materialize route subscriber flow identity: %w", err)
			}
			subscriber.AgentPlan, err = agentidentity.NewPlan(name, agentRoute)
			if err != nil {
				return false, nil, fmt.Errorf("materialize route subscriber concrete identity: %w", err)
			}
		}
		for _, pattern := range subscriberTemplate.Patterns {
			admittedSubscriber := subscriber
			if admittedSubscriber.Recipient.IsNode() {
				admittedSubscriber.targetHandler = subscriberTemplate.TargetHandler
			}
			if err := rt.addConnectRecipientLocked(templateDef.FlowID, templateDef.InputEvents, pattern.raw, admittedSubscriber, identity.RunID, instancePath); err != nil {
				return false, nil, err
			}
			resolvedPatterns := routeProjectAdmittedSubscriberPatterns(pattern.admission, templateDef.FlowID, instancePath, pattern.inputEvent, *inputProducers)
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(admittedSubscriber, resolved, identity.RunID, instancePath)
			}
		}
	}
	return true, newEventPaths, nil
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
	return exists && flowInstanceRouteIdentityEqual(owner, identity)
}

func (rt *RouteTable) flowInstanceTemplateID(identity runtimeflowidentity.Route) (string, bool) {
	if rt == nil {
		return "", false
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return "", false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	template, exists := rt.templates[identity.ScopeKey]
	return strings.TrimSpace(template.FlowID), exists
}

func (rt *RouteTable) flowInstanceRouteRemovalOwner(identity runtimeflowidentity.RunScopedFlowInstance) (runtimeflowidentity.RunScopedFlowInstance, bool, error) {
	if rt == nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf("route table is required")
	}
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, err
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.matchFlowInstanceRouteOwnerLocked(identity)
}

func (rt *RouteTable) RemoveFlowInstanceRoute(identity runtimeflowidentity.RunScopedFlowInstance) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
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
	instancePath := owner.Route.InstancePath
	delete(rt.instanceOwners, owner)
	delete(rt.publications, owner)
	delete(rt.instanceEventPath, owner)
	filtered := rt.patterns[:0]
	for _, pattern := range rt.patterns {
		if pattern.RunID == owner.RunID && (pattern.InstancePath == instancePath || pattern.SourceInstancePath == instancePath) {
			continue
		}
		filtered = append(filtered, pattern)
	}
	rt.patterns = filtered
	filteredConnect := rt.connectRecipients[:0]
	for _, registration := range rt.connectRecipients {
		if registration.runID == owner.RunID && registration.instancePath == instancePath {
			continue
		}
		filteredConnect = append(filteredConnect, registration)
	}
	rt.connectRecipients = filteredConnect
	delete(rt.connectRecipientsByInstance, routeConnectRecipientKey{runID: owner.RunID, instancePath: instancePath})
	for sourceTemplatePath, observers := range rt.templateObservers {
		filteredObservers := observers[:0]
		for _, observer := range observers {
			if observer.RunID == owner.RunID && observer.SubscriberInstancePath == instancePath {
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
	rt.rebuildEventPathsLocked()
	rt.rebuildLocked()
	rt.generation++
	return nil
}

func (rt *RouteTable) MaterializedRoutes(identity runtimeflowidentity.RunScopedFlowInstance) []FlowInstanceRouteRecord {
	if rt == nil {
		return nil
	}
	return rt.materializedRouteRecordSets([]runtimeflowidentity.RunScopedFlowInstance{identity})[0].Routes
}

func (rt *RouteTable) materializedRouteRecordSets(identities []runtimeflowidentity.RunScopedFlowInstance) []FlowInstanceRouteRecordSet {
	sets := make([]FlowInstanceRouteRecordSet, 0, len(identities))
	for _, identity := range identities {
		sets = append(sets, FlowInstanceRouteRecordSet{Identity: identity})
	}
	if rt == nil || len(sets) == 0 {
		return sets
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	type instanceKey struct{ runID, instancePath string }
	type materializedRouteIdentity struct {
		instancePath string
		eventPattern string
		recipient    events.DeliveryRecipient
	}
	type recordGroup struct {
		identity runtimeflowidentity.RunScopedFlowInstance
		seen     map[materializedRouteIdentity]struct{}
		records  []FlowInstanceRouteRecord
	}
	groups := make(map[instanceKey]*recordGroup, len(sets))
	for _, set := range sets {
		identity, err := normalizeFlowInstanceRouteIdentity(set.Identity)
		if err != nil {
			continue
		}
		owner, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
		if err != nil || !exists || !flowInstanceRouteIdentityEqual(owner, identity) {
			continue
		}
		key := instanceKey{runID: identity.RunID, instancePath: identity.Route.InstancePath}
		if _, exists := groups[key]; !exists {
			groups[key] = &recordGroup{
				identity: identity,
				seen:     make(map[materializedRouteIdentity]struct{}),
				records:  make([]FlowInstanceRouteRecord, 0, 8),
			}
		}
	}
	for _, pattern := range rt.patterns {
		key := instanceKey{runID: pattern.RunID, instancePath: strings.Trim(strings.TrimSpace(pattern.InstancePath), "/")}
		group := groups[key]
		if group == nil {
			continue
		}
		record := FlowInstanceRouteRecord{
			Identity:       group.identity,
			EventPattern:   strings.TrimSpace(pattern.EventPattern),
			SubscriberType: pattern.Subscriber.Recipient.Code(),
			SubscriberID:   pattern.Subscriber.Recipient.ID(),
			SourceFlow:     group.identity.Route.ScopeKey,
		}
		recordKey := materializedRouteIdentity{
			instancePath: record.Identity.Route.InstancePath,
			eventPattern: record.EventPattern,
			recipient:    pattern.Subscriber.Recipient,
		}
		if _, exists := group.seen[recordKey]; exists {
			continue
		}
		group.seen[recordKey] = struct{}{}
		group.records = append(group.records, record)
	}
	for _, group := range groups {
		sort.Slice(group.records, func(i, j int) bool {
			if group.records[i].EventPattern != group.records[j].EventPattern {
				return group.records[i].EventPattern < group.records[j].EventPattern
			}
			if group.records[i].SubscriberType != group.records[j].SubscriberType {
				return group.records[i].SubscriberType < group.records[j].SubscriberType
			}
			return group.records[i].SubscriberID < group.records[j].SubscriberID
		})
	}
	for index := range sets {
		identity, err := normalizeFlowInstanceRouteIdentity(sets[index].Identity)
		if err != nil {
			continue
		}
		group := groups[instanceKey{runID: identity.RunID, instancePath: identity.Route.InstancePath}]
		if group != nil && flowInstanceRouteIdentityEqual(group.identity, identity) {
			sets[index].Routes = append(make([]FlowInstanceRouteRecord, 0, len(group.records)), group.records...)
		}
	}
	return sets
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
		generation:                  1,
		resolutionIndexDirty:        true,
		source:                      source,
		routes:                      make(map[routeResolutionKey][]Subscriber),
		eventPath:                   make(map[string]struct{}),
		authoredEventPath:           make(map[string]struct{}),
		authoredScopes:              make(map[string]struct{}),
		templates:                   make(map[string]routeFlowTemplate),
		instanceOwners:              make(map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.RunScopedFlowInstance),
		publications:                make(map[runtimeflowidentity.RunScopedFlowInstance]flowRoutePublicationRecord),
		instanceEventPath:           make(map[runtimeflowidentity.RunScopedFlowInstance][]string),
		templateObservers:           make(map[string][]routeTemplateSourceObserver),
		connectGraph:                graph,
		connectRecipientsByInstance: make(map[routeConnectRecipientKey][]routeConnectRecipientRegistration),
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
	return rt.addFlowInstanceRouteForContextWithInputProducers(ctx, req, nil)
}

// A supplied resolver belongs only to the current unchanged-source topology
// operation; the table retains its graph but never retains this resolver.
func (rt *RouteTable) addFlowInstanceRouteForContextWithInputProducers(ctx context.Context, req FlowInstanceRouteMaterializationRequest, inputProducers *runtimepinrouting.FlowInputProducerResolver) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}
	if ctx != nil {
		if lease, _ := ctx.Value(routeTableGenerationLeaseKey{}).(routeTableGenerationLease); lease.table == rt {
			return rt.addFlowInstanceRoute(req, inputProducers)
		}
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	return rt.addFlowInstanceRoute(req, inputProducers)
}

func (rt *RouteTable) removeFlowInstanceRouteForContext(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if ctx != nil {
		if lease, _ := ctx.Value(routeTableGenerationLeaseKey{}).(routeTableGenerationLease); lease.table == rt {
			return rt.removeFlowInstanceRoute(identity)
		}
	}
	return rt.RemoveFlowInstanceRoute(identity)
}

func (rt *RouteTable) addEventPathsLocked(basePath string, localEvents map[string]struct{}) ([]string, []string) {
	added := make([]string, 0, len(localEvents))
	newPaths := make([]string, 0, len(localEvents))
	scope := routeEventIdentityScope(basePath, localEvents, nil)
	for _, eventType := range sortedStringKeys(localEvents) {
		absolute := scope.ResolveEvent(eventType, nil)
		if absolute == "" || strings.Contains(absolute, "*") {
			continue
		}
		if _, exists := rt.eventPath[absolute]; !exists {
			newPaths = append(newPaths, absolute)
		}
		rt.eventPath[absolute] = struct{}{}
		added = append(added, absolute)
	}
	return added, newPaths
}

func (rt *RouteTable) addAuthoredEventPathsLocked(basePath string, localEvents map[string]struct{}) []string {
	added, _ := rt.addEventPathsLocked(basePath, localEvents)
	for _, eventType := range added {
		rt.authoredEventPath[eventType] = struct{}{}
	}
	return added
}

func (rt *RouteTable) rebuildEventPathsLocked() {
	rt.eventPath = make(map[string]struct{}, len(rt.authoredEventPath)+len(rt.instanceEventPath))
	for eventType := range rt.authoredEventPath {
		rt.eventPath[eventType] = struct{}{}
	}
	for _, eventTypes := range rt.instanceEventPath {
		for _, eventType := range eventTypes {
			rt.eventPath[eventType] = struct{}{}
		}
	}
}

func (rt *RouteTable) admitFlowInstanceRouteIdentityLocked(raw runtimeflowidentity.RunScopedFlowInstance) (runtimeflowidentity.RunScopedFlowInstance, bool, error) {
	identity, err := normalizeFlowInstanceRouteIdentity(raw)
	if err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, err
	}
	_, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
	if err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, err
	}
	if exists {
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
		if !flowInstanceRouteIdentityEqual(owner, identity) {
			return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf(
				"flow-instance path %q is owned by scope %q instance %q, not scope %q instance %q",
				identity.Route.InstancePath,
				owner.Route.ScopeKey,
				owner.Route.InstanceID,
				identity.Route.ScopeKey,
				identity.Route.InstanceID,
			)
		}
		return owner, true, nil
	}
	expected := runtimeflowidentity.StoredRoute(identity.Route.ScopeKey, identity.Route.InstanceID, "")
	singleton := identity.Route.InstancePath == identity.Route.ScopeKey &&
		identity.Route.InstanceID == runtimeflowidentity.LogicalInstanceID(identity.Route.ScopeKey)
	if !singleton && expected.InstancePath != identity.Route.InstancePath {
		return runtimeflowidentity.RunScopedFlowInstance{}, false, fmt.Errorf(
			"flow-instance route identity is inconsistent: scope %q and instance %q derive path %q, not %q",
			identity.Route.ScopeKey,
			identity.Route.InstanceID,
			expected.InstancePath,
			identity.Route.InstancePath,
		)
	}
	return runtimeflowidentity.RunScopedFlowInstance{}, false, nil
}

func flowInstanceRouteIdentityEqual(left, right runtimeflowidentity.RunScopedFlowInstance) bool {
	return left == right
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
	inputProducers runtimepinrouting.FlowInputProducerResolver,
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
			Recipient:      events.MustAgentDeliveryRecipient(name.AgentID),
			Path:           strings.Trim(strings.TrimSpace(agentPath), "/"),
			agentLifecycle: agentLifecycleAdmissionStaticDeclaration,
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
		subscriber.AgentPlan, err = agentidentity.NewPlan(name, route)
		if err != nil {
			return fmt.Errorf("route subscriber agent %s concrete identity: %w", key, err)
		}
		for _, rawPattern := range normalizeStringList(entry.Subscriptions) {
			if err := rt.addConnectRecipientLocked(agentFlowID, inputEvents, rawPattern, subscriber, "", ""); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatternsWithInputProducers(source, subscriberAgent, agentFlowID, inputEvents, agentPath, agentPath, localEvents, rawPattern, inputProducers)
			if err != nil {
				return err
			}
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(subscriber, resolved, "", "")
			}
		}
	}
	return nil
}

func (rt *RouteTable) addNodePatternsLocked(source semanticview.Source, routingFlowID, connectFlowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, nodes []routeExecutableNodeDeclaration, inputProducers runtimepinrouting.FlowInputProducerResolver) error {
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
			if err := rt.addConnectRecipientLocked(connectFlowID, inputEvents, rawPattern, admittedSubscriber, "", ""); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatternsWithInputProducers(source, subscriberNode, routingFlowID, inputEvents, basePath, basePath, localEvents, rawPattern, inputProducers)
			if err != nil {
				return err
			}
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(admittedSubscriber, resolved, "", "")
			}
		}
	}
	return nil
}

func (rt *RouteTable) addConnectRecipientLocked(flowID string, inputEvents []string, eventPattern string, subscriber Subscriber, runID, instancePath string) error {
	var recipient runtimepinrouting.ConnectRecipient
	var err error
	switch {
	case subscriber.Recipient.IsNode():
		recipient, err = runtimepinrouting.NewConnectNodeRecipient(
			subscriber.handlerNode, subscriber.Path,
		)
	case subscriber.Recipient.IsAgent():
		recipient, err = runtimepinrouting.NewConnectAgentRecipient(subscriber.Recipient.ID(), subscriber.Path, subscriber.AgentPlan)
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
			rt.nextConnectRecipientOrdinal++
			item := routeConnectRecipientRegistration{
				registration: registration,
				runID:        strings.TrimSpace(runID),
				instancePath: strings.Trim(strings.TrimSpace(instancePath), "/"),
				ordinal:      rt.nextConnectRecipientOrdinal,
			}
			rt.connectRecipients = append(rt.connectRecipients, item)
			key := routeConnectRecipientKey{runID: item.runID, instancePath: item.instancePath}
			rt.connectRecipientsByInstance[key] = append(rt.connectRecipientsByInstance[key], item)
		}
	}
	return nil
}

func (rt *RouteTable) addResolvedPatternLocked(subscriber Subscriber, resolved routeResolvedPattern, runID, subscriberInstancePath string) {
	resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
	sourceTemplatePath := eventidentity.Normalize(resolved.SourceTemplatePath)
	sourceLocalEvent := eventidentity.Normalize(resolved.SourceLocalEvent)
	if sourceTemplatePath != "" || sourceLocalEvent != "" {
		if sourceTemplatePath == "" || sourceLocalEvent == "" {
			return
		}
		rt.addTemplateSourceObserverLocked(routeTemplateSourceObserver{
			RunID:                  strings.TrimSpace(runID),
			SourceTemplatePath:     sourceTemplatePath,
			SourceLocalEvent:       sourceLocalEvent,
			Subscriber:             resolvedSubscriber,
			SubscriberInstancePath: strings.Trim(strings.TrimSpace(subscriberInstancePath), "/"),
		})
		return
	}
	rt.patterns = append(rt.patterns, routePattern{
		RunID:        strings.TrimSpace(runID),
		EventPattern: resolved.EventPattern,
		Subscriber:   resolvedSubscriber,
		InstancePath: strings.Trim(strings.TrimSpace(subscriberInstancePath), "/"),
	})
}

func (rt *RouteTable) addTemplateSourceObserverLocked(observer routeTemplateSourceObserver) {
	observer.SourceTemplatePath = eventidentity.Normalize(observer.SourceTemplatePath)
	observer.SourceLocalEvent = eventidentity.Normalize(observer.SourceLocalEvent)
	observer.RunID = strings.TrimSpace(observer.RunID)
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
	for owner := range rt.instanceOwners {
		if owner.Route.ScopeKey == observer.SourceTemplatePath && (observer.RunID == "" || observer.RunID == owner.RunID) {
			rt.materializeTemplateSourceObserverLocked(observer, owner)
		}
	}
}

func (rt *RouteTable) materializeTemplateSourceObserversLocked(owner runtimeflowidentity.RunScopedFlowInstance) {
	for _, observer := range rt.templateObservers[eventidentity.Normalize(owner.Route.ScopeKey)] {
		if observer.RunID == "" || observer.RunID == owner.RunID {
			rt.materializeTemplateSourceObserverLocked(observer, owner)
		}
	}
}

func (rt *RouteTable) materializeTemplateSourceObserverLocked(observer routeTemplateSourceObserver, owner runtimeflowidentity.RunScopedFlowInstance) {
	instancePath := eventidentity.Normalize(owner.Route.InstancePath)
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
		RunID:              owner.RunID,
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
	runID                  string
	sourceTemplatePath     string
	sourceLocalEvent       string
	subscriberRole         resolvedSubscriberRoleIdentity
	subscriberInstancePath string
}

func routeTemplateSourceObserverKey(observer routeTemplateSourceObserver) routeTemplateSourceObserverIdentity {
	return routeTemplateSourceObserverIdentity{
		runID:                  strings.TrimSpace(observer.RunID),
		sourceTemplatePath:     eventidentity.Normalize(observer.SourceTemplatePath),
		sourceLocalEvent:       eventidentity.Normalize(observer.SourceLocalEvent),
		subscriberRole:         resolvedSubscriberRoleKey(observer.Subscriber),
		subscriberInstancePath: strings.Trim(strings.TrimSpace(observer.SubscriberInstancePath), "/"),
	}
}

type routePatternIdentityKey struct {
	runID              string
	eventPattern       string
	subscriberRole     resolvedSubscriberRoleIdentity
	instancePath       string
	sourceInstancePath string
}

func routePatternIdentity(pattern routePattern) routePatternIdentityKey {
	return routePatternIdentityKey{
		runID:              strings.TrimSpace(pattern.RunID),
		eventPattern:       eventidentity.Normalize(pattern.EventPattern),
		subscriberRole:     resolvedSubscriberRoleKey(pattern.Subscriber),
		instancePath:       strings.Trim(strings.TrimSpace(pattern.InstancePath), "/"),
		sourceInstancePath: strings.Trim(strings.TrimSpace(pattern.SourceInstancePath), "/"),
	}
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

func (rt *RouteTable) rebuildLocked() {
	rt.routes = make(map[routeResolutionKey][]Subscriber)
	rt.exactPatternIndexes = make(map[string][]int)
	rt.wildcardPatternIndexes = nil
	eventTypes := sortedStringKeys(rt.eventPath)
	for index := range rt.patterns {
		rt.indexPatternLocked(index, eventTypes)
	}
	rt.resolutionIndexDirty = false
}

func (rt *RouteTable) indexPatternLocked(index int, eventTypes []string) {
	pattern := rt.patterns[index]
	if strings.Contains(pattern.EventPattern, "*") {
		rt.wildcardPatternIndexes = append(rt.wildcardPatternIndexes, index)
		for _, eventType := range eventTypes {
			rt.resolvePatternForEventLocked(pattern, eventType)
		}
		return
	}
	rt.exactPatternIndexes[pattern.EventPattern] = append(rt.exactPatternIndexes[pattern.EventPattern], index)
	rt.resolvePatternForEventLocked(pattern, pattern.EventPattern)
}

func (rt *RouteTable) resolvePatternForEventLocked(pattern routePattern, eventType string) {
	if strings.Contains(pattern.EventPattern, "*") && !RouteMatches(pattern.EventPattern, eventType) {
		return
	}
	subscriber := pattern.Subscriber
	if strings.TrimSpace(subscriber.MatchPattern) == "" {
		subscriber.MatchPattern = pattern.EventPattern
	}
	key := routeResolutionKey{runID: strings.TrimSpace(pattern.RunID), eventType: eventType}
	rt.routes[key] = appendUniqueSubscriber(rt.routes[key], subscriber)
}

// A newly admitted event can already have exact subscribers. Re-evaluate only
// its indexed patterns in their original order so wildcard observers do not
// move behind those subscribers.
func (rt *RouteTable) resolveNewEventPathLocked(eventType string) {
	exact := rt.exactPatternIndexes[eventType]
	for _, index := range exact {
		delete(rt.routes, routeResolutionKey{runID: strings.TrimSpace(rt.patterns[index].RunID), eventType: eventType})
	}
	wildcard := rt.wildcardPatternIndexes
	for len(exact) > 0 || len(wildcard) > 0 {
		var index int
		if len(wildcard) == 0 || len(exact) > 0 && exact[0] < wildcard[0] {
			index, exact = exact[0], exact[1:]
		} else {
			index, wildcard = wildcard[0], wildcard[1:]
		}
		rt.resolvePatternForEventLocked(rt.patterns[index], eventType)
	}
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
