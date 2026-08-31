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
}

func (s Subscriber) RouteSourceCode() string { return s.routeSource.code() }

type subscriberRouteSource uint8

const (
	subscriberRouteSourceSubscription subscriberRouteSource = iota + 1
	subscriberRouteSourcePinAutoWire
	subscriberRouteSourceRootInputProject
	subscriberRouteSourceRootInputFlow
	subscriberRouteSourceConnectRoutePlan
	subscriberRouteSourceImportBoundaryWildcardGrant
	subscriberRouteSourceImportBoundaryWildcardSubtree
)

func subscriberRouteSourceFromCode(code string) (subscriberRouteSource, bool) {
	switch strings.TrimSpace(code) {
	case "subscription":
		return subscriberRouteSourceSubscription, true
	case "pin_auto_wire":
		return subscriberRouteSourcePinAutoWire, true
	case "root_input_project":
		return subscriberRouteSourceRootInputProject, true
	case "root_input_flow":
		return subscriberRouteSourceRootInputFlow, true
	case "connect_route_plan":
		return subscriberRouteSourceConnectRoutePlan, true
	case "import_boundary_wildcard_grant":
		return subscriberRouteSourceImportBoundaryWildcardGrant, true
	case "import_boundary_wildcard_subtree":
		return subscriberRouteSourceImportBoundaryWildcardSubtree, true
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
	case subscriberRouteSourceRootInputProject:
		return "root_input_project"
	case subscriberRouteSourceRootInputFlow:
		return "root_input_flow"
	case subscriberRouteSourceConnectRoutePlan:
		return "connect_route_plan"
	case subscriberRouteSourceImportBoundaryWildcardGrant:
		return "import_boundary_wildcard_grant"
	case subscriberRouteSourceImportBoundaryWildcardSubtree:
		return "import_boundary_wildcard_subtree"
	default:
		return ""
	}
}

func (s subscriberRouteSource) importBoundaryWildcard() bool {
	return s == subscriberRouteSourceImportBoundaryWildcardGrant || s == subscriberRouteSourceImportBoundaryWildcardSubtree
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
	instanceOwners    map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.RunScopedFlowInstance
	instanceEventPath map[runtimeflowidentity.RunScopedFlowInstance][]string
	templateObservers map[string][]routeTemplateSourceObserver
	connectGraph      runtimepinrouting.CompiledConnectGraph
	connectRecipients []routeConnectRecipientRegistration
}

type routeConnectRecipientRegistration struct {
	registration runtimepinrouting.ConnectRecipientRegistration
	runID        string
	instancePath string
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

type routeSubscriberTemplate struct {
	IDTemplate    string
	Kind          subscriberKind
	PackageKey    string
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

	for _, scope := range semanticview.ProjectScopes(source) {
		agents, err := routeRootAgentDeclarationsForProjectScope(source, scope.Key)
		if err != nil {
			return nil, err
		}
		localEvents := routeProjectLocalEventSet(scope)
		agentFlowID := strings.TrimSpace(scope.OwningFlowID)
		agentPath := ""
		if agentFlowID != "" {
			agentPath = routeFlowPath(source, agentFlowID)
		}
		owningFlowID := ""
		basePath := ""
		var inputEvents []string
		if routeProjectScopeRequiresPinAliases(scope) {
			owningFlowID = agentFlowID
			basePath = agentPath
			inputEvents = source.FlowInputEvents(owningFlowID)
		}
		if importedFlowID := routeProjectScopeImportedFlowID(source, scope); importedFlowID != "" && routeProjectScopeOwnedByTemplateFlow(source, importedFlowID) {
			continue
		}
		if routeProjectScopeOwnedByTemplateFlow(source, owningFlowID) {
			continue
		}
		handlerFlowID := agentFlowID
		rt.addAuthoredEventPathsLocked(basePath, localEvents)
		if err := rt.addAgentPatternsLocked(source, agentFlowID, inputEvents, agentPath, localEvents, agents); err != nil {
			return nil, err
		}
		nodes, err := routeExecutableNodeDeclarations(source, scope.Key, handlerFlowID, scope.Nodes)
		if err != nil {
			return nil, err
		}
		if err := rt.addNodePatternsLocked(source, owningFlowID, agentFlowID, inputEvents, basePath, localEvents, nodes); err != nil {
			return nil, err
		}
	}

	for _, scope := range semanticview.FlowScopes(source) {
		flowPackageKey := routeFlowPackageKey(source, scope)
		agents, err := routeAgentDeclarationsForOwner(source, scope.ID)
		if err != nil {
			return nil, err
		}
		flowPath := routeFlowPath(source, scope.ID)
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
		nodes, err := routeExecutableNodeDeclarations(source, flowPackageKey, scope.ID, scope.Nodes)
		if err != nil {
			return nil, err
		}
		if err := rt.addNodePatternsLocked(source, scope.ID, scope.ID, scope.InputEvents, flowPath, localEvents, nodes); err != nil {
			return nil, err
		}
	}

	if err := rt.addTopLevelRootInputNodeRoutesLocked(source); err != nil {
		return nil, err
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
	for _, project := range source.ProjectScopes() {
		for _, ref := range project.Manifest.Flows {
			if strings.TrimSpace(ref.ID) == flowID && ref.HasStandingActivation() {
				return true
			}
		}
	}
	return false
}

func routeProjectScopeImportedFlowID(source semanticview.Source, scope semanticview.ProjectScope) string {
	if source == nil {
		return ""
	}
	key := strings.Trim(strings.TrimSpace(scope.Key), "/")
	if key == "" {
		key = "."
	}
	for _, parent := range semanticview.ProjectScopes(source) {
		for _, site := range semanticview.ImportBoundaryFlowSites(parent) {
			if strings.Trim(strings.TrimSpace(site.PackageKey), "/") == key {
				return strings.TrimSpace(site.FlowID)
			}
		}
	}
	return ""
}

func routeProjectScopeRequiresPinAliases(scope semanticview.ProjectScope) bool {
	return len(scope.Manifest.Requires.Inputs) > 0 || len(scope.Manifest.Requires.Outputs) > 0
}

func routeProjectScopeOwnedByTemplateFlow(source semanticview.Source, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return false
	}
	scope, ok := source.FlowScopeByID(flowID)
	return ok && strings.EqualFold(scope.Mode, "template")
}

func (rt *RouteTable) Resolve(eventType string) []Subscriber {
	return rt.ResolveForRun("", eventType)
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
	out := cloneSubscribers(rt.routes[eventType])
	for _, subscriber := range rt.rootInputRoutes[eventType] {
		out = appendUniqueRootInputSubscriber(out, subscriber)
	}
	if _, active := rt.eventPath[eventType]; !active {
		return out
	}
	for _, pattern := range rt.patterns {
		if pattern.RunID != runID {
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
			LocalizedEvent: string(recipient.HandlerEvent()), AgentPlan: recipient.AgentPlan(),
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
	instancePath := identity.Route.InstancePath
	templateScope := identity.Route.ScopeKey

	templateDef, ok := rt.templates[templateScope]
	if !ok {
		return fmt.Errorf("route template %q not found", templateScope)
	}
	rt.instanceOwners[identity] = identity
	rt.instanceEventPath[identity] = rt.addEventPathsLocked(instancePath, templateDef.LocalEvents)
	rt.materializeTemplateSourceObserversLocked(identity)
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
			agentRoute, err := identity.Route.AgentIdentityRoute()
			if err != nil {
				return fmt.Errorf("materialize route subscriber flow identity: %w", err)
			}
			subscriber.AgentPlan, err = agentidentity.NewPlan(name, agentRoute)
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
			if err := rt.addConnectRecipientLocked(templateDef.FlowID, templateDef.InputEvents, rawPattern, admittedSubscriber, identity.RunID, instancePath); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatterns(rt.source, subscriberTemplate.Kind, subscriberTemplate.PackageKey, templateDef.FlowID, templateDef.InputEvents, templateScope, instancePath, templateDef.LocalEvents, rawPattern)
			if err != nil {
				return err
			}
			for _, resolved := range resolvedPatterns {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				rt.addResolvedPatternLocked(admittedSubscriber, resolved, identity.RunID, instancePath)
			}
		}
	}
	rt.rebuildLocked()
	rt.generation++
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
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return nil
	}
	instancePath := identity.Route.InstancePath
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
		if pattern.RunID != identity.RunID || strings.Trim(strings.TrimSpace(pattern.InstancePath), "/") != instancePath {
			continue
		}
		record := FlowInstanceRouteRecord{
			Identity:       identity,
			EventPattern:   strings.TrimSpace(pattern.EventPattern),
			SubscriberType: pattern.Subscriber.Recipient.Code(),
			SubscriberID:   pattern.Subscriber.Recipient.ID(),
			SourceFlow:     identity.Route.ScopeKey,
		}
		key := materializedRouteIdentity{
			instancePath: record.Identity.Route.InstancePath,
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
		instanceOwners:    make(map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.RunScopedFlowInstance),
		instanceEventPath: make(map[runtimeflowidentity.RunScopedFlowInstance][]string),
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

func (rt *RouteTable) removeFlowInstanceRouteForContext(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if ctx != nil {
		if lease, _ := ctx.Value(routeTableGenerationLeaseKey{}).(routeTableGenerationLease); lease.table == rt {
			return rt.removeFlowInstanceRoute(identity)
		}
	}
	return rt.RemoveFlowInstanceRoute(identity)
}

func (rt *RouteTable) addTopLevelRootInputNodeRoutesLocked(source semanticview.Source) error {
	if rt == nil || source == nil {
		return nil
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil
	}
	rootProjects := bundle.RootProjectViews()
	if len(rootProjects) == 0 {
		return nil
	}
	rootInputs := routeRootInputEventSet(source)
	if len(rootInputs) == 0 {
		return nil
	}
	for _, eventType := range sortedStringKeys(rootInputs) {
		for _, project := range rootProjects {
			nodes, err := routeExecutableNodeDeclarations(source, project.Paths.Key, "", project.Nodes)
			if err != nil {
				return err
			}
			for _, declaration := range nodes {
				handlerNode := declaration.Node
				semanticNodeID := handlerNode.NodeID()
				if !routeNodeSubscribesToLocalExact(source, handlerNode, eventType) {
					continue
				}
				if rootInputFlowOwnsNodeRoute(source, handlerNode, eventType) {
					continue
				}
				subscriber := Subscriber{
					Recipient:    events.MustNodeDeliveryRecipient(handlerNode),
					MatchPattern: eventType,
					routeSource:  subscriberRouteSourceRootInputProject,
					handlerNode:  handlerNode,
				}
				targetHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(
					source, handlerNode,
				)
				if err != nil {
					return fmt.Errorf("admit root target handler %s for %s: %w", semanticNodeID, eventType, err)
				}
				subscriber.targetHandler = targetHandler
				if err := rt.addConnectRecipientLocked("", nil, eventType, subscriber, "", ""); err != nil {
					return fmt.Errorf("admit root connect recipient %s for %s: %w", handlerNode.Key(), eventType, err)
				}
				rt.rootInputRoutes[eventType] = appendUniqueRootInputSubscriber(rt.rootInputRoutes[eventType], subscriber)
			}
		}
	}
	return nil
}

func rootInputFlowOwnsNodeRoute(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) bool {
	for _, scope := range source.FlowScopes() {
		if strings.EqualFold(scope.Mode, "template") || !normalizedStringListContains(scope.InputEvents, eventType) {
			continue
		}
		if strings.TrimSpace(scope.ID) == node.FlowID() {
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
			nodes, err := routeExecutableNodeDeclarations(source, routeFlowPackageKey(source, scope), flowID, scope.Nodes)
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
	for _, eventType := range normalizeStringList(source.FlowInputEvents("")) {
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

func routeRootAgentDeclarationsForProjectScope(source semanticview.Source, packageKey string) ([]routeAgentDeclaration, error) {
	packageKey = normalizedRoutePackageKey(packageKey)
	candidates := make([]semanticview.AgentDeclaration, 0)
	for _, declaration := range semanticview.AgentDeclarationsForOwner(source, "") {
		if strings.TrimSpace(declaration.Source.Layer) != "project" || normalizedRoutePackageKey(declaration.Source.PackageKey) != packageKey {
			continue
		}
		candidates = append(candidates, declaration)
	}
	return routeAgentDeclarations(source, candidates)
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

func normalizedRoutePackageKey(packageKey string) string {
	packageKey = strings.Trim(strings.TrimSpace(packageKey), "/")
	if packageKey == "" {
		return runtimeidentity.RootPackageKey
	}
	return packageKey
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
			Recipient:      events.MustAgentDeliveryRecipient(name.AgentID),
			Path:           strings.Trim(strings.TrimSpace(agentPath), "/"),
			agentLifecycle: agentLifecycleAdmissionStaticDeclaration,
		}
		route := agentidentity.RootRoute()
		if subscriber.Path != "" {
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
			resolvedPatterns, err := routeResolveSubscriberPatterns(source, subscriberAgent, normalizedRoutePackageKey(declaration.Source.PackageKey), agentFlowID, inputEvents, agentPath, agentPath, localEvents, rawPattern)
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
			resolvedPatterns, err := routeResolveSubscriberPatterns(source, subscriberNode, handlerNode.PackageKey(), routingFlowID, inputEvents, basePath, basePath, localEvents, rawPattern)
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
			rt.connectRecipients = append(rt.connectRecipients, routeConnectRecipientRegistration{
				registration: registration,
				runID:        strings.TrimSpace(runID),
				instancePath: strings.Trim(strings.TrimSpace(instancePath), "/"),
			})
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
	subscriber.routeSource = resolved.routeSource
	subscriber.LocalizedEvent = eventidentity.Normalize(resolved.LocalizedEvent)
	if matchPattern := eventidentity.Normalize(resolved.MatchPattern); matchPattern != "" {
		subscriber.MatchPattern = matchPattern
	}
	if subscriber.Path == "" {
		if routePath := eventidentity.Normalize(resolved.RoutePath); routePath != "" {
			subscriber.Path = routePath
		} else if subscriber.routeSource.importBoundaryWildcard() {
			subscriber.Path = routeStaticPrefixBeforeWildcard(resolved.EventPattern)
		}
	}
	return subscriber
}

func routeStaticPrefixBeforeWildcard(pattern string) string {
	pattern = eventidentity.Normalize(pattern)
	if pattern == "" || !strings.Contains(pattern, "*") {
		return ""
	}
	segments := strings.Split(pattern, "/")
	prefix := make([]string, 0, len(segments))
	for _, segment := range segments {
		if strings.Contains(segment, "*") {
			break
		}
		prefix = append(prefix, segment)
	}
	return strings.Join(prefix, "/")
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

func routeProjectLocalEventSet(scope semanticview.ProjectScope) map[string]struct{} {
	return routeEventKeys(scope.Events)
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
		packageKey := normalizedRoutePackageKey(declaration.Source.PackageKey)
		patterns := normalizeStringList(entry.Subscriptions)
		if len(patterns) == 0 {
			continue
		}
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberAgent, packageKey, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber agent %s: %s", key, admission.Message())
			}
		}
		out = append(out, routeSubscriberTemplate{
			Kind:          subscriberAgent,
			PackageKey:    packageKey,
			RawPatterns:   append([]string{}, patterns...),
			AgentNamePlan: agent.NamePlan,
		})
	}
	nodes, err := routeExecutableNodeDeclarations(source, routeFlowPackageKey(source, scope), scope.ID, scope.Nodes)
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
			admission := routeClassifyAuthoredSubscription(source, subscriberNode, handlerNode.PackageKey(), scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber node %s: %s", semanticNodeID, admission.Message())
			}
		}
		out = append(out, routeSubscriberTemplate{
			Kind:        subscriberNode,
			PackageKey:  handlerNode.PackageKey(),
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

func routeExecutableNodeDeclarations(source semanticview.Source, packageKey, flowID string, expected map[string]runtimecontracts.SystemNodeContract) ([]routeExecutableNodeDeclaration, error) {
	packageKey = strings.Trim(strings.TrimSpace(packageKey), "/")
	if packageKey == "" {
		packageKey = runtimeidentity.RootPackageKey
	}
	flowID = strings.TrimSpace(flowID)
	found := make(map[string]struct{}, len(expected))
	out := make([]routeExecutableNodeDeclaration, 0, len(expected))
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			return nil, fmt.Errorf("admit route subscriber node %s identity: %w", record.LogicalID, err)
		}
		if node.FlowID() != flowID {
			continue
		}
		if node.PackageKey() != packageKey {
			continue
		}
		if _, ok := expected[record.LogicalID]; !ok {
			continue
		}
		if _, duplicate := found[record.LogicalID]; duplicate {
			return nil, fmt.Errorf("route subscriber node %q has multiple exact declaration owners in package %q flow %q", record.LogicalID, packageKey, flowID)
		}
		found[record.LogicalID] = struct{}{}
		out = append(out, routeExecutableNodeDeclaration{Node: node, Entry: record.Entry})
	}
	for nodeID := range expected {
		if _, ok := found[nodeID]; !ok {
			return nil, fmt.Errorf("route subscriber node %q has no exact declaration owner in package %q flow %q", nodeID, packageKey, flowID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node.Key() < out[j].Node.Key() })
	return out, nil
}

func routeFlowPackageKey(source semanticview.Source, scope semanticview.FlowScope) string {
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil {
		for _, view := range bundle.FlowViews() {
			if strings.TrimSpace(view.Paths.ID) != strings.TrimSpace(scope.ID) {
				continue
			}
			if scope.Path != "" && strings.Trim(strings.TrimSpace(view.Path), "/") != strings.Trim(strings.TrimSpace(scope.Path), "/") {
				continue
			}
			return bundle.ExecutableFlowViewPackageKey(&view)
		}
	}
	packageKey := strings.Trim(strings.TrimSpace(scope.PackageKey), "/")
	if packageKey == "" {
		return runtimeidentity.RootPackageKey
	}
	return packageKey
}

func routeFlowPath(source semanticview.Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
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

func routeResolveSubscriberPatterns(source semanticview.Source, kind subscriberKind, packageKey, flowID string, inputEvents []string, authorityPath, routePath string, localEvents map[string]struct{}, raw string) ([]routeResolvedPattern, error) {
	raw = eventidentity.Normalize(raw)
	flowID = strings.TrimSpace(flowID)
	if raw == "" {
		return nil, nil
	}
	admission := routeClassifyAuthoredSubscription(source, kind, packageKey, flowID, inputEvents, authorityPath, localEvents, raw)
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
	if admission.Class() == semanticview.AuthoredSubscriptionImportedPattern {
		resolution := semanticview.ResolveImportBoundaryWildcardSubscription(source, packageKey, flowID, authorityPath, localEvents, raw)
		out := make([]routeResolvedPattern, 0, len(resolution.Patterns))
		for _, pattern := range resolution.Patterns {
			eventPattern := eventidentity.Normalize(pattern.EventPattern)
			if eventPattern == "" {
				continue
			}
			routeSource, ok := subscriberRouteSourceFromCode(pattern.RouteSource)
			if !ok || !routeSource.importBoundaryWildcard() {
				continue
			}
			out = append(out, routeResolvedPattern{
				EventPattern:       eventPattern,
				MatchPattern:       eventPattern,
				routeSource:        routeSource,
				LocalizedEvent:     pattern.LocalizedEvent,
				RoutePath:          routeImportBoundarySubscriberPath(source, packageKey, flowID, routePath),
				SourceTemplatePath: pattern.SourceTemplatePath,
				SourceLocalEvent:   pattern.SourceLocalEvent,
			})
		}
		return out, nil
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

func routeClassifyAuthoredSubscription(source semanticview.Source, kind subscriberKind, packageKey, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, raw string) semanticview.AuthoredSubscriptionAdmission {
	consumerKind := semanticview.AuthoredSubscriptionConsumerNode
	if kind == subscriberAgent {
		consumerKind = semanticview.AuthoredSubscriptionConsumerAgent
	}
	return semanticview.ClassifyAuthoredSubscription(source, semanticview.AuthoredSubscriptionRequest{
		ConsumerKind: consumerKind,
		FlowID:       flowID,
		FlowPath:     basePath,
		PackageKey:   packageKey,
		LocalEvents:  localEvents,
		InputEvents:  inputEvents,
		Authored:     raw,
	})
}

func routeImportBoundarySubscriberPath(source semanticview.Source, packageKey, flowID, basePath string) string {
	if path := eventidentity.Normalize(basePath); path != "" {
		return path
	}
	if flowID = strings.TrimSpace(flowID); flowID != "" {
		return routeFlowPath(source, flowID)
	}
	packageKey = strings.Trim(strings.TrimSpace(packageKey), "/")
	if packageKey == "" {
		packageKey = "."
	}
	if source == nil {
		return ""
	}
	for _, parent := range semanticview.ProjectScopes(source) {
		for _, site := range semanticview.ImportBoundaryFlowSites(parent) {
			if strings.Trim(strings.TrimSpace(site.PackageKey), "/") == packageKey {
				return routeFlowPath(source, site.FlowID)
			}
		}
	}
	return ""
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
