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
	handlerFlowID  string
	handlerNodeID  string
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
	PackageKey  string
	FlowID      string
	InputEvents []string
	LocalEvents map[string]struct{}
	Subscribers []routeSubscriberTemplate
}

type routeSubscriberTemplate struct {
	IDTemplate     string
	Kind           subscriberKind
	RawPatterns    []string
	AgentNameOwner string
	HandlerFlowID  string
	HandlerNodeID  string
}

type subscriberKind uint8

const (
	subscriberNode subscriberKind = iota + 1
	subscriberAgent
)

func subscriberRecipient(kind subscriberKind, id string) (events.DeliveryRecipient, error) {
	if kind == subscriberNode {
		return events.NewNodeDeliveryRecipient(id)
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
	if err := validateTypedPubSubAuthorizations(source); err != nil {
		return nil, err
	}

	for _, scope := range semanticview.ProjectScopes(source) {
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
		if handlerFlowID == "" {
			handlerFlowID = strings.TrimSpace(source.WorkflowName())
		}
		rt.addAuthoredEventPathsLocked(basePath, localEvents)
		if err := rt.addAgentPatternsLocked(source, scope.Key, owningFlowID, agentFlowID, inputEvents, basePath, agentPath, localEvents, scope.Agents); err != nil {
			return nil, err
		}
		if err := rt.addNodePatternsLocked(source, scope.Key, owningFlowID, agentFlowID, handlerFlowID, inputEvents, basePath, localEvents, scope.Nodes); err != nil {
			return nil, err
		}
	}

	for _, scope := range semanticview.FlowScopes(source) {
		flowPath := routeFlowPath(source, scope.ID)
		localEvents := routeFlowLocalEventSet(source, scope)
		if strings.EqualFold(scope.Mode, "template") || routeFlowStanding(source, scope.ID) {
			subscribers, err := routeSubscriberTemplates(source, scope)
			if err != nil {
				return nil, err
			}
			rt.templates[flowPath] = routeFlowTemplate{
				PackageKey:  scope.PackageKey,
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
		if err := rt.addAgentPatternsLocked(source, scope.PackageKey, scope.ID, scope.ID, scope.InputEvents, flowPath, flowPath, localEvents, scope.Agents); err != nil {
			return nil, err
		}
		if err := rt.addNodePatternsLocked(source, scope.PackageKey, scope.ID, scope.ID, scope.ID, scope.InputEvents, flowPath, localEvents, scope.Nodes); err != nil {
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
		typedRecipient, err := subscriberRecipient(subscriberNode, recipient.ID())
		if recipient.Kind() == runtimepinrouting.ConnectRecipientAgent {
			typedRecipient, err = subscriberRecipient(subscriberAgent, recipient.ID())
		}
		if err != nil {
			continue
		}
		out = append(out, Subscriber{
			Recipient: typedRecipient, Path: recipient.Path(),
			LocalizedEvent: string(recipient.HandlerEvent()), AgentIdentity: recipient.AgentIdentity(),
			handlerFlowID: recipient.Handler().FlowID(), handlerNodeID: recipient.Handler().NodeID(),
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
	vars := flowInstanceRouteMaterializationVars(req, templateDef.FlowID)
	for _, subscriberTemplate := range templateDef.Subscribers {
		recipient, err := subscriberRecipient(subscriberTemplate.Kind, routeRenderTemplate(subscriberTemplate.IDTemplate, vars))
		if err != nil {
			return fmt.Errorf("materialize route subscriber: %w", err)
		}
		subscriber := Subscriber{
			Recipient: recipient, Path: instancePath,
			handlerFlowID: subscriberTemplate.HandlerFlowID, handlerNodeID: subscriberTemplate.HandlerNodeID,
		}
		if subscriber.Recipient.IsAgent() {
			name, err := agentidentity.DeclaredName(subscriber.Recipient.ID(), subscriberTemplate.AgentNameOwner)
			if err != nil {
				return fmt.Errorf("materialize route subscriber agent identity: %w", err)
			}
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
					rt.source, subscriber.handlerFlowID, subscriber.handlerNodeID,
				)
				if err != nil {
					return fmt.Errorf("materialize route subscriber target handler: %w", err)
				}
			}
			if err := rt.addConnectRecipientLocked(templateDef.FlowID, templateDef.InputEvents, rawPattern, admittedSubscriber, instancePath); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatterns(rt.source, subscriberTemplate.Kind, templateDef.PackageKey, templateDef.FlowID, templateDef.InputEvents, templateScope, instancePath, templateDef.LocalEvents, rawPattern)
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

func (rt *RouteTable) applyAtGeneration(ctx context.Context, snapshot routeTableSnapshotGeneration, apply func(context.Context) error) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if rt == nil {
		if snapshot.value != 0 {
			return false, nil
		}
		return true, apply(ctx)
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	if !rt.snapshotGenerationCurrent(snapshot) {
		return false, nil
	}
	leaseCtx := context.WithValue(ctx, routeTableGenerationLeaseKey{}, routeTableGenerationLease{table: rt})
	return true, apply(leaseCtx)
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

func (rt *RouteTable) addTopLevelRootInputNodeRoutesLocked(source semanticview.Source) error {
	if rt == nil || source == nil {
		return nil
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || len(bundle.Nodes) == 0 {
		return nil
	}
	rootInputs := routeRootInputEventSet(source)
	if len(rootInputs) == 0 {
		return nil
	}
	for _, eventType := range sortedStringKeys(rootInputs) {
		for _, key := range sortedStringKeys(bundle.Nodes) {
			entry := bundle.Nodes[key]
			semanticNodeID := strings.TrimSpace(key)
			subscriberID := strings.TrimSpace(entry.ID)
			if subscriberID == "" {
				subscriberID = semanticNodeID
			}
			if semanticNodeID == "" || !routeNodeSubscribesToLocalExact(source, semanticNodeID, eventType) {
				continue
			}
			if rootInputFlowOwnsNodeRoute(source, semanticNodeID, eventType) {
				continue
			}
			subscriber := Subscriber{
				Recipient:     events.MustNodeDeliveryRecipient(subscriberID),
				MatchPattern:  eventType,
				routeSource:   subscriberRouteSourceRootInputProject,
				handlerFlowID: strings.TrimSpace(source.WorkflowName()), handlerNodeID: semanticNodeID,
			}
			targetHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(
				source, subscriber.handlerFlowID, subscriber.handlerNodeID,
			)
			if err != nil {
				return fmt.Errorf("admit root target handler %s for %s: %w", semanticNodeID, eventType, err)
			}
			subscriber.targetHandler = targetHandler
			if err := rt.addConnectRecipientLocked("", nil, eventType, subscriber, ""); err != nil {
				return fmt.Errorf("admit root connect recipient %s for %s: %w", subscriberID, eventType, err)
			}
			rt.rootInputRoutes[eventType] = appendUniqueRootInputSubscriber(rt.rootInputRoutes[eventType], subscriber)
		}
	}
	return nil
}

func rootInputFlowOwnsNodeRoute(source semanticview.Source, nodeID string, eventType string) bool {
	for _, scope := range source.FlowScopes() {
		if strings.EqualFold(scope.Mode, "template") || !normalizedStringListContains(scope.InputEvents, eventType) {
			continue
		}
		for _, key := range sortedStringKeys(scope.Nodes) {
			scopedNodeID := strings.TrimSpace(key)
			if scopedNodeID == nodeID {
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
	if len(rootInputs) == 0 {
		return nil
	}
	for _, scope := range source.FlowScopes() {
		if strings.EqualFold(scope.Mode, "template") {
			continue
		}
		flowID := strings.TrimSpace(scope.ID)
		flowPath := strings.Trim(strings.TrimSpace(routeFlowPath(source, flowID)), "/")
		if flowID == "" || flowPath == "" {
			continue
		}
		for _, eventType := range sortedStringKeys(rootInputs) {
			if !normalizedStringListContains(scope.InputEvents, eventType) {
				continue
			}
			for _, key := range sortedStringKeys(scope.Nodes) {
				entry := scope.Nodes[key]
				semanticNodeID := strings.TrimSpace(key)
				subscriberID := strings.TrimSpace(entry.ID)
				if subscriberID == "" {
					subscriberID = semanticNodeID
				}
				if semanticNodeID == "" || !routeNodeSubscribesToLocalExact(source, semanticNodeID, eventType) {
					continue
				}
				subscriber := Subscriber{
					Recipient:     events.MustNodeDeliveryRecipient(subscriberID),
					Path:          flowPath,
					MatchPattern:  eventType,
					routeSource:   subscriberRouteSourceRootInputFlow,
					handlerFlowID: flowID, handlerNodeID: semanticNodeID,
				}
				var err error
				subscriber.targetHandler, err = runtimepipeline.AdmitDeliveryTargetHandler(
					source, flowID, semanticNodeID,
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

func routeRootInputEventSet(source semanticview.Source) map[string]struct{} {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.RootSchema == nil {
		return nil
	}
	out := make(map[string]struct{})
	for _, eventType := range normalizeStringList(bundle.RootSchema.Pins.Inputs.Events) {
		out[eventType] = struct{}{}
	}
	return out
}

func routeNodeSubscribesToLocalExact(source semanticview.Source, nodeID, eventType string) bool {
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return false
	}
	for _, authored := range source.NodeRuntimeSubscriptions(nodeID) {
		admission := semanticview.ClassifyNodeSubscription(source, nodeID, authored)
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

func (rt *RouteTable) addAgentPatternsLocked(
	source semanticview.Source,
	packageKey, routingFlowID, agentFlowID string,
	inputEvents []string,
	basePath, agentPath string,
	localEvents map[string]struct{},
	agents map[string]runtimecontracts.AgentRegistryEntry,
) error {
	for _, key := range sortedStringKeys(agents) {
		entry := agents[key]
		subscriber := Subscriber{
			Recipient: events.MustAgentDeliveryRecipient(strings.TrimSpace(entry.ID)),
			Path:      strings.Trim(strings.TrimSpace(agentPath), "/"),
		}
		owner, ok := semanticview.AgentDeclarationOwner(source, agentFlowID, key)
		if !ok {
			return fmt.Errorf("route subscriber agent %s missing scoped declaration owner", key)
		}
		name, err := agentidentity.DeclaredName(subscriber.Recipient.ID(), owner)
		if err != nil {
			return fmt.Errorf("route subscriber agent %s declaration identity: %w", key, err)
		}
		route := agentidentity.RootRoute()
		if subscriber.Path != "" {
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
			resolvedPatterns, err := routeResolveSubscriberPatterns(source, subscriberAgent, packageKey, agentFlowID, inputEvents, agentPath, agentPath, localEvents, rawPattern)
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

func (rt *RouteTable) addNodePatternsLocked(source semanticview.Source, packageKey, routingFlowID, connectFlowID, handlerFlowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, nodes map[string]runtimecontracts.SystemNodeContract) error {
	for _, key := range sortedStringKeys(nodes) {
		entry := nodes[key]
		semanticNodeID := strings.TrimSpace(key)
		subscriberID := strings.TrimSpace(entry.ID)
		if subscriberID == "" {
			subscriberID = semanticNodeID
		}
		subscriber := Subscriber{
			Recipient:     events.MustNodeDeliveryRecipient(subscriberID),
			Path:          strings.Trim(strings.TrimSpace(basePath), "/"),
			handlerFlowID: strings.TrimSpace(handlerFlowID), handlerNodeID: semanticNodeID,
		}
		patterns := runtimecontracts.EffectiveSystemNodeSubscriptions(entry)
		if source != nil {
			patterns = source.NodeRuntimeSubscriptions(semanticNodeID)
		}
		for _, rawPattern := range patterns {
			admittedSubscriber := subscriber
			targetHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(
				source, handlerFlowID, semanticNodeID,
			)
			if err != nil {
				return fmt.Errorf("admit route subscriber target handler %s for %s: %w", semanticNodeID, rawPattern, err)
			}
			admittedSubscriber.targetHandler = targetHandler
			if err := rt.addConnectRecipientLocked(connectFlowID, inputEvents, rawPattern, admittedSubscriber, ""); err != nil {
				return err
			}
			resolvedPatterns, err := routeResolveSubscriberPatterns(source, subscriberNode, packageKey, routingFlowID, inputEvents, basePath, basePath, localEvents, rawPattern)
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
			subscriber.Recipient.ID(), subscriber.Path, subscriber.handlerFlowID, subscriber.handlerNodeID,
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
	recipient              events.DeliveryRecipient
	subscriberPath         string
	routeSource            subscriberRouteSource
	localizedEvent         string
	subscriberInstancePath string
}

func routeTemplateSourceObserverKey(observer routeTemplateSourceObserver) routeTemplateSourceObserverIdentity {
	return routeTemplateSourceObserverIdentity{
		sourceTemplatePath:     eventidentity.Normalize(observer.SourceTemplatePath),
		sourceLocalEvent:       eventidentity.Normalize(observer.SourceLocalEvent),
		recipient:              observer.Subscriber.Recipient,
		subscriberPath:         strings.TrimSpace(observer.Subscriber.Path),
		routeSource:            observer.Subscriber.routeSource,
		localizedEvent:         eventidentity.Normalize(observer.Subscriber.LocalizedEvent),
		subscriberInstancePath: strings.Trim(strings.TrimSpace(observer.SubscriberInstancePath), "/"),
	}
}

type routePatternIdentityKey struct {
	eventPattern       string
	recipient          events.DeliveryRecipient
	subscriberPath     string
	routeSource        subscriberRouteSource
	localizedEvent     string
	instancePath       string
	sourceInstancePath string
}

func routePatternIdentity(pattern routePattern) routePatternIdentityKey {
	return routePatternIdentityKey{
		eventPattern:       eventidentity.Normalize(pattern.EventPattern),
		recipient:          pattern.Subscriber.Recipient,
		subscriberPath:     strings.TrimSpace(pattern.Subscriber.Path),
		routeSource:        pattern.Subscriber.routeSource,
		localizedEvent:     eventidentity.Normalize(pattern.Subscriber.LocalizedEvent),
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

func routeSubscriberTemplates(source semanticview.Source, scope semanticview.FlowScope) ([]routeSubscriberTemplate, error) {
	out := make([]routeSubscriberTemplate, 0, len(scope.Agents)+len(scope.Nodes))
	localEvents := routeFlowLocalEventSet(source, scope)
	for _, key := range sortedStringKeys(scope.Agents) {
		entry := scope.Agents[key]
		patterns := normalizeStringList(entry.Subscriptions)
		if len(patterns) == 0 {
			continue
		}
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberAgent, scope.PackageKey, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber agent %s: %s", key, admission.Message())
			}
		}
		owner, ok := semanticview.AgentDeclarationOwner(source, scope.ID, key)
		if !ok {
			return nil, fmt.Errorf("route subscriber template agent %s missing scoped declaration owner", key)
		}
		out = append(out, routeSubscriberTemplate{
			IDTemplate:     strings.TrimSpace(entry.ID),
			Kind:           subscriberAgent,
			RawPatterns:    append([]string{}, patterns...),
			AgentNameOwner: owner,
		})
	}
	for _, key := range sortedStringKeys(scope.Nodes) {
		entry := scope.Nodes[key]
		semanticNodeID := strings.TrimSpace(key)
		subscriberID := strings.TrimSpace(entry.ID)
		if subscriberID == "" {
			subscriberID = semanticNodeID
		}
		patterns := runtimecontracts.EffectiveSystemNodeSubscriptions(entry)
		if source != nil {
			patterns = source.NodeRuntimeSubscriptions(semanticNodeID)
		}
		if len(patterns) == 0 {
			continue
		}
		for _, pattern := range patterns {
			admission := routeClassifyAuthoredSubscription(source, subscriberNode, scope.PackageKey, scope.ID, scope.InputEvents, scope.Path, localEvents, pattern)
			if !admission.Admitted() {
				return nil, fmt.Errorf("route subscriber node %s: %s", key, admission.Message())
			}
		}
		out = append(out, routeSubscriberTemplate{
			IDTemplate:    subscriberID,
			Kind:          subscriberNode,
			RawPatterns:   append([]string{}, patterns...),
			HandlerFlowID: strings.TrimSpace(scope.ID), HandlerNodeID: semanticNodeID,
		})
	}
	return out, nil
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

func routeFlowHasInputEvent(inputEvents []string, eventType string) bool {
	return eventidentity.Scope{InputEvents: inputEvents}.HasInput(eventType)
}

func workflowScopeLocalEvents(scope semanticview.FlowScope) map[string]struct{} {
	out := make(map[string]struct{}, len(scope.Events)+len(scope.OutputEvents)+1)
	for eventType := range scope.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, eventType := range scope.OutputEvents {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	if autoEmit := strings.TrimSpace(scope.AutoEmitEvent); autoEmit != "" {
		out[autoEmit] = struct{}{}
	}
	return out
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

func routeRenderTemplate(raw string, vars map[string]string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(vars) == 0 {
		return raw
	}
	replacements := make([]string, 0, len(vars)*4)
	for key, value := range vars {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		replacements = append(replacements, "{"+key+"}", value, "{{"+key+"}}", value)
	}
	return strings.NewReplacer(replacements...).Replace(raw)
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

func appendUniqueSubscriber(in []Subscriber, subscriber Subscriber) []Subscriber {
	for _, existing := range in {
		if existing.Recipient == subscriber.Recipient && existing.Path == subscriber.Path &&
			existing.AgentIdentity == subscriber.AgentIdentity {
			return in
		}
	}
	return append(in, subscriber)
}

func appendUniqueRootInputSubscriber(in []Subscriber, subscriber Subscriber) []Subscriber {
	for idx, existing := range in {
		if existing.Recipient == subscriber.Recipient && existing.Path == subscriber.Path &&
			existing.AgentIdentity == subscriber.AgentIdentity {
			if subscriber.routeSource == subscriberRouteSourceRootInputFlow {
				in[idx].MatchPattern = subscriber.MatchPattern
				in[idx].routeSource = subscriber.routeSource
			}
			return in
		}
	}
	return append(in, subscriber)
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
