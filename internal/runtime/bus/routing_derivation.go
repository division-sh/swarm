package bus

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Subscriber struct {
	ID             string
	Type           string
	Path           string
	MatchPattern   string
	RouteSource    string
	LocalizedEvent string
}

type RouteTable struct {
	mu                sync.RWMutex
	source            semanticview.Source
	routes            map[string][]Subscriber
	rootInputRoutes   map[string][]Subscriber
	patterns          []routePattern
	eventPath         map[string]struct{}
	templates         map[string]routeFlowTemplate
	instances         map[string]struct{}
	instanceEventPath map[string][]string
}

type routePattern struct {
	EventPattern string
	Subscriber   Subscriber
	InstancePath string
}

type routeFlowTemplate struct {
	PackageKey  string
	FlowID      string
	InputEvents []string
	LocalEvents map[string]struct{}
	Subscribers []routeSubscriberTemplate
}

type routeSubscriberTemplate struct {
	IDTemplate  string
	Type        string
	RawPatterns []string
}

type routeResolvedPattern struct {
	EventPattern   string
	MatchPattern   string
	RouteSource    string
	LocalizedEvent string
	RoutePath      string
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
		owningFlowID := ""
		basePath := ""
		var inputEvents []string
		if routeProjectScopeRequiresPinAliases(scope) {
			owningFlowID = strings.TrimSpace(scope.OwningFlowID)
		}
		if importedFlowID := routeProjectScopeImportedFlowID(source, scope); importedFlowID != "" && routeProjectScopeOwnedByTemplateFlow(source, importedFlowID) {
			continue
		}
		if owningFlowID != "" {
			inputEvents = source.FlowInputEvents(owningFlowID)
			basePath = routeFlowPath(source, owningFlowID)
		}
		if routeProjectScopeOwnedByTemplateFlow(source, owningFlowID) {
			continue
		}
		rt.addEventPathsLocked(basePath, localEvents)
		rt.addAgentPatternsLocked(source, scope.Key, owningFlowID, inputEvents, basePath, localEvents, scope.Agents)
		rt.addNodePatternsLocked(source, scope.Key, owningFlowID, inputEvents, basePath, localEvents, scope.Nodes)
	}

	for _, scope := range semanticview.FlowScopes(source) {
		flowPath := routeFlowPath(source, scope.ID)
		localEvents := routeFlowLocalEventSet(source, scope)
		if strings.EqualFold(scope.Mode, "template") {
			rt.templates[flowPath] = routeFlowTemplate{
				PackageKey:  scope.PackageKey,
				FlowID:      scope.ID,
				InputEvents: append([]string{}, scope.InputEvents...),
				LocalEvents: cloneStringSet(localEvents),
				Subscribers: routeSubscriberTemplates(source, scope),
			}
			continue
		}
		rt.addEventPathsLocked(flowPath, localEvents)
		rt.addAgentPatternsLocked(source, scope.PackageKey, scope.ID, scope.InputEvents, flowPath, localEvents, scope.Agents)
		rt.addNodePatternsLocked(source, scope.PackageKey, scope.ID, scope.InputEvents, flowPath, localEvents, scope.Nodes)
	}

	rt.addTopLevelRootInputNodeRoutesLocked(source)
	rt.addRootInputFlowNodeRoutesLocked(source)
	rt.rebuildLocked()
	return rt, nil
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

func (rt *RouteTable) AddFlowInstanceRoute(req FlowInstanceRouteMaterializationRequest) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}

	req = req.Normalized()
	identity := req.Identity
	if !identity.Valid() {
		return fmt.Errorf("flow-instance route identity is required")
	}
	instancePath := identity.InstancePath

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if _, exists := rt.instances[instancePath]; exists {
		return nil
	}

	templateScope := strings.TrimSpace(identity.ScopeKey)
	if templateDef, ok := rt.templates[templateScope]; ok {
		rt.instances[instancePath] = struct{}{}
		rt.instanceEventPath[instancePath] = rt.addEventPathsLocked(instancePath, templateDef.LocalEvents)
		vars := flowInstanceRouteMaterializationVars(req, templateDef.FlowID)
		for _, subscriberTemplate := range templateDef.Subscribers {
			subscriber := Subscriber{
				ID:   routeRenderTemplate(subscriberTemplate.IDTemplate, vars),
				Type: subscriberTemplate.Type,
				Path: instancePath,
			}
			for _, rawPattern := range subscriberTemplate.RawPatterns {
				for _, resolved := range routeResolveSubscriberPatterns(rt.source, templateDef.PackageKey, templateDef.FlowID, templateDef.InputEvents, instancePath, templateDef.LocalEvents, rawPattern) {
					if strings.TrimSpace(resolved.EventPattern) == "" {
						continue
					}
					resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
					rt.patterns = append(rt.patterns, routePattern{
						EventPattern: resolved.EventPattern,
						Subscriber:   resolvedSubscriber,
						InstancePath: instancePath,
					})
				}
				if !routeFlowInputHasLoweredConnectReceiver(rt.source, templateDef.FlowID, rawPattern) {
					continue
				}
				for _, resolved := range routeReceiverCarrierSubscriberPatterns(templateDef.InputEvents, instancePath, rawPattern) {
					resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
					rt.patterns = append(rt.patterns, routePattern{
						EventPattern: resolved.EventPattern,
						Subscriber:   resolvedSubscriber,
						InstancePath: instancePath,
					})
				}
			}
		}
		rt.rebuildLocked()
		return nil
	}

	// Compatibility fallback for the current odd handoff signature.
	templateID := templateScope
	if strings.TrimSpace(req.Template.ID) == "" {
		return fmt.Errorf("route template %q not found", templateID)
	}
	localEvents := routeNodeLocalEventSet(req.Template)
	rt.instances[instancePath] = struct{}{}
	rt.instanceEventPath[instancePath] = rt.addEventPathsLocked(instancePath, localEvents)
	vars := flowInstanceRouteMaterializationVars(req, templateID)
	subscriber := Subscriber{
		ID:   routeRenderTemplate(req.Template.ID, vars),
		Type: "node",
		Path: instancePath,
	}
	for _, rawPattern := range runtimecontracts.EffectiveSystemNodeSubscriptions(req.Template) {
		for _, resolved := range routeResolveSubscriberPatterns(rt.source, "", templateID, nil, instancePath, localEvents, rawPattern) {
			if strings.TrimSpace(resolved.EventPattern) == "" {
				continue
			}
			resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
			rt.patterns = append(rt.patterns, routePattern{
				EventPattern: resolved.EventPattern,
				Subscriber:   resolvedSubscriber,
				InstancePath: instancePath,
			})
		}
	}
	rt.rebuildLocked()
	return nil
}

func (rt *RouteTable) HasFlowInstanceRoute(identity runtimeflowidentity.Route) bool {
	if rt == nil {
		return false
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	_, exists := rt.instances[identity.InstancePath]
	return exists
}

func (rt *RouteTable) RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) {
	if rt == nil {
		return
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return
	}
	instancePath := identity.InstancePath
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, exists := rt.instances[instancePath]; !exists {
		return
	}
	delete(rt.instances, instancePath)
	for _, eventType := range rt.instanceEventPath[instancePath] {
		delete(rt.eventPath, eventType)
	}
	delete(rt.instanceEventPath, instancePath)
	filtered := rt.patterns[:0]
	for _, pattern := range rt.patterns {
		if pattern.InstancePath == instancePath {
			continue
		}
		filtered = append(filtered, pattern)
	}
	rt.patterns = filtered
	rt.rebuildLocked()
}

func (rt *RouteTable) MaterializedRoutes(identity runtimeflowidentity.Route) []FlowInstanceRouteRecord {
	if rt == nil {
		return nil
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return nil
	}
	instancePath := identity.InstancePath
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	seen := make(map[string]struct{})
	out := make([]FlowInstanceRouteRecord, 0, 8)
	for _, pattern := range rt.patterns {
		if strings.Trim(strings.TrimSpace(pattern.InstancePath), "/") != instancePath {
			continue
		}
		record := FlowInstanceRouteRecord{
			Identity:       identity,
			EventPattern:   strings.TrimSpace(pattern.EventPattern),
			SubscriberType: strings.TrimSpace(pattern.Subscriber.Type),
			SubscriberID:   strings.TrimSpace(pattern.Subscriber.ID),
			SourceFlow:     identity.ScopeKey,
		}
		key := strings.Join([]string{
			record.Identity.InstancePath,
			record.EventPattern,
			record.SubscriberType,
			record.SubscriberID,
		}, "|")
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
		source:            source,
		routes:            make(map[string][]Subscriber),
		rootInputRoutes:   make(map[string][]Subscriber),
		eventPath:         make(map[string]struct{}),
		templates:         make(map[string]routeFlowTemplate),
		instances:         make(map[string]struct{}),
		instanceEventPath: make(map[string][]string),
	}
}

func (rt *RouteTable) addTopLevelRootInputNodeRoutesLocked(source semanticview.Source) {
	if rt == nil || source == nil {
		return
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || len(bundle.Nodes) == 0 {
		return
	}
	rootInputs := routeRootInputEventSet(source)
	if len(rootInputs) == 0 {
		return
	}
	for _, eventType := range sortedStringKeys(rootInputs) {
		for _, key := range sortedStringKeys(bundle.Nodes) {
			entry := bundle.Nodes[key]
			semanticNodeID := strings.TrimSpace(key)
			subscriberID := strings.TrimSpace(entry.ID)
			if subscriberID == "" {
				subscriberID = semanticNodeID
			}
			if semanticNodeID == "" || !normalizedStringListContains(source.NodeRuntimeSubscriptions(semanticNodeID), eventType) {
				continue
			}
			if rootInputFlowOwnsNodeRoute(source, semanticNodeID, eventType) {
				continue
			}
			rt.rootInputRoutes[eventType] = appendUniqueRootInputSubscriber(rt.rootInputRoutes[eventType], Subscriber{
				ID:           subscriberID,
				Type:         "node",
				MatchPattern: eventType,
				RouteSource:  "root_input_project",
			})
		}
	}
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

func (rt *RouteTable) addRootInputFlowNodeRoutesLocked(source semanticview.Source) {
	if rt == nil || source == nil {
		return
	}
	rootInputs := routeRootInputEventSet(source)
	if len(rootInputs) == 0 {
		return
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
				if semanticNodeID == "" || !normalizedStringListContains(source.NodeRuntimeSubscriptions(semanticNodeID), eventType) {
					continue
				}
				rt.rootInputRoutes[eventType] = appendUniqueSubscriber(rt.rootInputRoutes[eventType], Subscriber{
					ID:           subscriberID,
					Type:         "node",
					Path:         flowPath,
					MatchPattern: eventType,
					RouteSource:  "root_input_flow",
				})
			}
		}
	}
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

func (rt *RouteTable) addAgentPatternsLocked(source semanticview.Source, packageKey, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, agents map[string]runtimecontracts.AgentRegistryEntry) {
	for _, key := range sortedStringKeys(agents) {
		entry := agents[key]
		subscriber := Subscriber{
			ID:   strings.TrimSpace(entry.ID),
			Type: "agent",
			Path: strings.Trim(strings.TrimSpace(basePath), "/"),
		}
		for _, rawPattern := range normalizeStringList(entry.Subscriptions) {
			if routeFlowInputHasLoweredConnectReceiver(source, flowID, rawPattern) && !routeInputAliasRequiresExclusivePatterns(source, flowID, rawPattern) {
				rt.addReceiverCarrierPatternsLocked(inputEvents, basePath, subscriber, rawPattern)
			}
			for _, resolved := range routeResolveSubscriberPatterns(source, packageKey, flowID, inputEvents, basePath, localEvents, rawPattern) {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
				rt.patterns = append(rt.patterns, routePattern{
					EventPattern: resolved.EventPattern,
					Subscriber:   resolvedSubscriber,
				})
			}
		}
	}
}

func (rt *RouteTable) addNodePatternsLocked(source semanticview.Source, packageKey, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, nodes map[string]runtimecontracts.SystemNodeContract) {
	for _, key := range sortedStringKeys(nodes) {
		entry := nodes[key]
		semanticNodeID := strings.TrimSpace(key)
		subscriberID := strings.TrimSpace(entry.ID)
		if subscriberID == "" {
			subscriberID = semanticNodeID
		}
		subscriber := Subscriber{
			ID:   subscriberID,
			Type: "node",
			Path: strings.Trim(strings.TrimSpace(basePath), "/"),
		}
		patterns := runtimecontracts.EffectiveSystemNodeSubscriptions(entry)
		if source != nil {
			patterns = source.NodeRuntimeSubscriptions(semanticNodeID)
		}
		for _, rawPattern := range patterns {
			if routeFlowInputHasLoweredConnectReceiver(source, flowID, rawPattern) && !routeInputAliasRequiresExclusivePatterns(source, flowID, rawPattern) {
				rt.addReceiverCarrierPatternsLocked(inputEvents, basePath, subscriber, rawPattern)
			}
			for _, resolved := range routeResolveSubscriberPatterns(source, packageKey, flowID, inputEvents, basePath, localEvents, rawPattern) {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
				rt.patterns = append(rt.patterns, routePattern{
					EventPattern: resolved.EventPattern,
					Subscriber:   resolvedSubscriber,
				})
			}
			if routeInputAliasRequiresExclusivePatterns(source, flowID, rawPattern) {
				continue
			}
			if resolved := routeResolveNodeCanonicalPattern(source, basePath, semanticNodeID, rawPattern); strings.TrimSpace(resolved.EventPattern) != "" {
				resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
				rt.patterns = append(rt.patterns, routePattern{
					EventPattern: resolved.EventPattern,
					Subscriber:   resolvedSubscriber,
				})
			}
		}
	}
}

func (rt *RouteTable) addReceiverCarrierPatternsLocked(inputEvents []string, basePath string, subscriber Subscriber, rawPattern string) {
	for _, resolved := range routeReceiverCarrierSubscriberPatterns(inputEvents, basePath, rawPattern) {
		resolvedSubscriber := routeApplyResolvedPattern(subscriber, resolved)
		rt.patterns = append(rt.patterns, routePattern{
			EventPattern: resolved.EventPattern,
			Subscriber:   resolvedSubscriber,
		})
	}
}

func routeApplyResolvedPattern(subscriber Subscriber, resolved routeResolvedPattern) Subscriber {
	subscriber.RouteSource = strings.TrimSpace(resolved.RouteSource)
	subscriber.LocalizedEvent = eventidentity.Normalize(resolved.LocalizedEvent)
	if matchPattern := eventidentity.Normalize(resolved.MatchPattern); matchPattern != "" {
		subscriber.MatchPattern = matchPattern
	}
	if subscriber.Path == "" {
		if routePath := eventidentity.Normalize(resolved.RoutePath); routePath != "" {
			subscriber.Path = routePath
		} else if strings.HasPrefix(subscriber.RouteSource, "import_boundary_wildcard") {
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

func routeResolveNodeCanonicalPattern(source semanticview.Source, basePath, nodeID, rawPattern string) routeResolvedPattern {
	if source == nil || strings.Trim(strings.TrimSpace(basePath), "/") == "" || strings.TrimSpace(nodeID) == "" {
		return routeResolvedPattern{}
	}
	rawPattern = eventidentity.Normalize(rawPattern)
	if rawPattern == "" || strings.Contains(rawPattern, "*") {
		return routeResolvedPattern{}
	}
	resolved := eventidentity.Normalize(source.ResolveNodeEventReference(nodeID, rawPattern))
	if resolved == "" || resolved == rawPattern || strings.Contains(resolved, "*") {
		return routeResolvedPattern{}
	}
	return routeResolvedPattern{
		EventPattern: resolved,
		RouteSource:  "subscription",
	}
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
	resolution := semanticview.ResolveFlowInputProducer(source, flowID, eventType)
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

func routeFlowInputHasLoweredConnectReceiver(source semanticview.Source, flowID, eventType string) bool {
	if source == nil || strings.TrimSpace(flowID) == "" {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	for _, connect := range source.CompositionConnects() {
		to, err := connect.ToRef()
		if err != nil || strings.TrimSpace(to.FlowID) != strings.TrimSpace(flowID) {
			continue
		}
		inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin)
		if !ok {
			continue
		}
		if eventidentity.Normalize(inputPin.EventType()) == eventType {
			return true
		}
	}
	return false
}

func routeInputAliasRequiresExclusivePatterns(source semanticview.Source, flowID, eventType string) bool {
	if source == nil || strings.TrimSpace(flowID) == "" {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" || !source.FlowHasInputEvent(flowID, eventType) {
		return false
	}
	return semanticview.ImportBoundaryInputAliasRequired(source, flowID, eventType)
}

func routeNodeLocalEventSet(node runtimecontracts.SystemNodeContract) map[string]struct{} {
	out := make(map[string]struct{})
	for _, eventType := range runtimecontracts.EffectiveSystemNodeProduces(node) {
		out[eventType] = struct{}{}
	}
	if len(node.EventHandlers) == 0 {
		for _, eventType := range normalizeStringList(node.Produces) {
			out[eventType] = struct{}{}
		}
	}
	for _, timer := range node.Timers {
		if eventType := strings.TrimSpace(timer.Event); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	return out
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

func routeSubscriberTemplates(source semanticview.Source, scope semanticview.FlowScope) []routeSubscriberTemplate {
	out := make([]routeSubscriberTemplate, 0, len(scope.Agents)+len(scope.Nodes))
	for _, key := range sortedStringKeys(scope.Agents) {
		entry := scope.Agents[key]
		patterns := normalizeStringList(entry.Subscriptions)
		if len(patterns) == 0 {
			continue
		}
		out = append(out, routeSubscriberTemplate{
			IDTemplate:  strings.TrimSpace(entry.ID),
			Type:        "agent",
			RawPatterns: append([]string{}, patterns...),
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
		out = append(out, routeSubscriberTemplate{
			IDTemplate:  subscriberID,
			Type:        "node",
			RawPatterns: append([]string{}, patterns...),
		})
	}
	return out
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

func routeResolvedPatternsForList(source semanticview.Source, packageKey, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, patterns []string) []routeResolvedPattern {
	out := make([]routeResolvedPattern, 0, len(patterns))
	for _, raw := range patterns {
		out = append(out, routeResolveSubscriberPatterns(source, packageKey, flowID, inputEvents, basePath, localEvents, raw)...)
	}
	return out
}

func routeResolveSubscriberPatterns(source semanticview.Source, packageKey, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, raw string) []routeResolvedPattern {
	raw = eventidentity.Normalize(raw)
	flowID = strings.TrimSpace(flowID)
	if raw == "" {
		return nil
	}
	if flowID != "" && source != nil && source.FlowHasInputEvent(flowID, raw) {
		patterns := routeInputProducerPatterns(source.ResolveFlowInputAutoWire(flowID, raw))
		if len(patterns) > 0 {
			if len(semanticview.ImportBoundaryInputAliases(source, flowID, raw)) > 0 {
				for i := range patterns {
					patterns[i].RouteSource = "pin_bind_input_alias"
					patterns[i].LocalizedEvent = raw
				}
			}
			return patterns
		}
		if semanticview.ImportBoundaryInputAliasRequired(source, flowID, raw) {
			return nil
		}
	}
	if source != nil {
		outputAliases := semanticview.ImportBoundaryOutputAliasesForParentEvent(source, packageKey, flowID, raw)
		if len(outputAliases) > 0 {
			out := make([]routeResolvedPattern, 0, len(outputAliases))
			for _, alias := range outputAliases {
				pattern := eventidentity.Normalize(alias.EventPattern)
				if pattern == "" {
					continue
				}
				out = append(out, routeResolvedPattern{
					EventPattern:   pattern,
					RouteSource:    "pin_bind_output_alias",
					LocalizedEvent: raw,
				})
			}
			if len(out) > 0 {
				return out
			}
		}
		if strings.Contains(raw, "*") {
			outputAliases := semanticview.ImportBoundaryOutputAliasesForParent(source, packageKey, flowID)
			out := make([]routeResolvedPattern, 0, len(outputAliases))
			for _, alias := range outputAliases {
				parentEvent := eventidentity.Normalize(alias.ParentEvent)
				pattern := eventidentity.Normalize(alias.EventPattern)
				if parentEvent == "" || pattern == "" || !RouteMatches(raw, parentEvent) {
					continue
				}
				out = append(out, routeResolvedPattern{
					EventPattern:   pattern,
					MatchPattern:   raw,
					RouteSource:    "pin_bind_output_alias",
					LocalizedEvent: parentEvent,
				})
			}
			if len(out) > 0 {
				return out
			}
		}
		if strings.Contains(raw, "*") {
			resolution := semanticview.ResolveImportBoundaryWildcardSubscription(source, packageKey, flowID, basePath, localEvents, raw)
			if resolution.Scoped {
				out := make([]routeResolvedPattern, 0, len(resolution.Patterns))
				for _, pattern := range resolution.Patterns {
					eventPattern := eventidentity.Normalize(pattern.EventPattern)
					if eventPattern == "" {
						continue
					}
					out = append(out, routeResolvedPattern{
						EventPattern:   eventPattern,
						MatchPattern:   pattern.MatchPattern,
						RouteSource:    pattern.RouteSource,
						LocalizedEvent: pattern.LocalizedEvent,
						RoutePath:      routeImportBoundarySubscriberPath(source, packageKey, flowID, basePath),
					})
				}
				return out
			}
		}
	}
	scope := routeEventIdentityScope(basePath, localEvents, inputEvents)
	pattern := scope.ResolveSubscriptionPattern(raw, routeDescendantScopes(source, flowID))
	if pattern == raw && !strings.Contains(raw, "/") && strings.Trim(strings.TrimSpace(basePath), "/") != "" {
		for localEvent := range localEvents {
			if eventidentity.MatchPattern(raw, localEvent) {
				pattern = eventidentity.Normalize(strings.Trim(strings.TrimSpace(basePath), "/") + "/" + raw)
				break
			}
		}
	}
	if pattern == "" {
		return nil
	}
	return []routeResolvedPattern{{EventPattern: pattern, RouteSource: "subscription"}}
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

func routeReceiverCarrierSubscriberPatterns(inputEvents []string, instancePath, raw string) []routeResolvedPattern {
	raw = eventidentity.Normalize(raw)
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if raw == "" || strings.Contains(raw, "*") || instancePath == "" {
		return nil
	}
	if !routeFlowHasInputEvent(inputEvents, raw) {
		return nil
	}
	return []routeResolvedPattern{{
		EventPattern:   instancePath + "/" + raw,
		MatchPattern:   raw,
		RouteSource:    "receiver_carrier",
		LocalizedEvent: raw,
	}}
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
			RouteSource:  "pin_auto_wire",
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

func routeDescendantScopes(source semanticview.Source, flowID string) []eventidentity.DescendantScope {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return nil
	}
	parentPath := eventidentity.Normalize(source.FlowPath(flowID))
	if parentPath == "" {
		return nil
	}
	out := make([]eventidentity.DescendantScope, 0)
	for _, scope := range source.FlowScopes() {
		descendantPath := eventidentity.Normalize(scope.Path)
		if descendantPath == "" || !strings.HasPrefix(descendantPath, parentPath+"/") {
			continue
		}
		localEvents := sortedStringKeys(workflowScopeLocalEvents(scope))
		if len(localEvents) == 0 {
			continue
		}
		out = append(out, eventidentity.DescendantScope{
			Path:        descendantPath,
			LocalEvents: localEvents,
		})
	}
	return out
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
		if existing.ID == subscriber.ID && existing.Type == subscriber.Type && existing.Path == subscriber.Path {
			return in
		}
	}
	return append(in, subscriber)
}

func appendUniqueRootInputSubscriber(in []Subscriber, subscriber Subscriber) []Subscriber {
	for idx, existing := range in {
		if existing.ID == subscriber.ID && existing.Type == subscriber.Type && existing.Path == subscriber.Path {
			if strings.TrimSpace(subscriber.RouteSource) == "root_input_flow" {
				in[idx].MatchPattern = subscriber.MatchPattern
				in[idx].RouteSource = subscriber.RouteSource
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
