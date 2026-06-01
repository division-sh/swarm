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
	ID           string
	Type         string
	Path         string
	MatchPattern string
	RouteSource  string
}

type RouteTable struct {
	mu                sync.RWMutex
	source            semanticview.Source
	routes            map[string][]Subscriber
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
	EventPattern string
	RouteSource  string
}

func DeriveRouteTable(source semanticview.Source) (*RouteTable, error) {
	rt := newRouteTable(source)
	if source == nil {
		return rt, nil
	}

	for _, scope := range semanticview.ProjectScopes(source) {
		localEvents := routeProjectLocalEventSet(scope)
		rt.addEventPathsLocked("", localEvents)
		rt.addAgentPatternsLocked(source, "", nil, "", localEvents, scope.Agents)
		rt.addNodePatternsLocked(source, "", nil, "", localEvents, scope.Nodes)
	}

	for _, scope := range semanticview.FlowScopes(source) {
		flowPath := routeFlowPath(source, scope.ID)
		localEvents := routeFlowLocalEventSet(source, scope)
		if strings.EqualFold(scope.Mode, "template") {
			rt.templates[flowPath] = routeFlowTemplate{
				FlowID:      scope.ID,
				InputEvents: append([]string{}, scope.InputEvents...),
				LocalEvents: cloneStringSet(localEvents),
				Subscribers: routeSubscriberTemplates(source, scope),
			}
			continue
		}
		rt.addEventPathsLocked(flowPath, localEvents)
		rt.addAgentPatternsLocked(source, scope.ID, scope.InputEvents, flowPath, localEvents, scope.Agents)
		rt.addNodePatternsLocked(source, scope.ID, scope.InputEvents, flowPath, localEvents, scope.Nodes)
	}

	rt.rebuildLocked()
	return rt, nil
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

func (rt *RouteTable) AddFlowInstanceRoute(template runtimecontracts.SystemNodeContract, identity runtimeflowidentity.Route) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}

	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
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
		vars := map[string]string{
			"flow_instance_path": instancePath,
			"instance_id":        identity.InstanceID,
			"template_id":        templateDef.FlowID,
			"flow_scope_key":     templateScope,
		}
		for _, subscriberTemplate := range templateDef.Subscribers {
			subscriber := Subscriber{
				ID:   routeRenderTemplate(subscriberTemplate.IDTemplate, vars),
				Type: subscriberTemplate.Type,
				Path: instancePath,
			}
			for _, rawPattern := range subscriberTemplate.RawPatterns {
				for _, resolved := range routeResolveSubscriberPatterns(rt.source, templateDef.FlowID, templateDef.InputEvents, instancePath, templateDef.LocalEvents, rawPattern) {
					if strings.TrimSpace(resolved.EventPattern) == "" {
						continue
					}
					subscriber.RouteSource = resolved.RouteSource
					rt.patterns = append(rt.patterns, routePattern{
						EventPattern: resolved.EventPattern,
						Subscriber:   subscriber,
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
	if strings.TrimSpace(template.ID) == "" {
		return fmt.Errorf("route template %q not found", templateID)
	}
	localEvents := routeNodeLocalEventSet(template)
	rt.instances[instancePath] = struct{}{}
	rt.instanceEventPath[instancePath] = rt.addEventPathsLocked(instancePath, localEvents)
	subscriber := Subscriber{
		ID:   routeRenderTemplate(template.ID, map[string]string{"instance_id": identity.InstanceID}),
		Type: "node",
		Path: instancePath,
	}
	for _, rawPattern := range normalizeStringList(template.SubscribesTo) {
		for _, resolved := range routeResolveSubscriberPatterns(rt.source, templateID, nil, instancePath, localEvents, rawPattern) {
			if strings.TrimSpace(resolved.EventPattern) == "" {
				continue
			}
			subscriber.RouteSource = resolved.RouteSource
			rt.patterns = append(rt.patterns, routePattern{
				EventPattern: resolved.EventPattern,
				Subscriber:   subscriber,
				InstancePath: instancePath,
			})
		}
	}
	rt.rebuildLocked()
	return nil
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
		eventPath:         make(map[string]struct{}),
		templates:         make(map[string]routeFlowTemplate),
		instances:         make(map[string]struct{}),
		instanceEventPath: make(map[string][]string),
	}
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

func (rt *RouteTable) addAgentPatternsLocked(source semanticview.Source, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, agents map[string]runtimecontracts.AgentRegistryEntry) {
	for _, key := range sortedStringKeys(agents) {
		entry := agents[key]
		subscriber := Subscriber{
			ID:   strings.TrimSpace(entry.ID),
			Type: "agent",
			Path: strings.Trim(strings.TrimSpace(basePath), "/"),
		}
		for _, rawPattern := range normalizeStringList(entry.Subscriptions) {
			for _, resolved := range routeResolveSubscriberPatterns(source, flowID, inputEvents, basePath, localEvents, rawPattern) {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				subscriber.RouteSource = resolved.RouteSource
				rt.patterns = append(rt.patterns, routePattern{
					EventPattern: resolved.EventPattern,
					Subscriber:   subscriber,
				})
			}
		}
	}
}

func (rt *RouteTable) addNodePatternsLocked(source semanticview.Source, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, nodes map[string]runtimecontracts.SystemNodeContract) {
	for _, key := range sortedStringKeys(nodes) {
		entry := nodes[key]
		nodeID := strings.TrimSpace(entry.ID)
		if nodeID == "" {
			nodeID = strings.TrimSpace(key)
		}
		subscriber := Subscriber{
			ID:   nodeID,
			Type: "node",
			Path: strings.Trim(strings.TrimSpace(basePath), "/"),
		}
		patterns := normalizeStringList(entry.SubscribesTo)
		if source != nil {
			patterns = source.NodeRuntimeSubscriptions(nodeID)
		}
		for _, rawPattern := range patterns {
			for _, resolved := range routeResolveSubscriberPatterns(source, flowID, inputEvents, basePath, localEvents, rawPattern) {
				if strings.TrimSpace(resolved.EventPattern) == "" {
					continue
				}
				subscriber.RouteSource = resolved.RouteSource
				rt.patterns = append(rt.patterns, routePattern{
					EventPattern: resolved.EventPattern,
					Subscriber:   subscriber,
				})
			}
		}
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
					subscriber.MatchPattern = pattern.EventPattern
					rt.routes[eventType] = appendUniqueSubscriber(rt.routes[eventType], subscriber)
				}
			}
			continue
		}
		subscriber := pattern.Subscriber
		subscriber.MatchPattern = pattern.EventPattern
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
	return len(source.ResolveFlowInputAutoWire(flowID, eventType).Patterns) > 0
}

func routeNodeLocalEventSet(node runtimecontracts.SystemNodeContract) map[string]struct{} {
	out := make(map[string]struct{})
	for _, eventType := range normalizeStringList(node.Produces) {
		out[eventType] = struct{}{}
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
		nodeID := strings.TrimSpace(entry.ID)
		if nodeID == "" {
			nodeID = strings.TrimSpace(key)
		}
		patterns := normalizeStringList(entry.SubscribesTo)
		if source != nil {
			patterns = source.NodeRuntimeSubscriptions(nodeID)
		}
		if len(patterns) == 0 {
			continue
		}
		out = append(out, routeSubscriberTemplate{
			IDTemplate:  nodeID,
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

func routeResolvedPatternsForList(source semanticview.Source, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, patterns []string) []routeResolvedPattern {
	out := make([]routeResolvedPattern, 0, len(patterns))
	for _, raw := range patterns {
		out = append(out, routeResolveSubscriberPatterns(source, flowID, inputEvents, basePath, localEvents, raw)...)
	}
	return out
}

func routeResolveSubscriberPatterns(source semanticview.Source, flowID string, inputEvents []string, basePath string, localEvents map[string]struct{}, raw string) []routeResolvedPattern {
	raw = eventidentity.Normalize(raw)
	flowID = strings.TrimSpace(flowID)
	if raw == "" {
		return nil
	}
	if flowID != "" && source != nil && source.FlowHasInputEvent(flowID, raw) {
		patterns := routeInputProducerPatterns(source.ResolveFlowInputAutoWire(flowID, raw))
		if len(patterns) > 0 {
			return patterns
		}
	}
	scope := routeEventIdentityScope(basePath, localEvents, inputEvents)
	pattern := scope.ResolveSubscriptionPattern(raw, routeDescendantScopes(source, flowID))
	if pattern == "" {
		return nil
	}
	return []routeResolvedPattern{{EventPattern: pattern, RouteSource: "subscription"}}
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

func appendUniqueSubscriber(in []Subscriber, subscriber Subscriber) []Subscriber {
	for _, existing := range in {
		if existing.ID == subscriber.ID && existing.Type == subscriber.Type && existing.Path == subscriber.Path {
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
