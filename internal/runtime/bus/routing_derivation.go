package bus

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
)

type Subscriber struct {
	ID   string
	Type string
	Path string
}

type RouteTable struct {
	mu        sync.RWMutex
	routes    map[string][]Subscriber
	patterns  []routePattern
	eventPath map[string]struct{}
	templates map[string]routeFlowTemplate
	instances map[string]struct{}
}

type routePattern struct {
	EventPattern string
	Subscriber   Subscriber
}

type routeFlowTemplate struct {
	LocalEvents map[string]struct{}
	Subscribers []routeSubscriberTemplate
}

type routeSubscriberTemplate struct {
	IDTemplate string
	Type       string
	Patterns   []string
}

func DeriveRouteTable(bundle *runtimecontracts.WorkflowContractBundle) (*RouteTable, error) {
	rt := newRouteTable()
	if bundle == nil {
		return rt, nil
	}

	projectKeys := sortedStringKeys(bundle.ProjectContracts)
	for _, key := range projectKeys {
		view := bundle.ProjectContracts[key]
		localEvents := routeProjectLocalEventSet(view)
		rt.addEventPathsLocked("", localEvents)
		rt.addAgentPatternsLocked("", localEvents, view.Agents)
		rt.addNodePatternsLocked("", localEvents, view.Nodes)
	}

	flowIDs := sortedStringKeys(bundle.FlowContracts)
	for _, flowID := range flowIDs {
		view := bundle.FlowContracts[flowID]
		flowPath := routeFlowPath(bundle, flowID)
		localEvents := routeFlowLocalEventSet(view)
		if strings.EqualFold(routeFlowMode(view), "template") {
			rt.templates[flowID] = routeFlowTemplate{
				LocalEvents: cloneStringSet(localEvents),
				Subscribers: routeSubscriberTemplates(view),
			}
			continue
		}
		rt.addEventPathsLocked(flowPath, localEvents)
		rt.addAgentPatternsLocked(flowPath, localEvents, view.Agents)
		rt.addNodePatternsLocked(flowPath, localEvents, view.Nodes)
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
	return cloneSubscribers(rt.routes[eventType])
}

func (rt *RouteTable) AddFlowInstance(template runtimecontracts.SystemNodeContract, instancePath string) error {
	if rt == nil {
		return fmt.Errorf("route table is required")
	}

	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if instancePath == "" {
		return fmt.Errorf("instance path is required")
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if _, exists := rt.instances[instancePath]; exists {
		return nil
	}

	templateID := routeFirstPathSegment(instancePath)
	if templateDef, ok := rt.templates[templateID]; ok {
		rt.instances[instancePath] = struct{}{}
		rt.addEventPathsLocked(instancePath, templateDef.LocalEvents)
		instanceID := routeLastPathSegment(instancePath)
		vars := map[string]string{
			"flow_instance_path": instancePath,
			"instance_id":        instanceID,
			"template_id":        templateID,
			"vertical_id":        instanceID,
		}
		for _, subscriberTemplate := range templateDef.Subscribers {
			subscriber := Subscriber{
				ID:   routeRenderTemplate(subscriberTemplate.IDTemplate, vars),
				Type: subscriberTemplate.Type,
				Path: instancePath,
			}
			for _, rawPattern := range subscriberTemplate.Patterns {
				pattern := routeResolvePattern(instancePath, templateDef.LocalEvents, rawPattern)
				if pattern == "" {
					continue
				}
				rt.patterns = append(rt.patterns, routePattern{
					EventPattern: pattern,
					Subscriber:   subscriber,
				})
			}
		}
		rt.rebuildLocked()
		return nil
	}

	// Compatibility fallback for the current odd handoff signature.
	if strings.TrimSpace(template.ID) == "" {
		return fmt.Errorf("route template %q not found", templateID)
	}
	localEvents := routeNodeLocalEventSet(template)
	rt.instances[instancePath] = struct{}{}
	rt.addEventPathsLocked(instancePath, localEvents)
	subscriber := Subscriber{
		ID:   routeRenderTemplate(template.ID, map[string]string{"instance_id": routeLastPathSegment(instancePath), "vertical_id": routeLastPathSegment(instancePath)}),
		Type: "node",
		Path: instancePath,
	}
	for _, rawPattern := range normalizeStringList(template.SubscribesTo) {
		pattern := routeResolvePattern(instancePath, localEvents, rawPattern)
		if pattern == "" {
			continue
		}
		rt.patterns = append(rt.patterns, routePattern{
			EventPattern: pattern,
			Subscriber:   subscriber,
		})
	}
	rt.rebuildLocked()
	return nil
}

func newRouteTable() *RouteTable {
	return &RouteTable{
		routes:    make(map[string][]Subscriber),
		eventPath: make(map[string]struct{}),
		templates: make(map[string]routeFlowTemplate),
		instances: make(map[string]struct{}),
	}
}

func (rt *RouteTable) addEventPathsLocked(basePath string, localEvents map[string]struct{}) {
	for _, eventType := range sortedStringKeys(localEvents) {
		absolute := routeResolvePattern(basePath, localEvents, eventType)
		if absolute == "" || strings.Contains(absolute, "*") {
			continue
		}
		rt.eventPath[absolute] = struct{}{}
	}
}

func (rt *RouteTable) addAgentPatternsLocked(basePath string, localEvents map[string]struct{}, agents map[string]runtimecontracts.AgentRegistryEntry) {
	for _, key := range sortedStringKeys(agents) {
		entry := agents[key]
		subscriber := Subscriber{
			ID:   strings.TrimSpace(entry.ID),
			Type: "agent",
			Path: strings.Trim(strings.TrimSpace(basePath), "/"),
		}
		for _, rawPattern := range normalizeStringList(entry.Subscriptions) {
			pattern := routeResolvePattern(basePath, localEvents, rawPattern)
			if pattern == "" {
				continue
			}
			rt.patterns = append(rt.patterns, routePattern{
				EventPattern: pattern,
				Subscriber:   subscriber,
			})
		}
	}
}

func (rt *RouteTable) addNodePatternsLocked(basePath string, localEvents map[string]struct{}, nodes map[string]runtimecontracts.SystemNodeContract) {
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
		for _, rawPattern := range normalizeStringList(entry.SubscribesTo) {
			pattern := routeResolvePattern(basePath, localEvents, rawPattern)
			if pattern == "" {
				continue
			}
			rt.patterns = append(rt.patterns, routePattern{
				EventPattern: pattern,
				Subscriber:   subscriber,
			})
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
					rt.routes[eventType] = appendUniqueSubscriber(rt.routes[eventType], pattern.Subscriber)
				}
			}
			continue
		}
		rt.routes[pattern.EventPattern] = appendUniqueSubscriber(rt.routes[pattern.EventPattern], pattern.Subscriber)
	}
}

func routeProjectLocalEventSet(view runtimecontracts.ProjectContractView) map[string]struct{} {
	return routeEventKeys(view.Events)
}

func routeFlowLocalEventSet(view runtimecontracts.FlowContractView) map[string]struct{} {
	out := routeEventKeys(view.Events)
	for _, eventType := range view.Schema.Pins.Inputs.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, eventType := range view.Schema.Pins.Outputs.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	if autoEmit := strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event); autoEmit != "" {
		out[autoEmit] = struct{}{}
	}
	return out
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

func routeSubscriberTemplates(view runtimecontracts.FlowContractView) []routeSubscriberTemplate {
	out := make([]routeSubscriberTemplate, 0, len(view.Agents)+len(view.Nodes))
	for _, key := range sortedStringKeys(view.Agents) {
		entry := view.Agents[key]
		patterns := normalizeStringList(entry.Subscriptions)
		if len(patterns) == 0 {
			continue
		}
		out = append(out, routeSubscriberTemplate{
			IDTemplate: strings.TrimSpace(entry.ID),
			Type:       "agent",
			Patterns:   patterns,
		})
	}
	for _, key := range sortedStringKeys(view.Nodes) {
		entry := view.Nodes[key]
		patterns := normalizeStringList(entry.SubscribesTo)
		if len(patterns) == 0 {
			continue
		}
		nodeID := strings.TrimSpace(entry.ID)
		if nodeID == "" {
			nodeID = strings.TrimSpace(key)
		}
		out = append(out, routeSubscriberTemplate{
			IDTemplate: nodeID,
			Type:       "node",
			Patterns:   patterns,
		})
	}
	return out
}

func routeFlowPath(bundle *runtimecontracts.WorkflowContractBundle, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	if bundle != nil {
		for path, view := range bundle.FlowTree.ByPath {
			if view != nil && strings.TrimSpace(view.Paths.ID) == flowID {
				path = strings.Trim(strings.TrimSpace(path), "/")
				if path != "" {
					return path
				}
			}
		}
	}
	return flowID
}

func routeFlowMode(view runtimecontracts.FlowContractView) string {
	if mode := strings.TrimSpace(view.Schema.Mode); mode != "" {
		return mode
	}
	return strings.TrimSpace(view.Paths.Mode)
}

func routeResolvePattern(basePath string, localEvents map[string]struct{}, raw string) string {
	raw = strings.Trim(strings.TrimSpace(raw), "/")
	basePath = strings.Trim(strings.TrimSpace(basePath), "/")
	switch {
	case raw == "":
		return ""
	case strings.Contains(raw, "://"):
		return raw
	case strings.HasPrefix(raw, "*/"), strings.HasPrefix(raw, "**/"):
		if basePath == "" {
			return raw
		}
		return basePath + "/" + raw
	case strings.Contains(raw, "/"):
		return raw
	case routeIsLocalEvent(localEvents, raw):
		if basePath == "" {
			return raw
		}
		return basePath + "/" + raw
	default:
		return raw
	}
}

func routeIsLocalEvent(localEvents map[string]struct{}, raw string) bool {
	if len(localEvents) == 0 {
		return false
	}
	_, ok := localEvents[strings.TrimSpace(raw)]
	return ok
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

func routeFirstPathSegment(raw string) string {
	parts := splitRouteSegments(raw)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func routeLastPathSegment(raw string) string {
	parts := splitRouteSegments(raw)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[len(parts)-1])
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
