package contracts

import (
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	"sort"
	"strings"
)

func (b *WorkflowContractBundle) WorkflowName() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.Name)
}
func (b *WorkflowContractBundle) WorkflowVersion() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.Version)
}
func (b *WorkflowContractBundle) WorkflowEntitySchema() EntitySchema {
	if b == nil {
		return EntitySchema{}
	}
	return b.Semantics.EntitySchema
}
func (b *WorkflowContractBundle) WorkflowStages() []WorkflowStageContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Stages
}
func (b *WorkflowContractBundle) WorkflowTerminalStages() []string {
	if b == nil {
		return nil
	}
	return b.Semantics.TerminalStages
}
func (b *WorkflowContractBundle) WorkflowTransitions() []WorkflowTransitionContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Transitions
}
func (b *WorkflowContractBundle) WorkflowInitialStage() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.InitialStage)
}
func (b *WorkflowContractBundle) WorkflowTimers() []WorkflowTimerContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Timers
}
func (b *WorkflowContractBundle) WorkflowTimerByID(id string) (WorkflowTimerContract, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return WorkflowTimerContract{}, false
	}
	for _, timer := range b.Semantics.Timers {
		if strings.TrimSpace(timer.ID) == id {
			return timer, true
		}
	}
	return WorkflowTimerContract{}, false
}
func (b *WorkflowContractBundle) FlowViewByID(id string) (*FlowContractView, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return nil, false
	}
	if view, ok := b.FlowTree.ByID[id]; ok && view != nil {
		return view, true
	}
	return nil, false
}
func (b *WorkflowContractBundle) FlowSchemaByID(id string) (FlowSchemaDocument, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return FlowSchemaDocument{}, false
	}
	schema, ok := b.FlowSchemas[id]
	return schema, ok
}
func (b *WorkflowContractBundle) HasFlow(id string) bool {
	_, ok := b.FlowViewByID(id)
	return ok
}
func (b *WorkflowContractBundle) ProjectViews() []ProjectContractView {
	if b == nil || len(b.projectContracts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(b.projectContracts))
	for key := range b.projectContracts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	views := make([]ProjectContractView, 0, len(keys))
	for _, key := range keys {
		views = append(views, b.projectContracts[key])
	}
	return views
}
func (b *WorkflowContractBundle) ProjectViewByKey(key string) (ProjectContractView, bool) {
	key = strings.TrimSpace(key)
	if b == nil || key == "" {
		return ProjectContractView{}, false
	}
	view, ok := b.projectContracts[key]
	return view, ok
}
func (b *WorkflowContractBundle) RootProjectViews() []ProjectContractView {
	if b == nil || len(b.PackageTree) == 0 {
		return nil
	}
	views := make([]ProjectContractView, 0, len(b.PackageTree))
	for _, pkg := range b.PackageTree {
		if strings.TrimSpace(pkg.ParentKey) != "" {
			continue
		}
		if view, ok := b.ProjectViewByKey(pkg.Key); ok {
			views = append(views, view)
		}
	}
	return views
}
func (b *WorkflowContractBundle) FlowViews() []FlowContractView {
	if b == nil {
		return nil
	}
	return flowmodel.ViewsByPath(
		b.FlowTree,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		func(view *FlowContractView) string { return strings.TrimSpace(view.Path) },
		flowViewChildren,
	)
}
func (b *WorkflowContractBundle) NodeEntries() map[string]SystemNodeContract {
	if b == nil {
		return nil
	}
	if len(b.Nodes) == 0 {
		out := make(map[string]SystemNodeContract)
		for _, view := range b.ProjectViews() {
			for key, entry := range view.Nodes {
				out[key] = entry
			}
		}
		for _, view := range b.FlowViews() {
			for key, entry := range view.Nodes {
				out[key] = entry
			}
		}
		return out
	}
	return cloneSystemNodeContractMap(b.Nodes)
}
func (b *WorkflowContractBundle) NodeEntry(id string) (SystemNodeContract, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return SystemNodeContract{}, false
	}
	entry, ok := b.Nodes[id]
	return entry, ok
}
func (b *WorkflowContractBundle) HasNode(id string) bool {
	_, ok := b.NodeEntry(id)
	return ok
}
func (b *WorkflowContractBundle) AgentEntries() map[string]AgentRegistryEntry {
	if b == nil {
		return nil
	}
	return cloneAgentRegistryEntryMap(b.Agents)
}
func (b *WorkflowContractBundle) AgentEntry(id string) (AgentRegistryEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return AgentRegistryEntry{}, false
	}
	entry, ok := b.Agents[id]
	return entry, ok
}
func (b *WorkflowContractBundle) HasAgent(id string) bool {
	_, ok := b.AgentEntry(id)
	return ok
}
func (b *WorkflowContractBundle) ToolEntries() map[string]ToolSchemaEntry {
	if b == nil {
		return nil
	}
	return cloneToolSchemaEntryMap(b.Tools)
}
func (b *WorkflowContractBundle) ToolEntryForAgent(agentID, toolID string) (ToolSchemaEntry, bool) {
	agentID = strings.TrimSpace(agentID)
	toolID = strings.TrimSpace(toolID)
	if b == nil || agentID == "" || toolID == "" {
		return ToolSchemaEntry{}, false
	}
	source, ok := b.AgentContractSource(agentID)
	if !ok {
		entry, ok := b.Tools[toolID]
		return entry, ok
	}
	if entry, ok := b.scopedTools[contractScopeKey(source, toolID)]; ok {
		return entry, true
	}
	entry, ok := b.Tools[toolID]
	return entry, ok
}
func (b *WorkflowContractBundle) EventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	return cloneEventCatalogEntryMap(b.Events)
}
func (b *WorkflowContractBundle) EventEntry(eventType string) (EventCatalogEntry, bool) {
	eventType = strings.TrimSpace(eventType)
	if b == nil || eventType == "" {
		return EventCatalogEntry{}, false
	}
	entry, ok := b.Events[eventType]
	return entry, ok
}
func (b *WorkflowContractBundle) HasEvent(eventType string) bool {
	_, ok := b.EventEntry(eventType)
	return ok
}
func (b *WorkflowContractBundle) ResolvedPolicyForFlow(flowID string) PolicyDocument {
	if b == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	return flowmodel.ResolvePolicyByID(
		b.Policy,
		b.FlowTree,
		flowID,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		func(view *FlowContractView) PolicyDocument { return view.Policy },
		flowViewChildren,
	)
}
func (b *WorkflowContractBundle) PolicyValueForFlow(flowID, key string) (PolicyValue, bool) {
	doc := b.ResolvedPolicyForFlow(flowID)
	value, ok := doc.Values[strings.TrimSpace(key)]
	return value, ok
}
func (b *WorkflowContractBundle) ResolvedPolicyForNode(nodeID string) PolicyDocument {
	if b == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	if source, ok := b.NodeContractSource(nodeID); ok {
		return b.ResolvedPolicyForFlow(source.FlowID)
	}
	return b.ResolvedPolicyForFlow("")
}
func (b *WorkflowContractBundle) PolicyValueForNode(nodeID, key string) (PolicyValue, bool) {
	doc := b.ResolvedPolicyForNode(nodeID)
	value, ok := doc.Values[strings.TrimSpace(key)]
	return value, ok
}
func (b *WorkflowContractBundle) FlowPath(flowID string) string {
	if b == nil {
		return ""
	}
	return flowmodel.PathForID(b.FlowTree, flowID, func(view *FlowContractView) string { return view.Path })
}
func (b *WorkflowContractBundle) ResolvedEventCatalog() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	if b.FlowTree.Root == nil {
		return cloneEventCatalogEntryMap(b.Events)
	}
	return flowmodel.ResolveEntries(
		b.FlowTree,
		flowViewChildren,
		func(view *FlowContractView) map[string]EventCatalogEntry { return view.Events },
	)
}
func (b *WorkflowContractBundle) ResolveFlowEventCatalogEntry(flowID, eventType string) (EventCatalogEntry, string, bool) {
	if b == nil {
		return EventCatalogEntry{}, "", false
	}
	catalog := b.ResolvedEventCatalog()
	rawKey := eventidentity.Normalize(eventType)
	if entry, ok := catalog[rawKey]; ok {
		return entry, rawKey, true
	}
	resolvedKey := b.ResolveFlowEventReference(flowID, eventType)
	if resolvedKey == rawKey {
		return EventCatalogEntry{}, "", false
	}
	entry, ok := catalog[resolvedKey]
	if !ok {
		return EventCatalogEntry{}, "", false
	}
	return entry, resolvedKey, true
}
func clonePolicyDocument(in PolicyDocument) PolicyDocument {
	return flowmodel.ClonePolicyDocument(in)
}
func (b *WorkflowContractBundle) GuardEntries() []GuardActionEntry {
	if b == nil {
		return nil
	}
	return b.Semantics.Guards
}
func (b *WorkflowContractBundle) ActionEntries() []GuardActionEntry {
	if b == nil {
		return nil
	}
	return b.Semantics.Actions
}
func (b *WorkflowContractBundle) GuardEntryByID(id string) (GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return GuardActionEntry{}, false
	}
	entry, ok := b.Semantics.GuardByID[id]
	return entry, ok
}
func (b *WorkflowContractBundle) ActionEntryByID(id string) (GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return GuardActionEntry{}, false
	}
	entry, ok := b.Semantics.ActionByID[id]
	return entry, ok
}
func (b *WorkflowContractBundle) FlowInitialStage(flowID string) string {
	if b == nil {
		return ""
	}
	flowID = strings.TrimSpace(flowID)
	if initial := strings.TrimSpace(b.Semantics.FlowInitial[flowID]); initial != "" {
		return initial
	}
	if flowID != "" && flowID == b.WorkflowName() {
		return b.WorkflowInitialStage()
	}
	return ""
}
func (b *WorkflowContractBundle) FlowStates(flowID string) []string {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if states := b.Semantics.FlowStates[flowID]; len(states) > 0 {
		return append([]string{}, states...)
	}
	if flowID != "" && flowID == b.WorkflowName() {
		out := make([]string, 0, len(b.Semantics.Stages))
		for _, stage := range b.Semantics.Stages {
			stageID := strings.TrimSpace(stage.ID)
			if stageID == "" {
				continue
			}
			out = append(out, stageID)
		}
		return out
	}
	return nil
}
func (b *WorkflowContractBundle) FlowTerminalStages(flowID string) []string {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if terminal := b.Semantics.FlowTerminal[flowID]; len(terminal) > 0 {
		return append([]string{}, terminal...)
	}
	if flowID != "" && flowID == b.WorkflowName() {
		return append([]string{}, b.Semantics.TerminalStages...)
	}
	return nil
}
func (b *WorkflowContractBundle) FlowNamespace(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowNamespace[strings.TrimSpace(flowID)])
}
func (b *WorkflowContractBundle) FlowNamespacePrefix(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowPrefix[strings.TrimSpace(flowID)])
}
func (b *WorkflowContractBundle) FlowNamespaceRule(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowRules[strings.TrimSpace(flowID)])
}
func (b *WorkflowContractBundle) FlowInputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowInputs[strings.TrimSpace(flowID)]...)
}
func (b *WorkflowContractBundle) FlowOutputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowOutputs[strings.TrimSpace(flowID)]...)
}
func (b *WorkflowContractBundle) FlowReadPins(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowReads[strings.TrimSpace(flowID)]...)
}
func (b *WorkflowContractBundle) FlowWritePins(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowWrites[strings.TrimSpace(flowID)]...)
}
func (b *WorkflowContractBundle) FlowHasInputEvent(flowID, eventType string) bool {
	return b.flowEventScope(flowID).HasInput(eventType)
}
func (b *WorkflowContractBundle) FlowHasOutputEvent(flowID, eventType string) bool {
	return b.flowEventScope(flowID).HasOutput(eventType)
}
func (b *WorkflowContractBundle) ResolveFlowInputAutoWire(targetFlowID, eventType string) FlowInputAutoWireResolution {
	targetFlowID = strings.TrimSpace(targetFlowID)
	eventType = eventidentity.Normalize(eventType)
	if b == nil || targetFlowID == "" || eventType == "" || !b.FlowHasInputEvent(targetFlowID, eventType) {
		return FlowInputAutoWireResolution{EventType: eventType}
	}

	out := FlowInputAutoWireResolution{EventType: eventType}
	seenPatterns := make(map[string]struct{})
	appendPattern := func(value string) {
		value = eventidentity.Normalize(value)
		if value == "" {
			return
		}
		if _, ok := seenPatterns[value]; ok {
			return
		}
		seenPatterns[value] = struct{}{}
		out.Patterns = append(out.Patterns, value)
	}

	for _, view := range b.ProjectViews() {
		if _, ok := view.Events[eventType]; ok {
			appendPattern(eventType)
		}
	}

	seenFlows := make(map[string]struct{})
	for _, view := range b.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		if flowID == "" || flowID == targetFlowID || !b.FlowHasOutputEvent(flowID, eventType) {
			continue
		}
		if _, ok := seenFlows[flowID]; ok {
			continue
		}
		seenFlows[flowID] = struct{}{}
		out.ProducerFlows = append(out.ProducerFlows, flowID)
	}

	sort.Strings(out.ProducerFlows)
	if len(out.ProducerFlows) == 1 {
		appendPattern(b.ResolveFlowEventReference(out.ProducerFlows[0], eventType))
	}
	sort.Strings(out.Patterns)
	return out
}
func (b *WorkflowContractBundle) FlowInputProducerPatterns(targetFlowID, eventType string) []string {
	return append([]string{}, b.ResolveFlowInputAutoWire(targetFlowID, eventType).Patterns...)
}
func (b *WorkflowContractBundle) ResolveFlowEventReference(flowID, eventType string) string {
	scope := b.flowEventScope(flowID)
	return scope.ResolveEvent(eventType, b.flowEventDescendants(flowID))
}
func (b *WorkflowContractBundle) ResolveFlowEventPattern(flowID, pattern string) string {
	scope := b.flowEventScope(flowID)
	return scope.ResolveSubscriptionPattern(pattern, b.flowEventDescendants(flowID))
}
func (b *WorkflowContractBundle) FlowEventMatches(flowID, subscription, eventType string) bool {
	scope := b.flowEventScope(flowID)
	return scope.Matches(subscription, eventType, b.flowEventDescendants(flowID))
}
func (b *WorkflowContractBundle) FlowRequiredAgents(flowID string) []FlowRequiredAgent {
	if b == nil {
		return nil
	}
	agents := b.Semantics.FlowAgents[strings.TrimSpace(flowID)]
	out := make([]FlowRequiredAgent, len(agents))
	copy(out, agents)
	return out
}
func (b *WorkflowContractBundle) RootRequiredAgents() []FlowRequiredAgent {
	if b == nil || b.RootSchema == nil {
		return nil
	}
	out := make([]FlowRequiredAgent, len(b.RootSchema.RequiredAgents))
	copy(out, b.RootSchema.RequiredAgents)
	return out
}
func (b *WorkflowContractBundle) WritePinOwners(pin string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.WritePinOwners[strings.TrimSpace(pin)]...)
}
func (b *WorkflowContractBundle) NodeContractSource(nodeID string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	source, ok := b.nodeSources[nodeID]
	if ok {
		return source, true
	}
	for _, view := range b.ProjectViews() {
		for key, entry := range view.Nodes {
			if strings.TrimSpace(key) != nodeID && strings.TrimSpace(entry.ID) != nodeID {
				continue
			}
			return ContractItemSource{
				PackageKey: strings.TrimSpace(view.Paths.Key),
				Layer:      "project",
			}, true
		}
	}
	for _, view := range b.FlowViews() {
		for key, entry := range view.Nodes {
			if strings.TrimSpace(key) != nodeID && strings.TrimSpace(entry.ID) != nodeID {
				continue
			}
			return ContractItemSource{
				PackageKey: strings.TrimSpace(view.Paths.PackageKey),
				FlowID:     strings.TrimSpace(view.Paths.ID),
				Layer:      "flow",
			}, true
		}
	}
	return ContractItemSource{}, false
}
func (b *WorkflowContractBundle) EventContractSource(eventType string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.eventSources[strings.TrimSpace(eventType)]
	return source, ok
}
func (b *WorkflowContractBundle) AgentContractSource(agentID string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.agentSources[strings.TrimSpace(agentID)]
	return source, ok
}
func (b *WorkflowContractBundle) ResolveNodeEventReference(nodeID, eventType string) string {
	if b == nil {
		return eventidentity.Normalize(eventType)
	}
	return b.nodeEventScope(nodeID).ResolveEvent(eventType, b.flowEventDescendants(b.nodeFlowID(nodeID)))
}
func (b *WorkflowContractBundle) NodeRuntimeSubscriptions(nodeID string) []string {
	nodeID = strings.TrimSpace(nodeID)
	if b == nil || nodeID == "" {
		return nil
	}
	entry, ok := b.nodeContract(nodeID)
	if !ok {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(entry.SubscribesTo)+len(entry.EventHandlers))
	appendSubscription := func(value string) {
		value = eventidentity.Normalize(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, eventType := range entry.SubscribesTo {
		appendSubscription(eventType)
	}
	for _, eventType := range b.NodeHandlerSubscriptions(nodeID) {
		appendSubscription(eventType)
	}
	sort.Strings(out)
	return out
}
func (b *WorkflowContractBundle) ScopedAgentEntries() map[string]AgentRegistryEntry {
	if b == nil {
		return nil
	}
	return cloneAgentRegistryEntryMap(b.scopedAgents)
}
func (b *WorkflowContractBundle) ScopedEventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	return cloneEventCatalogEntryMap(b.scopedEvents)
}
func (b *WorkflowContractBundle) NodeEventHandlers(nodeID string) map[string]SystemNodeEventHandler {
	if b == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok || len(handlers) == 0 {
		entry, ok := b.nodeContract(nodeID)
		if !ok || len(entry.EventHandlers) == 0 {
			return nil
		}
		handlers = entry.EventHandlers
	}
	out := make(map[string]SystemNodeEventHandler, len(handlers))
	for eventType, handler := range handlers {
		out[eventType] = handler
	}
	return out
}
func (b *WorkflowContractBundle) NodeHandlerSubscriptions(nodeID string) []string {
	handlers := b.NodeEventHandlers(nodeID)
	if len(handlers) == 0 {
		return nil
	}
	out := make([]string, 0, len(handlers))
	for eventType := range handlers {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out = append(out, eventType)
		}
	}
	sort.Strings(out)
	return out
}

type NodeEventHandlerResolution struct {
	NodeID             string
	RawEventType       string
	LocalizedEventType string
	AuthoredEventType  string
	CanonicalEventType string
	Handler            SystemNodeEventHandler
	Matched            bool
}

func (b *WorkflowContractBundle) nodeContract(nodeID string) (SystemNodeContract, bool) {
	nodeID = strings.TrimSpace(nodeID)
	if b == nil || nodeID == "" {
		return SystemNodeContract{}, false
	}
	if entry, ok := b.Nodes[nodeID]; ok {
		return entry, true
	}
	for _, entry := range b.Nodes {
		if strings.TrimSpace(entry.ID) == nodeID {
			return entry, true
		}
	}
	for _, view := range b.ProjectViews() {
		if entry, ok := view.Nodes[nodeID]; ok {
			return entry, true
		}
		for _, entry := range view.Nodes {
			if strings.TrimSpace(entry.ID) == nodeID {
				return entry, true
			}
		}
	}
	for _, view := range b.FlowViews() {
		if entry, ok := view.Nodes[nodeID]; ok {
			return entry, true
		}
		for _, entry := range view.Nodes {
			if strings.TrimSpace(entry.ID) == nodeID {
				return entry, true
			}
		}
	}
	return SystemNodeContract{}, false
}
func (b *WorkflowContractBundle) NodeEventHandler(nodeID, eventType string) (SystemNodeEventHandler, bool) {
	resolved := b.ResolveNodeEventHandler(nodeID, eventType)
	if !resolved.Matched {
		return SystemNodeEventHandler{}, false
	}
	return b.externalizeNodeHandler(nodeID, resolved.Handler), true
}

func (b *WorkflowContractBundle) ResolveNodeEventHandler(nodeID, eventType string) NodeEventHandlerResolution {
	resolved := NodeEventHandlerResolution{
		NodeID:       strings.TrimSpace(nodeID),
		RawEventType: eventidentity.Normalize(eventType),
	}
	if b == nil {
		return resolved
	}
	nodeID = resolved.NodeID
	rawEventType := resolved.RawEventType
	localizedEventType := b.localizeNodeEventType(nodeID, rawEventType)
	resolved.LocalizedEventType = localizedEventType
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok {
		return resolved
	}
	if handler, ok := handlers[localizedEventType]; ok {
		return b.nodeEventHandlerResolution(resolved, localizedEventType, handler)
	}
	if rawEventType != "" && rawEventType != localizedEventType {
		if handler, ok := handlers[rawEventType]; ok {
			return b.nodeEventHandlerResolution(resolved, rawEventType, handler)
		}
	}
	for pattern, handler := range handlers {
		if handlerPatternMatches(pattern, localizedEventType) {
			return b.nodeEventHandlerResolution(resolved, pattern, handler)
		}
	}
	if rawEventType != "" && rawEventType != localizedEventType {
		for pattern, handler := range handlers {
			if handlerPatternMatches(pattern, rawEventType) {
				return b.nodeEventHandlerResolution(resolved, pattern, handler)
			}
		}
	}
	return resolved
}

func (b *WorkflowContractBundle) nodeEventHandlerResolution(resolved NodeEventHandlerResolution, authoredEventType string, handler SystemNodeEventHandler) NodeEventHandlerResolution {
	authoredEventType = strings.TrimSpace(authoredEventType)
	resolved.AuthoredEventType = authoredEventType
	resolved.CanonicalEventType = b.ResolveNodeEventReference(resolved.NodeID, authoredEventType)
	resolved.Handler = handler
	resolved.Matched = true
	return resolved
}
func (b *WorkflowContractBundle) RuntimeEventOwners(eventType string) []string {
	if b == nil {
		return nil
	}
	rawEventType := eventidentity.Normalize(eventType)
	owners := b.runtimeEventOwnersForQuery(rawEventType)
	for nodeID, handlers := range b.Semantics.NodeHandlers {
		for pattern := range handlers {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" || !strings.Contains(pattern, "*") {
				continue
			}
			canonicalPattern := strings.TrimSpace(b.resolveNodeEventOwnerPattern(nodeID, pattern))
			if canonicalPattern == "" {
				canonicalPattern = pattern
			}
			if handlerPatternMatches(canonicalPattern, rawEventType) {
				owners = appendIfMissingString(owners, nodeID)
				break
			}
		}
	}
	return owners
}

func (b *WorkflowContractBundle) runtimeEventOwnersForQuery(eventType string) []string {
	eventType = eventidentity.Normalize(eventType)
	if b == nil || eventType == "" {
		return nil
	}
	if owners := b.Semantics.EventOwners[eventType]; len(owners) > 0 {
		return append([]string{}, owners...)
	}
	if strings.Contains(eventType, "/") {
		return nil
	}

	var matched []string
	for canonical, owners := range b.Semantics.EventOwners {
		canonical = eventidentity.Normalize(canonical)
		if eventidentity.LeafName(canonical) != eventType {
			continue
		}
		if matched != nil {
			// A local name that resolves to more than one canonical owner is
			// ambiguous; callers must use the scoped event identity.
			return nil
		}
		matched = append([]string{}, owners...)
	}
	return matched
}

func (b *WorkflowContractBundle) resolveNodeEventOwnerPattern(nodeID, pattern string) string {
	pattern = eventidentity.Normalize(pattern)
	if b == nil || pattern == "" {
		return pattern
	}
	flowID := b.nodeFlowID(nodeID)
	scope := b.nodeEventScope(nodeID)
	resolved := strings.TrimSpace(scope.ResolveSubscriptionPattern(pattern, b.flowEventDescendants(flowID)))
	if resolved == "" || resolved != pattern || strings.Contains(pattern, "/") {
		return resolved
	}
	path := eventidentity.Normalize(scope.Path)
	if path == "" {
		return resolved
	}
	for _, localEvent := range scope.LocalEvents {
		if eventidentity.MatchPattern(pattern, localEvent) {
			return path + "/" + pattern
		}
	}
	return resolved
}

func (b *WorkflowContractBundle) localizeNodeEventType(nodeID, eventType string) string {
	return b.nodeEventScope(nodeID).LocalizeInput(eventType)
}

func (b *WorkflowContractBundle) externalizeNodeEventType(nodeID, eventType string) string {
	return b.ResolveNodeEventReference(nodeID, eventType)
}

func (b *WorkflowContractBundle) flowLocalEvents(flowID string) []string {
	flowID = strings.TrimSpace(flowID)
	if b == nil || flowID == "" {
		return nil
	}
	view, ok := b.FlowViewByID(flowID)
	if !ok || view == nil {
		return nil
	}
	out := make([]string, 0, len(view.Events)+1)
	for eventType := range view.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out = append(out, eventType)
		}
	}
	for _, eventType := range view.Schema.Pins.Outputs.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out = append(out, eventType)
		}
	}
	if autoEmit := strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event); autoEmit != "" {
		out = append(out, autoEmit)
	}
	return out
}

func (b *WorkflowContractBundle) flowEventScope(flowID string) eventidentity.Scope {
	flowID = strings.TrimSpace(flowID)
	if b == nil || flowID == "" {
		return eventidentity.Scope{}
	}
	view, ok := b.FlowViewByID(flowID)
	if !ok || view == nil {
		return eventidentity.Scope{Path: b.FlowPath(flowID)}
	}
	return eventidentity.Scope{
		Path:         strings.Trim(strings.TrimSpace(view.Path), "/"),
		LocalEvents:  b.flowLocalEvents(flowID),
		InputEvents:  append([]string{}, view.Schema.Pins.Inputs.Events...),
		OutputEvents: append([]string{}, view.Schema.Pins.Outputs.Events...),
	}
}

func (b *WorkflowContractBundle) flowEventDescendants(flowID string) []eventidentity.DescendantScope {
	flowID = strings.TrimSpace(flowID)
	if b == nil || flowID == "" {
		return nil
	}
	scope := b.flowEventScope(flowID)
	parentPath := eventidentity.Normalize(scope.Path)
	if parentPath == "" {
		return nil
	}
	out := make([]eventidentity.DescendantScope, 0)
	for _, view := range b.FlowViews() {
		descendantFlowID := strings.TrimSpace(view.Paths.ID)
		if descendantFlowID == "" || descendantFlowID == flowID {
			continue
		}
		descendantPath := eventidentity.Normalize(view.Path)
		if descendantPath == "" || !strings.HasPrefix(descendantPath, parentPath+"/") {
			continue
		}
		localEvents := b.flowLocalEvents(descendantFlowID)
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

func (b *WorkflowContractBundle) nodeEventScope(nodeID string) eventidentity.Scope {
	if b == nil {
		return eventidentity.Scope{}
	}
	flowID := b.nodeFlowID(nodeID)
	if flowID == "" {
		return eventidentity.Scope{}
	}
	return b.flowEventScope(flowID)
}

func (b *WorkflowContractBundle) externalizeNodeHandler(nodeID string, handler SystemNodeEventHandler) SystemNodeEventHandler {
	handler.Emit = b.externalizeEmitSpec(nodeID, handler.Emit)
	if handler.FanOut != nil {
		clone := *handler.FanOut
		clone.Emit = b.externalizeEmitSpec(nodeID, clone.Emit)
		handler.FanOut = &clone
	}
	if len(handler.Rules) > 0 {
		rules := make([]HandlerRuleEntry, 0, len(handler.Rules))
		for _, rule := range handler.Rules {
			rule.Emit = b.externalizeEmitSpec(nodeID, rule.Emit)
			if rule.FanOut != nil {
				clone := *rule.FanOut
				clone.Emit = b.externalizeEmitSpec(nodeID, clone.Emit)
				rule.FanOut = &clone
			}
			rules = append(rules, rule)
		}
		handler.Rules = rules
	}
	if len(handler.OnComplete) > 0 {
		rules := make([]HandlerRuleEntry, 0, len(handler.OnComplete))
		for _, rule := range handler.OnComplete {
			rule.Emit = b.externalizeEmitSpec(nodeID, rule.Emit)
			if rule.FanOut != nil {
				clone := *rule.FanOut
				clone.Emit = b.externalizeEmitSpec(nodeID, clone.Emit)
				rule.FanOut = &clone
			}
			rules = append(rules, rule)
		}
		handler.OnComplete = rules
	}
	if handler.Accumulate != nil {
		clone := *handler.Accumulate
		if len(clone.OnComplete) > 0 {
			rules := make([]HandlerRuleEntry, 0, len(clone.OnComplete))
			for _, rule := range clone.OnComplete {
				rule.Emit = b.externalizeEmitSpec(nodeID, rule.Emit)
				if rule.FanOut != nil {
					fanOut := *rule.FanOut
					fanOut.Emit = b.externalizeEmitSpec(nodeID, fanOut.Emit)
					rule.FanOut = &fanOut
				}
				rules = append(rules, rule)
			}
			clone.OnComplete = rules
		}
		if clone.OnTimeout != nil {
			onTimeout := *clone.OnTimeout
			onTimeout.Emit = b.externalizeEmitSpec(nodeID, onTimeout.Emit)
			if onTimeout.FanOut != nil {
				fanOut := *onTimeout.FanOut
				fanOut.Emit = b.externalizeEmitSpec(nodeID, fanOut.Emit)
				onTimeout.FanOut = &fanOut
			}
			clone.OnTimeout = &onTimeout
		}
		handler.Accumulate = &clone
	}
	return handler
}

func (b *WorkflowContractBundle) externalizeEmitSpec(nodeID string, spec EmitSpec) EmitSpec {
	spec = cloneEmitSpec(spec)
	spec.Event = b.externalizeNodeEventType(nodeID, spec.Event)
	return spec
}

func (b *WorkflowContractBundle) externalizeEventEmission(nodeID string, emission EventEmission) EventEmission {
	values := emission.Values()
	if len(values) == 0 {
		return EventEmission{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, b.externalizeNodeEventType(nodeID, value))
	}
	if len(out) == 1 {
		return EventEmission{Single: out[0]}
	}
	return EventEmission{Many: out}
}

func (b *WorkflowContractBundle) nodeFlowID(nodeID string) string {
	if b == nil {
		return ""
	}
	source, ok := b.NodeContractSource(nodeID)
	if !ok {
		return ""
	}
	return strings.TrimSpace(source.FlowID)
}

func (b *WorkflowContractBundle) flowHasLocalEvent(flowID, eventType string) bool {
	if b == nil {
		return false
	}
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if flowID == "" || eventType == "" {
		return false
	}
	if view, ok := b.FlowViewByID(flowID); ok && view != nil {
		if _, ok := view.Events[eventType]; ok {
			return true
		}
		for _, candidate := range view.Schema.Pins.Inputs.Events {
			if strings.TrimSpace(candidate) == eventType {
				return true
			}
		}
		for _, candidate := range view.Schema.Pins.Outputs.Events {
			if strings.TrimSpace(candidate) == eventType {
				return true
			}
		}
		if strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event) == eventType {
			return true
		}
	}
	return false
}
func (b *WorkflowContractBundle) DerivedHandlerTransitions() []HandlerTransitionSemantic {
	if b == nil {
		return nil
	}
	out := make([]HandlerTransitionSemantic, len(b.Semantics.HandlerTransitions))
	copy(out, b.Semantics.HandlerTransitions)
	return out
}
func (b *WorkflowContractBundle) DerivedHandlerTransition(nodeID, eventType string) (HandlerTransitionSemantic, bool) {
	if b == nil {
		return HandlerTransitionSemantic{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	transitions, ok := b.Semantics.HandlerTransitionIndex[nodeID]
	if !ok {
		return HandlerTransitionSemantic{}, false
	}
	if transition, ok := transitions[eventType]; ok {
		return transition, true
	}
	for pattern, transition := range transitions {
		if handlerPatternMatches(pattern, eventType) {
			return transition, true
		}
	}
	return HandlerTransitionSemantic{}, false
}
func (b *WorkflowContractBundle) TransitionIDsByOwner() map[string][]string {
	out := map[string][]string{}
	if b == nil {
		return out
	}
	for _, transition := range b.WorkflowTransitions() {
		owner := strings.TrimSpace(transition.Node)
		if owner == "" {
			continue
		}
		out[owner] = append(out[owner], strings.TrimSpace(transition.ID))
	}
	for owner := range out {
		sort.Strings(out[owner])
	}
	return out
}
