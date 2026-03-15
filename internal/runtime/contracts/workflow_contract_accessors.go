package contracts

import (
	flowmodel "empireai/internal/runtime/flowmodel"
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
	return strings.TrimSpace(b.Semantics.FlowInitial[strings.TrimSpace(flowID)])
}
func (b *WorkflowContractBundle) FlowStates(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowStates[strings.TrimSpace(flowID)]...)
}
func (b *WorkflowContractBundle) FlowTerminalStages(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowTerminal[strings.TrimSpace(flowID)]...)
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
	source, ok := b.nodeSources[strings.TrimSpace(nodeID)]
	return source, ok
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
	if !ok {
		return nil
	}
	out := make(map[string]SystemNodeEventHandler, len(handlers))
	for eventType, handler := range handlers {
		out[eventType] = handler
	}
	return out
}
func (b *WorkflowContractBundle) NodeEventHandler(nodeID, eventType string) (SystemNodeEventHandler, bool) {
	if b == nil {
		return SystemNodeEventHandler{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok {
		return SystemNodeEventHandler{}, false
	}
	if handler, ok := handlers[eventType]; ok {
		return handler, true
	}
	for pattern, handler := range handlers {
		if handlerPatternMatches(pattern, eventType) {
			return handler, true
		}
	}
	return SystemNodeEventHandler{}, false
}
func (b *WorkflowContractBundle) RuntimeEventOwners(eventType string) []string {
	if b == nil {
		return nil
	}
	eventType = strings.TrimSpace(eventType)
	owners := append([]string{}, b.Semantics.EventOwners[eventType]...)
	for nodeID, handlers := range b.Semantics.NodeHandlers {
		for pattern := range handlers {
			if strings.TrimSpace(pattern) == eventType {
				continue
			}
			if handlerPatternMatches(pattern, eventType) {
				owners = appendIfMissingString(owners, nodeID)
				break
			}
		}
	}
	return owners
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
