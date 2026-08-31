package contracts

import (
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
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
func (b *WorkflowContractBundle) WorkflowJoins() []WorkflowJoinPlan {
	if b == nil {
		return nil
	}
	return append([]WorkflowJoinPlan(nil), b.Semantics.Joins...)
}
func (b *WorkflowContractBundle) WorkflowLoops() []WorkflowLoopPlan {
	if b == nil {
		return nil
	}
	return append([]WorkflowLoopPlan(nil), b.Semantics.Loops...)
}
func (b *WorkflowContractBundle) WorkflowGates() []WorkflowGatePlan {
	if b == nil {
		return nil
	}
	return append([]WorkflowGatePlan(nil), b.Semantics.Gates...)
}
func (b *WorkflowContractBundle) WorkflowGateForStage(flowID, stage string) (WorkflowGatePlan, bool) {
	flowID, stage = strings.TrimSpace(flowID), strings.TrimSpace(stage)
	if b == nil || stage == "" {
		return WorkflowGatePlan{}, false
	}
	for _, plan := range b.Semantics.Gates {
		if strings.TrimSpace(plan.FlowID) == flowID && strings.TrimSpace(plan.Stage) == stage {
			return plan, true
		}
	}
	return WorkflowGatePlan{}, false
}
func (b *WorkflowContractBundle) WorkflowStageTopology(flowID string) (WorkflowStageTopology, bool) {
	if b == nil {
		return WorkflowStageTopology{}, false
	}
	topology, ok := b.Semantics.StageTopologies[strings.TrimSpace(flowID)]
	return topology, ok
}
func (b *WorkflowContractBundle) WorkflowTimerForNode(node runtimeidentity.ExecutableNode, id string) (WorkflowTimerContract, bool) {
	id = strings.TrimSpace(id)
	if b == nil || !node.Valid() || id == "" {
		return WorkflowTimerContract{}, false
	}
	for _, timer := range b.Semantics.Timers {
		if timer.Node.Equal(node) && strings.TrimSpace(timer.ID) == id && !timer.StageOwned {
			return timer, true
		}
	}
	return WorkflowTimerContract{}, false
}

func (b *WorkflowContractBundle) WorkflowStageTimerByID(flowID, id string) (WorkflowTimerContract, bool) {
	flowID, id = strings.TrimSpace(flowID), strings.TrimSpace(id)
	if b == nil || id == "" {
		return WorkflowTimerContract{}, false
	}
	for _, timer := range b.Semantics.Timers {
		if timer.StageOwned && !timer.Node.Valid() && strings.TrimSpace(timer.FlowID) == flowID && strings.TrimSpace(timer.ID) == id {
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
	if id == "." && b.RootSchema != nil {
		return *b.RootSchema, true
	}
	schema, ok := b.FlowSchemas[id]
	return schema, ok
}
func (b *WorkflowContractBundle) HasFlow(id string) bool {
	_, ok := b.FlowViewByID(id)
	return ok
}
func (b *WorkflowContractBundle) FlowViews() []FlowContractView {
	if b == nil {
		return nil
	}
	return flowmodel.ViewsByPath(
		b.FlowTree,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.FlowPath) },
		func(view *FlowContractView) string { return strings.TrimSpace(view.Path) },
		flowViewChildren,
	)
}

// ScopedNodeRecords returns every authored node with its exact declaration
// owner. Consumers that interpret executable node semantics must use this
// projection instead of the lossy global node aliases.
func (b *WorkflowContractBundle) ScopedNodeRecords() []ScopedNodeRecord {
	if b == nil {
		return nil
	}
	if b.FlowTree.Root != nil {
		return scopedNodeRecordsFromExportedTree(b.FlowTree.Root)
	}
	if len(b.scopedNodes) > 0 {
		keys := make([]string, 0, len(b.scopedNodes))
		for key := range b.scopedNodes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]ScopedNodeRecord, 0, len(keys))
		for _, key := range keys {
			entry := b.scopedNodes[key]
			source := b.scopedNodeSources[key]
			identity, err := runtimeidentity.ParseDeclarationIdentityKey(key)
			if err != nil {
				continue
			}
			out = append(out, ScopedNodeRecord{
				LogicalID: identity.SemanticPath(),
				Entry:     entry,
				Source:    source,
			})
		}
		return out
	}
	// Hand-built root-only semantic sources have no exported tree. In that
	// closed shape Nodes is the authored declaration table, not a merged alias
	// projection.
	if len(b.Nodes) > 0 {
		keys := make([]string, 0, len(b.Nodes))
		for key := range b.Nodes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]ScopedNodeRecord, 0, len(keys))
		for _, key := range keys {
			source := b.nodeSources[key]
			if strings.TrimSpace(source.FlowPath) == "" {
				source.FlowPath = "."
			}
			if strings.TrimSpace(source.Family) == "" {
				source.Family = "nodes"
			}
			out = append(out, ScopedNodeRecord{LogicalID: strings.TrimSpace(key), Entry: b.Nodes[key], Source: source})
		}
		return out
	}
	return nil
}

func (b *WorkflowContractBundle) ExecutableNode(ref runtimeidentity.ExecutableNode) (ScopedNodeRecord, bool) {
	if b == nil || !ref.Valid() {
		return ScopedNodeRecord{}, false
	}
	for _, record := range b.ScopedNodeRecords() {
		candidate, err := record.Identity()
		if err == nil && candidate.Equal(ref) {
			return record, true
		}
	}
	return ScopedNodeRecord{}, false
}

func (b *WorkflowContractBundle) executableNodeEventScope(ref runtimeidentity.ExecutableNode, semanticScope ExecutableNodeSemanticScope) eventidentity.Scope {
	if ref.FlowPath() == "." {
		localEvents := b.rootLocalEvents()
		if declaration, ok := semanticScope.DeclarationView(); ok {
			for eventType := range declaration.Events {
				localEvents = append(localEvents, strings.TrimSpace(eventType))
			}
		}
		return eventidentity.Scope{
			LocalEvents:  normalizedStrings(localEvents),
			InputEvents:  b.FlowInputEvents("."),
			OutputEvents: b.FlowOutputEvents("."),
		}
	}
	view, ok := semanticScope.OwningFlow()
	if !ok {
		return eventidentity.Scope{}
	}
	inputEvents := b.FlowInputEvents(view.Paths.FlowPath)
	outputEvents := b.FlowOutputEvents(view.Paths.FlowPath)
	localEvents := make([]string, 0, len(view.Events)+len(inputEvents)+len(outputEvents)+1)
	for eventType := range view.Events {
		localEvents = append(localEvents, strings.TrimSpace(eventType))
	}
	if declaration, ok := semanticScope.DeclarationView(); ok && declaration != view {
		for eventType := range declaration.Events {
			localEvents = append(localEvents, strings.TrimSpace(eventType))
		}
	}
	localEvents = append(localEvents, inputEvents...)
	localEvents = append(localEvents, outputEvents...)
	if eventType := strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event); eventType != "" {
		localEvents = append(localEvents, eventType)
	}
	return eventidentity.Scope{
		Path:         strings.Trim(strings.TrimSpace(view.Path), "/"),
		LocalEvents:  normalizedStrings(localEvents),
		InputEvents:  inputEvents,
		OutputEvents: outputEvents,
	}
}

func (b *WorkflowContractBundle) executableNodeEventDescendants(semanticScope ExecutableNodeSemanticScope) []eventidentity.DescendantScope {
	view, ok := semanticScope.OwningFlow()
	if !ok {
		return nil
	}
	parentPath := eventidentity.Normalize(view.Path)
	if parentPath == "" {
		return nil
	}
	out := make([]eventidentity.DescendantScope, 0)
	var walk func([]FlowContractView)
	walk = func(children []FlowContractView) {
		for index := range children {
			candidate := children[index]
			candidatePath := eventidentity.Normalize(candidate.Path)
			if candidatePath != "" && candidatePath != parentPath && strings.HasPrefix(candidatePath, parentPath+"/") {
				inputEvents := b.FlowInputEvents(candidate.Paths.FlowPath)
				outputEvents := b.FlowOutputEvents(candidate.Paths.FlowPath)
				localEvents := make([]string, 0, len(candidate.Events)+len(inputEvents)+len(outputEvents))
				for eventType := range candidate.Events {
					localEvents = append(localEvents, strings.TrimSpace(eventType))
				}
				localEvents = append(localEvents, inputEvents...)
				localEvents = append(localEvents, outputEvents...)
				if len(localEvents) != 0 {
					out = append(out, eventidentity.DescendantScope{Path: candidatePath, LocalEvents: normalizedStrings(localEvents)})
				}
			}
			walk(candidate.Children)
		}
	}
	walk(view.Children)
	return out
}

type executableNodeEventResolution struct {
	semanticScope ExecutableNodeSemanticScope
	eventScope    eventidentity.Scope
	descendants   []eventidentity.DescendantScope
}

func (b *WorkflowContractBundle) resolveExecutableNodeEvents(ref runtimeidentity.ExecutableNode) (executableNodeEventResolution, bool) {
	if b == nil || !ref.Valid() {
		return executableNodeEventResolution{}, false
	}
	semanticScope, err := b.ExecutableNodeSemanticScope(ref)
	if err != nil {
		return executableNodeEventResolution{}, false
	}
	return executableNodeEventResolution{
		semanticScope: semanticScope,
		eventScope:    b.executableNodeEventScope(ref, semanticScope),
		descendants:   b.executableNodeEventDescendants(semanticScope),
	}, true
}

func (r executableNodeEventResolution) resolveEvent(eventType string) string {
	return r.eventScope.ResolveEvent(eventType, r.descendants)
}

func (b *WorkflowContractBundle) ResolveExecutableNodeEventReference(ref runtimeidentity.ExecutableNode, eventType string) string {
	resolution, ok := b.resolveExecutableNodeEvents(ref)
	if !ok {
		return eventidentity.Normalize(eventType)
	}
	return resolution.resolveEvent(eventType)
}

func (b *WorkflowContractBundle) resolveAuthoredExecutableNodeEventCatalogEntry(ref runtimeidentity.ExecutableNode, eventType string) (EventCatalogEntry, string, bool) {
	if b == nil || !ref.Valid() {
		return EventCatalogEntry{}, "", false
	}
	resolution, resolvedScope := b.resolveExecutableNodeEvents(ref)
	authored := eventidentity.Normalize(eventType)
	canonical := authored
	if resolvedScope {
		canonical = resolution.resolveEvent(authored)
	}
	lookup := func(entries map[string]EventCatalogEntry) (EventCatalogEntry, string, bool) {
		for key, entry := range entries {
			key = eventidentity.Normalize(key)
			if key == "" {
				continue
			}
			resolved := key
			if resolvedScope {
				resolved = resolution.resolveEvent(key)
			}
			if key == authored || key == canonical || resolved == canonical {
				return entry, resolved, true
			}
		}
		return EventCatalogEntry{}, "", false
	}
	if resolvedScope {
		view, ok := resolution.semanticScope.OwningFlow()
		if ok {
			if entry, key, found := lookup(view.Events); found {
				return entry, key, true
			}
		}
	}
	if ref.FlowPath() == "." {
		if entry, key, found := lookup(b.Events); found {
			return entry, key, true
		}
	}
	if entry, ok := b.GeneratedActivityEventEntries()[canonical]; ok {
		return entry, canonical, true
	}
	return EventCatalogEntry{}, "", false
}

func (b *WorkflowContractBundle) ResolveExecutableNodeEventPattern(ref runtimeidentity.ExecutableNode, pattern string) string {
	if b == nil || !ref.Valid() {
		return eventidentity.Normalize(pattern)
	}
	pattern = eventidentity.Normalize(pattern)
	resolution, ok := b.resolveExecutableNodeEvents(ref)
	if !ok {
		return pattern
	}
	resolved := strings.TrimSpace(resolution.eventScope.ResolveSubscriptionPattern(pattern, nil))
	if resolved == "" || resolved != pattern || strings.Contains(pattern, "/") {
		return resolved
	}
	path := eventidentity.Normalize(resolution.eventScope.Path)
	if path == "" {
		return resolved
	}
	for _, localEvent := range resolution.eventScope.LocalEvents {
		if eventidentity.MatchPattern(pattern, localEvent) {
			return path + "/" + pattern
		}
	}
	return resolved
}

func (b *WorkflowContractBundle) ExternalizeExecutableNodeHandler(ref runtimeidentity.ExecutableNode, handler SystemNodeEventHandler) SystemNodeEventHandler {
	resolution, resolved := b.resolveExecutableNodeEvents(ref)
	externalize := func(spec EmitSpec) EmitSpec {
		spec = cloneEmitSpec(spec)
		if resolved {
			spec.Event = resolution.resolveEvent(spec.Event)
		} else {
			spec.Event = eventidentity.Normalize(spec.Event)
		}
		return spec
	}
	handler.Emit = externalize(handler.Emit)
	handler.OnSuccess.Emit = externalize(handler.OnSuccess.Emit)
	if handler.FanOut != nil {
		clone := *handler.FanOut
		clone.Emit = externalize(clone.Emit)
		handler.FanOut = &clone
	}
	if len(handler.Rules) > 0 {
		rules := append([]HandlerRuleEntry(nil), handler.Rules...)
		for index := range rules {
			rules[index].Emit = externalize(rules[index].Emit)
			if rules[index].FanOut != nil {
				clone := *rules[index].FanOut
				clone.Emit = externalize(clone.Emit)
				rules[index].FanOut = &clone
			}
		}
		handler.Rules = rules
	}
	if len(handler.OnComplete) > 0 {
		rules := append([]HandlerRuleEntry(nil), handler.OnComplete...)
		for index := range rules {
			rules[index].Emit = externalize(rules[index].Emit)
			if rules[index].FanOut != nil {
				clone := *rules[index].FanOut
				clone.Emit = externalize(clone.Emit)
				rules[index].FanOut = &clone
			}
		}
		handler.OnComplete = rules
	}
	if handler.Accumulate != nil {
		clone := *handler.Accumulate
		handler.Accumulate = &clone
	}
	return handler
}

func scopedNodeRecordsFromExportedTree(root *FlowContractView) []ScopedNodeRecord {
	if root == nil {
		return nil
	}
	out := make([]ScopedNodeRecord, 0)
	var walk func(*FlowContractView)
	walk = func(view *FlowContractView) {
		if view == nil {
			return
		}
		flowPath := strings.TrimSpace(view.Paths.FlowPath)
		nodeIDs := make([]string, 0, len(view.Nodes))
		for nodeID := range view.Nodes {
			nodeIDs = append(nodeIDs, nodeID)
		}
		sort.Strings(nodeIDs)
		for _, nodeID := range nodeIDs {
			out = append(out, ScopedNodeRecord{
				LogicalID: strings.TrimSpace(nodeID),
				Entry:     view.Nodes[nodeID],
				Source:    ContractItemSource{FlowPath: flowPath, Family: "nodes", File: strings.TrimSpace(view.Paths.NodesFile)},
			})
		}
		children := flowViewChildren(view)
		sort.SliceStable(children, func(i, j int) bool {
			return exportedFlowTreeViewOrderKey(children[i]) < exportedFlowTreeViewOrderKey(children[j])
		})
		for _, child := range children {
			walk(child)
		}
	}
	walk(root)
	return out
}

func scopedNodeDeclarationKey(record ScopedNodeRecord) string {
	sourceFile := strings.TrimSpace(record.Source.File)
	if sourceFile == "" {
		return ""
	}
	return strings.Join([]string{
		filepath.Clean(sourceFile),
		strings.TrimSpace(record.Source.FlowPath),
		strings.TrimSpace(record.LogicalID),
	}, "\x00")
}

func exportedFlowTreeViewOrderKey(view *FlowContractView) string {
	if view == nil {
		return ""
	}
	return strings.Join([]string{
		strings.TrimSpace(view.Paths.FlowPath),
		strings.TrimSpace(view.Path),
		strings.TrimSpace(view.Paths.NodesFile),
	}, "\x00")
}
func (b *WorkflowContractBundle) AgentEntries() map[string]AgentRegistryEntry {
	if b == nil {
		return nil
	}
	return EffectiveAgentRegistryEntries(b.Agents)
}
func (b *WorkflowContractBundle) AgentEntry(id string) (AgentRegistryEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return AgentRegistryEntry{}, false
	}
	entry, ok := b.Agents[id]
	if !ok {
		return AgentRegistryEntry{}, false
	}
	return EffectiveAgentRegistryEntry(id, entry), true
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
func (b *WorkflowContractBundle) AuthoredEventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	return cloneEventCatalogEntryMap(b.Events)
}
func (b *WorkflowContractBundle) EventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	out := b.AuthoredEventEntries()
	for eventType, entry := range b.GeneratedActivityEventEntries() {
		out[eventType] = entry
	}
	return out
}
func (b *WorkflowContractBundle) EventEntry(eventType string) (EventCatalogEntry, bool) {
	eventType = strings.TrimSpace(eventType)
	if b == nil || eventType == "" {
		return EventCatalogEntry{}, false
	}
	entry, ok := b.EventEntries()[eventType]
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
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.FlowPath) },
		func(view *FlowContractView) PolicyDocument { return view.Policy },
		flowViewChildren,
	)
}
func (b *WorkflowContractBundle) ResolvedPolicyForExecutableNode(node runtimeidentity.ExecutableNode) PolicyDocument {
	if b == nil || !node.Valid() {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	doc := clonePolicyDocument(b.Policy)
	scope, err := b.ExecutableNodeSemanticScope(node)
	if err != nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	if view, ok := scope.OwningFlow(); ok {
		chain := make([]*FlowContractView, 0)
		for current := view; current != nil; current = current.Parent {
			if strings.TrimSpace(current.Paths.FlowPath) != "" {
				chain = append(chain, current)
			}
		}
		for index := len(chain) - 1; index >= 0; index-- {
			mergeContractPolicyDocument(&doc, chain[index].Policy)
		}
	}
	return doc
}

func mergeContractPolicyDocument(target *PolicyDocument, overlay PolicyDocument) {
	if target == nil {
		return
	}
	if target.Values == nil {
		target.Values = map[string]PolicyValue{}
	}
	if target.Criteria == nil {
		target.Criteria = map[string]PolicyCriteriaSet{}
	}
	if target.Validation == nil {
		target.Validation = map[string]PolicyValidationSet{}
	}
	if target.Modules == nil {
		target.Modules = map[string]PolicyModule{}
	}
	cloned := clonePolicyDocument(overlay)
	for key, value := range cloned.Values {
		target.Values[key] = value
	}
	for key, value := range cloned.Criteria {
		target.Criteria[key] = value
	}
	for key, value := range cloned.Validation {
		target.Validation[key] = value
	}
	for key, value := range cloned.Modules {
		target.Modules[key] = value
	}
}
func (b *WorkflowContractBundle) PolicyValueForFlow(flowID, key string) (PolicyValue, bool) {
	doc := b.ResolvedPolicyForFlow(flowID)
	value, ok := doc.Values[strings.TrimSpace(key)]
	return value, ok
}
func (b *WorkflowContractBundle) FlowPath(flowID string) string {
	if b == nil {
		return ""
	}
	return flowmodel.PathForID(b.FlowTree, flowID, func(view *FlowContractView) string { return view.Path })
}
func (b *WorkflowContractBundle) AuthoredResolvedEventCatalog() map[string]EventCatalogEntry {
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
func (b *WorkflowContractBundle) ResolvedEventCatalog() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	out := b.AuthoredResolvedEventCatalog()
	for eventType, entry := range b.GeneratedActivityEventEntries() {
		out[eventType] = entry
	}
	return out
}
func (b *WorkflowContractBundle) resolveAuthoredFlowEventCatalogEntry(flowID, eventType string) (EventCatalogEntry, string, bool) {
	if b == nil {
		return EventCatalogEntry{}, "", false
	}
	flowID = strings.TrimSpace(flowID)
	rawKey := eventidentity.Normalize(eventType)
	if rawKey == "" {
		return EventCatalogEntry{}, "", false
	}
	resolvedKey := b.ResolveFlowEventReference(flowID, eventType)
	entries := b.Events
	if b.FlowTree.Root != nil {
		view, ok := b.exactFlowEventDeclarationView(flowID)
		if !ok || view == nil {
			return EventCatalogEntry{}, "", false
		}
		entries = view.Events
	}
	for _, localKey := range sortedContractKeys(entries) {
		entry := entries[localKey]
		localKey = eventidentity.Normalize(localKey)
		canonicalKey := b.ResolveFlowEventReference(flowID, localKey)
		if localKey == rawKey || localKey == resolvedKey || canonicalKey == rawKey || canonicalKey == resolvedKey {
			return entry, canonicalKey, true
		}
	}
	if entry, ok := b.GeneratedActivityEventEntries()[rawKey]; ok {
		return entry, rawKey, true
	}
	if resolvedKey != rawKey {
		if entry, ok := b.GeneratedActivityEventEntries()[resolvedKey]; ok {
			return entry, resolvedKey, true
		}
	}
	return EventCatalogEntry{}, "", false
}

func (b *WorkflowContractBundle) exactFlowEventDeclarationView(flowID string) (*FlowContractView, bool) {
	if b == nil || b.FlowTree.Root == nil {
		return nil, false
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" || flowID == "." {
		return b.FlowTree.Root, true
	}
	return b.FlowViewByID(flowID)
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
	if flowID == "." {
		return b.WorkflowInitialStage()
	}
	if initial := strings.TrimSpace(b.Semantics.FlowInitial[flowID]); initial != "" {
		return initial
	}
	return ""
}
func (b *WorkflowContractBundle) FlowStates(flowID string) []string {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "." {
		if b.RootSchema == nil {
			return workflowSemanticRootStates(b.Semantics)
		}
		return rootSchemaStates(b.RootSchema)
	}
	if states := b.Semantics.FlowStates[flowID]; len(states) > 0 {
		return append([]string{}, states...)
	}
	return nil
}

func workflowSemanticRootStates(semantics WorkflowSemanticView) []string {
	out := make([]string, 0, len(semantics.Stages))
	seen := make(map[string]struct{}, len(semantics.Stages))
	for _, stage := range semantics.Stages {
		if strings.TrimSpace(stage.Phase) != "" {
			continue
		}
		id := strings.TrimSpace(stage.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func rootSchemaStates(root *FlowSchemaDocument) []string {
	if root == nil {
		return nil
	}
	return root.LoweredStates()
}

func (b *WorkflowContractBundle) FlowTerminalStages(flowID string) []string {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "." {
		if b.RootSchema == nil {
			return append([]string{}, b.Semantics.TerminalStages...)
		}
		return rootSchemaTerminalStates(b.RootSchema)
	}
	if terminal := b.Semantics.FlowTerminal[flowID]; len(terminal) > 0 {
		return append([]string{}, terminal...)
	}
	return nil
}

func rootSchemaTerminalStates(root *FlowSchemaDocument) []string {
	if root == nil {
		return nil
	}
	return root.LoweredTerminalStates()
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
	pins := b.FlowInputEventPins(flowID)
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}
func (b *WorkflowContractBundle) FlowOutputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	pins := b.FlowOutputEventPins(flowID)
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}
func (b *WorkflowContractBundle) FlowInputEventPins(flowID string) []CompiledFlowInputPin {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	return cloneCompiledFlowInputPins(b.Semantics.flowInputEventPins[flowID])
}
func (b *WorkflowContractBundle) FlowOutputEventPins(flowID string) []CompiledFlowOutputPin {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	return cloneCompiledFlowOutputPins(b.Semantics.flowOutputEventPins[flowID])
}
func (b *WorkflowContractBundle) FlowInputEventPin(flowID, pinName string) (CompiledFlowInputPin, bool) {
	pinName = strings.TrimSpace(pinName)
	if pinName == "" {
		return CompiledFlowInputPin{}, false
	}
	for _, pin := range b.FlowInputEventPins(flowID) {
		if pin.EventType() == pinName {
			return pin, true
		}
	}
	return CompiledFlowInputPin{}, false
}
func (b *WorkflowContractBundle) FlowOutputEventPin(flowID, pinName string) (CompiledFlowOutputPin, bool) {
	pinName = strings.TrimSpace(pinName)
	if pinName == "" {
		return CompiledFlowOutputPin{}, false
	}
	for _, pin := range b.FlowOutputEventPins(flowID) {
		if pin.EventType() == pinName {
			return pin, true
		}
	}
	return CompiledFlowOutputPin{}, false
}
func (b *WorkflowContractBundle) CompositionConnects() []FlowConnect {
	if b == nil {
		return nil
	}
	return cloneFlowConnects(b.Semantics.CompositionConnects)
}
func (b *WorkflowContractBundle) FlowReadPins(flowID string) []string {
	if b == nil {
		return nil
	}
	return b.Semantics.flowReads[strings.TrimSpace(flowID)].Fields()
}
func (b *WorkflowContractBundle) FlowWritePins(flowID string) []string {
	if b == nil {
		return nil
	}
	return b.Semantics.flowWrites[strings.TrimSpace(flowID)].Fields()
}
func (b *WorkflowContractBundle) FlowHasInputEvent(flowID, eventType string) bool {
	return b.flowEventScope(flowID).HasInput(eventType)
}
func (b *WorkflowContractBundle) FlowHasOutputEvent(flowID, eventType string) bool {
	return b.flowEventScope(flowID).HasOutput(eventType)
}
func (b *WorkflowContractBundle) ResolveFlowInputAutoWire(targetFlowID, eventType string) FlowInputAutoWireResolution {
	eventType = eventidentity.Normalize(eventType)
	return FlowInputAutoWireResolution{EventType: eventType}
}
func (b *WorkflowContractBundle) FlowInputProducerPatterns(targetFlowID, eventType string) []string {
	return append([]string{}, b.ResolveFlowInputAutoWire(targetFlowID, eventType).Patterns...)
}
func (b *WorkflowContractBundle) ResolveFlowEventReference(flowID, eventType string) string {
	if resolved, ok := b.resolveDeclaredLocalFlowEventReference(flowID, eventType); ok {
		return resolved
	}
	scope := b.flowEventScope(flowID)
	return scope.ResolveEvent(eventType, b.flowEventDescendants(flowID))
}

func (b *WorkflowContractBundle) resolveDeclaredLocalFlowEventReference(flowID, eventType string) (string, bool) {
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if b == nil || strings.Contains(eventType, "/") {
		return "", false
	}
	if flowID == "." {
		return eventType, eventType != ""
	}
	view, ok := b.FlowViewByID(flowID)
	if !ok || view == nil {
		return "", false
	}
	local := false
	if _, local = view.Events[eventType]; !local {
		local = slices.Contains(eventidentity.NormalizeList(b.FlowOutputEvents(flowID)), eventType)
	}
	if !local {
		for _, row := range eventSchemaOwnershipRowsForReceiver(b, flowID) {
			if packageEndpointLocalEvent(b, flowID, row.receiverEvent, true) == eventType {
				local = true
				break
			}
		}
	}
	if !local {
		local = eventidentity.Normalize(view.Schema.AutoEmitOnCreate.Event) == eventType
	}
	if !local {
		return "", false
	}
	return eventidentity.ExternalizeForFlow(b.FlowPath(flowID), []string{eventType}, eventType), true
}
func (b *WorkflowContractBundle) ResolveFlowEventPattern(flowID, pattern string) string {
	scope := b.flowEventScope(flowID)
	return scope.ResolveSubscriptionPattern(pattern, nil)
}
func (b *WorkflowContractBundle) FlowEventMatches(flowID, subscription, eventType string) bool {
	scope := b.flowEventScope(flowID)
	return scope.Matches(subscription, eventType, nil)
}
func (b *WorkflowContractBundle) FlowRequiredAgents(flowID string) []FlowRequiredAgent {
	return FlowRequiredAgentsFromFacts(b.FlowRequiredAgentFacts(flowID))
}
func (b *WorkflowContractBundle) FlowRequiredAgentFacts(flowID string) []RequiredAgentFact {
	if b == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return nil
	}
	if schema, ok := b.FlowSchemas[flowID]; ok {
		if RequiredAgentsDeclared(schema) {
			return explicitRequiredAgentFacts(schema.RequiredAgents, b.flowRequiredAgentSchemaFile(flowID))
		}
		return b.inferredRequiredAgentFacts(flowID)
	}
	if facts := b.Semantics.FlowAgentFacts[flowID]; len(facts) > 0 {
		return cloneRequiredAgentFacts(facts)
	}
	return explicitRequiredAgentFacts(b.Semantics.FlowAgents[flowID], b.flowRequiredAgentSchemaFile(flowID))
}
func (b *WorkflowContractBundle) RootRequiredAgents() []FlowRequiredAgent {
	return FlowRequiredAgentsFromFacts(b.RootRequiredAgentFacts())
}
func (b *WorkflowContractBundle) RootRequiredAgentFacts() []RequiredAgentFact {
	if b == nil || b.RootSchema == nil {
		return nil
	}
	if RequiredAgentsDeclared(*b.RootSchema) {
		return explicitRequiredAgentFacts(b.RootSchema.RequiredAgents, b.flowRequiredAgentSchemaFile("."))
	}
	return b.inferredRequiredAgentFacts(".")
}

func (b *WorkflowContractBundle) inferredRequiredAgentFacts(ownerFlowID string) []RequiredAgentFact {
	ownerFlowID = strings.TrimSpace(ownerFlowID)
	if b == nil {
		return nil
	}
	records := b.AgentDeclarationRecords()
	out := make([]RequiredAgentFact, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.OwnerFlowID) != ownerFlowID {
			continue
		}
		role := strings.TrimSpace(record.LogicalID)
		if role == "" {
			continue
		}
		out = append(out, RequiredAgentFact{
			Role:         role,
			SubscribesTo: normalizeStrings(record.Entry.Subscriptions),
			Emits:        normalizeStrings(record.Entry.EmitEvents),
			Source:       RequiredAgentSourceInferred,
			SourceFile:   strings.TrimSpace(record.Source.File),
		})
	}
	return out
}
func (b *WorkflowContractBundle) flowRequiredAgentSchemaFile(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if b == nil || flowID == "" {
		return ""
	}
	if view, ok := b.FlowViewByID(flowID); ok && view != nil {
		return strings.TrimSpace(view.Paths.SchemaFile)
	}
	return ""
}
func (b *WorkflowContractBundle) WritePinOwners(pin string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.writePinOwners[strings.TrimSpace(pin)]...)
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

func (b *WorkflowContractBundle) ScopedAgentContractSource(scope ContractItemSource, agentID string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.scopedAgentSources[contractScopeKey(scope, strings.TrimSpace(agentID))]
	return source, ok
}
func (b *WorkflowContractBundle) ScopedAgentEntries() map[string]AgentRegistryEntry {
	if b == nil {
		return nil
	}
	return EffectiveAgentRegistryEntries(b.scopedAgents)
}
func (b *WorkflowContractBundle) ScopedEventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	return cloneEventCatalogEntryMap(b.scopedEvents)
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
	for _, eventType := range b.FlowOutputEvents(flowID) {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out = append(out, eventType)
		}
	}
	for _, row := range effectiveEventSchemaOwnershipRows(b) {
		if row.receiverFlowID == flowID {
			out = append(out, packageEndpointLocalEvent(b, flowID, row.receiverEvent, true))
		}
	}
	if autoEmit := strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event); autoEmit != "" {
		out = append(out, autoEmit)
	}
	for _, site := range b.ActivitySites() {
		if site.Node.FlowPath() != flowID {
			continue
		}
		result := ActivityResultEventsForSite(site)
		out = append(out,
			eventidentity.LeafName(result.SuccessEvent),
			eventidentity.LeafName(result.FailureEvent),
		)
		if site.Spec.Approval != nil {
			out = append(out,
				eventidentity.LeafName(result.RevisionRequested),
				eventidentity.LeafName(result.Rejected),
			)
		}
	}
	return uniqueOrderedStrings(out)
}

func (b *WorkflowContractBundle) flowEventScope(flowID string) eventidentity.Scope {
	flowID = strings.TrimSpace(flowID)
	if b == nil {
		return eventidentity.Scope{}
	}
	if flowID == "." {
		return eventidentity.Scope{
			Path:         "",
			LocalEvents:  b.rootLocalEvents(),
			InputEvents:  b.FlowInputEvents("."),
			OutputEvents: b.FlowOutputEvents("."),
		}
	}
	view, ok := b.FlowViewByID(flowID)
	if !ok || view == nil {
		return eventidentity.Scope{Path: b.FlowPath(flowID)}
	}
	return eventidentity.Scope{
		Path:         strings.Trim(strings.TrimSpace(view.Path), "/"),
		LocalEvents:  b.flowLocalEvents(flowID),
		InputEvents:  b.FlowInputEvents(flowID),
		OutputEvents: b.FlowOutputEvents(flowID),
	}
}

func (b *WorkflowContractBundle) rootLocalEvents() []string {
	if b == nil {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	appendEvent := func(eventType string) {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			return
		}
		if _, ok := seen[eventType]; ok {
			return
		}
		seen[eventType] = struct{}{}
		out = append(out, eventType)
	}
	if b.RootSchema != nil {
		for _, eventType := range b.FlowInputEvents(".") {
			appendEvent(eventType)
		}
		for _, eventType := range b.FlowOutputEvents(".") {
			appendEvent(eventType)
		}
		if autoEmit := strings.TrimSpace(b.RootSchema.AutoEmitOnCreate.Event); autoEmit != "" {
			appendEvent(autoEmit)
		}
	}
	if root, ok := b.FlowViewByID("."); ok && root != nil {
		for eventType := range root.Events {
			appendEvent(eventType)
		}
	}
	sort.Strings(out)
	return out
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
		descendantFlowID := strings.TrimSpace(view.Paths.FlowPath)
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

func (b *WorkflowContractBundle) DerivedHandlerTransitions() []HandlerTransitionSemantic {
	if b == nil {
		return nil
	}
	out := make([]HandlerTransitionSemantic, len(b.Semantics.HandlerTransitions))
	copy(out, b.Semantics.HandlerTransitions)
	return out
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
