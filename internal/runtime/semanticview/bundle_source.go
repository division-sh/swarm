package semanticview

import (
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
)

type bundleSource struct {
	bundle *runtimecontracts.WorkflowContractBundle
}

type sourceCore struct {
	bundle *runtimecontracts.WorkflowContractBundle
}

func Wrap(bundle *runtimecontracts.WorkflowContractBundle) Source {
	if bundle == nil {
		return nil
	}
	return bundleSource{bundle: bundle}
}

func Bundle(source Source) (*runtimecontracts.WorkflowContractBundle, bool) {
	if source == nil {
		return nil, false
	}
	core := source.semanticSourceCore()
	return core.bundle, core.bundle != nil
}

func (s bundleSource) SemanticCapabilities() Capabilities { return Capabilities{} }
func (s bundleSource) semanticSourceCore() sourceCore     { return sourceCore{bundle: s.bundle} }

func (s bundleSource) WorkflowVersion() string { return s.bundle.WorkflowVersion() }
func (s bundleSource) WorkflowName() string    { return s.bundle.WorkflowName() }
func (s bundleSource) DurableDataDeclarations() []runtimecontracts.DurableDataDeclaration {
	return s.bundle.DurableDataDeclarations()
}
func (s bundleSource) StaticData() []durabledata.StaticData { return s.bundle.StaticData() }
func (s bundleSource) StaticDataForAgent(packageKey, flowID, logicalID string) []durabledata.StaticData {
	return s.bundle.StaticDataForAgent(packageKey, flowID, logicalID)
}

func (s bundleSource) DurableDataForAgent(packageKey, flowID, logicalID string) []durabledata.DeclarationRef {
	return s.bundle.DurableDataForAgent(packageKey, flowID, logicalID)
}
func (s bundleSource) DataProjectionRequired() bool { return s.bundle.DataProjectionRequired() }
func (s bundleSource) PlatformSpec() runtimecontracts.PlatformSpecDocument {
	return s.bundle.Platform
}
func (s bundleSource) WorkflowEntitySchema() runtimecontracts.EntitySchema {
	return s.bundle.WorkflowEntitySchema()
}
func (s bundleSource) WorkflowStages() []runtimecontracts.WorkflowStageContract {
	return s.bundle.WorkflowStages()
}
func (s bundleSource) WorkflowTerminalStages() []string { return s.bundle.WorkflowTerminalStages() }
func (s bundleSource) WorkflowTransitions() []runtimecontracts.WorkflowTransitionContract {
	return s.bundle.WorkflowTransitions()
}
func (s bundleSource) WorkflowInitialStage() string { return s.bundle.WorkflowInitialStage() }
func (s bundleSource) WorkflowTimers() []runtimecontracts.WorkflowTimerContract {
	return s.bundle.WorkflowTimers()
}
func (s bundleSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan {
	return effectiveWorkflowJoins(s, s.bundle.WorkflowJoins())
}
func (s bundleSource) ResolveFanOutEffectiveSemantics(node runtimeidentity.ExecutableNode, eventType string, spec runtimecontracts.FanOutSpec) (runtimecontracts.FanOutEffectiveSemantics, error) {
	return s.bundle.ResolveFanOutEffectiveSemantics(node, eventType, spec)
}
func (s bundleSource) WorkflowGates() []runtimecontracts.WorkflowGatePlan {
	return s.bundle.WorkflowGates()
}
func (s bundleSource) WorkflowGateForStage(flowID, stage string) (runtimecontracts.WorkflowGatePlan, bool) {
	return s.bundle.WorkflowGateForStage(flowID, stage)
}
func (s bundleSource) WorkflowLoops() []runtimecontracts.WorkflowLoopPlan {
	return s.bundle.WorkflowLoops()
}
func (s bundleSource) WorkflowStageTopology(flowID string) (runtimecontracts.WorkflowStageTopology, bool) {
	return s.bundle.WorkflowStageTopology(flowID)
}

func WorkflowLoops(source Source) []runtimecontracts.WorkflowLoopPlan {
	if source == nil {
		return nil
	}
	type provider interface {
		WorkflowLoops() []runtimecontracts.WorkflowLoopPlan
	}
	if loops, ok := source.(provider); ok {
		return loops.WorkflowLoops()
	}
	if bundle, ok := Bundle(source); ok {
		return bundle.WorkflowLoops()
	}
	return nil
}

func WorkflowStageTopology(source Source, flowID string) (runtimecontracts.WorkflowStageTopology, bool) {
	if source == nil {
		return runtimecontracts.WorkflowStageTopology{}, false
	}
	type provider interface {
		WorkflowStageTopology(string) (runtimecontracts.WorkflowStageTopology, bool)
	}
	if topology, ok := source.(provider); ok {
		return topology.WorkflowStageTopology(flowID)
	}
	if bundle, ok := Bundle(source); ok {
		return bundle.WorkflowStageTopology(flowID)
	}
	return runtimecontracts.WorkflowStageTopology{}, false
}
func (s bundleSource) WorkflowStageTimerByID(flowID, id string) (runtimecontracts.WorkflowTimerContract, bool) {
	return s.bundle.WorkflowStageTimerByID(flowID, id)
}
func (s bundleSource) GuardInstructions() []runtimeregistry.GuardInstruction {
	entries := s.bundle.GuardEntries()
	out := make([]runtimeregistry.GuardInstruction, 0, len(entries))
	for _, entry := range entries {
		out = append(out, runtimeregistry.GuardFromContract(entry))
	}
	return out
}
func (s bundleSource) GuardInstructionByID(id string) (runtimeregistry.GuardInstruction, bool) {
	entry, ok := s.bundle.GuardEntryByID(id)
	if !ok {
		return runtimeregistry.GuardInstruction{}, false
	}
	return runtimeregistry.GuardFromContract(entry), true
}
func (s bundleSource) ActionInstructions() []runtimeregistry.ActionInstruction {
	entries := s.bundle.ActionEntries()
	out := make([]runtimeregistry.ActionInstruction, 0, len(entries))
	for _, entry := range entries {
		out = append(out, runtimeregistry.ActionFromContract(entry))
	}
	return out
}
func (s bundleSource) ActionInstructionByID(id string) (runtimeregistry.ActionInstruction, bool) {
	entry, ok := s.bundle.ActionEntryByID(id)
	if !ok {
		return runtimeregistry.ActionInstruction{}, false
	}
	return runtimeregistry.ActionFromContract(entry), true
}
func (s bundleSource) FlowSchemaEntries() map[string]runtimecontracts.FlowSchemaDocument {
	if s.bundle == nil {
		return nil
	}
	out := make(map[string]runtimecontracts.FlowSchemaDocument, len(s.bundle.FlowSchemas))
	for key, value := range s.bundle.FlowSchemas {
		out[key] = value
	}
	return out
}

func (s bundleSource) RootFlowSchema() (runtimecontracts.FlowSchemaDocument, bool) {
	if s.bundle == nil || s.bundle.RootSchema == nil {
		return runtimecontracts.FlowSchemaDocument{}, false
	}
	return *s.bundle.RootSchema, true
}

func (s bundleSource) FlowInitialStage(flowID string) string {
	return s.bundle.FlowInitialStage(flowID)
}
func (s bundleSource) FlowStates(flowID string) []string { return s.bundle.FlowStates(flowID) }
func (s bundleSource) FlowTerminalStages(flowID string) []string {
	return s.bundle.FlowTerminalStages(flowID)
}
func (s bundleSource) ProjectScopes() []ProjectScope {
	if s.bundle == nil {
		return nil
	}
	views := s.bundle.ProjectViews()
	if len(views) == 0 && s.bundle.FlowTree.Root != nil && strings.TrimSpace(s.bundle.FlowTree.Root.Paths.ID) == "" &&
		(len(s.bundle.FlowTree.Root.Nodes) > 0 || len(s.bundle.FlowTree.Root.Events) > 0) {
		root := s.bundle.FlowTree.Root
		packageKey := strings.Trim(strings.TrimSpace(root.Paths.PackageKey), "/")
		if packageKey == "" {
			packageKey = runtimeidentity.RootPackageKey
		}
		return []ProjectScope{{
			Key:      packageKey,
			Manifest: s.bundle.Package,
			Nodes:    root.Nodes,
			Events:   root.Events,
			Agents:   runtimecontracts.EffectiveAgentRegistryEntries(root.Agents),
			Tools:    toolEntryMapSnapshot(root.Tools),
			Policy:   root.Policy,
		}}
	}
	out := make([]ProjectScope, 0, len(views))
	for _, view := range views {
		out = append(out, ProjectScope{
			Key:          strings.TrimSpace(view.Paths.Key),
			OwningFlowID: owningFlowIDForProjectView(s.bundle, view),
			Depth:        view.Paths.Depth,
			Manifest:     view.Manifest,
			Nodes:        view.Nodes,
			Events:       view.Events,
			Agents:       runtimecontracts.EffectiveAgentRegistryEntries(view.Agents),
			AgentURIs:    cloneStringMap(view.AgentURIs),
			Tools:        toolEntryMapSnapshot(view.Tools),
			Policy:       view.Policy,
		})
	}
	return out
}

func owningFlowIDForProjectView(bundle *runtimecontracts.WorkflowContractBundle, view runtimecontracts.ProjectContractView) string {
	if bundle == nil {
		return ""
	}
	return bundle.PackageOwningFlowID(view.Paths.Key)
}

func (s bundleSource) FlowScopes() []FlowScope {
	if s.bundle == nil {
		return nil
	}
	views := s.bundle.FlowViews()
	out := make([]FlowScope, 0, len(views))
	for _, view := range views {
		out = append(out, flowScopeFromView(view))
	}
	return out
}
func (s bundleSource) FlowScopeByID(id string) (FlowScope, bool) {
	id = strings.TrimSpace(id)
	if s.bundle == nil || id == "" {
		return FlowScope{}, false
	}
	view, ok := s.bundle.FlowViewByID(id)
	if !ok || view == nil {
		return FlowScope{}, false
	}
	return flowScopeFromView(*view), true
}
func (s bundleSource) FlowSchemaByID(id string) (runtimecontracts.FlowSchemaDocument, bool) {
	return s.bundle.FlowSchemaByID(id)
}
func (s bundleSource) FlowPath(flowID string) string { return s.bundle.FlowPath(flowID) }
func (s bundleSource) FlowInputEvents(flowID string) []string {
	return s.bundle.FlowInputEvents(flowID)
}
func (s bundleSource) FlowOutputEvents(flowID string) []string {
	return s.bundle.FlowOutputEvents(flowID)
}
func (s bundleSource) FlowInputEventPins(flowID string) []runtimecontracts.FlowInputEventPin {
	return s.bundle.FlowInputEventPins(flowID)
}
func (s bundleSource) FlowOutputEventPins(flowID string) []runtimecontracts.FlowOutputEventPin {
	return s.bundle.FlowOutputEventPins(flowID)
}
func (s bundleSource) FlowInputEventPin(flowID, pinName string) (runtimecontracts.FlowInputEventPin, bool) {
	return s.bundle.FlowInputEventPin(flowID, pinName)
}
func (s bundleSource) FlowOutputEventPin(flowID, pinName string) (runtimecontracts.FlowOutputEventPin, bool) {
	return s.bundle.FlowOutputEventPin(flowID, pinName)
}
func (s bundleSource) FlowWritePins(flowID string) []string { return s.bundle.FlowWritePins(flowID) }
func (s bundleSource) WritePinOwners(pin string) []string   { return s.bundle.WritePinOwners(pin) }
func (s bundleSource) FlowHasInputEvent(flowID, eventType string) bool {
	return s.bundle.FlowHasInputEvent(flowID, eventType)
}
func (s bundleSource) FlowHasOutputEvent(flowID, eventType string) bool {
	return s.bundle.FlowHasOutputEvent(flowID, eventType)
}
func (s bundleSource) ResolveFlowEventReference(flowID, eventType string) string {
	return s.bundle.ResolveFlowEventReference(flowID, eventType)
}
func (s bundleSource) ResolveFlowEventPattern(flowID, pattern string) string {
	if resolution := ResolveImportBoundaryWildcardSubscription(
		s,
		"",
		flowID,
		s.FlowPath(flowID),
		flowLocalEventSetForWildcardSource(s, flowID),
		pattern,
	); resolution.Scoped {
		if len(resolution.Patterns) == 1 {
			return resolution.Patterns[0].EventPattern
		}
		return ""
	}
	return s.bundle.ResolveFlowEventPattern(flowID, pattern)
}
func (s bundleSource) FlowEventMatches(flowID, subscription, eventType string) bool {
	if matched, scoped := ImportBoundaryWildcardSubscriptionMatches(
		s,
		"",
		flowID,
		s.FlowPath(flowID),
		flowLocalEventSetForWildcardSource(s, flowID),
		subscription,
		eventType,
	); scoped {
		return matched
	}
	return s.bundle.FlowEventMatches(flowID, subscription, eventType)
}
func (s bundleSource) RequiredAgents() []runtimecontracts.FlowRequiredAgent {
	return s.bundle.RootRequiredAgents()
}
func (s bundleSource) FlowRequiredAgents(flowID string) []runtimecontracts.FlowRequiredAgent {
	return s.bundle.FlowRequiredAgents(flowID)
}
func (s bundleSource) ResolvedPolicyForFlow(flowID string) runtimecontracts.PolicyDocument {
	return ResolvePolicyForFlow(s, flowID)
}
func (s bundleSource) ResolvedPolicyForExecutableNode(node runtimeidentity.ExecutableNode) runtimecontracts.PolicyDocument {
	return s.bundle.ResolvedPolicyForExecutableNode(node)
}
func (s bundleSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	return s.bundle.ResolvedEventCatalog()
}
func (s bundleSource) ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	return s.bundle.EffectiveEventCatalogEntryForFlowEvent(flowID, eventType)
}
func (s bundleSource) DerivedHandlerTransitions() []runtimecontracts.HandlerTransitionSemantic {
	return s.bundle.DerivedHandlerTransitions()
}
func (s bundleSource) RuntimeEventOwners(eventType string) []runtimeidentity.ExecutableNode {
	return RuntimeEventOwners(s, eventType)
}
func (s bundleSource) ExecutableNodeRecords() []runtimecontracts.ScopedNodeRecord {
	return s.bundle.ScopedNodeRecords()
}
func (s bundleSource) ExecutableNode(node runtimeidentity.ExecutableNode) (runtimecontracts.ScopedNodeRecord, bool) {
	return s.bundle.ExecutableNode(node)
}
func (s bundleSource) ExecutableNodeSource(node runtimeidentity.ExecutableNode) (runtimecontracts.ContractItemSource, bool) {
	record, ok := s.bundle.ExecutableNode(node)
	return record.Source, ok
}
func (s bundleSource) ExecutableNodeEventHandlers(node runtimeidentity.ExecutableNode) map[string]runtimecontracts.SystemNodeEventHandler {
	record, ok := s.bundle.ExecutableNode(node)
	if !ok {
		return nil
	}
	out := make(map[string]runtimecontracts.SystemNodeEventHandler, len(record.Entry.EventHandlers))
	for eventType, handler := range record.Entry.EventHandlers {
		qualified, err := runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
		if err != nil {
			continue
		}
		handler = qualified
		handler = runtimecontracts.DefaultSystemNodeHandlerSourceEvent(handler, eventType)
		out[eventType] = s.bundle.ExternalizeExecutableNodeHandler(node, handler)
	}
	return out
}
func (s bundleSource) ExecutableNodeEventHandler(node runtimeidentity.ExecutableNode, eventType string) (runtimecontracts.SystemNodeEventHandler, bool) {
	resolved := ResolveExecutableNodeSubscriptionHandler(s, node, strings.TrimSpace(eventType))
	return resolved.Handler, resolved.Matched
}
func (s bundleSource) ResolveExecutableNodeEventReference(node runtimeidentity.ExecutableNode, eventType string) string {
	return s.bundle.ResolveExecutableNodeEventReference(node, eventType)
}
func (s bundleSource) ResolveExecutableNodeEventPattern(node runtimeidentity.ExecutableNode, pattern string) string {
	return s.bundle.ResolveExecutableNodeEventPattern(node, pattern)
}
func (s bundleSource) ResolveExecutableNodeEventCatalogEntry(node runtimeidentity.ExecutableNode, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	return s.bundle.ResolveExecutableNodeEventCatalogEntry(node, eventType)
}
func (s bundleSource) ExecutableNodeRuntimeSubscriptions(node runtimeidentity.ExecutableNode) []string {
	record, ok := s.bundle.ExecutableNode(node)
	if !ok {
		return nil
	}
	return runtimecontracts.EffectiveSystemNodeSubscriptions(record.Entry)
}
func (s bundleSource) ExecutableNodeEffectiveProduces(node runtimeidentity.ExecutableNode) []string {
	record, ok := s.bundle.ExecutableNode(node)
	if !ok {
		return nil
	}
	return append(
		runtimecontracts.EffectiveSystemNodeProduces(record.Entry),
		s.bundle.GeneratedActivityEventsForExecutableNode(node)...,
	)
}
func (s bundleSource) AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry {
	return s.bundle.AuthoredEventEntries()
}
func (s bundleSource) EventEntries() map[string]runtimecontracts.EventCatalogEntry {
	return s.bundle.EventEntries()
}
func (s bundleSource) EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool) {
	return s.bundle.EventEntry(eventType)
}
func (s bundleSource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	return s.bundle.ToolEntries()
}
func (s bundleSource) AuthoredResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	return s.bundle.AuthoredResolvedEventCatalog()
}

func flowLocalEventSetForWildcardSource(source Source, flowID string) map[string]struct{} {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return nil
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return nil
	}
	return importBoundaryFlowLocalEventSet(source, scope)
}
