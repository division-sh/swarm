package semanticview

import (
	"path/filepath"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
)

type bundleSource struct {
	bundle *runtimecontracts.WorkflowContractBundle
}

func Wrap(bundle *runtimecontracts.WorkflowContractBundle) Source {
	if bundle == nil {
		return nil
	}
	return bundleSource{bundle: bundle}
}

func Bundle(source Source) (*runtimecontracts.WorkflowContractBundle, bool) {
	switch typed := source.(type) {
	case bundleSource:
		if typed.bundle == nil {
			return nil, false
		}
		return typed.bundle, true
	case *bundleSource:
		if typed == nil || typed.bundle == nil {
			return nil, false
		}
		return typed.bundle, true
	default:
		return nil, false
	}
}

func (s bundleSource) WorkflowVersion() string { return s.bundle.WorkflowVersion() }
func (s bundleSource) WorkflowName() string    { return s.bundle.WorkflowName() }
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
func (s bundleSource) WorkflowTimerByID(id string) (runtimecontracts.WorkflowTimerContract, bool) {
	return s.bundle.WorkflowTimerByID(id)
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
	out := make([]ProjectScope, 0, len(views))
	for _, view := range views {
		out = append(out, ProjectScope{
			Key:          strings.TrimSpace(view.Paths.Key),
			OwningFlowID: owningFlowIDForProjectView(s.bundle, view),
			Depth:        view.Paths.Depth,
			Manifest:     view.Manifest,
			PromptsDir:   strings.TrimSpace(view.Paths.ProjectPromptsDir),
			Nodes:        view.Nodes,
			Events:       view.Events,
			Agents:       runtimecontracts.EffectiveAgentRegistryEntries(view.Agents),
			Tools:        view.Tools,
			Policy:       view.Policy,
		})
	}
	return out
}

func owningFlowIDForProjectView(bundle *runtimecontracts.WorkflowContractBundle, view runtimecontracts.ProjectContractView) string {
	if bundle == nil {
		return ""
	}
	return owningFlowIDForPackage(bundle, strings.TrimSpace(view.Paths.Key))
}

func owningFlowIDForPackage(bundle *runtimecontracts.WorkflowContractBundle, packageKey string) string {
	packageKey = strings.TrimSpace(packageKey)
	if bundle == nil || packageKey == "" {
		return ""
	}
	pkg, ok := bundlePackageByKey(bundle, packageKey)
	if !ok || strings.TrimSpace(pkg.ParentKey) == "" {
		return ""
	}
	parent, ok := bundlePackageByKey(bundle, pkg.ParentKey)
	if !ok {
		return ""
	}
	if flowID := packageParentFlowID(parent, pkg); flowID != "" {
		return flowID
	}
	return owningFlowIDForPackage(bundle, parent.Key)
}

func bundlePackageByKey(bundle *runtimecontracts.WorkflowContractBundle, packageKey string) (runtimecontracts.LoadedProjectPackage, bool) {
	for _, pkg := range bundle.PackageTree {
		if strings.TrimSpace(pkg.Key) == packageKey {
			return pkg, true
		}
	}
	return runtimecontracts.LoadedProjectPackage{}, false
}

func packageParentFlowID(parent, child runtimecontracts.LoadedProjectPackage) string {
	childDir := filepath.Clean(strings.TrimSpace(child.Paths.Dir))
	if childDir != "" && childDir != "." {
		for _, flow := range parent.Paths.Flows {
			flowID := strings.TrimSpace(flow.ID)
			flowDir := filepath.Clean(strings.TrimSpace(flow.Dir))
			if flowID == "" || flowDir == "" || flowDir == "." {
				continue
			}
			if childDir == flowDir || strings.HasPrefix(childDir, flowDir+string(filepath.Separator)) {
				return flowID
			}
		}
	}
	if len(parent.Paths.Flows) == 1 {
		return strings.TrimSpace(parent.Paths.Flows[0].ID)
	}
	return ""
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
func (s bundleSource) CompositionConnects() []runtimecontracts.FlowPackageConnect {
	return s.bundle.CompositionConnects()
}
func (s bundleSource) CompositionConnectsTo(flowID, pinName string) []runtimecontracts.FlowPackageConnect {
	return s.bundle.CompositionConnectsTo(flowID, pinName)
}
func (s bundleSource) CompositionConnectsFrom(flowID, pinName string) []runtimecontracts.FlowPackageConnect {
	return s.bundle.CompositionConnectsFrom(flowID, pinName)
}
func (s bundleSource) FlowWritePins(flowID string) []string { return s.bundle.FlowWritePins(flowID) }
func (s bundleSource) WritePinOwners(pin string) []string   { return s.bundle.WritePinOwners(pin) }
func (s bundleSource) FlowHasInputEvent(flowID, eventType string) bool {
	return s.bundle.FlowHasInputEvent(flowID, eventType)
}
func (s bundleSource) FlowHasOutputEvent(flowID, eventType string) bool {
	return s.bundle.FlowHasOutputEvent(flowID, eventType)
}
func (s bundleSource) ResolveFlowInputAutoWire(flowID, eventType string) runtimecontracts.FlowInputAutoWireResolution {
	return ResolveFlowInputAutoWire(s, flowID, eventType)
}
func (s bundleSource) FlowInputProducerPatterns(flowID, eventType string) []string {
	return FlowInputProducerPatterns(s, flowID, eventType)
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
func (s bundleSource) ResolvedPolicyForNode(nodeID string) runtimecontracts.PolicyDocument {
	return ResolvePolicyForNode(s, nodeID)
}
func (s bundleSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	return s.bundle.ResolvedEventCatalog()
}
func (s bundleSource) ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	return s.bundle.ResolveFlowEventCatalogEntry(flowID, eventType)
}
func (s bundleSource) DerivedHandlerTransitions() []runtimecontracts.HandlerTransitionSemantic {
	return s.bundle.DerivedHandlerTransitions()
}
func (s bundleSource) RuntimeEventOwners(eventType string) []string {
	return RuntimeEventOwners(s, eventType)
}
func (s bundleSource) NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool) {
	return s.bundle.NodeContractSource(nodeID)
}
func (s bundleSource) AgentContractSource(agentID string) (runtimecontracts.ContractItemSource, bool) {
	return s.bundle.AgentContractSource(agentID)
}
func (s bundleSource) ResolveNodeEventReference(nodeID, eventType string) string {
	return s.bundle.ResolveNodeEventReference(nodeID, eventType)
}
func (s bundleSource) NodeRuntimeSubscriptions(nodeID string) []string {
	return s.bundle.NodeRuntimeSubscriptions(nodeID)
}
func (s bundleSource) NodeHandlerSubscriptions(nodeID string) []string {
	return s.bundle.NodeHandlerSubscriptions(nodeID)
}
func (s bundleSource) NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler {
	return s.bundle.NodeEventHandlers(nodeID)
}
func (s bundleSource) NodeEventHandler(nodeID, eventType string) (runtimecontracts.SystemNodeEventHandler, bool) {
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	for pattern := range s.bundle.NodeEventHandlers(nodeID) {
		if !strings.Contains(pattern, "*") {
			continue
		}
		matched, scoped := ImportBoundaryWildcardSubscriptionMatchesNode(s, nodeID, pattern, eventType)
		if !scoped {
			continue
		}
		if matched {
			return s.bundle.NodeEventHandler(nodeID, pattern)
		}
	}
	if ImportBoundaryWildcardHandlerFallbackDenied(s, nodeID, eventType) {
		return runtimecontracts.SystemNodeEventHandler{}, false
	}
	return s.bundle.NodeEventHandler(nodeID, eventType)
}
func (s bundleSource) NodeEntries() map[string]runtimecontracts.SystemNodeContract {
	return s.bundle.NodeEntries()
}
func (s bundleSource) AgentEntries() map[string]runtimecontracts.AgentRegistryEntry {
	return runtimecontracts.EffectiveAgentRegistryEntries(s.bundle.AgentEntries())
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
func (s bundleSource) ToolEntryForAgent(agentID, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	return s.bundle.ToolEntryForAgent(agentID, toolID)
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
