package semanticview

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeregistry "empireai/internal/runtime/core/registry"
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
			Key:        strings.TrimSpace(view.Paths.Key),
			Depth:      view.Paths.Depth,
			Manifest:   view.Manifest,
			PromptsDir: strings.TrimSpace(view.Paths.ProjectPromptsDir),
			Nodes:      view.Nodes,
			Events:     view.Events,
			Agents:     view.Agents,
			Tools:      view.Tools,
			Policy:     view.Policy,
		})
	}
	return out
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
func (s bundleSource) FlowWritePins(flowID string) []string { return s.bundle.FlowWritePins(flowID) }
func (s bundleSource) RequiredAgents() []runtimecontracts.FlowRequiredAgent {
	return s.bundle.RootRequiredAgents()
}
func (s bundleSource) FlowRequiredAgents(flowID string) []runtimecontracts.FlowRequiredAgent {
	return s.bundle.FlowRequiredAgents(flowID)
}
func (s bundleSource) ResolvedPolicyForFlow(flowID string) runtimecontracts.PolicyDocument {
	return s.bundle.ResolvedPolicyForFlow(flowID)
}
func (s bundleSource) ResolvedPolicyForNode(nodeID string) runtimecontracts.PolicyDocument {
	return s.bundle.ResolvedPolicyForNode(nodeID)
}
func (s bundleSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	return s.bundle.ResolvedEventCatalog()
}
func (s bundleSource) DerivedHandlerTransitions() []runtimecontracts.HandlerTransitionSemantic {
	return s.bundle.DerivedHandlerTransitions()
}
func (s bundleSource) RuntimeEventOwners(eventType string) []string {
	return s.bundle.RuntimeEventOwners(eventType)
}
func (s bundleSource) NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool) {
	return s.bundle.NodeContractSource(nodeID)
}
func (s bundleSource) NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler {
	return s.bundle.NodeEventHandlers(nodeID)
}
func (s bundleSource) NodeEventHandler(nodeID, eventType string) (runtimecontracts.SystemNodeEventHandler, bool) {
	return s.bundle.NodeEventHandler(nodeID, eventType)
}
func (s bundleSource) NodeEntries() map[string]runtimecontracts.SystemNodeContract {
	return s.bundle.NodeEntries()
}
func (s bundleSource) AgentEntries() map[string]runtimecontracts.AgentRegistryEntry {
	return s.bundle.AgentEntries()
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
