package semanticview

import (
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeregistry "empireai/internal/runtime/core/registry"
)

type Source interface {
	WorkflowVersion() string
	WorkflowName() string
	PlatformSpec() runtimecontracts.PlatformSpecDocument
	WorkflowEntitySchema() runtimecontracts.EntitySchema
	WorkflowStages() []runtimecontracts.WorkflowStageContract
	WorkflowTerminalStages() []string
	WorkflowTransitions() []runtimecontracts.WorkflowTransitionContract
	WorkflowTimers() []runtimecontracts.WorkflowTimerContract
	WorkflowTimerByID(id string) (runtimecontracts.WorkflowTimerContract, bool)
	GuardInstructions() []runtimeregistry.GuardInstruction
	GuardInstructionByID(id string) (runtimeregistry.GuardInstruction, bool)
	ActionInstructions() []runtimeregistry.ActionInstruction
	ActionInstructionByID(id string) (runtimeregistry.ActionInstruction, bool)
	FlowSchemaEntries() map[string]runtimecontracts.FlowSchemaDocument
	FlowInitialStage(flowID string) string
	FlowStates(flowID string) []string
	FlowTerminalStages(flowID string) []string
	ProjectScopes() []ProjectScope
	FlowScopes() []FlowScope
	FlowScopeByID(id string) (FlowScope, bool)
	FlowSchemaByID(id string) (runtimecontracts.FlowSchemaDocument, bool)
	FlowPath(flowID string) string
	FlowInputEvents(flowID string) []string
	FlowOutputEvents(flowID string) []string
	FlowWritePins(flowID string) []string
	RequiredAgents() []runtimecontracts.FlowRequiredAgent
	FlowRequiredAgents(flowID string) []runtimecontracts.FlowRequiredAgent
	ResolvedPolicyForFlow(flowID string) runtimecontracts.PolicyDocument
	ResolvedPolicyForNode(nodeID string) runtimecontracts.PolicyDocument
	ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry
	DerivedHandlerTransitions() []runtimecontracts.HandlerTransitionSemantic
	RuntimeEventOwners(eventType string) []string
	NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool)
	NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler
	NodeEventHandler(nodeID, eventType string) (runtimecontracts.SystemNodeEventHandler, bool)
	NodeEntries() map[string]runtimecontracts.SystemNodeContract
	AgentEntries() map[string]runtimecontracts.AgentRegistryEntry
	EventEntries() map[string]runtimecontracts.EventCatalogEntry
	EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool)
	ToolEntries() map[string]runtimecontracts.ToolSchemaEntry
}
