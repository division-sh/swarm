package semanticview

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
)

type Source interface {
	WorkflowVersion() string
	WorkflowName() string
	PlatformSpec() runtimecontracts.PlatformSpecDocument
	WorkflowEntitySchema() runtimecontracts.EntitySchema
	WorkflowStages() []runtimecontracts.WorkflowStageContract
	WorkflowTerminalStages() []string
	WorkflowTransitions() []runtimecontracts.WorkflowTransitionContract
	WorkflowInitialStage() string
	WorkflowTimers() []runtimecontracts.WorkflowTimerContract
	WorkflowJoins() []runtimecontracts.WorkflowJoinPlan
	ResolveFanOutEffectiveSemantics(flowID, eventType string, spec runtimecontracts.FanOutSpec) (runtimecontracts.FanOutEffectiveSemantics, error)
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
	FlowInputEventPins(flowID string) []runtimecontracts.FlowInputEventPin
	FlowOutputEventPins(flowID string) []runtimecontracts.FlowOutputEventPin
	FlowInputEventPin(flowID, pinName string) (runtimecontracts.FlowInputEventPin, bool)
	FlowOutputEventPin(flowID, pinName string) (runtimecontracts.FlowOutputEventPin, bool)
	CompositionConnects() []runtimecontracts.FlowPackageConnect
	CompositionConnectsTo(flowID, pinName string) []runtimecontracts.FlowPackageConnect
	CompositionConnectsFrom(flowID, pinName string) []runtimecontracts.FlowPackageConnect
	FlowWritePins(flowID string) []string
	WritePinOwners(pin string) []string
	FlowHasInputEvent(flowID, eventType string) bool
	FlowHasOutputEvent(flowID, eventType string) bool
	ResolveFlowInputAutoWire(flowID, eventType string) runtimecontracts.FlowInputAutoWireResolution
	FlowInputProducerPatterns(flowID, eventType string) []string
	ResolveFlowEventReference(flowID, eventType string) string
	ResolveFlowEventPattern(flowID, pattern string) string
	FlowEventMatches(flowID, subscription, eventType string) bool
	RequiredAgents() []runtimecontracts.FlowRequiredAgent
	FlowRequiredAgents(flowID string) []runtimecontracts.FlowRequiredAgent
	ResolvedPolicyForFlow(flowID string) runtimecontracts.PolicyDocument
	ResolvedPolicyForNode(nodeID string) runtimecontracts.PolicyDocument
	ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry
	ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool)
	DerivedHandlerTransitions() []runtimecontracts.HandlerTransitionSemantic
	RuntimeEventOwners(eventType string) []string
	NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool)
	AgentContractSource(agentID string) (runtimecontracts.ContractItemSource, bool)
	ResolveNodeEventReference(nodeID, eventType string) string
	NodeRuntimeSubscriptions(nodeID string) []string
	NodeHandlerSubscriptions(nodeID string) []string
	NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler
	NodeEventHandler(nodeID, eventType string) (runtimecontracts.SystemNodeEventHandler, bool)
	NodeEntries() map[string]runtimecontracts.SystemNodeContract
	AgentEntries() map[string]runtimecontracts.AgentRegistryEntry
	AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry
	EventEntries() map[string]runtimecontracts.EventCatalogEntry
	EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool)
	ToolEntries() map[string]runtimecontracts.ToolSchemaEntry
	ToolEntryForAgent(agentID, toolID string) (runtimecontracts.ToolSchemaEntry, bool)
	AuthoredResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry
}
