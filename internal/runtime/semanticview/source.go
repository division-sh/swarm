package semanticview

import (
	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
)

type Source interface {
	SemanticCapabilities() Capabilities
	semanticSourceCore() sourceCore
	WorkflowVersion() string
	WorkflowName() string
	DurableDataDeclarations() []runtimecontracts.DurableDataDeclaration
	StaticData() []durabledata.StaticData
	StaticDataForAgent(packageKey, flowID, logicalID string) []durabledata.StaticData
	DurableDataForAgent(packageKey, flowID, logicalID string) []durabledata.DeclarationRef
	DataProjectionRequired() bool
	PlatformSpec() runtimecontracts.PlatformSpecDocument
	WorkflowEntitySchema() runtimecontracts.EntitySchema
	WorkflowStages() []runtimecontracts.WorkflowStageContract
	WorkflowTerminalStages() []string
	WorkflowTransitions() []runtimecontracts.WorkflowTransitionContract
	WorkflowInitialStage() string
	WorkflowTimers() []runtimecontracts.WorkflowTimerContract
	WorkflowJoins() []runtimecontracts.WorkflowJoinPlan
	FanOutPlanForSite(site runtimecontracts.FanOutSiteRef) (runtimecontracts.FanOutCompiledPlan, bool)
	FanOutPlanForElement(ref runtimecontracts.FanOutElementRef) (runtimecontracts.FanOutCompiledPlan, bool)
	FanOutPlans() []runtimecontracts.FanOutCompiledPlan
	FanOutPlansForHandler(node runtimeidentity.ExecutableNode, eventType string) []runtimecontracts.FanOutCompiledPlan
	FanOutPlanFailures() []runtimecontracts.FanOutPlanFailure
	WorkflowGates() []runtimecontracts.WorkflowGatePlan
	WorkflowGateForStage(flowID, stage string) (runtimecontracts.WorkflowGatePlan, bool)
	WorkflowStageTimerByID(flowID, id string) (runtimecontracts.WorkflowTimerContract, bool)
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
	FlowInputEventPins(flowID string) []runtimecontracts.CompiledFlowInputPin
	FlowOutputEventPins(flowID string) []runtimecontracts.CompiledFlowOutputPin
	FlowInputEventPin(flowID, pinName string) (runtimecontracts.CompiledFlowInputPin, bool)
	FlowOutputEventPin(flowID, pinName string) (runtimecontracts.CompiledFlowOutputPin, bool)
	FlowWritePins(flowID string) []string
	WritePinOwners(pin string) []string
	FlowHasInputEvent(flowID, eventType string) bool
	FlowHasOutputEvent(flowID, eventType string) bool
	ResolveFlowEventReference(flowID, eventType string) string
	ResolveFlowEventPattern(flowID, pattern string) string
	FlowEventMatches(flowID, subscription, eventType string) bool
	RequiredAgents() []runtimecontracts.FlowRequiredAgent
	FlowRequiredAgents(flowID string) []runtimecontracts.FlowRequiredAgent
	ResolvedPolicyForFlow(flowID string) runtimecontracts.PolicyDocument
	ResolvedPolicyForExecutableNode(node runtimeidentity.ExecutableNode) runtimecontracts.PolicyDocument
	ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry
	ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool)
	ResolveFlowEventStructuralType(flowID, eventType string) (runtimecontracts.ResolvedCatalogType, bool)
	DerivedHandlerTransitions() []runtimecontracts.HandlerTransitionSemantic
	RuntimeEventOwners(eventType string) []runtimeidentity.ExecutableNode
	ExecutableNodeRecords() []runtimecontracts.ScopedNodeRecord
	ExecutableNode(node runtimeidentity.ExecutableNode) (runtimecontracts.ScopedNodeRecord, bool)
	ExecutableNodeSource(node runtimeidentity.ExecutableNode) (runtimecontracts.ContractItemSource, bool)
	ExecutableNodeEventHandlers(node runtimeidentity.ExecutableNode) map[string]runtimecontracts.SystemNodeEventHandler
	ExecutableNodeEventHandler(node runtimeidentity.ExecutableNode, eventType string) (runtimecontracts.SystemNodeEventHandler, bool)
	ResolveExecutableNodeEventReference(node runtimeidentity.ExecutableNode, eventType string) string
	ResolveExecutableNodeEventPattern(node runtimeidentity.ExecutableNode, pattern string) string
	ResolveExecutableNodeEventCatalogEntry(node runtimeidentity.ExecutableNode, eventType string) (runtimecontracts.EventCatalogEntry, string, bool)
	ExecutableNodeRuntimeSubscriptions(node runtimeidentity.ExecutableNode) []string
	ExecutableNodeEffectiveProduces(node runtimeidentity.ExecutableNode) []string
	AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry
	EventEntries() map[string]runtimecontracts.EventCatalogEntry
	EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool)
	ToolEntries() map[string]runtimecontracts.ToolSchemaEntry
	AuthoredResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry
}
