package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func checkWritePinOwnershipValidation(c *checkerContext) []Finding { return c.writePinOwnership() }
func checkInputPinWiring(c *checkerContext) []Finding              { return c.inputPinWiring() }
func checkCrossFlowPinAmbiguityValidation(c *checkerContext) []Finding {
	return c.crossFlowPinAmbiguityValidation()
}
func checkFlowBoundaryCreateEntityValidation(c *checkerContext) []Finding {
	return c.flowBoundaryCreateEntityValidation()
}

func (c *checkerContext) writePinOwnership() []Finding {
	if c.writePinLoaded {
		return c.writePinFindings
	}
	c.writePinLoaded = true
	pins := map[string]struct{}{}
	for flowID := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		for _, pin := range c.source.FlowWritePins(flowID) {
			pin = strings.TrimSpace(pin)
			if pin != "" {
				pins[pin] = struct{}{}
			}
		}
	}
	for _, pin := range sortedSetKeysLocal(pins) {
		owners := c.source.WritePinOwners(pin)
		if len(owners) <= 1 {
			continue
		}
		c.writePinFindings = append(c.writePinFindings, Finding{
			CheckID:  "write_pin_ownership_validation",
			Severity: "error",
			Message:  fmt.Sprintf("write pin %s is owned by multiple flows: %s", pin, strings.Join(owners, ", ")),
			Location: strings.TrimSpace(pin),
		})
	}
	return c.writePinFindings
}

func (c *checkerContext) inputPinWiring() []Finding {
	if c.inputPinLoaded {
		return c.inputPinFindings
	}
	c.inputPinLoaded = true

	flowIDs := make([]string, 0, len(c.source.FlowSchemaEntries()))
	for flowID := range c.source.FlowSchemaEntries() {
		if flowID = strings.TrimSpace(flowID); flowID != "" {
			flowIDs = append(flowIDs, flowID)
		}
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		for _, eventType := range c.source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			producerProof := c.inputPinProducerSourceProof(flowID, eventType)
			if producerProof.hasConflictingHarnessSource() {
				c.inputPinFindings = append(c.inputPinFindings, Finding{
					CheckID:     "input_pin_wiring",
					Severity:    SeverityHardInvalidity,
					Message:     producerProof.conflictingHarnessMessage(flowID, eventType),
					Location:    inputPinFlowLabel(flowID),
					Remediation: "Keep source: harness only for an intentionally producer-less validation fixture, or remove it and use the real production source.",
					Evidence:    producerProof.evidence(),
				})
				continue
			}
			if producerProof.hasAmbiguousBoundary() {
				c.inputPinFindings = append(c.inputPinFindings, Finding{
					CheckID:     "input_pin_wiring",
					Severity:    SeverityHardInvalidity,
					Message:     producerProof.ambiguousMessage(flowID, eventType),
					Location:    inputPinFlowLabel(flowID),
					Remediation: "Choose exactly one boundary producer source for this input pin; do not let routing infer authority from overlapping ingress mechanisms.",
					Evidence:    producerProof.evidence(),
				})
				continue
			}
			if producerProof.hasAny() {
				continue
			}
			c.inputPinFindings = append(c.inputPinFindings, Finding{
				CheckID:     "input_pin_wiring",
				Severity:    SeverityHardInvalidity,
				Message:     producerProof.message(flowID, eventType, c.inputPinTargetRefs(flowID, eventType)),
				Location:    inputPinFlowLabel(flowID),
				Remediation: producerProof.remediation(flowID, eventType, c.inputPinTargetRefs(flowID, eventType)),
				Evidence:    producerProof.evidence(),
			})
		}
	}

	return c.inputPinFindings
}

func inputPinFlowLabel(flowID string) string {
	if flowID = strings.TrimSpace(flowID); flowID != "" {
		return flowID
	}
	return "root"
}

type inputPinProducerSourceProof struct {
	resolution runtimecontracts.FlowInputProducerResolution
}

func (p inputPinProducerSourceProof) hasAny() bool {
	return p.resolution.HasEvidence()
}

func (p inputPinProducerSourceProof) hasAmbiguousBoundary() bool {
	return p.resolution.HasAmbiguousBoundaryEvidence()
}

func (p inputPinProducerSourceProof) hasConflictingHarnessSource() bool {
	return p.resolution.HasConflictingHarnessEvidence()
}

func (p inputPinProducerSourceProof) conflictingHarnessMessage(flowID, eventType string) string {
	return fmt.Sprintf(
		"Flow %s declares input pin event %s with source: harness and another accepted producer source: %s. Harness source is validation-only and must be the exclusive producer intent for that input.",
		inputPinFlowLabel(flowID),
		strings.TrimSpace(eventType),
		p.nonHarnessProofDetails(),
	)
}

func (p inputPinProducerSourceProof) message(flowID, eventType, targetRefs string) string {
	flowID = inputPinFlowLabel(flowID)
	eventType = strings.TrimSpace(eventType)
	targetRefs = strings.TrimSpace(targetRefs)
	return fmt.Sprintf(
		"Flow %s declares input pin event %s but no accepted producer source was found in the authored bundle. Expected a producer proof for input pin target %s.\n\nChecked producer source classes:\n- Boundary external ingress: %s\n- Intrinsic ingress input pin: %s\n- Parent connect: %s\n- Validation-only harness input: %s\n- Platform source: %s\n- Internal topology producer: %s\n\nFix one of:\n- Add a connect entry in the nearest common ancestor schema.yaml into %s\n- Mark the input event pin with source: external only when it is true intrinsic/external ingress\n- Use a platform-owned event if this is platform-produced\n- Produce the event through the intra-flow topology, or remove the input pin if it is not boundary-facing\n- For a validation fixture only, set source: harness on the input pin; this will remain non-production-valid\n\nDo not rely on events.yaml swarm.source as input-pin producer proof; event-level source metadata is non-input compatibility/documentation only.",
		flowID,
		eventType,
		targetRefs,
		p.detailsForKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress),
		p.detailsForKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress),
		p.detailsForKind(runtimecontracts.FlowInputProducerBoundaryParentConnect),
		p.detailsForKind(runtimecontracts.FlowInputProducerBoundaryHarnessInjection),
		p.detailsForKind(runtimecontracts.FlowInputProducerPlatformSource),
		p.detailsForKind(runtimecontracts.FlowInputProducerInternalTopology),
		targetRefs,
	)
}

func (p inputPinProducerSourceProof) remediation(flowID, eventType, targetRefs string) string {
	targetRefs = strings.TrimSpace(targetRefs)
	if targetRefs == "" {
		targetRefs = inputPinTargetRef(flowID, eventType)
	}
	return fmt.Sprintf("Provide one resolver-backed production source: parent connect into %s, input-pin source: external for true ingress, platform-owned source, or internal topology production. For a validation fixture only, set source: harness on the input pin; it will remain non-production-valid.", targetRefs)
}

func (p inputPinProducerSourceProof) evidence() []string {
	evidence := make([]string, 0, len(p.resolution.Evidence)+1)
	evidence = append(evidence, "events.yaml swarm.source is not input-pin producer proof")
	for _, item := range p.resolution.Evidence {
		detail := strings.TrimSpace(item.Detail)
		if detail == "" {
			detail = strings.TrimSpace(item.EventType)
		}
		if detail == "" {
			detail = strings.TrimSpace(item.Kind)
		}
		evidence = append(evidence, fmt.Sprintf("%s: %s", strings.TrimSpace(item.Kind), detail))
	}
	return evidence
}

func (p inputPinProducerSourceProof) ambiguousMessage(flowID, eventType string) string {
	return fmt.Sprintf(
		"Flow %s declares input pin event %s with multiple boundary producer sources: %s. Choose one boundary source so routing cannot infer authority from overlapping ingress mechanisms.",
		inputPinFlowLabel(flowID),
		strings.TrimSpace(eventType),
		p.boundaryDetails(),
	)
}

func (p inputPinProducerSourceProof) detailsForKind(kind string) string {
	details := make([]string, 0)
	for _, evidence := range p.resolution.Evidence {
		if strings.TrimSpace(evidence.Kind) != kind {
			continue
		}
		detail := strings.TrimSpace(evidence.Detail)
		if detail == "" {
			detail = strings.TrimSpace(evidence.EventType)
		}
		if detail != "" {
			details = append(details, detail)
		}
	}
	if len(details) == 0 {
		return "not found"
	}
	sort.Strings(details)
	return strings.Join(details, ", ")
}

func (p inputPinProducerSourceProof) boundaryDetails() string {
	details := make([]string, 0)
	for _, evidence := range p.resolution.BoundaryEvidence() {
		detail := strings.TrimSpace(evidence.Detail)
		if detail == "" {
			detail = strings.TrimSpace(evidence.Kind)
		}
		if detail != "" {
			details = append(details, detail)
		}
	}
	if len(details) == 0 {
		return "none"
	}
	sort.Strings(details)
	return strings.Join(details, ", ")
}

func (p inputPinProducerSourceProof) nonHarnessProofDetails() string {
	details := make([]string, 0)
	for _, evidence := range p.resolution.Evidence {
		kind := strings.TrimSpace(evidence.Kind)
		if kind == runtimecontracts.FlowInputProducerBoundaryHarnessInjection || !runtimecontracts.FlowInputProducerEvidenceKindIsProof(kind) {
			continue
		}
		detail := strings.TrimSpace(evidence.Detail)
		if detail == "" {
			detail = kind
		}
		details = append(details, kind+": "+detail)
	}
	if len(details) == 0 {
		return "none"
	}
	sort.Strings(details)
	return strings.Join(details, ", ")
}

func (c *checkerContext) inputPinProducerSourceProof(flowID, eventType string) inputPinProducerSourceProof {
	return inputPinProducerSourceProof{
		resolution: runtimepinrouting.ResolveFlowInputProducer(c.source, flowID, eventType),
	}
}

func (c *checkerContext) inputPinTargetRefs(flowID, eventType string) string {
	if c.source == nil {
		return inputPinTargetRef(flowID, eventType)
	}
	refs := make([]string, 0)
	association := semanticview.BuildAuthoredEventEndpointCensus(c.source).ResolveDeclaredInputEndpoint(flowID, eventType)
	if endpoint, ok := association.Endpoint(); ok {
		pinName := strings.TrimSpace(endpoint.PinName)
		if pinName == "" {
			pinName = strings.TrimSpace(endpoint.Event.Authored)
		}
		if pinName != "" {
			refs = append(refs, inputPinTargetRef(flowID, pinName))
		}
	}
	if len(refs) == 0 {
		refs = append(refs, inputPinTargetRef(flowID, eventType))
	}
	sort.Strings(refs)
	return strings.Join(refs, ", ")
}

func inputPinTargetRef(flowID, pinName string) string {
	flowID = strings.TrimSpace(flowID)
	pinName = strings.TrimSpace(pinName)
	if flowID == "." {
		return pinName
	}
	if pinName == "" {
		return flowID
	}
	return flowID + "." + pinName
}

func (c *checkerContext) crossFlowPinAmbiguityValidation() []Finding {
	if c.crossFlowPinAmbiguityLoaded {
		return c.crossFlowPinAmbiguityFindings
	}
	c.crossFlowPinAmbiguityLoaded = true
	for flowID := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		for _, eventType := range c.source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			resolution := runtimepinrouting.ResolveFlowInputProducer(c.source, flowID, eventType)
			if !resolution.HasAmbiguousBoundaryEvidence() {
				continue
			}
			c.crossFlowPinAmbiguityFindings = append(c.crossFlowPinAmbiguityFindings, Finding{
				CheckID:  "cross_flow_pin_ambiguity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s input pin %s is ambiguous across boundary producer sources %s; choose one boundary source", flowID, eventType, inputPinProducerSourceProof{resolution: resolution}.boundaryDetails()),
				Location: flowID,
			})
		}
	}
	return c.crossFlowPinAmbiguityFindings
}

func (c *checkerContext) flowBoundaryCreateEntityValidation() []Finding {
	if c.flowBoundaryCreateEntityLoaded {
		return c.flowBoundaryCreateEntityFindings
	}
	c.flowBoundaryCreateEntityLoaded = true
	for _, validationScope := range c.flowAcquisitionValidationScopes() {
		if strings.EqualFold(strings.TrimSpace(validationScope.schema.Mode), "template") {
			continue
		}
		if !validationScope.stateful {
			continue
		}
		if len(validationScope.inputs) == 0 {
			continue
		}
		for nodeID, node := range validationScope.nodes {
			nodeID = strings.TrimSpace(nodeID)
			for eventType, handler := range node.EventHandlers {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if _, ok := validationScope.inputs[eventType]; !ok {
					continue
				}
				nodeRef, _ := semanticview.ResolveExecutableNodeDeclaration(c.source, validationScope.semanticFlowID, nodeID)
				policy, policyErr := runtimepipeline.CompileDeliveryTargetCompatibilityPolicy(c.source, nodeRef, validationScope.semanticFlowID, events.EventType(eventType), handler)
				if validationScope.retiredStatic {
					if handler.CreateEntity {
						c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
							CheckID:  "flow_boundary_create_entity_validation",
							Severity: "error",
							Message:  retiredStaticMultiEntityAcquisitionMessage(validationScope.displayFlowID, eventType, nodeID, "create_entity"),
							Location: validationScope.displayFlowID,
						})
					}
					if !handler.CreateEntity &&
						bootverifyHandlerMaterializesEntity(c.source, nodeRef, eventType, validationScope.semanticFlowID, handler) {
						c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
							CheckID:  "flow_boundary_create_entity_validation",
							Severity: "error",
							Message:  retiredStaticMultiEntityAcquisitionMessage(validationScope.displayFlowID, eventType, nodeID, "implicit entity materialization"),
							Location: validationScope.displayFlowID,
						})
					}
					continue
				}
				if validationScope.normalPrimary &&
					bootverifyHandlerMaterializesEntity(c.source, nodeRef, eventType, validationScope.semanticFlowID, handler) &&
					flowInputEventDeclaresPayloadField(c.source, validationScope.semanticFlowID, eventType, "entity_id") {
					c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
						CheckID:  "flow_boundary_create_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s materializes entity state from caller-selected entity_id, but normal flow instances must write the canonical primary entity", validationScope.displayFlowID, eventType, nodeID),
						Location: validationScope.displayFlowID,
					})
				}
				if validationScope.normalPrimary {
					continue
				}
				if standingActivatedFlow(c.source, validationScope.semanticFlowID) {
					continue
				}
				if policyErr == nil && policy.Dependency == runtimepipeline.DeliveryTargetEntityMaterializing {
					continue
				}
				if flowInputHandlerUsesResolutionMode(c.source, validationScope.semanticFlowID, eventType, runtimecontracts.FlowInputResolutionModeFanIn) {
					continue
				}
				c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
					CheckID:  "flow_boundary_create_entity_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s input pin handler %s on node %s requires state initialization at its composition-selected receiver", validationScope.displayFlowID, eventType, nodeID),
					Location: validationScope.displayFlowID,
				})
			}
		}
	}
	return c.flowBoundaryCreateEntityFindings
}

type flowAcquisitionValidationScope struct {
	displayFlowID  string
	semanticFlowID string
	schema         runtimecontracts.FlowSchemaDocument
	stateful       bool
	retiredStatic  bool
	normalPrimary  bool
	inputs         map[string]struct{}
	nodes          map[string]runtimecontracts.SystemNodeContract
}

func (c *checkerContext) flowAcquisitionValidationScopes() []flowAcquisitionValidationScope {
	scopes := []flowAcquisitionValidationScope{}
	for _, scope := range c.source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		schema, ok := c.source.FlowSchemaByID(flowID)
		if !ok {
			continue
		}
		displayFlowID := flowID
		if flowID == "." {
			displayFlowID = "root"
		}
		scopes = append(scopes, flowAcquisitionValidationScope{
			displayFlowID:  displayFlowID,
			semanticFlowID: flowID,
			schema:         schema,
			stateful:       bootverifyFlowStateful(c.source, flowID, schema),
			retiredStatic:  retiredStaticMultiEntityAcquisitionFlow(c.source, flowID, schema),
			normalPrimary:  normalPrimaryEntityFlow(c.source, flowID, schema),
			inputs:         normalizeStringSet(c.source.FlowInputEvents(flowID)),
			nodes:          scope.Nodes,
		})
	}
	return scopes
}

func bootverifyFlowInitialStage(source semanticview.Source, flowID string, schema runtimecontracts.FlowSchemaDocument) string {
	if initial := strings.TrimSpace(schema.LoweredInitialState()); initial != "" || schema.UsesAuthoredStages() || schema.HasLegacyLifecycleFields() {
		return initial
	}
	if source == nil {
		return ""
	}
	return strings.TrimSpace(source.FlowInitialStage(strings.TrimSpace(flowID)))
}

func bootverifyFlowStateful(source semanticview.Source, flowID string, schema runtimecontracts.FlowSchemaDocument) bool {
	return bootverifyFlowInitialStage(source, flowID, schema) != ""
}

func retiredStaticMultiEntityAcquisitionFlow(source semanticview.Source, flowID string, schema runtimecontracts.FlowSchemaDocument) bool {
	mode := strings.TrimSpace(schema.Mode)
	return bootverifyFlowStateful(source, flowID, schema) && strings.EqualFold(mode, runtimecontracts.FlowModeStatic)
}

func retiredStaticMultiEntityAcquisitionMessage(flowID, eventType, nodeID, label string) string {
	return fmt.Sprintf("flow %s handler %s on node %s uses %s, but stateful static multi-row entity ownership is retired; model this as one primary entity with contained state, a mode: template flow instance, a mode: singleton coordinator, or a child flow", flowID, eventType, nodeID, label)
}

func normalPrimaryEntityFlow(source semanticview.Source, flowID string, schema runtimecontracts.FlowSchemaDocument) bool {
	return bootverifyFlowStateful(source, flowID, schema) && strings.TrimSpace(schema.Mode) == ""
}

func flowInputEventDeclaresPayloadField(source semanticview.Source, flowID, eventType, field string) bool {
	if source == nil {
		return false
	}
	if entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType); ok && eventEntryDeclaresPayloadField(entry, field) {
		return true
	}
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	return eventEntryDeclaresPayloadField(proof.Entry, field)
}

func flowInputHandlerUsesResolutionMode(source semanticview.Source, flowID, handlerEvent string, mode runtimecontracts.FlowInputResolutionMode) bool {
	if source == nil {
		return false
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	if handlerEvent == "" || mode == runtimecontracts.FlowInputResolutionModeNone {
		return false
	}
	endpoint, ok := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(flowID, handlerEvent).Endpoint()
	return ok && endpoint.ResolutionMode == mode
}

func eventEntryDeclaresPayloadField(entry runtimecontracts.EventCatalogEntry, field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	if _, ok := entry.Payload.Properties[field]; ok {
		return true
	}
	for _, required := range entry.Payload.Required {
		if strings.TrimSpace(required) == field {
			return true
		}
	}
	return false
}

func bootverifyHandlerMaterializesEntity(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType, flowID string, handler runtimecontracts.SystemNodeEventHandler) bool {
	if handler.CreateEntity {
		return true
	}
	if bootverifyHandlerActionMaterializesEntity(handler) {
		return true
	}
	if bootverifyHandlerMutatesEntityLifecycle(handler) {
		return true
	}
	if bootverifyEmitSitesReferenceEntity(source, node, eventType, handler) {
		return true
	}
	if bootverifyAccumulateReferencesEntity(handler.Accumulate) {
		return true
	}
	allowedFields := bootverifyWorkflowEntitySchemaFields(source, flowID)
	if len(allowedFields) == 0 {
		return false
	}
	if bootverifyDataWritesEntityFields(handler.DataAccumulation, allowedFields) {
		return true
	}
	if bootverifyComputeStoresEntityField(handler.Compute, allowedFields) {
		return true
	}
	for _, rule := range handler.Rules {
		if bootverifyRuleWritesEntityFields(rule, allowedFields) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if bootverifyRuleWritesEntityFields(rule, allowedFields) {
			return true
		}
	}
	return false
}

func bootverifyHandlerActionMaterializesEntity(handler runtimecontracts.SystemNodeEventHandler) bool {
	if bootverifyActionMaterializesEntity(handler.Action) {
		return true
	}
	for _, rule := range handler.Rules {
		if bootverifyActionMaterializesEntity(rule.Action) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if bootverifyActionMaterializesEntity(rule.Action) {
			return true
		}
	}
	return false
}

func bootverifyActionMaterializesEntity(action runtimecontracts.ActionSpec) bool {
	switch runtimecontracts.NormalizeHandlerActionID(action.ID) {
	case "record_evidence":
		return true
	default:
		return false
	}
}

func bootverifyHandlerMutatesEntityLifecycle(handler runtimecontracts.SystemNodeEventHandler) bool {
	if strings.TrimSpace(handler.AdvancesTo) != "" ||
		gateNameLocal(handler.SetsGate) != "" ||
		len(handler.ClearGates) > 0 {
		return true
	}
	for _, rule := range handler.Rules {
		if strings.TrimSpace(rule.AdvancesTo) != "" {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if strings.TrimSpace(rule.AdvancesTo) != "" {
			return true
		}
	}
	return false
}

func bootverifyRuleWritesEntityFields(rule runtimecontracts.HandlerRuleEntry, allowedFields map[string]struct{}) bool {
	return bootverifyDataWritesEntityFields(rule.DataAccumulation, allowedFields) ||
		bootverifyComputeStoresEntityField(rule.Compute, allowedFields)
}

func bootverifyWorkflowEntitySchemaFields(source semanticview.Source, flowID string) map[string]struct{} {
	contract, ok := entityruntime.ResolveForFlow(source, flowID)
	if !ok {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(contract.Entity.Fields))
	for _, field := range entityruntime.FieldNames(contract) {
		out[field] = struct{}{}
	}
	return out
}

func bootverifyDataWritesEntityFields(spec runtimecontracts.WorkflowDataAccumulation, allowedFields map[string]struct{}) bool {
	for _, write := range spec.Writes {
		targetField := bootverifyNormalizeEntityWriteTarget(write.Target())
		if targetField == "" {
			continue
		}
		if _, ok := allowedFields[targetField]; ok {
			return true
		}
	}
	return false
}

func bootverifyComputeStoresEntityField(spec *runtimecontracts.ComputeSpec, allowedFields map[string]struct{}) bool {
	if spec == nil {
		return false
	}
	targetField := bootverifyNormalizeEntityWriteTarget(spec.StoreAs)
	if targetField == "" {
		return false
	}
	_, ok := allowedFields[targetField]
	return ok
}

func bootverifyEmitSitesReferenceEntity(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, handler runtimecontracts.SystemNodeEventHandler) bool {
	if bootverifyEmitReferencesEntity(handler.Emit) {
		return true
	}
	for _, rule := range handler.Rules {
		if bootverifyEmitReferencesEntity(rule.Emit) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if bootverifyEmitReferencesEntity(rule.Emit) {
			return true
		}
	}
	if source != nil {
		for _, plan := range source.FanOutPlansForHandler(node, strings.TrimSpace(eventType)) {
			if bootverifyEmitReferencesEntity(plan.Emit) {
				return true
			}
		}
	}
	return false
}

func bootverifyEmitReferencesEntity(spec runtimecontracts.EmitSpec) bool {
	if strings.TrimSpace(spec.From) == runtimecontracts.EmitFromEntity {
		return true
	}
	for _, value := range spec.Fields {
		if value.Kind == runtimecontracts.ExpressionKindCEL && workflowexpr.ExpressionReferencesEntity(value.CEL) {
			return true
		}
		if value.Kind == runtimecontracts.ExpressionKindCEL && strings.TrimSpace(value.CEL) == runtimecontracts.EmitFromEntity {
			return true
		}
	}
	return false
}

func bootverifyAccumulateReferencesEntity(spec *runtimecontracts.AccumulateSpec) bool {
	if spec == nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(spec.From), "entity.") ||
		strings.HasPrefix(strings.TrimSpace(spec.Window), "entity.") ||
		strings.HasPrefix(strings.TrimSpace(spec.DedupBy), "entity.")
}

func bootverifyNormalizeEntityWriteTarget(target string) string {
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return ""
	}
	field, _, _ := strings.Cut(path, ".")
	return strings.TrimSpace(field)
}

func normalizeStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}
