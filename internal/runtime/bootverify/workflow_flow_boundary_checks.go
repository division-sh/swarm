package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func checkWritePinOwnershipValidation(c *checkerContext) []Finding { return c.writePinOwnership() }
func checkInputPinWiring(c *checkerContext) []Finding              { return c.inputPinWiring() }
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
			if producerProof.hasAny() {
				continue
			}
			c.inputPinFindings = append(c.inputPinFindings, Finding{
				CheckID:     "input_pin_wiring",
				Severity:    SeverityHardInvalidity,
				Message:     producerProof.message(flowID, eventType, c.inputPinTargetRefs(flowID, eventType), c.source),
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

func (p inputPinProducerSourceProof) message(flowID, eventType, targetRefs string, source semanticview.Source) string {
	flowID = inputPinFlowLabel(flowID)
	eventType = strings.TrimSpace(eventType)
	targetRefs = strings.TrimSpace(targetRefs)
	message := fmt.Sprintf(
		"Flow %s declares input pin event %s but no accepted producer source was found in the authored bundle. Expected a producer proof for input pin target %s.\n\nChecked producer source classes:\n- Selected-root public input: %s\n- Admitted provider ingress: %s\n- Parent connect: %s\n- Validation-only harness input: %s\n- Platform source: %s\n- Internal topology producer: %s\n\nFix one of:\n- Add a connect entry in the nearest common ancestor schema.yaml into %s\n- Select this flow as the source root for public input admission, or declare its provider ingress\n- Use a platform-owned event if this is platform-produced\n- Produce the event through the intra-flow topology, or remove the input pin if it is not boundary-facing\n- For a validation fixture only, set source: harness on the input pin; this will remain non-production-valid\n\nAuthored event role metadata and input-pin source: external are retired; catalog visibility never grants delivery or public write access.",
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
	if flowID != "." && flowID != "root" {
		if _, rootInput := semanticview.SelectedRootInputPin(source, eventType); rootInput {
			message += fmt.Sprintf("\nRoot input %s reaches child %s only through a connect edge in the parent's schema.yaml.", eventType, flowID)
		}
	}
	return message
}

func (p inputPinProducerSourceProof) remediation(flowID, eventType, targetRefs string) string {
	targetRefs = strings.TrimSpace(targetRefs)
	if targetRefs == "" {
		targetRefs = inputPinTargetRef(flowID, eventType)
	}
	return fmt.Sprintf("Provide one resolver-backed production source: parent connect into %s, selected-root public input or admitted provider ingress, platform-owned source, or internal topology production. For a validation fixture only, set source: harness on the input pin; it will remain non-production-valid.", targetRefs)
}

func (p inputPinProducerSourceProof) evidence() []string {
	evidence := make([]string, 0, len(p.resolution.Evidence)+1)
	evidence = append(evidence, "event schemas are not input-pin producer authority")
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

func (c *checkerContext) flowBoundaryCreateEntityValidation() []Finding {
	if c.flowBoundaryCreateEntityLoaded {
		return c.flowBoundaryCreateEntityFindings
	}
	c.flowBoundaryCreateEntityLoaded = true
	for _, validationScope := range c.flowAcquisitionValidationScopes() {
		if strings.EqualFold(strings.TrimSpace(validationScope.schema.EffectiveMode()), "template") {
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
				if bootverifyHandlerUsesCanonicalEntity(c.source, nodeRef, eventType, validationScope.semanticFlowID, handler) &&
					flowInputEventDeclaresPayloadField(c.source, validationScope.semanticFlowID, eventType, "entity_id") &&
					flowInputHasCallerSelectedIdentity(c.source, validationScope.semanticFlowID, eventType) {
					c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
						CheckID:  "flow_boundary_create_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses entity state with caller-selected entity_id, but normal flow instances must write the canonical primary entity", validationScope.displayFlowID, eventType, nodeID),
						Location: validationScope.displayFlowID,
					})
				}
			}
		}
	}
	return c.flowBoundaryCreateEntityFindings
}

type flowAcquisitionValidationScope struct {
	displayFlowID  string
	semanticFlowID string
	schema         runtimecontracts.FlowSchemaDocument
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
			inputs:         normalizeStringSet(c.source.FlowInputEvents(flowID)),
			nodes:          scope.Nodes,
		})
	}
	return scopes
}

func bootverifyFlowStateful(source semanticview.Source, flowID string) bool {
	return compiledInitialStageForFlow(source, flowID) != ""
}

func flowInputEventDeclaresPayloadField(source semanticview.Source, flowID, eventType, field string) bool {
	if source == nil {
		return false
	}
	_, ok := semanticview.ResolveEventSchema(source, flowID, eventType).Field(field)
	return ok
}

func flowInputHasCallerSelectedIdentity(source semanticview.Source, flowID, eventType string) bool {
	producer := runtimepinrouting.ResolveFlowInputProducer(source, flowID, eventType)
	// A provider envelope's entity_id is transport data, not entity acquisition
	// authority. Public or harness admission still permits caller-supplied data.
	return producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) ||
		producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryHarnessInjection) ||
		!producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress)
}

func bootverifyHandlerUsesCanonicalEntity(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType, flowID string, handler runtimecontracts.SystemNodeEventHandler) bool {
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
		strings.HasPrefix(strings.TrimSpace(spec.Key), "entity.")
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
