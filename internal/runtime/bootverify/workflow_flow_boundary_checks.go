package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
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
func checkSelectEntityValidation(c *checkerContext) []Finding {
	return c.selectEntityValidation()
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
			producerProof := c.inputPinProducerSourceProof(flowID, eventType)
			if producerProof.hasAmbiguousBoundary() {
				c.inputPinFindings = append(c.inputPinFindings, Finding{
					CheckID:     "input_pin_wiring",
					Severity:    SeverityHardInvalidity,
					Message:     producerProof.ambiguousMessage(flowID, eventType),
					Location:    flowID,
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
				Location:    flowID,
				Remediation: producerProof.remediation(flowID, eventType, c.inputPinTargetRefs(flowID, eventType)),
				Evidence:    producerProof.evidence(),
			})
		}
	}

	return c.inputPinFindings
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

func (p inputPinProducerSourceProof) message(flowID, eventType, targetRefs string) string {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	targetRefs = strings.TrimSpace(targetRefs)
	return fmt.Sprintf(
		"Flow %s declares input pin event %s but no accepted producer source was found in the authored bundle. Expected a producer proof for input pin target %s.\n\nChecked producer source classes:\n- Boundary external ingress: %s\n- Intrinsic ingress input pin: %s\n- Parent connect: %s\n- Explicit harness injection: %s\n- Platform source: %s\n- Internal topology producer: %s\n\nFix one of:\n- Add a parent package.yaml connect entry into %s\n- Mark the input event pin with source: external only when it is true intrinsic/external ingress\n- Register an explicit harness injection for validation-only fixtures\n- Use a platform-owned event if this is platform-produced\n- Produce the event through the intra-flow topology, or remove the input pin if it is not boundary-facing\n\nDo not rely on events.yaml swarm.source as input-pin producer proof; event-level source metadata is non-input compatibility/documentation only.",
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
	return fmt.Sprintf("Provide one resolver-backed producer source: parent connect into %s, input-pin source: external for true ingress, explicit harness injection, platform-owned source, or internal topology production.", targetRefs)
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
		strings.TrimSpace(flowID),
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

func (c *checkerContext) inputPinProducerSourceProof(flowID, eventType string) inputPinProducerSourceProof {
	// routing-example-census: different-concept issue=none owner=bootverify.input_pin_producer_source proof=TestVerifyBundle_InputPinProducerPathReturnsHardInvaliditySurface
	return inputPinProducerSourceProof{
		resolution: semanticview.ResolveFlowInputProducerWithOptions(c.source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{
			HarnessInjections: c.opts.HarnessInjections,
		}),
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
	if flowID == "" {
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
			resolution := semanticview.ResolveFlowInputProducerWithOptions(c.source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{
				HarnessInjections: c.opts.HarnessInjections,
			})
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

func flowHasScopedInputEscapeHatch(source semanticview.Source, flowID, eventType string) bool {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if source == nil || flowID == "" || eventType == "" {
		return false
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return false
	}
	for _, node := range scope.Nodes {
		for _, sub := range eventidentity.NormalizeList(runtimecontracts.EffectiveSystemNodeSubscriptions(node)) {
			if strings.Contains(sub, "/") && strings.HasSuffix(sub, "/"+eventType) {
				return true
			}
		}
	}
	for _, agent := range scope.Agents {
		for _, sub := range eventidentity.NormalizeList(agent.Subscriptions) {
			if strings.Contains(sub, "/") && strings.HasSuffix(sub, "/"+eventType) {
				return true
			}
		}
	}
	return false
}

func (c *checkerContext) selectEntityValidation() []Finding {
	findings := []Finding{}
	for _, validationScope := range c.flowAcquisitionValidationScopes() {
		for nodeID, node := range validationScope.nodes {
			nodeID = strings.TrimSpace(nodeID)
			for eventType, handler := range node.EventHandlers {
				eventType = strings.TrimSpace(eventType)
				hasSelectEntity := handler.SelectEntity != nil && !handler.SelectEntity.Empty()
				hasSelectOrCreateEntity := handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()
				if !hasSelectEntity && !hasSelectOrCreateEntity {
					continue
				}
				location := validationScope.displayFlowID
				label := "select_entity"
				if hasSelectOrCreateEntity && !hasSelectEntity {
					label = "select_or_create_entity"
				} else if hasSelectEntity && hasSelectOrCreateEntity {
					label = "select_entity/select_or_create_entity"
				}
				if handler.CreateEntity && (hasSelectEntity || hasSelectOrCreateEntity) {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s must not declare create_entity with select_entity or select_or_create_entity", validationScope.displayFlowID, eventType, nodeID),
						Location: location,
					})
				}
				if hasSelectEntity && hasSelectOrCreateEntity {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s must not declare both select_entity and select_or_create_entity", validationScope.displayFlowID, eventType, nodeID),
						Location: location,
					})
				}
				if validationScope.retiredStatic {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  retiredStaticMultiEntityAcquisitionMessage(validationScope.displayFlowID, eventType, nodeID, label),
						Location: location,
					})
				}
				if strings.EqualFold(strings.TrimSpace(validationScope.schema.Mode), "template") {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s, but template flows must use create_flow_instance routing rather than service-owned static entity acquisition", validationScope.displayFlowID, eventType, nodeID, label),
						Location: location,
					})
				}
				if !validationScope.stateful {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s, but stateless flows do not have stateful input-pin entity acquisition", validationScope.displayFlowID, eventType, nodeID, label),
						Location: location,
					})
				}
				if _, ok := validationScope.inputs[eventType]; !ok {
					findings = append(findings, Finding{
						CheckID:  "select_entity_validation",
						Severity: "error",
						Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s outside a declared input pin", validationScope.displayFlowID, eventType, nodeID, label),
						Location: location,
					})
				}
				findings = append(findings, validateSelectEntityBindings(c.source, validationScope.semanticFlowID, validationScope.displayFlowID, eventType, nodeID, "select_entity", handler.SelectEntity)...)
				findings = append(findings, validateSelectOrCreateEntityBindings(c.source, validationScope.semanticFlowID, validationScope.displayFlowID, eventType, nodeID, handler.SelectOrCreateEntity)...)
			}
		}
	}
	return findings
}

func validateSelectEntityBindings(source semanticview.Source, semanticFlowID, displayFlowID, eventType, nodeID, label string, spec *runtimecontracts.SelectEntitySpec) []Finding {
	if spec == nil || spec.Empty() {
		return nil
	}
	return validateEntityAcquisitionBindings(source, semanticFlowID, displayFlowID, eventType, nodeID, label, spec.Bindings)
}

func validateSelectOrCreateEntityBindings(source semanticview.Source, semanticFlowID, displayFlowID, eventType, nodeID string, spec *runtimecontracts.SelectOrCreateEntitySpec) []Finding {
	if spec == nil || spec.Empty() {
		return nil
	}
	return validateEntityAcquisitionBindings(source, semanticFlowID, displayFlowID, eventType, nodeID, "select_or_create_entity", spec.Bindings)
}

func validateEntityAcquisitionBindings(source semanticview.Source, semanticFlowID, displayFlowID, eventType, nodeID, label string, bindings []runtimecontracts.SelectEntityKeyBinding) []Finding {
	location := strings.TrimSpace(displayFlowID)
	contract, ok := entityruntime.ResolveForFlow(source, semanticFlowID)
	if !ok {
		return []Finding{{
			CheckID:  "select_entity_validation",
			Severity: "error",
			Message:  fmt.Sprintf("flow %s handler %s on node %s uses %s but the target flow entity contract is unavailable", displayFlowID, eventType, nodeID, label),
			Location: location,
		}}
	}
	findings := []Finding{}
	seen := map[string]struct{}{}
	for _, binding := range bindings {
		field := strings.TrimSpace(binding.Field)
		ref := strings.TrimSpace(binding.Ref)
		if _, ok := seen[field]; ok {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s is declared more than once", displayFlowID, eventType, nodeID, label, field),
				Location: location,
			})
			continue
		}
		seen[field] = struct{}{}
		if selectEntityReservedTargetField(field) {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s is not an entity contract field selection target", displayFlowID, eventType, nodeID, label, field),
				Location: location,
			})
			continue
		}
		if _, err := entityruntime.ResolveLeafField(contract, field); err != nil {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s is invalid: %v", displayFlowID, eventType, nodeID, label, field, err),
				Location: location,
			})
		}
		parsed := binding.RefPath
		if parsed.IsZero() {
			parsed = paths.Parse(ref)
		}
		if !parsed.HasExplicitRoot() || parsed.Root != paths.RootPayload {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s must resolve from payload.*, got %q", displayFlowID, eventType, nodeID, label, field, ref),
				Location: location,
			})
			continue
		}
		if len(parsed.Segments) == 0 {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s has an empty payload ref", displayFlowID, eventType, nodeID, label, field),
				Location: location,
			})
			continue
		}
		if selectEntityReservedPayloadRef(parsed) {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s must not use source envelope authority %q", displayFlowID, eventType, nodeID, label, field, ref),
				Location: location,
			})
			continue
		}
		if !selectEntityPayloadFieldDeclared(source, semanticFlowID, eventType, parsed.Segments[0]) {
			findings = append(findings, Finding{
				CheckID:  "select_entity_validation",
				Severity: "error",
				Message:  fmt.Sprintf("flow %s handler %s on node %s %s field %s references undeclared payload field %q", displayFlowID, eventType, nodeID, label, field, parsed.Segments[0]),
				Location: location,
			})
		}
	}
	return findings
}

func selectEntityReservedTargetField(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "entity_type", "flow_instance", "current_state", "workflow_name", "workflow_version":
		return true
	default:
		return false
	}
}

func selectEntityPayloadFieldDeclared(source semanticview.Source, flowID, eventType, field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	resolution := semanticview.ResolveEventSchema(source, flowID, eventType)
	if !resolution.HasSchema {
		return false
	}
	rawProps, ok := resolution.Schema.Schema["properties"]
	if !ok || rawProps == nil {
		return false
	}
	props, ok := rawProps.(map[string]any)
	if !ok {
		return false
	}
	_, ok = props[field]
	return ok
}

func selectEntityReservedPayloadRef(parsed paths.Path) bool {
	if parsed.Root != paths.RootPayload || len(parsed.Segments) == 0 {
		return false
	}
	switch strings.TrimSpace(parsed.Segments[0]) {
	case "entity_id", "entity_type", "flow_instance":
		return true
	default:
		return false
	}
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
						(handler.SelectEntity == nil || handler.SelectEntity.Empty()) &&
						(handler.SelectOrCreateEntity == nil || handler.SelectOrCreateEntity.Empty()) &&
						bootverifyHandlerMaterializesEntity(c.source, validationScope.semanticFlowID, handler) {
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
					bootverifyHandlerMaterializesEntity(c.source, validationScope.semanticFlowID, handler) &&
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
				if handler.CreateEntity {
					continue
				}
				if handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
					continue
				}
				if handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
					continue
				}
				if flowInputHandlerUsesResolutionMode(c.source, validationScope.semanticFlowID, eventType, runtimecontracts.FlowInputResolutionModeFanIn) {
					continue
				}
				c.flowBoundaryCreateEntityFindings = append(c.flowBoundaryCreateEntityFindings, Finding{
					CheckID:  "flow_boundary_create_entity_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s input pin handler %s on node %s must declare create_entity: true, select_entity, or select_or_create_entity", validationScope.displayFlowID, eventType, nodeID),
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
	if bundle, ok := semanticview.Bundle(c.source); ok && bundle != nil && bundle.RootSchema != nil {
		rootNodes := map[string]runtimecontracts.SystemNodeContract{}
		for _, view := range bundle.RootProjectViews() {
			for nodeID, node := range view.Nodes {
				nodeID = strings.TrimSpace(nodeID)
				if nodeID != "" {
					rootNodes[nodeID] = node
				}
			}
		}
		if len(rootNodes) > 0 {
			schema := *bundle.RootSchema
			scopes = append(scopes, flowAcquisitionValidationScope{
				displayFlowID:  "root",
				semanticFlowID: "",
				schema:         schema,
				stateful:       bootverifyFlowStateful(c.source, "", schema),
				retiredStatic:  retiredStaticMultiEntityAcquisitionFlow(c.source, "", schema),
				normalPrimary:  normalPrimaryEntityFlow(c.source, "", schema),
				inputs:         normalizeStringSet(bundle.RootSchema.Pins.Inputs.Events),
				nodes:          rootNodes,
			})
		}
	}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		scope, ok := c.source.FlowScopeByID(flowID)
		if !ok {
			continue
		}
		scopes = append(scopes, flowAcquisitionValidationScope{
			displayFlowID:  flowID,
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

func flowInputHandlerUsesResolutionMode(source semanticview.Source, flowID, handlerEvent, mode string) bool {
	if source == nil {
		return false
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	mode = strings.TrimSpace(mode)
	if handlerEvent == "" || mode == "" {
		return false
	}
	endpoint, ok := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(flowID, handlerEvent).Endpoint()
	return ok && strings.TrimSpace(endpoint.ResolutionMode) == mode
}

func eventEntryDeclaresPayloadField(entry runtimecontracts.EventCatalogEntry, field string) bool {
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	if _, ok := entry.Payload.Properties[field]; ok {
		return true
	}
	for _, required := range entry.Required {
		if strings.TrimSpace(required) == field {
			return true
		}
	}
	for _, required := range entry.Payload.Required {
		if strings.TrimSpace(required) == field {
			return true
		}
	}
	return false
}

func bootverifyHandlerMaterializesEntity(source semanticview.Source, flowID string, handler runtimecontracts.SystemNodeEventHandler) bool {
	if handler.CreateEntity {
		return true
	}
	if bootverifyHandlerActionMaterializesEntity(handler) {
		return true
	}
	if bootverifyHandlerMutatesEntityLifecycle(handler) {
		return true
	}
	if bootverifyEmitSitesReferenceEntity(handler) {
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
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if bootverifyRuleWritesEntityFields(rule, allowedFields) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil && bootverifyRuleWritesEntityFields(*handler.Accumulate.OnTimeout, allowedFields) {
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
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if bootverifyActionMaterializesEntity(rule.Action) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil && bootverifyActionMaterializesEntity(handler.Accumulate.OnTimeout.Action) {
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
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if strings.TrimSpace(rule.AdvancesTo) != "" {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil && strings.TrimSpace(handler.Accumulate.OnTimeout.AdvancesTo) != "" {
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

func bootverifyEmitSitesReferenceEntity(handler runtimecontracts.SystemNodeEventHandler) bool {
	if bootverifyEmitReferencesEntity(handler.Emit) {
		return true
	}
	if handler.FanOut != nil && bootverifyEmitReferencesEntity(handler.FanOut.Emit) {
		return true
	}
	for _, rule := range handler.Rules {
		if bootverifyEmitReferencesEntity(rule.Emit) {
			return true
		}
		if rule.FanOut != nil && bootverifyEmitReferencesEntity(rule.FanOut.Emit) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if bootverifyEmitReferencesEntity(rule.Emit) {
			return true
		}
		if rule.FanOut != nil && bootverifyEmitReferencesEntity(rule.FanOut.Emit) {
			return true
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if bootverifyEmitReferencesEntity(rule.Emit) {
				return true
			}
			if rule.FanOut != nil && bootverifyEmitReferencesEntity(rule.FanOut.Emit) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			rule := handler.Accumulate.OnTimeout
			if bootverifyEmitReferencesEntity(rule.Emit) {
				return true
			}
			if rule.FanOut != nil && bootverifyEmitReferencesEntity(rule.FanOut.Emit) {
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
	return strings.HasPrefix(strings.TrimSpace(spec.ExpectedFrom), "entity.")
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
