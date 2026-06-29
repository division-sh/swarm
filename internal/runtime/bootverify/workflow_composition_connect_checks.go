package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkCompositionConnectValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	var findings []Finding
	for _, connect := range c.source.CompositionConnects() {
		findings = append(findings, validateCompositionConnect(c.source, connect)...)
	}
	return findings
}

func validateCompositionConnect(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) []Finding {
	var findings []Finding
	from, fromErr := connect.FromRef()
	to, toErr := connect.ToRef()
	if fromErr != nil {
		findings = append(findings, compositionConnectFinding(connect, "producer_reference_invalid", fromErr.Error(), ""))
	}
	if toErr != nil {
		findings = append(findings, compositionConnectFinding(connect, "receiver_reference_invalid", toErr.Error(), ""))
	}
	if fromErr != nil || toErr != nil {
		return findings
	}

	if !from.Root {
		if _, ok := source.FlowSchemaByID(from.FlowID); !ok {
			findings = append(findings, compositionConnectFinding(connect, "producer_flow_missing", fmt.Sprintf("producer flow %s does not exist", from.FlowID), from.FlowID))
			return findings
		}
	}
	if to.Root {
		findings = append(findings, compositionConnectFinding(connect, "receiver_root_unsupported", "root receiver endpoints are not supported by this composition-routing slice", "root"))
		return findings
	}
	receiverSchema, ok := source.FlowSchemaByID(to.FlowID)
	if !ok {
		findings = append(findings, compositionConnectFinding(connect, "receiver_flow_missing", fmt.Sprintf("receiver flow %s does not exist", to.FlowID), to.FlowID))
		return findings
	}

	outputPin, ok := source.FlowOutputEventPin(from.FlowID, from.Pin)
	if !ok {
		location := from.FlowID
		producerLabel := fmt.Sprintf("producer flow %s", from.FlowID)
		if from.Root {
			location = "root"
			producerLabel = "root schema"
		}
		findings = append(findings, compositionConnectFinding(connect, "producer_output_pin_missing", fmt.Sprintf("%s does not declare output pin %s", producerLabel, from.Pin), location))
		return findings
	}
	inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin)
	if !ok {
		findings = append(findings, compositionConnectFinding(connect, "receiver_input_pin_missing", fmt.Sprintf("receiver flow %s does not declare input pin %s", to.FlowID, to.Pin), to.FlowID))
		return findings
	}

	if !compositionConnectEventCompatible(source, connect, from, to, outputPin, inputPin) {
		findings = append(findings, compositionConnectFinding(
			connect,
			"event_alias_or_adapter_invalid",
			fmt.Sprintf("producer output event %s and receiver input event %s differ without an explicit adapter or import-boundary alias", outputPin.EventType(), inputPin.EventType()),
			to.FlowID,
		))
	}

	instanceKeyFindings := validateCompositionConnectInstanceKey(source, connect, outputPin, inputPin, from.FlowID, to.FlowID)
	findings = append(findings, validateCompositionConnectDelivery(connect, inputPin, receiverSchema, to.FlowID, len(instanceKeyFindings) == 0)...)
	if len(instanceKeyFindings) > 0 {
		findings = append(findings, instanceKeyFindings...)
	} else {
		findings = append(findings, validateCompositionConnectAddress(source, connect, outputPin, inputPin, from.FlowID, to.FlowID)...)
	}
	return findings
}

func compositionConnectFinding(connect runtimecontracts.FlowPackageConnect, reason, detail, location string) Finding {
	if strings.TrimSpace(location) == "" {
		if to, err := connect.ToRef(); err == nil {
			location = strings.TrimSpace(to.FlowID)
		}
	}
	if strings.TrimSpace(location) == "" {
		location = "package.yaml"
	}
	return Finding{
		CheckID:  "composition_connect_validation",
		Severity: "error",
		Message:  fmt.Sprintf("connect %s -> %s is invalid: %s: %s", strings.TrimSpace(connect.From), strings.TrimSpace(connect.To), reason, detail),
		Location: location,
	}
}

func compositionConnectEventCompatible(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, from, to runtimecontracts.FlowPackagePinRef, outputPin runtimecontracts.FlowOutputEventPin, inputPin runtimecontracts.FlowInputEventPin) bool {
	outputEvent := eventidentity.Normalize(outputPin.EventType())
	inputEvent := eventidentity.Normalize(inputPin.EventType())
	if outputEvent == "" || inputEvent == "" || outputEvent == inputEvent {
		return true
	}
	if strings.TrimSpace(connect.Adapter) != "" {
		return true
	}
	candidates := map[string]struct{}{
		outputEvent: {},
		eventidentity.Normalize(source.ResolveFlowEventReference(from.FlowID, outputPin.EventType())): {},
	}
	candidateEvents := make([]string, 0, len(candidates))
	for candidate := range candidates {
		candidateEvents = append(candidateEvents, candidate)
	}
	for _, candidate := range candidateEvents {
		for _, parentEvent := range semanticview.ImportBoundaryOutputParentEventsForEvent(source, connect.PackageKey, "", candidate) {
			if parentEvent = eventidentity.Normalize(parentEvent); parentEvent != "" {
				candidates[parentEvent] = struct{}{}
			}
		}
	}
	if _, ok := candidates[inputEvent]; ok {
		return true
	}
	for _, alias := range semanticview.ImportBoundaryInputAliases(source, to.FlowID, inputPin.PinName()) {
		if _, ok := candidates[eventidentity.Normalize(alias.ParentEvent)]; ok {
			return true
		}
		if _, ok := candidates[eventidentity.Normalize(alias.EventPattern)]; ok {
			return true
		}
	}
	for _, alias := range semanticview.ImportBoundaryInputAliases(source, to.FlowID, inputPin.EventType()) {
		if _, ok := candidates[eventidentity.Normalize(alias.ParentEvent)]; ok {
			return true
		}
		if _, ok := candidates[eventidentity.Normalize(alias.EventPattern)]; ok {
			return true
		}
	}
	return false
}

func validateCompositionConnectDelivery(connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, receiverSchema runtimecontracts.FlowSchemaDocument, receiverFlowID string, hasInstanceKeyRoute bool) []Finding {
	var findings []Finding
	delivery := strings.TrimSpace(connect.Delivery)
	if delivery == "" && inputPin.Address != nil {
		delivery = strings.TrimSpace(inputPin.Address.Cardinality)
	}
	switch delivery {
	case "", "one", "many", "broadcast", "reply":
	default:
		findings = append(findings, compositionConnectFinding(connect, "delivery_topology_invalid", fmt.Sprintf("delivery %q is not one, many, broadcast, or reply", delivery), receiverFlowID))
	}
	if delivery == "reply" && len(connect.Reply) == 0 {
		findings = append(findings, compositionConnectFinding(connect, "reply_lineage_missing", "reply delivery requires a reply lineage declaration", receiverFlowID))
	}
	if inputPin.Address == nil {
		if compositionReceiverAddressRequired(receiverSchema) && !hasInstanceKeyRoute && delivery != "broadcast" {
			findings = append(findings, compositionConnectFinding(connect, "receiver_route_key_missing", fmt.Sprintf("receiver flow %s requires a matching instance key route or an explicit addressed input pin", receiverFlowID), receiverFlowID))
		}
		return findings
	}
	cardinality := strings.TrimSpace(inputPin.Address.Cardinality)
	switch cardinality {
	case "one", "many":
	default:
		findings = append(findings, compositionConnectFinding(connect, "delivery_topology_invalid", fmt.Sprintf("input pin address cardinality %q is not one or many", cardinality), receiverFlowID))
	}
	if delivery == "one" && cardinality == "many" {
		findings = append(findings, compositionConnectFinding(connect, "delivery_topology_invalid", "delivery one is incompatible with address cardinality many", receiverFlowID))
	}
	if delivery == "many" && cardinality == "one" {
		findings = append(findings, compositionConnectFinding(connect, "delivery_topology_invalid", "delivery many is incompatible with address cardinality one", receiverFlowID))
	}
	if delivery == "broadcast" && cardinality == "one" {
		findings = append(findings, compositionConnectFinding(connect, "delivery_topology_invalid", "delivery broadcast is incompatible with address cardinality one", receiverFlowID))
	}
	mode := strings.TrimSpace(inputPin.Address.Mode)
	switch mode {
	case "", "select_existing", "select_or_create", "static_instance":
	default:
		findings = append(findings, compositionConnectFinding(connect, "receiver_address_rule_invalid", fmt.Sprintf("address mode %q is not supported", mode), receiverFlowID))
	}
	return findings
}

func compositionReceiverAddressRequired(schema runtimecontracts.FlowSchemaDocument) bool {
	return strings.EqualFold(strings.TrimSpace(schema.Mode), "template")
}

func validateCompositionConnectInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, outputPin runtimecontracts.FlowOutputEventPin, inputPin runtimecontracts.FlowInputEventPin, producerFlowID, receiverFlowID string) []Finding {
	if inputPin.Address != nil {
		return nil
	}
	if strings.TrimSpace(connect.Delivery) == "broadcast" {
		return nil
	}
	receiverSchema, ok := source.FlowSchemaByID(receiverFlowID)
	if !ok || !compositionReceiverAddressRequired(receiverSchema) {
		return nil
	}
	if len(connect.Map) > 0 {
		for key := range connect.Map {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_unsupported", fmt.Sprintf("connect map key %s is a renamed-key adapter surface and is tracked separately from same-name instance-key routing", key), receiverFlowID)}
		}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return []Finding{compositionConnectFinding(connect, "receiver_instance_key_unavailable", "receiver instance key owner is unavailable for this semantic source", receiverFlowID)}
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlowID)
	if err != nil {
		return []Finding{compositionConnectFinding(connect, "receiver_instance_key_invalid", err.Error(), receiverFlowID)}
	}
	outputKey := strings.TrimSpace(outputPin.Key)
	if outputKey == "" {
		return []Finding{compositionConnectFinding(connect, "output_key_missing", "producer output pin must declare key before it can route to a receiver instance key", producerFlowID)}
	}
	carries := outputPinCarries(outputPin)
	if !stringSliceContains(carries, outputKey) {
		return []Finding{compositionConnectFinding(connect, "output_carries_instance_key", fmt.Sprintf("producer output pin key %s must also be listed in carries", outputKey), producerFlowID)}
	}
	if !stringSliceContains(instance.By, outputKey) {
		return []Finding{compositionConnectFinding(connect, "instance_key_mismatch", fmt.Sprintf("producer output key %s does not match receiver instance.by %v", outputKey, instance.By), receiverFlowID)}
	}
	for _, field := range instance.By {
		field = strings.TrimSpace(field)
		if !stringSliceContains(carries, field) {
			return []Finding{compositionConnectFinding(connect, "output_carries_instance_key", fmt.Sprintf("producer output pin carries must include receiver instance.by field %s", field), producerFlowID)}
		}
		sourceType, err := outputPinCarriedPayloadFieldType(source, producerFlowID, outputPin, field)
		if err != nil {
			return []Finding{compositionConnectFinding(connect, "output_carries_instance_key", err.Error(), producerFlowID)}
		}
		targetType, err := compositionConnectTargetType(source, receiverFlowID, "entity."+field)
		if err != nil {
			return []Finding{compositionConnectFinding(connect, "receiver_instance_key_invalid", err.Error(), receiverFlowID)}
		}
		if !compositionConnectTypesCompatible(sourceType, targetType) {
			return []Finding{compositionConnectFinding(connect, "key_types_incompatible", fmt.Sprintf("source payload.%s type %s is incompatible with receiver instance.by field entity.%s type %s", field, sourceType, field, targetType), receiverFlowID)}
		}
	}
	return nil
}

func validateCompositionConnectAddress(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, outputPin runtimecontracts.FlowOutputEventPin, inputPin runtimecontracts.FlowInputEventPin, producerFlowID, receiverFlowID string) []Finding {
	if inputPin.Address == nil {
		for key := range connect.Map {
			return []Finding{compositionConnectFinding(connect, "receiver_address_rule_missing", fmt.Sprintf("connect map key %s has no receiver input-pin address rule", key), receiverFlowID)}
		}
		return nil
	}
	address := runtimecontracts.FlowInputPinAddress{
		By:          strings.TrimSpace(inputPin.Address.By),
		Source:      strings.TrimSpace(inputPin.Address.Source),
		Target:      strings.TrimSpace(inputPin.Address.Target),
		Cardinality: strings.TrimSpace(inputPin.Address.Cardinality),
		Mode:        strings.TrimSpace(inputPin.Address.Mode),
	}
	if address.By == "" {
		return []Finding{compositionConnectFinding(connect, "receiver_address_rule_missing", "input pin address.by is required", receiverFlowID)}
	}
	for key := range connect.Map {
		if strings.TrimSpace(key) != address.By {
			return []Finding{compositionConnectFinding(connect, "connect_map_unknown_address_key", fmt.Sprintf("connect map key %s is not declared by receiver address key %s", key, address.By), receiverFlowID)}
		}
	}
	mapEntry := connect.Map[address.By]
	sourceExpr := firstNonEmpty(mapEntry.Source, address.Source)
	if sourceExpr == "" {
		sourceExpr = "payload." + address.By
	}
	targetExpr := firstNonEmpty(mapEntry.Target, address.Target)
	if targetExpr == "" {
		targetExpr = "entity." + address.By
	}
	sourceType, err := outputPinKeyCarriesSourceType(source, producerFlowID, outputPin, sourceExpr)
	if err != nil {
		return []Finding{compositionConnectFinding(connect, "output_carries_address_key", err.Error(), producerFlowID)}
	}
	if err := compositionConnectTargetIndexed(source, receiverFlowID, targetExpr); err != nil {
		return []Finding{compositionConnectFinding(connect, "receiver_address_rule_invalid", err.Error(), receiverFlowID)}
	}
	targetType, err := compositionConnectTargetType(source, receiverFlowID, targetExpr)
	if err != nil {
		return []Finding{compositionConnectFinding(connect, "receiver_address_rule_invalid", err.Error(), receiverFlowID)}
	}
	if !compositionConnectTypesCompatible(sourceType, targetType) {
		return []Finding{compositionConnectFinding(connect, "key_types_incompatible", fmt.Sprintf("source %s type %s is incompatible with target %s type %s", sourceExpr, sourceType, targetExpr, targetType), receiverFlowID)}
	}
	return nil
}

func compositionConnectTargetType(source semanticview.Source, flowID, expr string) (string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", fmt.Errorf("target expression is required")
	}
	if strings.HasPrefix(expr, "entity.") {
		fieldPath := strings.TrimPrefix(expr, "entity.")
		contract, ok := entityruntime.ResolveForFlow(source, flowID)
		if !ok {
			return "", fmt.Errorf("receiver flow %s has no entity contract for %s", flowID, expr)
		}
		field, err := entityruntime.ResolveLeafField(contract, fieldPath)
		if err != nil {
			return "", fmt.Errorf("receiver target %s is invalid: %v", expr, err)
		}
		return field.Type, nil
	}
	if strings.HasPrefix(expr, "config.") {
		field := strings.TrimPrefix(expr, "config.")
		schema, ok := source.FlowSchemaByID(flowID)
		if !ok {
			return "", fmt.Errorf("receiver flow %s does not exist", flowID)
		}
		variable, ok := schema.InstanceVariables.Variables[field]
		if !ok {
			return "", fmt.Errorf("receiver config field %s is not declared", field)
		}
		if strings.TrimSpace(variable.Type) == "" {
			return "", fmt.Errorf("receiver config field %s has no type", field)
		}
		return variable.Type, nil
	}
	if strings.HasPrefix(expr, "instance.") {
		return "string", nil
	}
	return "", fmt.Errorf("target expression %q must be entity.*, config.*, or instance.*", expr)
}

func compositionConnectTargetIndexed(source semanticview.Source, flowID, expr string) error {
	expr = strings.TrimSpace(expr)
	switch {
	case strings.HasPrefix(expr, "entity."):
		fieldPath := strings.TrimSpace(strings.TrimPrefix(expr, "entity."))
		switch fieldPath {
		case "", "entity_id":
			return nil
		}
		if strings.Contains(fieldPath, ".") {
			return fmt.Errorf("receiver target %s uses a nested entity path; #1479 supports only top-level indexed entity fields as descriptor/index route evidence", expr)
		}
		contract, ok := entityruntime.ResolveForFlow(source, flowID)
		if !ok {
			return fmt.Errorf("receiver flow %s has no entity contract for %s", flowID, expr)
		}
		field, err := entityruntime.ResolveLeafField(contract, fieldPath)
		if err != nil {
			return fmt.Errorf("receiver target %s is invalid: %v", expr, err)
		}
		if !field.FieldDecl.Indexed {
			return fmt.Errorf("receiver target %s must declare indexed: true before it can be used as descriptor/index route evidence", expr)
		}
		return nil
	case strings.HasPrefix(expr, "config."):
		return fmt.Errorf("receiver target %s has no typed descriptor/index owner; use an indexed entity field or an identity descriptor target", expr)
	case strings.HasPrefix(expr, "instance."):
		fieldPath := strings.TrimSpace(strings.TrimPrefix(expr, "instance."))
		switch fieldPath {
		case "flow_instance", "instance_id":
			return nil
		default:
			return fmt.Errorf("receiver target %s has no typed descriptor/index owner; use an indexed entity field or an identity descriptor target", expr)
		}
	default:
		return nil
	}
}

func compositionConnectTypesCompatible(sourceType, targetType string) bool {
	sourceType = compositionConnectTypeFamily(sourceType)
	targetType = compositionConnectTypeFamily(targetType)
	return sourceType != "" && targetType != "" && sourceType == targetType
}

func compositionConnectTypeFamily(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "string", "text", "uuid", "timestamp":
		return "string"
	case "integer", "number", "numeric", "float", "double", "real":
		return "number"
	case "boolean", "bool":
		return "boolean"
	default:
		return raw
	}
}

func compositionConnectsToInput(source semanticview.Source, flowID, pinOrEvent string) bool {
	if source == nil {
		return false
	}
	pinOrEvent = eventidentity.Normalize(pinOrEvent)
	if pinOrEvent == "" {
		return false
	}
	for _, pin := range source.FlowInputEventPins(flowID) {
		if eventidentity.Normalize(pin.PinName()) != pinOrEvent && eventidentity.Normalize(pin.EventType()) != pinOrEvent {
			continue
		}
		if len(source.CompositionConnectsTo(flowID, pin.PinName())) > 0 {
			return true
		}
	}
	return false
}

func compositionConnectsFromOutputEvent(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		if eventidentity.Normalize(pin.PinName()) != eventType && eventidentity.Normalize(pin.EventType()) != eventType {
			continue
		}
		if len(source.CompositionConnectsFrom(flowID, pin.PinName())) > 0 {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func stringSliceContains(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == needle {
			return true
		}
	}
	return false
}
