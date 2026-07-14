package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkCompositionConnectValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	var findings []Finding
	findings = append(findings, validateInputPinResolutions(c.source)...)
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
	adapter := connect.Using.Instance
	if !inputPin.Resolution.Empty() {
		switch inputPin.Resolution.Mode {
		case runtimecontracts.FlowInputResolutionModeCreate, runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate, runtimecontracts.FlowInputResolutionModeFanIn:
			if strings.TrimSpace(connect.Delivery) != "" && strings.TrimSpace(connect.Delivery) != "one" {
				return []Finding{compositionConnectFinding(connect, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s requires delivery one", inputPin.Resolution.Mode), receiverFlowID)}
			}
		}
		if adapter.Declared {
			return []Finding{compositionConnectFinding(connect, "instance_resolution_invalid", "connect.using.instance is incompatible with input pin resolution", receiverFlowID)}
		}
		if len(connect.Map) > 0 {
			return []Finding{compositionConnectFinding(connect, "instance_resolution_invalid", "connect.map is incompatible with input pin resolution", receiverFlowID)}
		}
		return nil
	}
	if inputPin.Address != nil {
		if adapter.Declared {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_invalid", "connect.using.instance is valid only for addressless template receiver instance-key routes", receiverFlowID)}
		}
		return nil
	}
	if strings.TrimSpace(connect.Delivery) == "broadcast" {
		if adapter.Declared {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_invalid", "connect.using.instance is incompatible with delivery broadcast", receiverFlowID)}
		}
		return nil
	}
	receiverSchema, ok := source.FlowSchemaByID(receiverFlowID)
	if !ok || !compositionReceiverAddressRequired(receiverSchema) {
		if adapter.Declared {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_invalid", "connect.using.instance requires a template receiver", receiverFlowID)}
		}
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
	if adapter.Declared {
		return validateCompositionConnectInstanceKeyAdapter(source, connect, adapter, carries, instance.By, outputPin, producerFlowID, receiverFlowID)
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

func validateInputPinResolutions(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	var findings []Finding
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		for _, pin := range source.FlowInputEventPins(flowID) {
			findings = append(findings, validateFlowInputCarryProjectionPolicy(flowID, pin)...)
			if pin.Resolution.Empty() {
				continue
			}
			findings = append(findings, validateInputPinResolution(source, flowID, pin)...)
		}
	}
	return findings
}

func validateFlowInputCarryProjectionPolicy(flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	var findings []Finding
	for name, carry := range pin.Carries {
		name = strings.TrimSpace(name)
		if carry.Optional {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "carry_projection_unsupported", fmt.Sprintf("carry %s optional is reserved for provider normalized-event projections and is not supported on flow input carries", name), flowID))
		}
		if conversion := strings.TrimSpace(carry.Convert); conversion != "" {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "carry_projection_unsupported", fmt.Sprintf("carry %s conversion %q is reserved for provider normalized-event projections and is not supported on flow input carries", name, conversion), flowID))
		}
	}
	return findings
}

func validateInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	var findings []Finding
	resolution := pin.Resolution
	location := flowID
	if pin.Address != nil {
		return []Finding{inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "input pin resolution is incompatible with legacy address", location)}
	}
	switch resolution.Mode {
	case runtimecontracts.FlowInputResolutionModeCreate:
		return validateCreateInputPinResolution(source, flowID, pin)
	case runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return validateCarriedKeyInputPinResolution(source, flowID, pin, resolution.Mode)
	case runtimecontracts.FlowInputResolutionModeFanIn:
		return validateFanInInputPinResolution(source, flowID, pin)
	case runtimecontracts.FlowInputResolutionModeReply:
		return validateReplyInputPinResolution(source, flowID, pin)
	case runtimecontracts.FlowInputResolutionModeFanOut:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_unimplemented", fmt.Sprintf("resolution mode %q is design-locked but not runnable in this slice", resolution.Mode), location))
	case "":
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution.mode is required", location))
	default:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %q is not supported", resolution.Mode), location))
	}
	return findings
}

func validateReplyInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	resolution := pin.Resolution
	location := flowID
	var findings []Finding
	if !resolution.InstanceKey.Empty() || resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution mode reply may only declare replies_to and correlation_key", location))
	}
	requestPinName := strings.TrimSpace(resolution.RepliesTo)
	if requestPinName == "" {
		return append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", "resolution mode reply requires replies_to", location))
	}
	requestPin, ok := source.FlowOutputEventPin(flowID, requestPinName)
	if !ok {
		return append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply replies_to %q must name a same-flow output pin", requestPinName), location))
	}
	correlationKey := strings.TrimSpace(resolution.CorrelationKey)
	if correlationKey != "" && !containsTrimmedString(requestPin.Carries, correlationKey) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply correlation_key %q must name a carry declared by output pin %s", correlationKey, requestPinName), location))
	}
	requestConnects := source.CompositionConnectsFrom(flowID, requestPinName)
	if len(requestConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", flowID, requestPinName, len(requestConnects)), location))
		return findings
	}
	replyConnects := source.CompositionConnectsTo(flowID, pin.PinName())
	if len(replyConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply input pin %s.%s must have exactly one connected provider output, got %d", flowID, pin.PinName(), len(replyConnects)), location))
		return findings
	}
	requestTarget, requestErr := requestConnects[0].ToRef()
	replySource, replyErr := replyConnects[0].FromRef()
	if requestErr != nil || replyErr != nil || requestTarget.Root || replySource.Root || strings.TrimSpace(requestTarget.FlowID) != strings.TrimSpace(replySource.FlowID) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", "resolution mode reply request and reply edges must connect the same provider flow", location))
	}
	return findings
}

func containsTrimmedString(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func validateFanInInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	var findings []Finding
	resolution := pin.Resolution
	aggregation := strings.ToLower(strings.TrimSpace(resolution.Aggregation))
	location := flowID
	if !resolution.InstanceKey.Empty() || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution mode fan-in may only declare aggregation, window, dedup_by, singleton, and carries", location))
	}
	if aggregation != "stream" && aggregation != "barrier" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode fan-in aggregation must be stream or barrier, got %q", resolution.Aggregation), location))
	}
	window := strings.TrimSpace(resolution.Window)
	if window == "" && aggregation == "stream" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution mode fan-in stream requires window", location))
	} else if window != "" && !validTopLevelPayloadPath(window) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode fan-in window %q must be one top-level payload field", window), location))
	} else if window != "" && !inputPinPayloadFieldExists(source, flowID, pin, window) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode fan-in window field %q is not declared on the receiver input event payload", window), location))
	}
	_, dedupOK, dedupDetail := validateFanInDedupBy(source, flowID, pin, aggregation, resolution.DedupBy)
	if !dedupOK {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", dedupDetail, location))
	}
	singleton := strings.Trim(strings.TrimSpace(resolution.Singleton), "/")
	if singleton == "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode fan-in %s requires explicit singleton receiver identity", aggregation), location))
	} else {
		bundle, ok := semanticview.Bundle(source)
		if !ok || bundle == nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "receiver_singleton_unavailable", "receiver singleton coordinator owner is unavailable for input pin resolution", location))
		} else if _, err := bundle.ResolveFlowSingletonCoordinator(flowID); err != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "receiver_singleton_invalid", err.Error(), location))
		}
		scopeKey := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, flowID)), "/")
		if scopeKey != "" && singleton != scopeKey && !strings.HasPrefix(singleton, scopeKey+"/") {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode fan-in singleton %q must be the receiver singleton route or a child of %q", singleton, scopeKey), location))
		}
	}
	if dedupOK {
		switch aggregation {
		case "stream":
			if window != "" {
				findings = append(findings, validateFanInAccumulatorConsistency(source, flowID, pin)...)
			}
		case "barrier":
			findings = append(findings, validateFanInBarrierJoinConsistency(source, flowID, pin)...)
		}
	}
	return findings
}

func validateFanInDedupBy(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin, aggregation string, dedupBy []string) (string, bool, string) {
	dedupBy = normalizedConnectAdapterFields(dedupBy)
	if len(dedupBy) == 0 {
		return "", false, fmt.Sprintf("resolution mode fan-in %s requires dedup_by; sender identity is not an implicit default", aggregation)
	}
	if len(dedupBy) != 1 {
		return "", false, fmt.Sprintf("resolution mode fan-in %s supports exactly one dedup_by field, got %v", aggregation, dedupBy)
	}
	dedup := strings.TrimSpace(dedupBy[0])
	if dedup == "event.id" && aggregation == "stream" {
		return dedup, true, ""
	}
	if !validTopLevelPayloadPath(dedup) {
		if aggregation == "barrier" && dedup == "event.id" {
			return "", false, "resolution mode fan-in barrier members are matched by a payload identity against the declared member list; event.id cannot appear in expected members"
		}
		suffix := ""
		if aggregation == "stream" {
			suffix = " or event.id"
		}
		return "", false, fmt.Sprintf("resolution mode fan-in %s dedup_by %q must be one top-level payload field%s", aggregation, dedup, suffix)
	}
	if !inputPinPayloadFieldExists(source, flowID, pin, dedup) {
		return "", false, fmt.Sprintf("resolution mode fan-in dedup_by field %q is not declared on the receiver input event payload", dedup)
	}
	return dedup, true, ""
}

func validateFanInBarrierJoinConsistency(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	if _, ok := source.FlowScopeByID(flowID); !ok {
		return []Finding{inputPinResolutionFinding(flowID, pin, "receiver_flow_missing", fmt.Sprintf("receiver flow %s does not exist", flowID), flowID)}
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	candidates := make([]string, 0, 2)
	findings := make([]Finding, 0)
	for _, endpoint := range census.MatchingConsumers(flowID, pin.EventType()) {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || strings.TrimSpace(endpoint.NodeID) == "" {
			continue
		}
		association := census.ResolveFanInInputForHandler(flowID, endpoint.NodeID, endpoint.HandlerEvent)
		matchedPin, ok := association.Endpoint()
		if !ok || strings.TrimSpace(matchedPin.PinName) != strings.TrimSpace(pin.PinName()) {
			continue
		}
		handler, ok := source.NodeEventHandler(endpoint.NodeID, endpoint.HandlerEvent)
		if !ok {
			continue
		}
		if handler.Accumulate != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s declares accumulate for a barrier fan-in; use handler.join as the sole finite-barrier owner", endpoint.NodeID, endpoint.HandlerEvent), flowID))
		}
		if handler.Join == nil {
			continue
		}
		candidates = append(candidates, endpoint.NodeID+"."+endpoint.HandlerEvent+" join "+handler.Join.EffectiveID())
		if authored := strings.TrimSpace(handler.Join.Members.By); authored != "" {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s join.members.by derives from resolution.dedup_by (%s); remove authored by: %s", endpoint.NodeID, endpoint.HandlerEvent, strings.Join(pin.Resolution.DedupBy, ", "), authored), flowID))
		}
		window := strings.TrimSpace(pin.Resolution.Window)
		if window == "" && handler.Join.Window != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s join.window requires resolution.window on the barrier input pin; declare the payload window once on the pin or remove join.window", endpoint.NodeID, endpoint.HandlerEvent), flowID))
		}
		if window != "" {
			if handler.Join.Window == nil || strings.TrimSpace(handler.Join.Window.From) == "" {
				findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s requires join.window.from to snapshot the lifecycle window paired with resolution.window %s", endpoint.NodeID, endpoint.HandlerEvent, window), flowID))
			} else if authored := strings.TrimSpace(handler.Join.Window.By); authored != "" {
				findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s join.window.by derives from resolution.window (%s); remove authored by: %s", endpoint.NodeID, endpoint.HandlerEvent, window, authored), flowID))
			}
		}
	}
	sort.Strings(candidates)
	switch len(candidates) {
	case 0:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver flow %s fan-in barrier input %s requires exactly one handler.join row for event %s; add the join row with members.from, output, on_complete, and timeout", flowID, pin.PinName(), pin.EventType()), flowID))
	case 1:
	default:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver flow %s fan-in barrier input %s matches multiple join rows %v; use distinct events or distinct stages per join", flowID, pin.PinName(), candidates), flowID))
	}
	return findings
}

func validateFanInAccumulatorConsistency(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	var findings []Finding
	if _, ok := source.FlowScopeByID(flowID); !ok {
		return []Finding{inputPinResolutionFinding(flowID, pin, "receiver_flow_missing", fmt.Sprintf("receiver flow %s does not exist", flowID), flowID)}
	}
	matchedHandler := false
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	for _, endpoint := range census.MatchingConsumers(flowID, pin.EventType()) {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || strings.TrimSpace(endpoint.NodeID) == "" {
			continue
		}
		association := census.ResolveFanInInputForHandler(flowID, endpoint.NodeID, endpoint.HandlerEvent)
		matchedPin, ok := association.Endpoint()
		if !ok || strings.TrimSpace(matchedPin.PinName) != strings.TrimSpace(pin.PinName()) {
			continue
		}
		handler, ok := source.NodeEventHandler(endpoint.NodeID, endpoint.HandlerEvent)
		if !ok {
			continue
		}
		matchedHandler = true
		if handler.Accumulate == nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s for fan-in input must declare accumulate", endpoint.NodeID, endpoint.HandlerEvent), flowID))
			continue
		}
		if dedup := strings.TrimSpace(handler.Accumulate.DedupBy); dedup != "" {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s accumulate.dedup_by %q must not redeclare fan-in dedup_by; declare it once on the receiver input pin resolution", endpoint.NodeID, endpoint.HandlerEvent, dedup), flowID))
		}
		if window := strings.TrimSpace(handler.Accumulate.Window); window != "" {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s accumulate.window %q must not redeclare fan-in window; declare it once on the receiver input pin resolution", endpoint.NodeID, endpoint.HandlerEvent, window), flowID))
		}
	}
	if !matchedHandler {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver flow %s has no handler for fan-in input event %s", flowID, pin.EventType()), flowID))
	}
	return findings
}

func validTopLevelPayloadPath(path string) bool {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return false
	}
	field := strings.TrimSpace(strings.TrimPrefix(path, "payload."))
	return field != "" && !strings.Contains(field, ".")
}

func inputPinPayloadFieldExists(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin, path string) bool {
	field := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(path), "payload."))
	if field == "" || strings.Contains(field, ".") {
		return false
	}
	if carry, ok := pin.Carries[field]; ok && strings.TrimSpace(carry.From) == "payload."+field {
		return true
	}
	entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, pin.EventType())
	if !ok {
		return false
	}
	if _, ok := entry.Payload.Properties[field]; ok {
		return true
	}
	for _, required := range append(entry.Required, entry.Payload.Required...) {
		if strings.TrimSpace(required) == field {
			return true
		}
	}
	return false
}

func validateCarriedKeyInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin, mode string) []Finding {
	var findings []Finding
	resolution := pin.Resolution
	location := flowID
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" || resolution.InstanceKey.Mint != "" || resolution.InstanceKey.As != "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s may only declare instance_key and carries", mode), location))
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_unavailable", "receiver instance key owner is unavailable for input pin resolution", location))
	}
	instance, err := bundle.ResolveFlowTemplateInstance(flowID)
	if err != nil {
		return append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_invalid", err.Error(), location))
	}
	key := strings.TrimSpace(resolution.InstanceKey.From)
	if key == "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s requires instance_key to name a carried field", mode), location))
		return findings
	}
	if len(instance.By) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s requires exactly one receiver instance.by field, got %v", mode, instance.By), location))
		return findings
	}
	if strings.TrimSpace(instance.By[0]) != key {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s instance_key %q must match the receiver's single instance.by field %v", mode, key, instance.By), location))
	}
	carry, ok := pin.Carries[key]
	if !ok {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s instance_key %s must name a declared carries.%s field", mode, key, key), location))
		return findings
	}
	wantFrom := "payload." + key
	if strings.TrimSpace(carry.From) != wantFrom {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("carry %s must use from: %s for resolution mode %s", key, wantFrom, mode), location))
	}
	if carry.Type != "" {
		targetType, err := compositionConnectTargetType(source, flowID, "entity."+key)
		if err != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_invalid", err.Error(), location))
		} else if !compositionConnectTypesCompatible(carry.Type, targetType) {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "key_types_incompatible", fmt.Sprintf("carry %s type %s is incompatible with receiver entity.%s type %s", key, carry.Type, key, targetType), location))
		}
	}
	return findings
}

func validateCreateInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	var findings []Finding
	resolution := pin.Resolution
	location := flowID
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution mode create may only declare instance_key and carries", location))
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_unavailable", "receiver instance key owner is unavailable for input pin resolution", location))
	}
	instance, err := bundle.ResolveFlowTemplateInstance(flowID)
	if err != nil {
		return append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_invalid", err.Error(), location))
	}
	mint := strings.TrimSpace(resolution.InstanceKey.Mint)
	switch mint {
	case runtimecontracts.FlowInputResolutionMintUUID, runtimecontracts.FlowInputResolutionMintEventID:
	default:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode create mint %q must be uuid or event_id", mint), location))
	}
	as := strings.TrimSpace(resolution.InstanceKey.As)
	if as == "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution mode create requires instance_key.as", location))
		return findings
	}
	if len(instance.By) != 1 || strings.TrimSpace(instance.By[0]) != as {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode create instance_key.as %q must match the receiver's single instance.by field %v", as, instance.By), location))
	}
	carry, ok := pin.Carries[as]
	if !ok {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode create must carry %s from instance.key.%s", as, as), location))
		return findings
	}
	wantFrom := "instance.key." + as
	if strings.TrimSpace(carry.From) != wantFrom {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("carry %s must use from: %s", as, wantFrom), location))
	}
	if carry.Type != "" {
		targetType, err := compositionConnectTargetType(source, flowID, "entity."+as)
		if err != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_invalid", err.Error(), location))
		} else if !compositionConnectTypesCompatible(carry.Type, targetType) {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "key_types_incompatible", fmt.Sprintf("carry %s type %s is incompatible with receiver entity.%s type %s", as, carry.Type, as, targetType), location))
		}
	}
	return findings
}

func inputPinResolutionFinding(flowID string, pin runtimecontracts.FlowInputEventPin, reason, detail, location string) Finding {
	if strings.TrimSpace(location) == "" {
		location = flowID
	}
	return Finding{
		CheckID:  "composition_connect_validation",
		Severity: "error",
		Message:  fmt.Sprintf("input pin %s.%s resolution is invalid: %s: %s", strings.TrimSpace(flowID), strings.TrimSpace(pin.PinName()), reason, detail),
		Location: location,
	}
}

func validateCompositionConnectInstanceKeyAdapter(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, adapter runtimecontracts.FlowPackageConnectInstanceAdapter, carries, receiverFields []string, outputPin runtimecontracts.FlowOutputEventPin, producerFlowID, receiverFlowID string) []Finding {
	sources := normalizedConnectAdapterFields(adapter.Source)
	targets := normalizedConnectAdapterFields(adapter.Target)
	if len(sources) == 0 {
		return []Finding{compositionConnectFinding(connect, "connect_key_adapter_missing_source", "connect.using.instance.source is required", receiverFlowID)}
	}
	if len(targets) == 0 {
		return []Finding{compositionConnectFinding(connect, "connect_key_adapter_missing_target", "connect.using.instance.target is required", receiverFlowID)}
	}
	if len(sources) != len(targets) {
		return []Finding{compositionConnectFinding(connect, "connect_key_adapter_cardinality", fmt.Sprintf("connect.using.instance source count %d must equal target count %d", len(sources), len(targets)), receiverFlowID)}
	}
	if duplicate := duplicateString(sources); duplicate != "" {
		return []Finding{compositionConnectFinding(connect, "connect_key_adapter_duplicate_source", fmt.Sprintf("connect.using.instance.source contains duplicate field %s", duplicate), producerFlowID)}
	}
	if duplicate := duplicateString(targets); duplicate != "" {
		return []Finding{compositionConnectFinding(connect, "connect_key_adapter_duplicate_target", fmt.Sprintf("connect.using.instance.target contains duplicate field %s", duplicate), receiverFlowID)}
	}
	receiverFields = normalizedConnectAdapterFields(receiverFields)
	for _, targetField := range targets {
		if !stringSliceContains(receiverFields, targetField) {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_target_missing", fmt.Sprintf("adapter target field %s is not declared in receiver instance.by %v", targetField, receiverFields), receiverFlowID)}
		}
	}
	if len(targets) != len(receiverFields) || !sameStringSet(targets, receiverFields) {
		return []Finding{compositionConnectFinding(connect, "connect_key_adapter_partial", fmt.Sprintf("connect.using.instance.target must map every receiver instance.by field %v", receiverFields), receiverFlowID)}
	}
	for idx, sourceField := range sources {
		targetField := targets[idx]
		if !stringSliceContains(carries, sourceField) {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_source_missing", fmt.Sprintf("adapter source field %s is not declared in producer output carries %v", sourceField, carries), producerFlowID)}
		}
		sourceType, err := outputPinCarriedPayloadFieldType(source, producerFlowID, outputPin, sourceField)
		if err != nil {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_source_missing", err.Error(), producerFlowID)}
		}
		targetType, err := compositionConnectTargetType(source, receiverFlowID, "entity."+targetField)
		if err != nil {
			return []Finding{compositionConnectFinding(connect, "connect_key_adapter_target_missing", err.Error(), receiverFlowID)}
		}
		if !compositionConnectTypesCompatible(sourceType, targetType) {
			return []Finding{compositionConnectFinding(connect, "key_types_incompatible", fmt.Sprintf("adapter source payload.%s type %s is incompatible with receiver instance.by field entity.%s type %s", sourceField, sourceType, targetField, targetType), receiverFlowID)}
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
	if expr == "_entity.id" {
		return "uuid", nil
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
	return "", fmt.Errorf("target expression %q must be _entity.id, entity.*, config.*, or instance.*", expr)
}

func compositionConnectTargetIndexed(source semanticview.Source, flowID, expr string) error {
	expr = strings.TrimSpace(expr)
	switch {
	case expr == "_entity.id":
		return nil
	case strings.HasPrefix(expr, "entity."):
		fieldPath := strings.TrimSpace(strings.TrimPrefix(expr, "entity."))
		switch fieldPath {
		case "", "entity_id":
			return nil
		}
		if strings.Contains(fieldPath, ".") {
			return fmt.Errorf("receiver target %s uses a nested entity path; descriptor/index route evidence supports only top-level indexed entity fields", expr)
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

func normalizedConnectAdapterFields(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, field := range in {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func duplicateString(in []string) string {
	seen := map[string]struct{}{}
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			return item
		}
		seen[item] = struct{}{}
	}
	return ""
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := map[string]struct{}{}
	for _, item := range left {
		item = strings.TrimSpace(item)
		if item == "" {
			return false
		}
		seen[item] = struct{}{}
	}
	for _, item := range right {
		item = strings.TrimSpace(item)
		if item == "" {
			return false
		}
		if _, ok := seen[item]; !ok {
			return false
		}
	}
	return true
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

func compositionConnectsFromOutputEvent(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	for _, edge := range routingtopology.Build(source).Edges {
		if edge.Scope != routingtopology.DeliveryScopeInterFlowConnect || strings.TrimSpace(edge.Producer.FlowID) != strings.TrimSpace(flowID) {
			continue
		}
		if eventidentity.Normalize(edge.Producer.Event.Authored) == eventType || eventidentity.Normalize(edge.Producer.Event.Local) == eventType || eventidentity.Normalize(edge.Producer.Event.Canonical) == eventType {
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
