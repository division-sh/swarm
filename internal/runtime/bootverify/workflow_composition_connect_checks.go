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
	for _, issue := range routingtopology.Build(c.source).Issues {
		if strings.TrimSpace(issue.Failure) != routingtopology.FailureConnectReceiverPinCollision {
			continue
		}
		findings = append(findings, Finding{
			CheckID:     "composition_connect_validation",
			Severity:    "error",
			Location:    strings.TrimSpace(issue.Location),
			Message:     strings.TrimSpace(issue.Message),
			Remediation: strings.TrimSpace(issue.Remediation),
			Evidence: []string{
				"classification: " + routingtopology.FailureConnectReceiverPinCollision,
				"source: " + strings.TrimSpace(issue.From),
				"subscriber: " + strings.TrimSpace(issue.To),
			},
		})
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
	if from.Root {
		flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey)
		if !ok {
			findings = append(findings, compositionConnectFinding(connect, "producer_flow_missing", fmt.Sprintf("package %s has no owning flow for root output pin %s", connect.PackageKey, from.Pin), connect.PackageKey))
			return findings
		}
		from.FlowID, from.Root = flowID, flowID == ""
	}
	if to.Root {
		flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey)
		if !ok {
			findings = append(findings, compositionConnectFinding(connect, "receiver_flow_missing", fmt.Sprintf("package %s has no owning flow for root input pin %s", connect.PackageKey, to.Pin), connect.PackageKey))
			return findings
		}
		to.FlowID, to.Root = flowID, flowID == ""
	}

	if !from.Root {
		if _, ok := source.FlowSchemaByID(from.FlowID); !ok {
			findings = append(findings, compositionConnectFinding(connect, "producer_flow_missing", fmt.Sprintf("producer flow %s does not exist", from.FlowID), from.FlowID))
			return findings
		}
	}
	receiverSchema := runtimecontracts.FlowSchemaDocument{}
	if !to.Root {
		var ok bool
		receiverSchema, ok = source.FlowSchemaByID(to.FlowID)
		if !ok {
			findings = append(findings, compositionConnectFinding(connect, "receiver_flow_missing", fmt.Sprintf("receiver flow %s does not exist", to.FlowID), to.FlowID))
			return findings
		}
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
		receiverLabel := fmt.Sprintf("receiver flow %s", to.FlowID)
		location := to.FlowID
		if to.Root {
			receiverLabel = "root schema"
			location = "root"
		}
		findings = append(findings, compositionConnectFinding(connect, "receiver_input_pin_missing", fmt.Sprintf("%s does not declare input pin %s", receiverLabel, to.Pin), location))
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
	findings = append(findings, validateCompositionConnectSyntheticCarryCollisions(source, connect, from, outputPin, inputPin)...)
	if to.Root {
		if !inputPin.Resolution.Empty() {
			findings = append(findings, compositionConnectFinding(connect, "root_receiver_resolution_invalid", "root input pins are static receivers and cannot declare instance resolution", "root"))
		}
		return findings
	}
	if compositionReceiverResolutionRequired(receiverSchema) && inputPin.Resolution.Empty() {
		findings = append(findings, compositionConnectFinding(connect, "receiver_resolution_missing", fmt.Sprintf("receiver flow %s is a template and requires receiver-owned resolution.mode plus a same-named instance carry", to.FlowID), to.FlowID))
	}
	return findings
}

func validateCompositionConnectSyntheticCarryCollisions(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, from runtimecontracts.FlowPackagePinRef, outputPin runtimecontracts.FlowOutputEventPin, inputPin runtimecontracts.FlowInputEventPin) []Finding {
	if from.Root || inputPin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeCreate {
		return nil
	}
	syntheticFields := map[string]string{}
	for field, carry := range inputPin.Carries {
		field = strings.TrimSpace(field)
		sourceField := strings.TrimSpace(carry.From)
		sourceOwner, err := runtimecontracts.ResolveFlowInputInstanceSource(inputPin.Resolution.Mode, sourceField)
		if field == "" || err != nil || !sourceOwner.RequiresDeliveryProjection() {
			continue
		}
		syntheticFields[field] = sourceField
	}
	if len(syntheticFields) == 0 {
		return nil
	}
	wantEvent := eventidentity.Normalize(source.ResolveFlowEventReference(from.FlowID, outputPin.EventType()))
	var findings []Finding
	for _, site := range semanticview.AuthoredEmitSites(source) {
		if strings.TrimSpace(site.FlowID) != strings.TrimSpace(from.FlowID) {
			continue
		}
		siteEvent := eventidentity.Normalize(source.ResolveFlowEventReference(site.FlowID, site.Spec.EventType()))
		if siteEvent == "" || siteEvent != wantEvent {
			continue
		}
		for field, carrySource := range syntheticFields {
			if _, authored := site.Spec.Fields[field]; !authored {
				continue
			}
			producer := strings.TrimSpace(site.NodeID)
			if producer == "" {
				producer = strings.TrimSpace(site.SiteKey)
			}
			findings = append(findings, compositionConnectFinding(
				connect,
				"synthetic_carry_payload_collision",
				fmt.Sprintf("producer %s emit field %s conflicts with receiver-owned carry %s -> payload.%s; remove or rename the producer field, or choose a different carry as field", producer, field, carrySource, field),
				from.FlowID,
			))
		}
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

func compositionReceiverResolutionRequired(schema runtimecontracts.FlowSchemaDocument) bool {
	return strings.EqualFold(strings.TrimSpace(schema.Mode), "template")
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
	switch resolution.Mode {
	case runtimecontracts.FlowInputResolutionModeCreate, runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return validateCanonicalInstanceInputPinResolution(source, flowID, pin)
	case runtimecontracts.FlowInputResolutionModeFanIn:
		return validateFanInInputPinResolution(source, flowID, pin)
	case runtimecontracts.FlowInputResolutionModeReply:
		return validateReplyInputPinResolution(source, flowID, pin)
	case runtimecontracts.FlowInputResolutionModeFanOut:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_unimplemented", fmt.Sprintf("resolution mode %q is design-locked but not runnable in this slice", resolution.Mode.String()), location))
	case runtimecontracts.FlowInputResolutionModeNone:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution.mode is required", location))
	default:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %q is not supported", resolution.Mode.String()), location))
	}
	return findings
}

func validateReplyInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	resolution := pin.Resolution
	location := flowID
	var findings []Finding
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" {
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
	requestConnects := semanticview.ResolvedCompositionConnectsFrom(source, flowID, requestPinName)
	if len(requestConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", flowID, requestPinName, len(requestConnects)), location))
		return findings
	}
	replyConnects := semanticview.ResolvedCompositionConnectsTo(source, flowID, pin.PinName())
	if len(replyConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply input pin %s.%s must have exactly one connected provider output, got %d", flowID, pin.PinName(), len(replyConnects)), location))
		return findings
	}
	requestTarget := requestConnects[0].To
	replySource := replyConnects[0].From
	if requestTarget.Root || replySource.Root || strings.TrimSpace(requestTarget.FlowID) != strings.TrimSpace(replySource.FlowID) {
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
	if resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
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
	dedupBy = normalizeCompositionFields(dedupBy)
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

func validateCanonicalInstanceInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.FlowInputEventPin) []Finding {
	var findings []Finding
	resolution := pin.Resolution
	mode := resolution.Mode
	modeText := mode.String()
	location := flowID
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s may only declare mode and carries", modeText), location))
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_unavailable", "receiver instance key owner is unavailable for input pin resolution", location))
	}
	instance, err := bundle.ResolveFlowTemplateInstance(flowID)
	if err != nil {
		return append(findings, inputPinResolutionFinding(flowID, pin, "receiver_instance_key_invalid", err.Error(), location))
	}
	if instance.Field.Empty() {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %s requires receiver `instance: <field>`", modeText), location))
		return findings
	}
	key := instance.Field.String()
	carry, ok := pin.Carries[key]
	if !ok {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("flow %s is one instance per %s; input pin %s must declare a carry named %s (add carries: %s: {from: payload.<field>})", flowID, key, pin.PinName(), key, key), location))
		return findings
	}
	instanceSource, err := runtimecontracts.ResolveFlowInputInstanceSource(mode, carry.From)
	if err != nil {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("carry %s source %q is invalid for resolution mode %s: %v", key, strings.TrimSpace(carry.From), modeText, err), location))
	} else if instanceSource.Kind == runtimecontracts.FlowInputInstanceSourcePayload && !inputPinPayloadFieldExists(source, flowID, pin, instanceSource.Path) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("carry %s source %s is not declared by input event %s", key, instanceSource.Path, pin.EventType()), location))
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

func compositionConnectTypesCompatible(sourceType, targetType string) bool {
	sourceType = compositionConnectTypeFamily(sourceType)
	targetType = compositionConnectTypeFamily(targetType)
	return sourceType != "" && targetType != "" && sourceType == targetType
}

func normalizeCompositionFields(in []string) []string {
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
