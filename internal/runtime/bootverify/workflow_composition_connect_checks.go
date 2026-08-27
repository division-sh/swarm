package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkCompositionConnectValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	var findings []Finding
	findings = append(findings, validateInputPinResolutions(c.source)...)
	graph := runtimepinrouting.CompileConnectGraph(c.source)
	for _, issue := range graph.Issues() {
		context := issue.DiagnosticContext()
		if context != "" {
			context += ": "
		}
		findings = append(findings, Finding{
			CheckID: "composition_connect_validation", Severity: "error",
			Location: strings.TrimSpace(issue.AuthoredLocation),
			Message:  fmt.Sprintf("%scompiled connect is invalid: %s: %s", context, issue.Failure.Code(), strings.TrimSpace(issue.Detail)),
			Evidence: []string{"classification: " + issue.Failure.Code()},
		})
	}
	for _, collision := range graph.ReceiverPinCollisions() {
		findings = append(findings, Finding{
			CheckID:     "composition_connect_validation",
			Severity:    "error",
			Location:    strings.TrimSpace(collision.AuthoredLocation()),
			Message:     collision.Message(),
			Remediation: "Route the source event to distinct subscribers or targets, or consolidate the receiver pins behind one handler. One event x subscriber cannot select multiple receiver-local handlers.",
			Evidence: []string{
				"classification: " + runtimepinrouting.ConnectReceiverPinCollisionFailure,
				"source: " + collision.SourceDiagnostic(),
				"subscriber: " + collision.SubscriberType() + ":" + collision.SubscriberID(),
			},
		})
	}
	return findings
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
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_unimplemented", fmt.Sprintf("resolution mode %q is design-locked but not runnable in this slice", runtimecontracts.FlowInputResolutionModeCode(resolution.Mode)), location))
	case runtimecontracts.FlowInputResolutionModeNone:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", "resolution.mode is required", location))
	default:
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("resolution mode %q is not supported", runtimecontracts.FlowInputResolutionModeCode(resolution.Mode)), location))
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
	graph := runtimepinrouting.CompileConnectGraph(source)
	requestConnects := graph.PlansFromOutputPin(strings.TrimSpace(flowID), requestPin)
	replyConnects := graph.PlansToInputPin(strings.TrimSpace(flowID), pin)
	if len(requestConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", flowID, requestPinName, len(requestConnects)), location))
		return findings
	}
	if len(replyConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply input pin %s.%s must have exactly one connected provider output, got %d", flowID, pin.PinName(), len(replyConnects)), location))
		return findings
	}
	requestTarget := requestConnects[0].ReceiverEndpoint()
	replySource := replyConnects[0].SourceEndpoint()
	if requestTarget.IsRoot() || replySource.IsRoot() || !runtimepinrouting.ConnectEndpointsShareFlow(requestTarget, replySource) {
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
	for _, plan := range source.WorkflowJoins() {
		if plan.Node.FlowID() != strings.TrimSpace(flowID) {
			continue
		}
		association := census.ResolveFanInInputForHandler(plan.Node, plan.HandlerEvent)
		matchedPin, ok := association.Endpoint()
		if !ok || strings.TrimSpace(matchedPin.PinName) != strings.TrimSpace(pin.PinName()) {
			continue
		}
		label := plan.Node.PackageKey() + ":" + plan.Node.FlowID() + ":" + plan.Node.NodeID()
		candidates = append(candidates, label+"."+plan.HandlerEvent+" join "+plan.Spec.EffectiveID())
		handler, ok := source.ExecutableNodeEventHandler(plan.Node, plan.HandlerEvent)
		if !ok || handler.Join == nil {
			continue
		}
		if handler.Accumulate != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s declares accumulate for a barrier fan-in; use handler.join as the sole finite-barrier owner", label, plan.HandlerEvent), flowID))
		}
		if authored := strings.TrimSpace(handler.Join.Members.By); authored != "" {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s join.members.by derives from resolution.dedup_by (%s); remove authored by: %s", label, plan.HandlerEvent, strings.Join(pin.Resolution.DedupBy, ", "), authored), flowID))
		}
		window := strings.TrimSpace(pin.Resolution.Window)
		if window == "" && handler.Join.Window != nil {
			findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s join.window requires resolution.window on the barrier input pin; declare the payload window once on the pin or remove join.window", label, plan.HandlerEvent), flowID))
		}
		if window != "" {
			if handler.Join.Window == nil || strings.TrimSpace(handler.Join.Window.From) == "" {
				findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s requires join.window.from to snapshot the lifecycle window paired with resolution.window %s", label, plan.HandlerEvent, window), flowID))
			} else if authored := strings.TrimSpace(handler.Join.Window.By); authored != "" {
				findings = append(findings, inputPinResolutionFinding(flowID, pin, "instance_resolution_invalid", fmt.Sprintf("receiver handler %s.%s join.window.by derives from resolution.window (%s); remove authored by: %s", label, plan.HandlerEvent, window, authored), flowID))
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
		node, err := runtimeidentity.ParseExecutableNode(endpoint.PackageKey, endpoint.FlowID, endpoint.NodeID)
		if err != nil {
			continue
		}
		association := census.ResolveFanInInputForHandler(node, endpoint.HandlerEvent)
		matchedPin, ok := association.Endpoint()
		if !ok || strings.TrimSpace(matchedPin.PinName) != strings.TrimSpace(pin.PinName()) {
			continue
		}
		handler, ok := source.ExecutableNodeEventHandler(node, endpoint.HandlerEvent)
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
	for _, required := range entry.Payload.Required {
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
	modeText := runtimecontracts.FlowInputResolutionModeCode(mode)
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
	if _, err := bundle.ResolveFlowInputInstanceSourceType(source, flowID, pin, instance); err != nil {
		reason := "instance_resolution_invalid"
		if strings.Contains(err.Error(), "key_types_incompatible") {
			reason = "key_types_incompatible"
		}
		findings = append(findings, inputPinResolutionFinding(flowID, pin, reason, err.Error(), location))
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

// compositionConnectTypeFamily remains the output-pin compatibility owner.
// Instance-key source compatibility is owned by contracts.
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
