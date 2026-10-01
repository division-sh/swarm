package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
			if pin.Resolution().Empty() {
				continue
			}
			findings = append(findings, validateInputPinResolution(source, flowID, pin)...)
		}
	}
	return findings
}
func validateInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.CompiledFlowInputPin) []Finding {
	var findings []Finding
	resolution := pin.Resolution()
	location := flowID
	switch resolution.Mode {
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

func validateReplyInputPinResolution(source semanticview.Source, flowID string, pin runtimecontracts.CompiledFlowInputPin) []Finding {
	resolution := pin.Resolution()
	location := flowID
	var findings []Finding
	requestPinName := strings.TrimSpace(resolution.RepliesTo)
	if requestPinName == "" {
		return append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", "resolution mode reply requires replies_to", location))
	}
	requestPin, ok := source.FlowOutputEventPin(flowID, requestPinName)
	if !ok {
		return append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply replies_to %q must name a same-flow output pin", requestPinName), location))
	}
	correlationKey := strings.TrimSpace(resolution.CorrelationKey)
	if correlationKey != "" && !outputPinRequiredPayloadFieldExists(source, flowID, requestPin, correlationKey) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply correlation_key %q must name a required payload field declared by output event %s", correlationKey, requestPin.EventType()), location))
	}
	graph := runtimepinrouting.CompileConnectGraph(source)
	requestConnects := graph.PlansFromOutputPin(strings.TrimSpace(flowID), requestPin)
	replyConnects := graph.PlansToInputPin(strings.TrimSpace(flowID), pin)
	if len(requestConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", flowID, requestPinName, len(requestConnects)), location))
		return findings
	}
	if len(replyConnects) != 1 {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", fmt.Sprintf("resolution mode reply input pin %s.%s must have exactly one connected provider output, got %d", flowID, pin.EventType(), len(replyConnects)), location))
		return findings
	}
	requestTarget := requestConnects[0].ReceiverEndpoint()
	replySource := replyConnects[0].SourceEndpoint()
	if requestTarget.IsRoot() || replySource.IsRoot() || !runtimepinrouting.ConnectEndpointsShareFlow(requestTarget, replySource) {
		findings = append(findings, inputPinResolutionFinding(flowID, pin, "reply_lineage_missing", "resolution mode reply request and reply edges must connect the same provider flow", location))
	}
	return findings
}

func outputPinRequiredPayloadFieldExists(source semanticview.Source, flowID string, pin runtimecontracts.CompiledFlowOutputPin, field string) bool {
	field = strings.TrimSpace(field)
	if field == "" || strings.Contains(field, ".") {
		return false
	}
	resolved, ok := semanticview.ResolveEventSchema(source, flowID, pin.EventType()).Field(field)
	return ok && !resolved.IsOptional
}

func inputPinResolutionFinding(flowID string, pin runtimecontracts.CompiledFlowInputPin, reason, detail, location string) Finding {
	if strings.TrimSpace(location) == "" {
		location = flowID
	}
	return Finding{
		CheckID:  "composition_connect_validation",
		Severity: "error",
		Message:  fmt.Sprintf("input pin %s.%s resolution is invalid: %s: %s", strings.TrimSpace(flowID), strings.TrimSpace(pin.EventType()), reason, detail),
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
