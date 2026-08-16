package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const outputPinKeyCarriesCheckID = "output_pin_key_carries_validation"

type outputPinKeyCarriesIdentity struct {
	FlowID  string
	PinName string
}

func checkOutputPinKeyCarriesValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	source := c.source
	var findings []Finding
	for _, flowID := range outputPinKeyCarriesFlowIDs(source) {
		findings = append(findings, validateOutputPinKeyCarriesForFlow(source, flowID)...)
	}
	findings = append(findings, validateOutputPinKeyCarriesNodeEmitSites(source)...)
	findings = append(findings, validateOutputPinKeyCarriesNonNodeProducerSites(source)...)
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Location != findings[j].Location {
			return findings[i].Location < findings[j].Location
		}
		return findings[i].Message < findings[j].Message
	})
	return findings
}

func validateOutputPinKeyCarriesForFlow(source semanticview.Source, flowID string) []Finding {
	var findings []Finding
	seenEventKeys := map[string]string{}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		if outputPinHasKeyCarries(pin) {
			findings = append(findings, validateOutputPinKeyCarriesDeclaration(source, flowID, pin)...)
		}
		if strings.TrimSpace(pin.Key) == "" {
			continue
		}
		eventKey := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, pin.EventType()))
		if eventKey == "" {
			eventKey = eventidentity.Normalize(pin.EventType())
		}
		ambiguityKey := eventKey + "\x00" + strings.TrimSpace(pin.Key)
		if previous := seenEventKeys[ambiguityKey]; previous != "" {
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "ambiguous_output_key", fmt.Sprintf("output pins %s and %s both declare key %s for event %s", previous, pin.PinName(), strings.TrimSpace(pin.Key), pin.EventType())))
			continue
		}
		seenEventKeys[ambiguityKey] = pin.PinName()
	}
	return findings
}

func validateOutputPinKeyCarriesDeclaration(source semanticview.Source, flowID string, pin runtimecontracts.FlowOutputEventPin) []Finding {
	var findings []Finding
	key := strings.TrimSpace(pin.Key)
	carries := outputPinCarries(pin)
	if key == "" && len(carries) > 0 {
		findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "missing_key", "output pin declares carries without a key"))
	}
	if key != "" && !outputPinStringSetContains(carries, key) {
		findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "key_not_carried", fmt.Sprintf("output pin key %s must also appear in carries", key)))
	}
	seen := map[string]struct{}{}
	for _, field := range carries {
		if strings.TrimSpace(field) == "" {
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "empty_carry_field", "output pin carries includes an empty field"))
			continue
		}
		if strings.Contains(field, ".") {
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "nested_carry_field", fmt.Sprintf("output pin carry %s must be a top-level payload field in this slice", field)))
			continue
		}
		if _, ok := seen[field]; ok {
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "duplicate_carry_field", fmt.Sprintf("output pin carries declares %s more than once", field)))
			continue
		}
		seen[field] = struct{}{}
	}
	for _, field := range outputPinRequiredFields(pin) {
		typ, err := outputPinPayloadFieldType(source, flowID, pin.EventType(), field)
		if err != nil {
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "payload_field_unproven", err.Error()))
			continue
		}
		if !outputPinScalarType(typ) {
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "payload_field_not_scalar", fmt.Sprintf("producer output event %s payload field %s type %s is not a scalar key type", pin.EventType(), field, typ)))
		}
	}
	return findings
}

func validateOutputPinKeyCarriesNonNodeProducerSites(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	var findings []Finding
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
		if endpoint.Kind != semanticview.EventEndpointAgent && endpoint.Kind != semanticview.EventEndpointTimer && endpoint.Kind != semanticview.EventEndpointAutoEmit {
			continue
		}
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, endpoint.FlowID, endpoint.Event.EventKey()) {
			if len(outputPinRequiredFields(pin)) == 0 {
				continue
			}
			switch endpoint.Kind {
			case semanticview.EventEndpointAgent:
				findings = append(findings, outputPinKeyCarriesFinding(endpoint.FlowID, pin, "agent_emit_payload_unproven", fmt.Sprintf("agent %s emit_events declares output pin %s event %s, but agent emit_events has no static payload construction surface for key/carries fields %s", endpoint.AgentID, pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
			case semanticview.EventEndpointTimer:
				findings = append(findings, outputPinKeyCarriesFinding(endpoint.FlowID, pin, "timer_payload_unproven", fmt.Sprintf("workflow timer %s declares output pin %s event %s, but timer payload construction cannot be statically proven for key/carries fields %s", endpoint.TimerID, pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
			case semanticview.EventEndpointAutoEmit:
				prefix := "auto_emit_on_create"
				if strings.TrimSpace(endpoint.FlowID) == "" {
					prefix = "root auto_emit_on_create"
				}
				findings = append(findings, outputPinKeyCarriesFinding(endpoint.FlowID, pin, "auto_emit_payload_unproven", fmt.Sprintf("%s declares output pin %s event %s, but activation config payload cannot be statically proven for key/carries fields %s", prefix, pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
			}
		}
	}
	return findings
}

func validateOutputPinKeyCarriesNodeEmitSites(source semanticview.Source) []Finding {
	var findings []Finding
	seen := map[string]struct{}{}
	for _, site := range pinRoutingEmitSites(source) {
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, site.FlowID(), site.Spec.EventType()) {
			for _, field := range outputPinRequiredFields(pin) {
				if _, ok := site.Spec.Fields[field]; ok {
					continue
				}
				key := site.ID + "\x00" + pin.PinName() + "\x00" + field
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, outputPinKeyCarriesFinding(site.FlowID(), pin, "emit_payload_missing_key", fmt.Sprintf("node %s emit site %s emits output pin %s event %s but emit.fields does not statically prove carried field %s", site.Node.Key(), site.Site, pin.PinName(), pin.EventType(), field)))
			}
		}
	}
	return findings
}

func outputPinPayloadFieldType(source semanticview.Source, flowID, eventType, field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return "", fmt.Errorf("producer output event %s key/carries field is empty", eventType)
	}
	resolution := semanticview.ResolveEventSchema(source, flowID, eventType)
	if !resolution.HasSchema {
		return "", fmt.Errorf("producer output event %s has no payload schema", eventType)
	}
	props, _ := resolution.Schema.Schema["properties"].(map[string]any)
	raw, ok := props[field]
	if !ok {
		return "", fmt.Errorf("producer output event %s does not declare payload field %s required by output pin key/carries", eventType, field)
	}
	prop, _ := raw.(map[string]any)
	typ, _ := prop["type"].(string)
	typ = strings.TrimSpace(typ)
	if typ == "" {
		return "", fmt.Errorf("producer output event %s payload field %s has no scalar type", eventType, field)
	}
	return typ, nil
}

func outputPinKeyCarriesFlowIDs(source semanticview.Source) []string {
	seen := map[string]struct{}{"": {}}
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		seen[flowID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for flowID := range seen {
		out = append(out, flowID)
	}
	sort.Strings(out)
	return out
}

func outputPinKeyCarriesPinsForEvent(source semanticview.Source, flowID, eventType string) []runtimecontracts.FlowOutputEventPin {
	if source == nil {
		return nil
	}
	var out []runtimecontracts.FlowOutputEventPin
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).MatchingOutputPins(flowID, eventType) {
		if pin, ok := source.FlowOutputEventPin(flowID, endpoint.PinName); ok {
			out = append(out, pin)
		}
	}
	return out
}

func outputPinHasKeyCarries(pin runtimecontracts.FlowOutputEventPin) bool {
	return strings.TrimSpace(pin.Key) != "" || len(outputPinCarries(pin)) > 0
}

func outputPinCarries(pin runtimecontracts.FlowOutputEventPin) []string {
	out := make([]string, 0, len(pin.Carries))
	for _, field := range pin.Carries {
		out = append(out, strings.TrimSpace(field))
	}
	return out
}

func outputPinRequiredFields(pin runtimecontracts.FlowOutputEventPin) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, field := range append([]string{strings.TrimSpace(pin.Key)}, outputPinCarries(pin)...) {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

func outputPinScalarType(typ string) bool {
	switch compositionConnectTypeFamily(typ) {
	case "string", "number", "boolean":
		return true
	default:
		return false
	}
}

func outputPinStringSetContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func outputPinKeyCarriesFinding(flowID string, pin runtimecontracts.FlowOutputEventPin, reason, detail string) Finding {
	location := strings.TrimSpace(flowID)
	label := "flow " + location
	if location == "" {
		location = "root"
		label = "root"
	}
	return Finding{
		CheckID:  outputPinKeyCarriesCheckID,
		Severity: "error",
		Message:  fmt.Sprintf("%s output pin %s is invalid: %s: %s", label, pin.PinName(), reason, detail),
		Location: location,
	}
}
