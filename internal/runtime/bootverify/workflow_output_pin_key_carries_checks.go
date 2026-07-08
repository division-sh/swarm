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
	addressedConnects := outputPinKeyCarriesAddressedConnects(source)
	var findings []Finding
	for _, flowID := range outputPinKeyCarriesFlowIDs(source) {
		findings = append(findings, validateOutputPinKeyCarriesForFlow(source, flowID, addressedConnects)...)
	}
	findings = append(findings, validateOutputPinKeyCarriesNodeEmitSites(source)...)
	findings = append(findings, validateOutputPinKeyCarriesAgentEmitSites(source)...)
	findings = append(findings, validateOutputPinKeyCarriesAutoEmitSites(source)...)
	findings = append(findings, validateOutputPinKeyCarriesTimerSites(source)...)
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Location != findings[j].Location {
			return findings[i].Location < findings[j].Location
		}
		return findings[i].Message < findings[j].Message
	})
	return findings
}

func validateOutputPinKeyCarriesForFlow(source semanticview.Source, flowID string, addressedConnects map[outputPinKeyCarriesIdentity]struct{}) []Finding {
	var findings []Finding
	seenEventKeys := map[string]string{}
	for _, pin := range source.FlowOutputEventPins(flowID) {
		identity := outputPinKeyCarriesIdentity{FlowID: strings.TrimSpace(flowID), PinName: pin.PinName()}
		_, requiredByAddressedConnect := addressedConnects[identity]
		if requiredByAddressedConnect || outputPinHasKeyCarries(pin) {
			findings = append(findings, validateOutputPinKeyCarriesDeclaration(source, flowID, pin, requiredByAddressedConnect)...)
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

func validateOutputPinKeyCarriesDeclaration(source semanticview.Source, flowID string, pin runtimecontracts.FlowOutputEventPin, required bool) []Finding {
	var findings []Finding
	key := strings.TrimSpace(pin.Key)
	carries := outputPinCarries(pin)
	if required && key == "" {
		findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "missing_key", "connected output pin must declare key before it can provide instance-key producer evidence"))
	}
	if required && len(carries) == 0 {
		findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "missing_carries", "connected output pin must declare carries before it can provide instance-key producer evidence"))
	}
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

func validateOutputPinKeyCarriesAutoEmitSites(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	var findings []Finding
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil && bundle.RootSchema != nil {
		eventType := strings.TrimSpace(bundle.RootSchema.AutoEmitOnCreate.Event)
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, "", eventType) {
			if len(outputPinRequiredFields(pin)) == 0 {
				continue
			}
			findings = append(findings, outputPinKeyCarriesFinding("", pin, "auto_emit_payload_unproven", fmt.Sprintf("root auto_emit_on_create declares output pin %s event %s, but activation config payload cannot be statically proven for key/carries fields %s", pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
		}
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		eventType := strings.TrimSpace(scope.AutoEmitEvent)
		if flowID == "" || eventType == "" {
			continue
		}
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, flowID, eventType) {
			if len(outputPinRequiredFields(pin)) == 0 {
				continue
			}
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "auto_emit_payload_unproven", fmt.Sprintf("auto_emit_on_create declares output pin %s event %s, but activation config payload cannot be statically proven for key/carries fields %s", pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
		}
	}
	return findings
}

func validateOutputPinKeyCarriesTimerSites(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	var findings []Finding
	for _, timer := range source.WorkflowTimers() {
		flowID := strings.TrimSpace(timer.FlowID)
		eventType := strings.TrimSpace(timer.Event)
		if eventType == "" {
			continue
		}
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, flowID, eventType) {
			if len(outputPinRequiredFields(pin)) == 0 {
				continue
			}
			findings = append(findings, outputPinKeyCarriesFinding(flowID, pin, "timer_payload_unproven", fmt.Sprintf("workflow timer %s declares output pin %s event %s, but timer payload construction cannot be statically proven for key/carries fields %s", strings.TrimSpace(timer.ID), pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
		}
	}
	return findings
}

func validateOutputPinKeyCarriesNodeEmitSites(source semanticview.Source) []Finding {
	var findings []Finding
	seen := map[string]struct{}{}
	for _, site := range pinRoutingEmitSites(source) {
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, site.FlowID, site.Spec.EventType()) {
			for _, field := range outputPinRequiredFields(pin) {
				if _, ok := site.Spec.Fields[field]; ok {
					continue
				}
				key := site.ID + "\x00" + pin.PinName() + "\x00" + field
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, outputPinKeyCarriesFinding(site.FlowID, pin, "emit_payload_missing_key", fmt.Sprintf("node %s emit site %s emits output pin %s event %s but emit.fields does not statically prove carried field %s", site.NodeID, site.Site, pin.PinName(), pin.EventType(), field)))
			}
		}
	}
	return findings
}

func validateOutputPinKeyCarriesAgentEmitSites(source semanticview.Source) []Finding {
	var findings []Finding
	for _, site := range pinRoutingAgentEmitSites(source) {
		for _, pin := range outputPinKeyCarriesPinsForEvent(source, site.FlowID, site.EventType) {
			if len(outputPinRequiredFields(pin)) == 0 {
				continue
			}
			findings = append(findings, outputPinKeyCarriesFinding(site.FlowID, pin, "agent_emit_payload_unproven", fmt.Sprintf("agent %s emit_events declares output pin %s event %s, but agent emit_events has no static payload construction surface for key/carries fields %s", site.AgentID, pin.PinName(), pin.EventType(), strings.Join(outputPinRequiredFields(pin), ", "))))
		}
	}
	return findings
}

func outputPinKeyCarriesSourceType(source semanticview.Source, flowID string, pin runtimecontracts.FlowOutputEventPin, expr string) (string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", fmt.Errorf("source expression is required")
	}
	if !strings.HasPrefix(expr, "payload.") {
		return "", fmt.Errorf("source expression %q must reference the producer output pin key as payload.<key>", expr)
	}
	field := strings.TrimSpace(strings.TrimPrefix(expr, "payload."))
	if field == "" || strings.Contains(field, ".") {
		return "", fmt.Errorf("source expression %q must reference a single top-level payload field", expr)
	}
	key := strings.TrimSpace(pin.Key)
	if key == "" {
		return "", fmt.Errorf("producer output pin %s must declare key before it can supply %s", pin.PinName(), expr)
	}
	if field != key {
		return "", fmt.Errorf("source expression %s does not match producer output pin key %s; renamed-key adapters are not supported", expr, key)
	}
	carries := outputPinCarries(pin)
	if !outputPinStringSetContains(carries, key) {
		return "", fmt.Errorf("producer output pin %s must include key %s in carries", pin.PinName(), key)
	}
	typ, err := outputPinPayloadFieldType(source, flowID, pin.EventType(), field)
	if err != nil {
		return "", err
	}
	if !outputPinScalarType(typ) {
		return "", fmt.Errorf("producer output event %s payload field %s type %s is not a scalar key type", pin.EventType(), field, typ)
	}
	return typ, nil
}

func outputPinCarriedPayloadFieldType(source semanticview.Source, flowID string, pin runtimecontracts.FlowOutputEventPin, field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" || strings.Contains(field, ".") {
		return "", fmt.Errorf("producer output pin %s carried instance-key field %q must be a single top-level payload field", pin.PinName(), field)
	}
	key := strings.TrimSpace(pin.Key)
	if key == "" {
		return "", fmt.Errorf("producer output pin %s must declare key before it can supply payload.%s", pin.PinName(), field)
	}
	carries := outputPinCarries(pin)
	if !outputPinStringSetContains(carries, key) {
		return "", fmt.Errorf("producer output pin %s must include key %s in carries", pin.PinName(), key)
	}
	if !outputPinStringSetContains(carries, field) {
		return "", fmt.Errorf("producer output pin %s must include receiver instance.by field %s in carries", pin.PinName(), field)
	}
	typ, err := outputPinPayloadFieldType(source, flowID, pin.EventType(), field)
	if err != nil {
		return "", err
	}
	if !outputPinScalarType(typ) {
		return "", fmt.Errorf("producer output event %s payload field %s type %s is not a scalar key type", pin.EventType(), field, typ)
	}
	return typ, nil
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

func outputPinKeyCarriesAddressedConnects(source semanticview.Source) map[outputPinKeyCarriesIdentity]struct{} {
	out := map[outputPinKeyCarriesIdentity]struct{}{}
	if source == nil {
		return out
	}
	for _, connect := range source.CompositionConnects() {
		from, fromErr := connect.FromRef()
		to, toErr := connect.ToRef()
		if fromErr != nil || toErr != nil || to.Root {
			continue
		}
		inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin)
		if !ok || inputPin.Address == nil {
			continue
		}
		out[outputPinKeyCarriesIdentity{FlowID: strings.TrimSpace(from.FlowID), PinName: strings.TrimSpace(from.Pin)}] = struct{}{}
	}
	return out
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
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return nil
	}
	normalizedEvent := eventidentity.Normalize(eventType)
	resolvedEvent := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventType))
	var out []runtimecontracts.FlowOutputEventPin
	for _, pin := range source.FlowOutputEventPins(flowID) {
		pinEvent := eventidentity.Normalize(pin.EventType())
		pinResolved := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, pin.EventType()))
		if normalizedEvent == pinEvent || (resolvedEvent != "" && resolvedEvent == pinResolved) {
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
