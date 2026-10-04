package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectSchemaConnectValue(value yamlsource.Value) ([]FlowConnect, error) {
	items, err := schemaValueSequence(value, false)
	if err != nil {
		return nil, err
	}
	out := make([]FlowConnect, 0, len(items))
	for _, item := range items {
		fields, err := schemaValueFields(item, "connect", flowConnectFieldOptions, true)
		if err != nil {
			return nil, err
		}
		row := FlowConnect{SourceLine: item.Location().Line}
		if _, present := fields["event"]; !present {
			return nil, nodeValueError(item, fmt.Errorf("endpoint-centric connect rows are unsupported; declare event, from and to"))
		}
		for _, key := range []string{"from", "to"} {
			if _, present := fields[key]; !present {
				return nil, nodeValueError(item, fmt.Errorf("connect requires non-empty event, from, and to"))
			}
		}
		if err := schemaValueRequiredTexts(item, fields, map[string]*string{"event": &row.Event, "from": &row.From, "to": &row.To}); err != nil {
			return nil, err
		}
		if err := schemaValueTexts(fields, map[string]*string{
			"rename": &row.Rename, "replies_to": &row.RepliesTo, "correlation_key": &row.CorrelationKey,
		}, true); err != nil {
			return nil, err
		}
		if !eventidentity.IsValidName(row.Event) || row.Rename != "" && !eventidentity.IsValidName(row.Rename) {
			return nil, nodeValueError(item, fmt.Errorf("connect event/rename must be an exact canonical event identity"))
		}
		if row.Rename != "" && eventidentity.Normalize(row.Rename) == eventidentity.Normalize(row.Event) {
			return nil, nodeValueError(item, fmt.Errorf("connect.rename is redundant with event"))
		}
		if err := projectSchemaConnectResolutionValue(fields, &row); err != nil {
			return nil, err
		}
		if err := validateAuthoredConnectReply(row); err != nil {
			return nil, nodeValueError(item, err)
		}
		out = append(out, row)
	}
	return out, nil
}

func projectSchemaConnectResolutionValue(fields map[string]yamlsource.Value, row *FlowConnect) error {
	if resolution, present := fields["resolution"]; present {
		text, err := schemaValueText(resolution, true)
		if err != nil {
			return err
		}
		row.Resolution, err = ParseFlowInputResolutionMode(text)
		if err != nil || !ordinaryInstanceResolution(row.Resolution) {
			return nodeValueError(resolution, fmt.Errorf("connect.resolution must be create, select or select-or-create"))
		}
	}
	key, present := fields["key_from"]
	if !present {
		return nil
	}
	var err error
	row.KeyFrom, err = schemaValueText(key, true)
	if err != nil {
		return err
	}
	if !ordinaryInstanceResolution(row.Resolution) {
		return nodeValueError(key, fmt.Errorf("connect.key_from requires an explicit connect.resolution"))
	}
	if _, err := ResolveFlowInputInstanceSource(row.Resolution, row.KeyFrom); err != nil {
		return nodeValueError(key, err)
	}
	return nil
}

func validateAuthoredConnectReply(row FlowConnect) error {
	if row.RepliesTo == "" {
		if row.CorrelationKey != "" {
			return fmt.Errorf("connect.correlation_key requires replies_to")
		}
		return nil
	}
	if !eventidentity.IsValidName(row.RepliesTo) || strings.ContainsAny(row.RepliesTo, "/*") {
		return fmt.Errorf("connect.replies_to must be an exact local event identity")
	}
	if row.Resolution != FlowInputResolutionModeNone || row.KeyFrom != "" {
		return fmt.Errorf("connect.replies_to cannot declare resolution or key_from")
	}
	if row.CorrelationKey != "" && strings.ContainsAny(row.CorrelationKey, "./*") {
		return fmt.Errorf("connect.correlation_key must name a payload field")
	}
	return nil
}

func ordinaryInstanceResolution(mode FlowInputResolutionMode) bool {
	return mode == FlowInputResolutionModeCreate || mode == FlowInputResolutionModeSelect || mode == FlowInputResolutionModeSelectOrCreate
}

func projectSchemaImportsValue(value yamlsource.Value) (FlowSchemaImports, error) {
	fields, err := schemaValueFields(value, "imports", map[string]struct{}{"connector_packs": {}, "provider_trigger_events": {}}, true)
	var out FlowSchemaImports
	if err != nil {
		return out, err
	}
	for _, family := range sortedContractKeys(fields) {
		items, err := schemaValueSequence(fields[family], true)
		if err != nil {
			return out, err
		}
		for _, item := range items {
			key := "tool"
			if family == "provider_trigger_events" {
				key = "event"
			}
			row, err := schemaValueFields(item, family+" import", map[string]struct{}{"provider": {}, key: {}}, true)
			if err != nil {
				return out, err
			}
			var provider, name string
			if err := schemaValueRequiredTexts(item, row, map[string]*string{"provider": &provider, key: &name}); err != nil {
				return out, err
			}
			if family == "connector_packs" {
				entry := ConnectorPackImport{Provider: provider, Tool: name}
				if normalized := entry.normalized(); normalized != entry {
					return out, nodeValueError(item, fmt.Errorf("connector import must use exact canonical tokens"))
				}
				out.ConnectorPacks = append(out.ConnectorPacks, entry)
			} else {
				entry := ProviderTriggerEventImport{Provider: provider, Event: name}
				if normalized := entry.normalized(); normalized != entry {
					return out, nodeValueError(item, fmt.Errorf("provider trigger import must use exact canonical tokens"))
				}
				out.ProviderTriggerEvents = append(out.ProviderTriggerEvents, entry)
			}
		}
	}
	return out, nil
}

func projectSchemaPinsValue(value yamlsource.Value) (FlowPins, error) {
	fields, err := schemaValueFields(value, "pins", map[string]struct{}{"inputs": {}, "outputs": {}}, true)
	var out FlowPins
	if err != nil {
		return out, err
	}
	for _, direction := range sortedContractKeys(fields) {
		items, err := schemaValueSequence(fields[direction], true)
		if err != nil {
			return out, err
		}
		{
			seen := map[string]bool{}
			for _, item := range items {
				input, output, err := projectSchemaPinValue(item, direction)
				if err != nil {
					return out, err
				}
				event := input.Event
				if direction == "outputs" {
					event = output.Event
				}
				if seen[event] {
					return out, nodeValueError(item, fmt.Errorf("pin event %q is declared more than once", event))
				}
				seen[event] = true
				if direction == "inputs" {
					out.Inputs.EventPins = append(out.Inputs.EventPins, input)
				} else {
					out.Outputs.EventPins = append(out.Outputs.EventPins, output)
				}
			}
		}
	}
	return out, nil
}

func projectSchemaPinValue(value yamlsource.Value, direction string) (FlowInputEventPin, FlowOutputEventPin, error) {
	input := FlowInputEventPin{sourceLine: value.Location().Line, sourceCol: value.Location().Column}
	output := FlowOutputEventPin{sourceLine: value.Location().Line, sourceCol: value.Location().Column}
	var event string
	var err error
	if value.Presence() == yamlsource.PresenceScalar {
		event, err = schemaValueText(value, true)
	} else if direction == "inputs" {
		fields, fieldErr := schemaValueFields(value, direction+" event pin", inputEventPinFieldOptions, true)
		if fieldErr != nil {
			return input, output, fieldErr
		}
		if err = schemaValueRequiredTexts(value, fields, map[string]*string{"event": &event}); err != nil {
			return input, output, err
		}
		if err == nil {
			if initialize, present := fields["initialize"]; present {
				input.Initialize, err = projectSchemaInitializeValue(initialize)
			}
		}
		if err == nil && len(input.Initialize) == 0 {
			err = fmt.Errorf("input event pin mapping requires non-empty initialize")
		}
	} else {
		_, err = schemaValueText(value, true)
	}
	if err != nil {
		return input, output, nodeValueError(value, err)
	}
	if !eventidentity.IsValidName(event) || strings.ContainsAny(event, "/*") {
		return input, output, nodeValueError(value, fmt.Errorf("pin event %q must be an exact local canonical event identity", event))
	}
	input.Event, output.Event = event, event
	if direction == "inputs" {
		err = validateAuthoredFlowInputPin(input)
	} else {
		err = validateAuthoredFlowOutputPin(output)
	}
	if err != nil {
		return input, output, nodeValueError(value, err)
	}
	return input, output, nil
}

func projectSchemaInitializeValue(value yamlsource.Value) (map[string]string, error) {
	fields, err := schemaValueDeclarations(value, true)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, field := range fields {
		path, err := schemaValueText(field.Value, true)
		if err != nil {
			return nil, err
		}
		if _, err := receiverPayloadPath(path); err != nil {
			return nil, nodeValueError(field.Value, err)
		}
		out[field.Name] = path
	}
	return out, nil
}
