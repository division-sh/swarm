package contracts

import (
	"fmt"
	"sort"
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
		if err := schemaValueTexts(fields, map[string]*string{"rename": &row.Rename}, true); err != nil {
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
			return nodeValueError(resolution, fmt.Errorf("connect.resolution must be create, select or select-or-create; reply and fan-out remain input-pin policies"))
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
		key := "reads"
		if direction == "outputs" {
			key = "writes"
		}
		members, err := schemaValueFields(fields[direction], "pins."+direction, map[string]struct{}{"events": {}, key: {}}, true)
		if err != nil {
			return out, err
		}
		if names, present := members[key]; present {
			list, err := projectSchemaPinFieldsValue(names)
			if err != nil {
				return out, err
			}
			if direction == "inputs" {
				out.Inputs.Reads = list
			} else {
				out.Outputs.Writes = list
			}
		}
		if events, present := members["events"]; present {
			items, err := schemaValueSequence(events, true)
			if err != nil {
				return out, err
			}
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

func projectSchemaPinFieldsValue(value yamlsource.Value) ([]string, error) {
	items, err := schemaValueSequence(value, true)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		text, err := schemaValueText(item, true)
		if err != nil {
			return nil, err
		}
		if seen[text] {
			return nil, nodeValueError(item, fmt.Errorf("field %q is declared more than once", text))
		}
		seen[text] = true
		out = append(out, text)
	}
	sort.Strings(out)
	return out, nil
}

func projectSchemaPinValue(value yamlsource.Value, direction string) (FlowInputEventPin, FlowOutputEventPin, error) {
	input := FlowInputEventPin{sourceLine: value.Location().Line, sourceCol: value.Location().Column}
	output := FlowOutputEventPin{sourceLine: value.Location().Line, sourceCol: value.Location().Column}
	var event string
	var err error
	if value.Presence() == yamlsource.PresenceScalar {
		event, err = schemaValueText(value, true)
	} else {
		allowed := inputEventPinFieldOptions
		if direction == "outputs" {
			allowed = outputEventPinFieldOptions
		}
		fields, fieldErr := schemaValueFields(value, direction+" event pin", allowed, true)
		if fieldErr != nil {
			return input, output, fieldErr
		}
		if err = schemaValueRequiredTexts(value, fields, map[string]*string{"event": &event}); err != nil {
			return input, output, err
		}
		if source, present := fields["source"]; present {
			var text string
			text, err = schemaValueText(source, true)
			if err == nil {
				input.Source, err = ParseFlowInputPinSource(text)
			}
		}
		if err == nil {
			if sink, present := fields["sink"]; present {
				var text string
				text, err = schemaValueText(sink, true)
				if err == nil {
					output.Sink, err = ParseFlowOutputSink(text)
				}
			}
		}
		if err == nil {
			if resolution, present := fields["resolution"]; present {
				input.Resolution, err = projectSchemaResolutionValue(resolution)
			}
		}
		if err == nil {
			if initialize, present := fields["initialize"]; present {
				input.Initialize, err = projectSchemaInitializeValue(initialize)
			}
		}
		if err == nil && direction == "inputs" && input.Source.Empty() && input.Resolution.Empty() && len(input.Initialize) == 0 {
			err = fmt.Errorf("input event pin mapping requires a non-default source, resolution or initialize; use a scalar event when no options are needed")
		}
		if err == nil && direction == "outputs" && output.Sink == FlowOutputSinkNone {
			err = fmt.Errorf("output event pin mapping requires a non-default sink; use a scalar event when no options are needed")
		}
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

func projectSchemaResolutionValue(value yamlsource.Value) (FlowInputPinResolution, error) {
	fields, err := schemaValueFields(value, "input pin resolution", inputEventPinResolutionFieldOptions, true)
	var out FlowInputPinResolution
	if err != nil {
		return out, err
	}
	var mode string
	if err := schemaValueRequiredTexts(value, fields, map[string]*string{"mode": &mode}); err != nil {
		return out, err
	}
	out.Mode, err = ParseFlowInputResolutionMode(mode)
	if err != nil {
		return out, nodeValueError(value, err)
	}
	if err := schemaValueTexts(fields, map[string]*string{
		"replies_to": &out.RepliesTo, "correlation_key": &out.CorrelationKey,
	}, true); err != nil {
		return out, err
	}
	if err := validateCompiledFlowInputResolution(out); err != nil {
		return out, nodeValueError(value, err)
	}
	return out, nil
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
