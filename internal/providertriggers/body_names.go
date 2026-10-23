package providertriggers

import (
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func qualifyTriggerBodyNames(root yamlsource.Value, definition *manifestDefinition) error {
	if err := qualifyTriggerBodyRawName(root, definition); err != nil {
		return err
	}
	if len(definition.NormalizedEvents) == 0 {
		return nil
	}
	field, err := root.Lookup("normalized_events")
	if err != nil {
		return err
	}
	rows, err := field.Value.Sequence()
	if err != nil {
		return err
	}
	for i := range definition.NormalizedEvents {
		event, err := rows[i].Lookup("event")
		if err != nil {
			return err
		}
		name, err := qualifyTriggerBodyEvent(event.Value, definition.Provider, definition.NormalizedEvents[i].Event)
		if err != nil {
			return err
		}
		definition.NormalizedEvents[i].Event = name
	}
	return nil
}

func qualifyTriggerBodyRawName(root yamlsource.Value, definition *manifestDefinition) error {
	eventName, err := root.Lookup("event_name")
	if err != nil {
		return err
	}
	if definition.EventName.Literal != "" {
		literal, err := eventName.Value.Lookup("literal")
		if err != nil {
			return err
		}
		name, err := qualifyTriggerBodyEvent(literal.Value, definition.Provider, definition.EventName.Literal)
		if err != nil {
			return err
		}
		definition.EventName.Literal = name
	}
	if definition.EventName.Template == "" {
		return nil
	}
	if definition.EventName.Template != "{event_type}" && definition.EventName.Template != "raw.{event_type}" {
		template, err := eventName.Value.Lookup("template")
		if err != nil {
			return err
		}
		return triggerSourceError(template.Value, "event_name.template must use the short {event_type} or raw.{event_type} form; the provider directory supplies inbound.<directory>.")
	}
	definition.EventName.Template = "inbound." + definition.Provider + "." + definition.EventName.Template
	return nil
}

func qualifyTriggerBodyEvent(value yamlsource.Value, key, local string) (string, error) {
	qualified, err := eventidentity.QualifyProviderEvent(key, local)
	if err != nil {
		return "", triggerSourceError(value, err.Error())
	}
	return qualified, nil
}
