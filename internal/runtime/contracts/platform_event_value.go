package contracts

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const retiredPlatformRequired = "platform event required lists are retired; fields are required by default and optional fields use one trailing ? on their type"

func admitPlatformEventCatalogValue(value yamlsource.Value) (map[string]EventCatalogEntry, error) {
	f, err := platformValueFields(value)
	if err != nil {
		return nil, err
	}
	out := make(map[string]EventCatalogEntry, len(f))
	for _, name := range sortedContractKeys(f) {
		v := f[name]
		members, err := platformValueFields(v)
		if err != nil {
			return nil, err
		}
		if required, ok := members["required"]; ok {
			return nil, nodeValueError(required, fmt.Errorf("%s", retiredPlatformRequired))
		}
		entry := EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{}}}
		if payload, ok := members["payload"]; ok {
			entry.Payload.Properties, entry.Payload.Required, err = admitPlatformPayloadValue(payload)
			if err != nil {
				return nil, err
			}
		}
		key := eventidentity.Normalize(name)
		if key == "" {
			return nil, nodeValueError(v, fmt.Errorf("platform event identity is required"))
		}
		if _, duplicate := out[key]; duplicate {
			return nil, nodeValueError(v, fmt.Errorf("duplicate normalized platform event %s", key))
		}
		out[key] = entry
	}
	return out, nil
}

func admitPlatformPayloadValue(value yamlsource.Value) (map[string]EventFieldSpec, []string, error) {
	f, err := platformValueFields(value)
	if err != nil {
		return nil, nil, err
	}
	if v, ok := f["required"]; ok {
		return nil, nil, nodeValueError(v, fmt.Errorf("%s", retiredPlatformRequired))
	}
	out := make(map[string]EventFieldSpec, len(f))
	var required []string
	for _, name := range sortedContractKeys(f) {
		if name == "properties" {
			continue
		}
		field, optional, err := admitPlatformEventFieldValue(f[name])
		if err != nil {
			return nil, nil, err
		}
		out[name] = field
		if !optional {
			required = append(required, name)
		}
	}
	if properties, ok := f["properties"]; ok {
		children, childRequired, err := admitPlatformPayloadValue(properties)
		if err != nil {
			return nil, nil, err
		}
		for name, field := range children {
			if _, duplicate := out[name]; duplicate {
				return nil, nil, nodeValueError(properties, fmt.Errorf("duplicate platform payload field %s", name))
			}
			out[name] = field
		}
		required = append(required, childRequired...)
	}
	sort.Strings(required)
	return out, required, nil
}

func admitPlatformEventFieldValue(value yamlsource.Value) (EventFieldSpec, bool, error) {
	if value.Presence() == yamlsource.PresenceScalar {
		text, err := schemaValueText(value, true)
		if err != nil {
			return EventFieldSpec{}, false, err
		}
		typeName, optional, err := admitEventFieldTypeMarker(text, value.SemanticPath())
		if err != nil {
			return EventFieldSpec{}, false, nodeValueError(value, err)
		}
		return EventFieldSpec{Type: normalizePlatformEventFieldType(typeName)}, optional, nil
	}
	f, err := platformValueFields(value)
	if err != nil {
		return EventFieldSpec{}, false, err
	}
	if _, ok := f["type"]; ok {
		f, err = platformValueFields(value, "type", "description")
		if err != nil {
			return EventFieldSpec{}, false, err
		}
		var field EventFieldSpec
		if err = schemaValueRequiredTexts(value, f, map[string]*string{"type": &field.Type}); err != nil {
			return field, false, err
		}
		if err = schemaValueTexts(f, map[string]*string{"description": &field.Description}, false); err != nil {
			return field, false, err
		}
		typeName, optional, err := admitEventFieldTypeMarker(field.Type, value.SemanticPath())
		field.Type = normalizePlatformEventFieldType(typeName)
		if err != nil {
			return field, false, nodeValueError(value, err)
		}
		return field, optional, nil
	}
	properties, required, err := admitPlatformPayloadValue(value)
	if err != nil {
		return EventFieldSpec{}, false, err
	}
	raw := make(map[string]any, len(properties))
	for name, field := range properties {
		raw[name] = platformEventFieldJSONSchema(field)
	}
	exact, err := AdmitToolInputSchemaMap(map[string]any{"type": "object", "properties": raw, "required": required, "additionalProperties": false})
	if err != nil {
		return EventFieldSpec{}, false, nodeValueError(value, err)
	}
	return EventFieldSpec{Type: "object", ExactSchema: &exact}, false, nil
}
