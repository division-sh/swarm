package contracts

import (
	"sort"
	"strings"
)

// EventPayloadFields is derived from the generated schema registry rather than
// maintained as a second parallel artifact. It preserves a stable copy of the
// generated top-level payload property names for parity checks and emit schemas.
func EventPayloadFields(registry map[string]EventSchema) map[string][]string {
	return cloneEventPayloadFields(deriveEventPayloadFields(registry))
}

func deriveEventPayloadFields(registry map[string]EventSchema) map[string][]string {
	out := make(map[string][]string, len(registry))
	for eventType, entry := range registry {
		fields := payloadFieldNamesForSchema(entry.Schema)
		sort.Strings(fields)
		out[eventType] = fields
	}
	return out
}

func payloadFieldNamesForSchema(schema map[string]any) []string {
	if len(schema) == 0 {
		return nil
	}
	rawProps, ok := schema["properties"]
	if !ok || rawProps == nil {
		return nil
	}
	props, ok := rawProps.(map[string]any)
	if !ok {
		return nil
	}
	fields := make([]string, 0, len(props))
	for field := range props {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		fields = append(fields, field)
	}
	return fields
}

func cloneEventPayloadFields(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for eventType, fields := range in {
		out[eventType] = append([]string(nil), fields...)
	}
	return out
}
