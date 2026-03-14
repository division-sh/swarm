package contracts

import (
	"fmt"
	"strings"
	"sync"
)

type EventSchema struct {
	Description string
	Schema      map[string]any
}

var (
	activeRegistryMu sync.RWMutex
	activeRegistry   map[string]EventSchema
)

func EventSchemaRegistryFromCatalog(entries map[string]EventCatalogEntry) map[string]EventSchema {
	out := make(map[string]EventSchema, len(entries))
	for eventType, entry := range entries {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out[eventType] = eventSchemaFromCatalogEntry(eventType, entry)
	}
	return out
}

func eventSchemaFromCatalogEntry(eventType string, entry EventCatalogEntry) EventSchema {
	properties := make(map[string]any, len(entry.Payload.Properties))
	for fieldName, field := range entry.Payload.Properties {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			continue
		}
		prop := map[string]any{}
		if fieldType := strings.TrimSpace(field.Type); fieldType != "" {
			prop["type"] = fieldType
		}
		if description := strings.TrimSpace(field.Description); description != "" {
			prop["description"] = description
		}
		properties[fieldName] = prop
	}
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if required := normalizeStrings(entry.Required); len(required) > 0 {
		schema["required"] = required
	}
	return EventSchema{
		Description: fmt.Sprintf("Emit %s event", eventType),
		Schema:      schema,
	}
}

func SetActiveEventSchemaRegistry(registry map[string]EventSchema) {
	activeRegistryMu.Lock()
	defer activeRegistryMu.Unlock()
	activeRegistry = cloneEventSchemaRegistry(registry)
}

func EventSchemaRegistry() map[string]EventSchema {
	activeRegistryMu.RLock()
	defer activeRegistryMu.RUnlock()
	return cloneEventSchemaRegistry(activeRegistry)
}

func cloneEventSchemaRegistry(in map[string]EventSchema) map[string]EventSchema {
	if len(in) == 0 {
		return map[string]EventSchema{}
	}
	out := make(map[string]EventSchema, len(in))
	for eventType, schema := range in {
		out[strings.TrimSpace(eventType)] = EventSchema{
			Description: strings.TrimSpace(schema.Description),
			Schema:      cloneEventSchemaMap(schema.Schema),
		}
	}
	return out
}

func cloneEventSchemaMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneEventSchemaValue(value)
	}
	return out
}

func cloneEventSchemaValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneEventSchemaMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i := range typed {
			out[i] = cloneEventSchemaValue(typed[i])
		}
		return out
	default:
		return typed
	}
}
