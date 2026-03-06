package runtime

import (
	"sort"
	"strings"
	"sync"

	"empireai/internal/commgraph"
	llm "empireai/internal/runtime/llm"
	runtimetools "empireai/internal/runtime/tools"
)

var (
	emitToolIndexOnce sync.Once
	emitToolToEvent   map[string]string
	generatedSchemas  map[string]struct{}
)

type EventSchema = runtimetools.EmitSchema

func ValidateEventPayloadAgainstSchema(eventType string, payload map[string]any) error {
	s := schemaForEventType(eventType)
	return runtimetools.ValidatePayloadAgainstSchema(s.Schema, payload)
}

func ensureEventSchemaRegistry() {
	emitToolIndexOnce.Do(func() {
		generatedSchemas = make(map[string]struct{})
		for eventType, schema := range runtimetools.StrictDefaultEventSchemas {
			if _, ok := EventSchemaRegistry[eventType]; ok {
				continue
			}
			EventSchemaRegistry[eventType] = schema
		}
		missing := runtimetools.MissingProducerEventSchemas(commgraph.ProducerRoles, commgraph.ProducerEventsForRole, EventSchemaRegistry)
		if len(missing) > 0 {
			for _, eventType := range missing {
				generatedSchemas[eventType] = struct{}{}
			}
			runtimeWarnOnce(
				"event-schema-missing-explicit",
				"event-schema-registry",
				"missing explicit schemas for %d known produced events: %s",
				len(missing),
				summarizeLogList(missing, 20),
			)
		}
		runtimetools.EnsureSchemaContextFields(EventSchemaRegistry)
		runtimetools.EnsureSchemaPayloadParity(EventSchemaRegistry, contractEventPayloadFields)
		emitToolToEvent = make(map[string]string, len(EventSchemaRegistry))
		for eventType := range EventSchemaRegistry {
			emitToolToEvent[runtimetools.EmitToolName(eventType)] = eventType
		}
	})
}

func GenerateEmitTools(role string) []llm.ToolDefinition {
	ensureEventSchemaRegistry()
	return runtimetools.GenerateEmitTools(
		role,
		commgraph.ProducerEventsForRole,
		func(eventType string) (runtimetools.EmitSchema, bool) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				return runtimetools.EmitSchema{}, false
			}
			if _, ok := EventSchemaRegistry[eventType]; !ok {
				return runtimetools.EmitSchema{}, false
			}
			schema := schemaForEventType(eventType)
			return runtimetools.EmitSchema{Description: schema.Description, Schema: schema.Schema}, true
		},
		runtimeWarnOnce,
	)
}

// GeneratedEmitSchemas returns event types that currently rely on permissive,
// auto-generated schemas rather than explicit spec-authored definitions.
func GeneratedEmitSchemas() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, len(generatedSchemas))
	for eventType := range generatedSchemas {
		out = append(out, eventType)
	}
	sort.Strings(out)
	return out
}

// GeneratedEmitSchemasForAgentRoles returns generated/permissive schemas that
// are reachable through at least one agent role's emit tool allowlist.
func GeneratedEmitSchemasForAgentRoles() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, 64)
	seen := make(map[string]struct{}, 128)
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			if _, ok := generatedSchemas[eventType]; !ok {
				continue
			}
			if _, dup := seen[eventType]; dup {
				continue
			}
			seen[eventType] = struct{}{}
			out = append(out, eventType)
		}
	}
	sort.Strings(out)
	return out
}

func IsEmitToolAllowedForRole(role, toolName string) bool {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return false
	}
	for _, evt := range commgraph.ProducerEventsForRole(role) {
		if strings.TrimSpace(evt) == eventType {
			return true
		}
	}
	return false
}

// EventSchemaSnapshot returns a copy of the current event schema registry.
// Used by diagnostics and dashboard tooling.
func EventSchemaSnapshot() map[string]EventSchema {
	ensureEventSchemaRegistry()
	return runtimetools.SnapshotEmitSchemas(EventSchemaRegistry)
}

func eventTypeFromEmitToolName(toolName string) (string, bool) {
	ensureEventSchemaRegistry()
	return runtimetools.EventTypeFromEmitToolName(toolName, emitToolToEvent)
}

func emitToolName(eventType string) string {
	return runtimetools.EmitToolName(eventType)
}

func schemaForEventType(eventType string) EventSchema {
	ensureEventSchemaRegistry()
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		runtimeWarnOnce("schema-for-empty-event-type", "event-schema-registry", "schema requested for empty event type")
		return EventSchema{
			Description: "Emit event",
			Schema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		}
	}
	if s, ok := EventSchemaRegistry[eventType]; ok {
		return s
	}
	runtimeWarnOnce(
		"schema-for-unknown-"+eventType,
		"event-schema-registry",
		"schema requested for unknown event type %q; returning strict defensive empty schema",
		eventType,
	)
	return EventSchema{
		Description: "Emit " + eventType + " event",
		Schema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}
}

func uniqueNonEmpty(values []string) []string {
	return runtimetools.UniqueNonEmpty(values)
}
