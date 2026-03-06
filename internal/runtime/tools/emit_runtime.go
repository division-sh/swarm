package tools

import (
	"context"
	"strings"
	"sync"

	"empireai/internal/commgraph"
	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
)

var (
	emitToolIndexOnce sync.Once
	emitToolToEvent   map[string]string
	activeSchemas     map[string]EmitSchema
)

func ensureEventSchemaRegistry() {
	emitToolIndexOnce.Do(func() {
		activeSchemas = runtimecontracts.EventSchemaRegistry()
		emitToolToEvent = make(map[string]string, len(activeSchemas))
		for eventType := range activeSchemas {
			emitToolToEvent[EmitToolName(eventType)] = eventType
		}
	})
}

func ValidateEventPayloadAgainstSchema(eventType string, payload map[string]any) error {
	s := schemaForEventType(eventType)
	return ValidatePayloadAgainstSchema(s.Schema, payload)
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

func eventTypeFromEmitToolName(toolName string) (string, bool) {
	ensureEventSchemaRegistry()
	return EventTypeFromEmitToolName(toolName, emitToolToEvent)
}

func schemaForEventType(eventType string) EmitSchema {
	ensureEventSchemaRegistry()
	eventType = strings.TrimSpace(eventType)
	if schema, ok := activeSchemas[eventType]; ok {
		return schema
	}
	return EmitSchema{
		Description: "Emit " + eventType + " event",
		Schema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	return runtimebus.InboundEventFromContext(ctx)
}

type EmittedEventsRecorder = runtimebus.EmittedEventsRecorder

func EmittedEventsRecorderFromContext(ctx context.Context) (*EmittedEventsRecorder, bool) {
	return runtimebus.EmittedEventsRecorderFromContext(ctx)
}
