package tools

import (
	"context"
	"sort"
	"strings"
	"sync"

	"empireai/internal/commgraph"
	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/semanticview"
)

var (
	emitRegistryMu    sync.RWMutex
	emitToolToEvent   map[string]string
	activeSchemas     map[string]EmitSchema
	generatedSchemas  map[string]struct{}
	emitRegistryReady bool
)

func InitEventSchemaRegistry(source semanticview.Source) {
	emitRegistryMu.Lock()
	defer emitRegistryMu.Unlock()

	var catalog map[string]runtimecontracts.EventCatalogEntry
	if source != nil {
		catalog = source.EventEntries()
	}
	activeSchemas = runtimecontracts.EventSchemaRegistryFromCatalog(catalog)
	runtimecontracts.SetActiveEventSchemaRegistry(activeSchemas)
	generatedSchemas = make(map[string]struct{})
	if source != nil {
		for _, entry := range source.AgentEntries() {
			for _, eventType := range entry.EmitEvents {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if _, ok := activeSchemas[eventType]; ok {
					continue
				}
				generatedSchemas[eventType] = struct{}{}
			}
		}
	} else {
		missing := missingProducerEventSchemas(commgraph.ProducerRoles, commgraph.ProducerEventsForRole, activeSchemas)
		for _, eventType := range missing {
			generatedSchemas[eventType] = struct{}{}
		}
	}
	emitToolToEvent = make(map[string]string, len(activeSchemas))
	for eventType := range activeSchemas {
		emitToolToEvent[EmitToolName(eventType)] = eventType
	}
	emitRegistryReady = true
}

func ensureEventSchemaRegistry() {
	emitRegistryMu.RLock()
	ready := emitRegistryReady
	emitRegistryMu.RUnlock()
	if ready {
		return
	}
	InitEventSchemaRegistry(nil)
}

func missingProducerEventSchemas(producerRoles func() []string, producerEvents func(string) []string, registry map[string]EmitSchema) []string {
	missing := make([]string, 0, 16)
	for _, role := range producerRoles() {
		for _, eventType := range producerEvents(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := registry[eventType]; ok {
				continue
			}
			missing = append(missing, eventType)
		}
	}
	return UniqueNonEmpty(missing)
}

func GenerateEmitToolsForRole(role string, warn func(string, string, string, ...any)) []llm.ToolDefinition {
	ensureEventSchemaRegistry()
	return GenerateEmitTools(
		role,
		commgraph.ProducerEventsForRole,
		func(eventType string) (EmitSchema, bool) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				return EmitSchema{}, false
			}
			schema, ok := activeSchemas[eventType]
			if !ok {
				return EmitSchema{}, false
			}
			return schema, true
		},
		warn,
	)
}

func GeneratedEmitSchemasForAgentRoles() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, len(generatedSchemas))
	for eventType := range generatedSchemas {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out = append(out, eventType)
	}
	sort.Strings(out)
	return out
}

func EventSchemaSnapshot() map[string]EmitSchema {
	ensureEventSchemaRegistry()
	return SnapshotEmitSchemas(activeSchemas)
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
