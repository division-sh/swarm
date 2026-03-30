package tools

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
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
		provider := runtimeauthority.Active()
		missing := missingProducerEventSchemas(provider.ProducerRoles, provider.ProducerEventsForRole, activeSchemas)
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
	provider := runtimeauthority.Active()
	return GenerateEmitTools(
		role,
		provider.ProducerEventsForRole,
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

func GenerateEmitToolsForEvents(eventTypes []string, warn func(string, string, string, ...any)) []llm.ToolDefinition {
	ensureEventSchemaRegistry()
	allowed := UniqueNonEmpty(eventTypes)
	if len(allowed) == 0 {
		return nil
	}
	tools := make([]llm.ToolDefinition, 0, len(allowed))
	for _, eventType := range allowed {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		schema, ok := activeSchemas[eventType]
		if !ok {
			if warn != nil {
				warn(
					"emit-tool-missing-schema-"+eventType,
					"event-schema-registry",
					"skipping emit tool generation for %q because no explicit schema exists",
					eventType,
				)
			}
			continue
		}
		tools = append(tools, llm.ToolDefinition{
			Name:        EmitToolName(eventType),
			Description: schema.Description,
			Schema:      schema.Schema,
		})
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	return tools
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
	for _, evt := range runtimeauthority.Active().ProducerEventsForRole(role) {
		if strings.TrimSpace(evt) == eventType {
			return true
		}
	}
	return false
}

func IsEmitToolAllowedForConfig(raw json.RawMessage, toolName string) bool {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return false
	}
	for _, configured := range configuredEmitEvents(raw) {
		if strings.TrimSpace(configured) == eventType {
			return true
		}
	}
	return false
}

func eventTypeFromEmitToolName(toolName string) (string, bool) {
	ensureEventSchemaRegistry()
	return EventTypeFromEmitToolName(normalizeNativeToolName(toolName), emitToolToEvent)
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

func configuredEmitEvents(raw json.RawMessage) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	eventsRaw, ok := payload["emit_events"]
	if !ok {
		return nil
	}
	items, ok := eventsRaw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		eventType := strings.TrimSpace(asString(item))
		if eventType == "" {
			continue
		}
		out = append(out, eventType)
	}
	return UniqueNonEmpty(out)
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	return runtimebus.InboundEventFromContext(ctx)
}

type EmittedEventsRecorder = runtimebus.EmittedEventsRecorder

func EmittedEventsRecorderFromContext(ctx context.Context) (*EmittedEventsRecorder, bool) {
	return runtimebus.EmittedEventsRecorderFromContext(ctx)
}
