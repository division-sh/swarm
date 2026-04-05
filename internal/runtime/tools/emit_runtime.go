package tools

import (
	"context"
	"sort"
	"strings"
	"sync"

	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
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
		catalog = source.ResolvedEventCatalog()
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
				if _, ok := emitSchemaForEventTypeLocal(eventType); ok {
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
	if source != nil {
		for _, entry := range source.AgentEntries() {
			for _, eventType := range entry.EmitEvents {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if _, ok := emitSchemaForEventTypeLocal(eventType); !ok {
					continue
				}
				emitToolToEvent[EmitToolName(eventType)] = eventType
			}
		}
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
			return emitSchemaForEventTypeLocal(eventType)
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
		schema, ok := emitSchemaForEventTypeLocal(eventType)
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

func GenerateEmitToolsForActor(actor models.AgentConfig, warn func(string, string, string, ...any)) []llm.ToolDefinition {
	if configured := UniqueNonEmpty(actor.EmitEvents); len(configured) > 0 {
		return GenerateEmitToolsForEvents(configured, warn)
	}
	return GenerateEmitToolsForRole(actor.Role, warn)
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
		if emitEventTypesEquivalent(evt, eventType) {
			return true
		}
	}
	return false
}

func IsEmitToolAllowedForActor(actor models.AgentConfig, toolName string) bool {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return false
	}
	for _, configured := range actor.EmitEvents {
		if emitEventTypesEquivalent(configured, eventType) {
			return true
		}
	}
	return false
}

func eventTypeFromEmitToolName(toolName string) (string, bool) {
	ensureEventSchemaRegistry()
	return EventTypeFromEmitToolName(normalizeNativeToolName(toolName), emitToolToEvent)
}

func emitEventTypesEquivalent(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	if left == right {
		return true
	}
	return localEmitEventType(left) == localEmitEventType(right)
}

func schemaForEventType(eventType string) EmitSchema {
	ensureEventSchemaRegistry()
	if schema, ok := emitSchemaForEventTypeLocal(eventType); ok {
		return schema
	}
	eventType = strings.TrimSpace(eventType)
	return EmitSchema{
		Description: "Emit " + eventType + " event",
		Schema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}
}

func emitSchemaForEventTypeLocal(eventType string) (EmitSchema, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return EmitSchema{}, false
	}
	if schema, ok := activeSchemas[eventType]; ok {
		return schema, true
	}
	local := localEmitEventType(eventType)
	if local == "" || local == eventType {
		return EmitSchema{}, false
	}
	schema, ok := activeSchemas[local]
	return schema, ok
}

func InboundEventFromContext(ctx context.Context) (events.Event, bool) {
	return runtimebus.InboundEventFromContext(ctx)
}

type EmittedEventsRecorder = runtimebus.EmittedEventsRecorder

func EmittedEventsRecorderFromContext(ctx context.Context) (*EmittedEventsRecorder, bool) {
	return runtimebus.EmittedEventsRecorderFromContext(ctx)
}
