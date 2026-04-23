package tools

import (
	"sort"
	"strings"

	runtimeauthority "swarm/internal/runtime/authority"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

type EmitRegistry struct {
	source           semanticview.Source
	provider         runtimeauthority.Provider
	activeSchemas    map[string]EmitSchema
	generatedSchemas map[string]struct{}
	toolToEvent      map[string]string
}

func NewEmitRegistry(source semanticview.Source, provider runtimeauthority.Provider) *EmitRegistry {
	provider = runtimeauthority.ProviderOrNoop(provider)
	catalog := map[string]runtimecontracts.EventCatalogEntry{}
	activeSchemas := map[string]EmitSchema{}
	if source != nil {
		catalog = source.ResolvedEventCatalog()
		if bundle, ok := semanticview.Bundle(source); ok && bundle != nil {
			activeSchemas = runtimecontracts.EventSchemaRegistryFromBundle(bundle)
		}
	}
	if len(activeSchemas) == 0 {
		activeSchemas = runtimecontracts.EventSchemaRegistryFromCatalog(catalog)
	}
	generatedSchemas := make(map[string]struct{})
	if source != nil {
		for _, entry := range source.AgentEntries() {
			for _, eventType := range entry.EmitEvents {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if _, ok := emitSchemaForEventType(activeSchemas, eventType); ok {
					continue
				}
				generatedSchemas[eventType] = struct{}{}
			}
		}
	} else {
		missing := missingProducerEventSchemas(provider.ProducerRoles, provider.ProducerEventsForRole, activeSchemas)
		for _, eventType := range missing {
			generatedSchemas[eventType] = struct{}{}
		}
	}

	toolToEvent := make(map[string]string, len(activeSchemas))
	for eventType := range activeSchemas {
		toolToEvent[EmitToolName(eventType)] = eventType
	}
	if source != nil {
		for _, entry := range source.AgentEntries() {
			for _, eventType := range entry.EmitEvents {
				eventType = strings.TrimSpace(eventType)
				if eventType == "" {
					continue
				}
				if _, ok := emitSchemaForEventType(activeSchemas, eventType); !ok {
					continue
				}
				toolToEvent[EmitToolName(eventType)] = eventType
			}
		}
	}

	return &EmitRegistry{
		source:           source,
		provider:         provider,
		activeSchemas:    activeSchemas,
		generatedSchemas: generatedSchemas,
		toolToEvent:      toolToEvent,
	}
}

func (r *EmitRegistry) GenerateEmitToolsForRole(role string, warn func(string, string, string, ...any)) []llm.ToolDefinition {
	if r == nil {
		return nil
	}
	return GenerateEmitTools(
		role,
		r.provider.ProducerEventsForRole,
		func(eventType string) (EmitSchema, bool) {
			return emitSchemaForEventType(r.activeSchemas, eventType)
		},
		warn,
	)
}

func (r *EmitRegistry) GenerateEmitToolsForEvents(eventTypes []string, warn func(string, string, string, ...any)) []llm.ToolDefinition {
	if r == nil {
		return nil
	}
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
		schema, ok := emitSchemaForEventType(r.activeSchemas, eventType)
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

func (r *EmitRegistry) GenerateEmitToolsForActor(actor models.AgentConfig, warn func(string, string, string, ...any)) []llm.ToolDefinition {
	if r == nil {
		return nil
	}
	if configured := UniqueNonEmpty(actor.EmitEvents); len(configured) > 0 {
		tools := make([]llm.ToolDefinition, 0, len(configured))
		for _, eventType := range configured {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			schema, ok := r.schemaForActorEvent(actor, eventType)
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
	return r.GenerateEmitToolsForRole(actor.Role, warn)
}

func (r *EmitRegistry) schemaForActorEvent(actor models.AgentConfig, eventType string) (EmitSchema, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return EmitSchema{}, false
	}
	if schema, ok := emitSchemaForEventType(r.activeSchemas, eventType); ok {
		return schema, true
	}
	if r.source == nil {
		return EmitSchema{}, false
	}
	flowID := strings.TrimSpace(actor.Mode)
	if flowID == "" {
		return EmitSchema{}, false
	}
	proof := semanticview.ResolveFlowEventProof(r.source, flowID, eventType)
	if !proof.HasSchema {
		return EmitSchema{}, false
	}
	if bundle, ok := semanticview.Bundle(r.source); ok && bundle != nil {
		if schema, _, ok := runtimecontracts.EventSchemaForFlowEvent(bundle, flowID, eventType); ok {
			return schema, true
		}
	}
	registry := runtimecontracts.EventSchemaRegistryFromCatalog(map[string]runtimecontracts.EventCatalogEntry{
		proof.CatalogKey: proof.Entry,
	})
	schema, ok := registry[proof.CatalogKey]
	return schema, ok
}

func (r *EmitRegistry) GeneratedEmitSchemasForAgentRoles() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.generatedSchemas))
	for eventType := range r.generatedSchemas {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out = append(out, eventType)
	}
	sort.Strings(out)
	return out
}

func (r *EmitRegistry) EventSchemaSnapshot() map[string]EmitSchema {
	if r == nil {
		return map[string]EmitSchema{}
	}
	return SnapshotEmitSchemas(r.activeSchemas)
}

func (r *EmitRegistry) ValidateEventPayloadAgainstSchema(eventType string, payload map[string]any) error {
	return ValidatePayloadAgainstSchema(r.SchemaForEventType(eventType).Schema, payload)
}

func (r *EmitRegistry) IsEmitToolAllowedForRole(role, toolName string) bool {
	if r == nil {
		return false
	}
	eventType, ok := r.EventTypeFromToolName(toolName)
	if !ok {
		return false
	}
	for _, evt := range r.provider.ProducerEventsForRole(role) {
		if emitEventTypesEquivalent(evt, eventType) {
			return true
		}
	}
	return false
}

func (r *EmitRegistry) IsEmitToolAllowedForActor(actor models.AgentConfig, toolName string) bool {
	if r == nil {
		return false
	}
	eventType, ok := r.EventTypeFromToolName(toolName)
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

func (r *EmitRegistry) EventTypeFromToolName(toolName string) (string, bool) {
	if r == nil {
		return "", false
	}
	return EventTypeFromEmitToolName(normalizeNativeToolName(toolName), r.toolToEvent)
}

func (r *EmitRegistry) SchemaForEventType(eventType string) EmitSchema {
	if schema, ok := emitSchemaForEventType(r.activeSchemas, eventType); ok {
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

func emitSchemaForEventType(activeSchemas map[string]EmitSchema, eventType string) (EmitSchema, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return EmitSchema{}, false
	}
	if schema, ok := activeSchemas[eventType]; ok {
		return schema, true
	}
	local := localEmitEventType(eventType)
	if local == "" {
		return EmitSchema{}, false
	}
	if local != eventType {
		schema, ok := activeSchemas[local]
		if ok {
			return schema, true
		}
	}
	var matched EmitSchema
	matchCount := 0
	for schemaEventType, schema := range activeSchemas {
		if localEmitEventType(schemaEventType) != local {
			continue
		}
		matched = schema
		matchCount++
		if matchCount > 1 {
			return EmitSchema{}, false
		}
	}
	if matchCount == 1 {
		return matched, true
	}
	return EmitSchema{}, false
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
