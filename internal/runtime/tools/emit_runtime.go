package tools

import (
	"fmt"
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
	collisions := duplicateEmitToolNames(allowed)
	tools := make([]llm.ToolDefinition, 0, len(allowed))
	for _, eventType := range allowed {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		toolName := EmitToolName(eventType)
		if collisions[toolName] > 1 {
			if warn != nil {
				warn(
					"emit-tool-ambiguous-name-"+toolName,
					"event-schema-registry",
					"skipping emit tool generation for %q because emit tool name %q is ambiguous across configured events",
					eventType,
					toolName,
				)
			}
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
			Name:        toolName,
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
		collisions := duplicateEmitToolNames(configured)
		tools := make([]llm.ToolDefinition, 0, len(configured))
		for _, eventType := range configured {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			toolName := EmitToolName(eventType)
			if collisions[toolName] > 1 {
				if warn != nil {
					warn(
						"emit-tool-ambiguous-name-"+toolName,
						"event-schema-registry",
						"skipping emit tool generation for %q because emit tool name %q is ambiguous across actor-configured events",
						eventType,
						toolName,
					)
				}
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
				Name:        toolName,
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

func ValidateGeneratedEmitToolSchemasForSource(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	var errs []error
	for _, actor := range providerSchemaValidationActors(source) {
		for _, tool := range registry.GenerateEmitToolsForActor(actor, nil) {
			if err := llm.ValidateProviderToolSchema(tool.Name, tool.Schema); err != nil {
				errs = append(errs, fmt.Errorf("agent %s: %w", strings.TrimSpace(actor.ID), err))
			}
		}
	}
	return errs
}

func providerSchemaValidationActors(source semanticview.Source) []models.AgentConfig {
	if source == nil {
		return nil
	}
	var actors []models.AgentConfig
	seen := map[string]struct{}{}
	appendActor := func(actor models.AgentConfig) {
		key := strings.Join([]string{
			strings.TrimSpace(actor.ID),
			strings.TrimSpace(actor.Role),
			strings.TrimSpace(actor.Mode),
			strings.Join(UniqueNonEmpty(actor.EmitEvents), ","),
		}, "|")
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		actors = append(actors, actor)
	}
	for _, scope := range source.ProjectScopes() {
		for _, id := range sortedEmitSchemaAgentIDs(scope.Agents) {
			entry := scope.Agents[id]
			proof := semanticview.ResolveAgentSessionScopeProof(source, semanticview.AgentSessionScopeLocator{
				AgentID:         id,
				ProjectScopeKey: scope.Key,
			})
			appendActor(models.AgentConfig{
				ID:               strings.TrimSpace(id),
				Type:             strings.TrimSpace(entry.Type),
				Role:             strings.TrimSpace(entry.Role),
				Mode:             strings.TrimSpace(proof.OwningFlowID),
				ModelTier:        strings.TrimSpace(entry.ModelTier),
				ConversationMode: strings.TrimSpace(entry.ConversationMode),
				SessionScope:     strings.TrimSpace(entry.SessionScope),
				MaxTurnsPerTask:  entry.MaxTurnsPerTask,
				Subscriptions:    UniqueNonEmpty(append(append([]string{}, entry.Subscriptions...), append(entry.SubscriptionsBootstrap, entry.SubscribesTo...)...)),
				EmitEvents:       UniqueNonEmpty(entry.EmitEvents),
				Tools:            UniqueNonEmpty(entry.ConfiguredTools()),
				Permissions:      UniqueNonEmpty(entry.Permissions),
				FlowPath:         strings.Trim(strings.TrimSpace(proof.FlowPath), "/"),
			})
		}
	}
	for _, scope := range source.FlowScopes() {
		for _, id := range sortedEmitSchemaAgentIDs(scope.Agents) {
			entry := scope.Agents[id]
			appendActor(models.AgentConfig{
				ID:               strings.TrimSpace(id),
				Type:             strings.TrimSpace(entry.Type),
				Role:             strings.TrimSpace(entry.Role),
				Mode:             strings.TrimSpace(scope.ID),
				ModelTier:        strings.TrimSpace(entry.ModelTier),
				ConversationMode: strings.TrimSpace(entry.ConversationMode),
				SessionScope:     strings.TrimSpace(entry.SessionScope),
				MaxTurnsPerTask:  entry.MaxTurnsPerTask,
				Subscriptions:    UniqueNonEmpty(append(append([]string{}, entry.Subscriptions...), append(entry.SubscriptionsBootstrap, entry.SubscribesTo...)...)),
				EmitEvents:       UniqueNonEmpty(entry.EmitEvents),
				Tools:            UniqueNonEmpty(entry.ConfiguredTools()),
				Permissions:      UniqueNonEmpty(entry.Permissions),
				FlowPath:         strings.Trim(strings.TrimSpace(scope.Path), "/"),
			})
		}
	}
	for _, id := range sortedEmitSchemaAgentIDs(source.AgentEntries()) {
		entry := source.AgentEntries()[id]
		appendActor(models.AgentConfig{
			ID:               strings.TrimSpace(id),
			Type:             strings.TrimSpace(entry.Type),
			Role:             strings.TrimSpace(entry.Role),
			ModelTier:        strings.TrimSpace(entry.ModelTier),
			ConversationMode: strings.TrimSpace(entry.ConversationMode),
			SessionScope:     strings.TrimSpace(entry.SessionScope),
			MaxTurnsPerTask:  entry.MaxTurnsPerTask,
			Subscriptions:    UniqueNonEmpty(append(append([]string{}, entry.Subscriptions...), append(entry.SubscriptionsBootstrap, entry.SubscribesTo...)...)),
			EmitEvents:       UniqueNonEmpty(entry.EmitEvents),
			Tools:            UniqueNonEmpty(entry.ConfiguredTools()),
			Permissions:      UniqueNonEmpty(entry.Permissions),
		})
	}
	return actors
}

func sortedEmitSchemaAgentIDs(agents map[string]runtimecontracts.AgentRegistryEntry) []string {
	ids := make([]string, 0, len(agents))
	for id := range agents {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
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

func (r *EmitRegistry) EventSchemaForActorTool(actor models.AgentConfig, toolName string) (string, EmitSchema, bool) {
	if r == nil {
		return "", EmitSchema{}, false
	}
	toolName = normalizeNativeToolName(toolName)
	if toolName == "" {
		return "", EmitSchema{}, false
	}
	if configured := UniqueNonEmpty(actor.EmitEvents); len(configured) > 0 {
		var matchedEventType string
		var matchedSchema EmitSchema
		matchCount := 0
		for _, eventType := range configured {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" || EmitToolName(eventType) != toolName {
				continue
			}
			schema, ok := r.schemaForActorEvent(actor, eventType)
			if !ok {
				continue
			}
			matchedEventType = eventType
			matchedSchema = schema
			matchCount++
		}
		if matchCount == 1 {
			return matchedEventType, matchedSchema, true
		}
		if matchCount > 1 {
			return "", EmitSchema{}, false
		}
	}
	eventType, ok := r.EventTypeFromToolName(toolName)
	if !ok {
		return "", EmitSchema{}, false
	}
	schema, ok := emitSchemaForEventType(r.activeSchemas, eventType)
	if !ok {
		return "", EmitSchema{}, false
	}
	return eventType, schema, true
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
	eventType, _, ok := r.EventSchemaForActorTool(actor, toolName)
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

func duplicateEmitToolNames(eventTypes []string) map[string]int {
	counts := make(map[string]int, len(eventTypes))
	for _, eventType := range eventTypes {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		counts[EmitToolName(eventType)]++
	}
	return counts
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
