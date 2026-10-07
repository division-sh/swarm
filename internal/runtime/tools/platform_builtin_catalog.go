package tools

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func builtinExecutionTools(source semanticview.Source, actor *models.AgentConfig) (map[string]ExecutionTool, error) {
	entries := builtinRuntimeContractSchemas(source, actor)
	out := make(map[string]ExecutionTool, len(entries))
	for name, entry := range entries {
		execution, err := admitBuiltinExecutionTool(name, entry)
		if err != nil {
			return nil, err
		}
		out[name] = execution
	}
	return out, nil
}

func admitBuiltinExecutionTool(name string, entry builtinToolDraft) (ExecutionTool, error) {
	input, err := runtimecontracts.AdmitToolInputSchemaMap(entry.InputSchema)
	if err != nil {
		return ExecutionTool{}, fmt.Errorf("builtin tool %s input_schema: %w", name, err)
	}
	var output runtimecontracts.ToolInputSchema
	if entry.OutputSchema != nil {
		output, err = runtimecontracts.AdmitToolInputSchemaMap(entry.OutputSchema)
		if err != nil {
			return ExecutionTool{}, fmt.Errorf("builtin tool %s output_schema: %w", name, err)
		}
	}
	contract, err := runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolCategory(entry.Category),
		runtimecontracts.WithToolDescription(entry.Description),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin),
		runtimecontracts.WithToolSchemas(input, output),
		runtimecontracts.WithToolGeneratedSchema(entry.GeneratedSchema),
	)
	if err != nil {
		return ExecutionTool{}, fmt.Errorf("builtin tool %s: %w", name, err)
	}
	execution, include := executionToolFromAdmitted(name, contract)
	if !include {
		return ExecutionTool{}, fmt.Errorf("builtin tool %s did not produce an executable contract", name)
	}
	return execution, nil
}

func builtinRuntimeContractSchemas(source semanticview.Source, actor *models.AgentConfig) map[string]builtinToolDraft {
	hitl := hitlRuntimeContractSchemas()
	if actor != nil {
		out := flowDataToolSchemaEntriesForActor(source, *actor)
		out["schedule"] = scheduleContractSchema()
		for name, entry := range hitl {
			out[name] = entry
		}
		if contract, ok := resolveEntityToolContract(source, actor); ok {
			for name, entry := range roleScopedEntityToolSchemaEntriesForActor(source, *actor, contract) {
				out[name] = entry
			}
		}
		return out
	}
	readContracts := actorOwnedReadTargetContracts(source, actor)
	readTargetSchema := entityReadTargetInputSchemaForContracts(readContracts)
	out := genericEntityRuntimeContractSchemas(readTargetSchema)
	for name, entry := range hitl {
		out[name] = entry
	}
	out["schedule"] = scheduleContractSchema()
	if contract, ok := resolveEntityToolContract(source, actor); ok {
		if len(readContracts) == 0 {
			readContracts = []entityruntime.Contract{contract}
		}
		for name, entry := range entityToolSchemaEntriesForContract(contract, readContracts, readTargetSchema) {
			out[name] = entry
		}
	}
	return out
}

func scheduleContractSchema() builtinToolDraft {
	return builtinToolDraft{
		Category:        "platform",
		Description:     "Create or replay one durable generic schedule activation for the current actor.",
		GeneratedSchema: true,
		InputSchema: ObjectSchema(map[string]any{
			"schedule_key": map[string]any{"type": "string", "minLength": 1},
			"event_type":   map[string]any{"type": "string", "minLength": 1},
			"mode": map[string]any{
				"type": "string",
				"enum": []any{"absolute", "delay", "cron", "every"},
			},
			"at":        map[string]any{"type": "string", "format": "date-time"},
			"delay":     map[string]any{"type": "string", "minLength": 1},
			"cron":      map[string]any{"type": "string", "minLength": 1},
			"every":     map[string]any{"type": "string", "minLength": 1},
			"agent_id":  map[string]any{"type": "string", "minLength": 1},
			"entity_id": map[string]any{"type": "string", "format": "uuid"},
			"task_id":   map[string]any{"type": "string", "minLength": 1},
			"payload": map[string]any{
				"type":                 "object",
				"additionalProperties": true,
			},
		}, "schedule_key", "event_type", "mode"),
		OutputSchema: ObjectSchema(map[string]any{
			"status":        map[string]any{"type": "string", "enum": []any{"active", "fired", "cancelled", "failed"}},
			"outcome":       map[string]any{"type": "string", "enum": []any{"created", "exact_replay"}},
			"activation_id": map[string]any{"type": "string", "format": "uuid"},
			"schedule_key":  map[string]any{"type": "string", "minLength": 1},
			"due_at":        map[string]any{"type": "string", "format": "date-time"},
		}, "status", "outcome", "activation_id", "schedule_key", "due_at"),
	}
}

func askHumanContractSchema() builtinToolDraft {
	return builtinToolDraft{
		Category:    "human_decision",
		Description: "Create a typed decision card when admitted work requires a human verdict.",
		InputSchema: ObjectSchema(map[string]any{
			"scope": map[string]any{
				"type": "string",
				"enum": []any{"entity", "flow", "global"},
			},
			"entity_id":      map[string]any{"type": "string", "format": "uuid"},
			"category":       map[string]any{"type": "string", "minLength": 1},
			"description":    map[string]any{"type": "string", "minLength": 1},
			"talking_points": map[string]any{"type": "array", "items": map[string]any{"type": "string", "minLength": 1}},
			"expected_value": map[string]any{"type": "string"},
			"priority": map[string]any{
				"type": "string",
				"enum": []any{"low", "medium", "high", "critical"},
			},
			"deadline_at": map[string]any{"type": "string", "format": "date-time"},
		}, "scope", "category", "description"),
		OutputSchema: ObjectSchema(map[string]any{
			"card_id": map[string]any{"type": "string", "format": "uuid"},
			"status":  map[string]any{"type": "string", "enum": []any{"pending"}},
		}, "card_id", "status"),
	}
}

func notifyHumanContractSchema() builtinToolDraft {
	return builtinToolDraft{
		Category: "platform",
		InputSchema: ObjectSchema(map[string]any{
			"summary": map[string]any{"type": "string", "minLength": 1},
			"context": map[string]any{},
		}, "summary"),
		OutputSchema: ObjectSchema(map[string]any{
			"status":     map[string]any{"type": "string", "enum": []any{"queued"}},
			"mailbox_id": map[string]any{"type": "string", "format": "uuid"},
		}, "status", "mailbox_id"),
	}
}

func flowDataToolSchemaEntriesForActor(source semanticview.Source, actor models.AgentConfig) map[string]builtinToolDraft {
	staticData := flowdata.AllowedStaticData(source, actor)
	resources := flowdata.AllowedResourceData(source, actor)
	if len(staticData) == 0 && len(resources) == 0 {
		return map[string]builtinToolDraft{}
	}
	staticIDs := make([]any, 0, len(staticData))
	for _, item := range staticData {
		staticIDs = append(staticIDs, item.StaticID)
	}
	kinds := make([]any, 0, 3)
	properties := map[string]any{}
	if len(staticIDs) > 0 {
		kinds = append(kinds, "static_file")
		properties["static_id"] = map[string]any{"type": "string", "enum": staticIDs}
		properties["cursor"] = map[string]any{"type": "string", "minLength": 1, "maxLength": durabledata.MaxBusinessKeyBytes}
	}
	if len(resources) > 0 {
		kinds = append(kinds, "resource_row", "resource_rows")
		flowPaths := make([]any, 0, len(resources))
		events := make([]any, 0, len(resources))
		for _, ref := range resources {
			flowPaths = append(flowPaths, ref.FlowPath)
			events = append(events, ref.EventName)
		}
		properties["declaration"] = ObjectSchema(map[string]any{
			"flow_path": map[string]any{"type": "string", "enum": flowPaths},
			"event":     map[string]any{"type": "string", "enum": events},
		}, "flow_path", "event")
		properties["position"] = map[string]any{"type": "integer", "minimum": 1, "maximum": durabledata.MaxResourceRows}
		properties["key"] = map[string]any{}
		properties["page"] = ObjectSchema(map[string]any{
			"limit":      map[string]any{"type": "integer", "minimum": 1, "maximum": durabledata.MaxPublicPageItems},
			"byte_limit": map[string]any{"type": "integer", "minimum": 1, "maximum": durabledata.MaxToolPageBytes},
			"cursor":     map[string]any{"type": "string", "minLength": 1, "maxLength": durabledata.MaxBusinessKeyBytes},
		})
	}
	properties["kind"] = map[string]any{"type": "string", "enum": kinds}
	return map[string]builtinToolDraft{
		flowdata.ToolName: {
			Category:        "flow_data",
			Description:     "Read only exact static bytes or pinned resource rows admitted for this actor and run.",
			GeneratedSchema: true,
			InputSchema:     ObjectSchema(properties, "kind"),
		},
	}
}

func resolveEntityToolContract(source semanticview.Source, actor *models.AgentConfig) (entityruntime.Contract, bool) {
	if source == nil {
		return entityruntime.Contract{}, false
	}
	if actor != nil {
		if contract, ok := entityruntime.ResolveForActor(source, *actor); ok {
			return contract, true
		}
	}
	return entityruntime.ResolveForFlow(source, "")
}

func genericEntityRuntimeContractSchemas(readTargetSchema map[string]any) map[string]builtinToolDraft {
	anyValueSchema := map[string]any{}
	return map[string]builtinToolDraft{
		"get_entity": {
			Category:    "entity_persistence",
			Description: "Read a full entity_state row by entity id.",
			InputSchema: ObjectSchema(map[string]any{
				"flow_instance": existingEntityFlowInstanceSchema(),
				"entity_id":     map[string]any{"type": "string"},
			}, "entity_id"),
		},
		"save_entity_field": {
			Category:    "entity_persistence",
			Description: "Write a single declared field or dotted subfield path on an entity.",
			InputSchema: ObjectSchema(map[string]any{
				"flow_instance": existingEntityFlowInstanceSchema(),
				"entity_id":     map[string]any{"type": "string"},
				"field":         map[string]any{"type": "string"},
				"value":         anyValueSchema,
			}, "entity_id", "field", "value"),
		},
		"query_entities": {
			Category:    "entity_persistence",
			Description: "Query entity_state rows using validated selectors and optional grouping.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type":   deepCloneJSONValue(readTargetSchema),
				"flow_instance": existingEntityFlowInstanceSchema(),
				"filter":        map[string]any{"type": "string"},
				"select": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"limit":    map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
				"group_by": map[string]any{"type": "string"},
			}),
		},
		"search_entities": {
			Category:    "entity_persistence",
			Description: "Query entity_state rows by state, metadata, and declared field matches.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type":   deepCloneJSONValue(readTargetSchema),
				"flow_instance": existingEntityFlowInstanceSchema(),
				"current_state": map[string]any{"type": "string"},
				"filter": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{},
					"additionalProperties": true,
				},
				"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
				"offset": map[string]any{"type": "integer", "minimum": 0, "maximum": 100000},
			}),
		},
		"query_metrics": {
			Category:    "entity_persistence",
			Description: "Aggregate metrics across entity_state rows.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type":   deepCloneJSONValue(readTargetSchema),
				"flow_instance": existingEntityFlowInstanceSchema(),
				"metric": map[string]any{
					"type": "string",
					"enum": []any{"count", "sum", "avg", "min", "max"},
				},
				"field":    map[string]any{"type": "string"},
				"group_by": map[string]any{"type": "string"},
				"filter":   map[string]any{"type": "string"},
			}, "metric"),
		},
	}
}

func existingEntityFlowInstanceSchema() map[string]any {
	return map[string]any{
		"type":        "string",
		"description": "Optional existing-entity flow guard/filter. Accepts a concrete flow instance path or a declared semantic flow root; roots match descendant instances.",
	}
}

func entityToolSchemaEntriesForContract(contract entityruntime.Contract, readContracts []entityruntime.Contract, readTargetSchema map[string]any) map[string]builtinToolDraft {
	writablePaths := entityToolWritablePathNames(contract)
	filterSelectors := entityToolReadLeafSelectorNames(readContracts)
	selectableSelectors := entityToolReadSelectableFieldNames(readContracts)
	filterProperties := make(map[string]any, len(filterSelectors))
	for _, name := range filterSelectors {
		filterProperties[name] = entityToolReadFilterPropertySchema(readContracts, name)
	}
	selectorEnum := make([]any, 0, len(selectableSelectors))
	for _, name := range selectableSelectors {
		selectorEnum = append(selectorEnum, name)
	}
	fieldEnum := make([]any, 0, len(writablePaths))
	for _, name := range writablePaths {
		fieldEnum = append(fieldEnum, name)
	}
	queryEntityProperties := map[string]any{
		"entity_type":   deepCloneJSONValue(readTargetSchema),
		"flow_instance": existingEntityFlowInstanceSchema(),
		"filter":        map[string]any{"type": "string"},
		"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
	}
	queryMetricProperties := map[string]any{
		"entity_type":   deepCloneJSONValue(readTargetSchema),
		"flow_instance": existingEntityFlowInstanceSchema(),
		"metric": map[string]any{
			"type": "string",
			"enum": []any{"count", "sum", "avg", "min", "max"},
		},
		"filter": map[string]any{"type": "string"},
	}
	if len(selectorEnum) > 0 {
		queryEntityProperties["select"] = map[string]any{
			"type": "array",
			"items": map[string]any{
				"type": "string",
				"enum": selectorEnum,
			},
		}
		queryEntityProperties["group_by"] = map[string]any{
			"type": "string",
			"enum": selectorEnum,
		}
		queryMetricProperties["field"] = map[string]any{
			"type": "string",
			"enum": selectorEnum,
		}
		queryMetricProperties["group_by"] = map[string]any{
			"type": "string",
			"enum": selectorEnum,
		}
	}
	entries := map[string]builtinToolDraft{
		"get_entity": {
			Category:    "entity_persistence",
			Description: "Read a full entity_state row by entity id.",
			InputSchema: ObjectSchema(map[string]any{
				"flow_instance": existingEntityFlowInstanceSchema(),
				"entity_id":     map[string]any{"type": "string"},
			}, "entity_id"),
		},
		"search_entities": {
			Category:    "entity_persistence",
			Description: "Query entity_state rows by state, metadata, and declared field matches.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type":   deepCloneJSONValue(readTargetSchema),
				"flow_instance": existingEntityFlowInstanceSchema(),
				"current_state": map[string]any{"type": "string"},
				"filter": map[string]any{
					"type":                 "object",
					"properties":           filterProperties,
					"additionalProperties": false,
				},
				"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
				"offset": map[string]any{"type": "integer", "minimum": 0, "maximum": 100000},
			}),
		},
		"query_entities": {
			Category:    "entity_persistence",
			Description: "Query entity_state rows using validated selectors and optional grouping.",
			InputSchema: ObjectSchema(queryEntityProperties),
		},
		"query_metrics": {
			Category:    "entity_persistence",
			Description: "Aggregate metrics across entity_state rows.",
			InputSchema: ObjectSchema(queryMetricProperties, "metric"),
		},
	}
	if len(fieldEnum) > 0 {
		entries["save_entity_field"] = builtinToolDraft{
			Category:    "entity_persistence",
			Description: "Write a single declared field or dotted subfield path on an entity.",
			InputSchema: ObjectSchema(map[string]any{
				"flow_instance": existingEntityFlowInstanceSchema(),
				"entity_id":     map[string]any{"type": "string"},
				"field": map[string]any{
					"type": "string",
					"enum": fieldEnum,
				},
				"value": map[string]any{},
			}, "entity_id", "field", "value"),
		}
	}
	return entries
}

func entityReadTargetInputSchema(source semanticview.Source) map[string]any {
	return entityReadTargetInputSchemaForContracts(entityruntime.ReadTargetContracts(source))
}

func entityReadTargetInputSchemaForContracts(contracts []entityruntime.Contract) map[string]any {
	schema := map[string]any{
		"type":        "string",
		"description": "Optional read target entity contract. Use the delivered flow-qualified form, for example flow_id.entity_type; omitted defaults to the caller flow-owned entity contract.",
	}
	names := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		if name := entityruntime.CanonicalReadTargetName(contract); name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) > 0 {
		enum := make([]any, 0, len(names))
		for _, name := range names {
			enum = append(enum, name)
		}
		schema["enum"] = enum
	}
	return schema
}

func entityToolReadSelectableFieldNames(contracts []entityruntime.Contract) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, contract := range contracts {
		for _, name := range entityToolSelectableFieldNames(contract) {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func entityToolReadLeafSelectorNames(contracts []entityruntime.Contract) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, contract := range contracts {
		for _, name := range entityToolLeafSelectorNames(contract) {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func entityToolReadFilterPropertySchema(contracts []entityruntime.Contract, name string) map[string]any {
	name = strings.TrimSpace(name)
	for _, contract := range contracts {
		field, err := entityruntime.ResolveLeafField(contract, name)
		if err != nil {
			continue
		}
		return entityContractJSONSchemaWithRefinements(contract, field.Type, field.Refinements, false, map[string]struct{}{})
	}
	return map[string]any{}
}

func entityToolSelectableFieldNames(contract entityruntime.Contract) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(entityStateTopLevelFields))
	for name := range entityStateTopLevelFields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	for _, name := range entityToolLeafSelectorNames(contract) {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func entityToolLeafSelectorNames(contract entityruntime.Contract) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, name := range entityruntime.FieldNames(contract) {
		decl, err := entityruntime.FieldDecl(contract, name)
		if err != nil {
			continue
		}
		collectEntityToolLeafSelectors(contract, strings.TrimSpace(name), strings.TrimSpace(decl.Type), seen, map[string]struct{}{}, &out)
	}
	sort.Strings(out)
	return out
}

func entityToolWritablePathNames(contract entityruntime.Contract) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, name := range entityruntime.FieldNames(contract) {
		decl, err := entityruntime.FieldDecl(contract, name)
		if err != nil {
			continue
		}
		if strings.TrimSpace(decl.MaterializeFrom) != "" {
			continue
		}
		collectEntityToolWritablePaths(contract, strings.TrimSpace(name), strings.TrimSpace(decl.Type), seen, map[string]struct{}{}, &out)
	}
	sort.Strings(out)
	return out
}

func collectEntityToolWritablePaths(contract entityruntime.Contract, path, typeRef string, seen map[string]struct{}, visiting map[string]struct{}, out *[]string) {
	path = strings.TrimSpace(path)
	typeRef = strings.TrimSpace(typeRef)
	if path == "" || typeRef == "" {
		return
	}
	if _, ok := seen[path]; !ok {
		seen[path] = struct{}{}
		*out = append(*out, path)
	}
	if !deliveryNamedType(contract, typeRef) {
		return
	}
	typeName := deliveryTypeName(contract, typeRef)
	if _, ok := visiting[typeName]; ok {
		return
	}
	visiting[typeName] = struct{}{}
	resolved, err := resolveEntityToolStructuralType(contract, typeRef)
	if err != nil {
		delete(visiting, typeName)
		return
	}
	for _, field := range resolved.Fields {
		collectEntityToolWritablePaths(contract, path+"."+field.Name, field.TypeRef, seen, visiting, out)
	}
	delete(visiting, typeName)
}

func collectEntityToolLeafSelectors(contract entityruntime.Contract, path, typeRef string, seen map[string]struct{}, visiting map[string]struct{}, out *[]string) {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case deliveryListType(typeRef):
		return
	case deliveryTextType(typeRef), deliveryIntegerType(contract, typeRef), deliveryNumericType(contract, typeRef), deliveryBooleanType(contract, typeRef), deliveryTimestampType(contract, typeRef), deliveryUUIDType(contract, typeRef), deliveryEnumType(contract, typeRef):
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		*out = append(*out, path)
	case deliveryNamedType(contract, typeRef):
		typeName := deliveryTypeName(contract, typeRef)
		if _, ok := visiting[typeName]; ok {
			return
		}
		visiting[typeName] = struct{}{}
		resolved, err := resolveEntityToolStructuralType(contract, typeRef)
		if err != nil {
			delete(visiting, typeName)
			return
		}
		for _, field := range resolved.Fields {
			collectEntityToolLeafSelectors(contract, path+"."+field.Name, field.TypeRef, seen, visiting, out)
		}
		delete(visiting, typeName)
	}
}

func entityContractJSONSchema(contract entityruntime.Contract, typeRef string, seen map[string]struct{}) map[string]any {
	resolved, err := resolveEntityToolStructuralType(contract, typeRef)
	if err != nil {
		return map[string]any{}
	}
	return entityResolvedContractJSONSchema(contract, resolved, seen)
}

func entityResolvedContractJSONSchema(contract entityruntime.Contract, resolved runtimecontracts.ResolvedCatalogType, seen map[string]struct{}) map[string]any {
	switch resolved.Kind {
	case runtimecontracts.CatalogTypeList:
		return map[string]any{
			"type":  "array",
			"items": entityResolvedContractJSONSchema(contract, *resolved.Element, seen),
		}
	case runtimecontracts.CatalogTypeMap:
		key := entityResolvedContractJSONSchema(contract, *resolved.Key, seen)
		key["minLength"] = 1
		return map[string]any{
			"type": "object", "propertyNames": key,
			"additionalProperties": entityResolvedContractJSONSchema(contract, *resolved.Value, seen),
		}
	case runtimecontracts.CatalogTypeText:
		schema := map[string]any{"type": "string"}
		if enum, ok := contract.Types.Enums[resolved.Name]; ok {
			values := make([]any, 0, len(enum.Values))
			for _, value := range enum.Values {
				values = append(values, strings.TrimSpace(value))
			}
			schema["enum"] = values
		} else if resolved.Format != "" {
			schema["format"] = resolved.Format
		}
		return schema
	case runtimecontracts.CatalogTypeInteger:
		return map[string]any{"type": "integer"}
	case runtimecontracts.CatalogTypeNumber:
		return map[string]any{"type": "number"}
	case runtimecontracts.CatalogTypeBoolean:
		return map[string]any{"type": "boolean"}
	case runtimecontracts.CatalogTypeObject:
		if _, ok := seen[resolved.Name]; ok {
			return ObjectSchema(map[string]any{})
		}
		seen[resolved.Name] = struct{}{}
		defer delete(seen, resolved.Name)
		props := make(map[string]any, len(resolved.Fields))
		required := make([]string, 0, len(resolved.Fields))
		for _, field := range resolved.Fields {
			value := entityResolvedContractJSONSchema(contract, field.Type, seen)
			applyEntitySchemaRefinements(value, field.Refinements, true)
			props[field.Name] = value
			if !field.IsOptional {
				required = append(required, field.Name)
			}
		}
		schema := ObjectSchema(props, required...)
		schema["additionalProperties"] = false
		return schema
	default:
		if resolved.Name == "array" {
			return map[string]any{"type": "array", "items": map[string]any{}}
		}
		return map[string]any{}
	}
}

func resolveEntityToolStructuralType(contract entityruntime.Contract, typeRef string) (runtimecontracts.ResolvedCatalogType, error) {
	return (runtimecontracts.CatalogTypeReference{Type: strings.TrimSpace(typeRef), Catalog: contract.Types}).Resolve()
}

func entityContractJSONSchemaWithRefinements(contract entityruntime.Contract, typeRef string, refinements runtimecontracts.SchemaRefinements, includeEquality bool, seen map[string]struct{}) map[string]any {
	schema := entityContractJSONSchema(contract, typeRef, seen)
	applyEntitySchemaRefinements(schema, refinements, includeEquality)
	return schema
}

func applyEntitySchemaRefinements(schema map[string]any, refinements runtimecontracts.SchemaRefinements, includeEquality bool) {
	if len(schema) == 0 || refinements.Empty() {
		return
	}
	if value := strings.TrimSpace(refinements.Pattern); value != "" {
		schema["pattern"] = value
	}
	if min := refinements.Length.Min; min != nil {
		if strings.TrimSpace(asString(schema["type"])) == "array" {
			schema["minItems"] = *min
		} else {
			schema["minLength"] = *min
		}
	}
	if max := refinements.Length.Max; max != nil {
		if strings.TrimSpace(asString(schema["type"])) == "array" {
			schema["maxItems"] = *max
		} else {
			schema["maxLength"] = *max
		}
	}
	if min := refinements.Range.Min; min != nil {
		schema["minimum"] = *min
	}
	if max := refinements.Range.Max; max != nil {
		schema["maximum"] = *max
	}
	if includeEquality {
		if value := strings.TrimSpace(refinements.EqualTo); value != "" {
			schema["x-swarm-equalTo"] = value
		}
	}
}

func deliveryTypeName(contract entityruntime.Contract, typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	if scalar, ok := contract.Types.Scalars[typeRef]; ok {
		return strings.TrimSpace(scalar.Base)
	}
	return typeRef
}

func deliveryNamedType(contract entityruntime.Contract, typeRef string) bool {
	_, ok := contract.Types.Types[deliveryTypeName(contract, typeRef)]
	return ok
}

func deliveryEnumType(contract entityruntime.Contract, typeRef string) bool {
	_, ok := contract.Types.Enums[deliveryTypeName(contract, typeRef)]
	return ok
}

func deliveryTextType(typeRef string) bool {
	typeRef = strings.ToLower(strings.TrimSpace(typeRef))
	return typeRef == "text" || typeRef == "string"
}

func deliveryIntegerType(contract entityruntime.Contract, typeRef string) bool {
	return strings.EqualFold(deliveryTypeName(contract, typeRef), "integer")
}

func deliveryNumericType(contract entityruntime.Contract, typeRef string) bool {
	raw := strings.ToLower(strings.TrimSpace(deliveryTypeName(contract, typeRef)))
	return raw == "numeric" || strings.HasPrefix(raw, "numeric(")
}

func deliveryBooleanType(contract entityruntime.Contract, typeRef string) bool {
	return strings.EqualFold(deliveryTypeName(contract, typeRef), "boolean")
}

func deliveryTimestampType(contract entityruntime.Contract, typeRef string) bool {
	return strings.EqualFold(deliveryTypeName(contract, typeRef), "timestamp")
}

func deliveryUUIDType(contract entityruntime.Contract, typeRef string) bool {
	return strings.EqualFold(deliveryTypeName(contract, typeRef), "uuid")
}

func deliveryListType(typeRef string) bool {
	typeRef = strings.TrimSpace(typeRef)
	return strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">") ||
		strings.HasSuffix(typeRef, "[]") ||
		strings.HasPrefix(typeRef, "[]")
}
