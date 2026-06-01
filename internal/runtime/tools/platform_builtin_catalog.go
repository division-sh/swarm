package tools

import (
	"sort"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func builtinRegisteredTools(source semanticview.Source, actor *models.AgentConfig) map[string]RegisteredTool {
	entries := builtinRuntimeContractSchemas(source, actor)
	out := make(map[string]RegisteredTool, len(entries))
	for name, entry := range entries {
		out[name] = RegisteredTool{
			Name:            strings.TrimSpace(name),
			Category:        strings.TrimSpace(entry.Category),
			Description:     strings.TrimSpace(entry.Description),
			Usage:           runtimeOwnedToolUsage(name),
			HandlerType:     implementationPlatformBuiltin,
			InputSchema:     deepCloneMap(entry.InputSchema),
			OutputSchema:    deepCloneMap(entry.OutputSchema),
			GeneratedSchema: entry.GeneratedSchema,
		}
	}
	return out
}

func builtinRuntimeContractSchemas(source semanticview.Source, actor *models.AgentConfig) map[string]ContractSchemaEntry {
	if actor != nil {
		out := flowDataToolSchemaEntriesForActor(source, *actor)
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

func flowDataToolSchemaEntriesForActor(source semanticview.Source, actor models.AgentConfig) map[string]ContractSchemaEntry {
	filenames := flowdata.AllowedFilenames(source, actor)
	if len(filenames) == 0 {
		return map[string]ContractSchemaEntry{}
	}
	enum := make([]any, 0, len(filenames))
	for _, filename := range filenames {
		enum = append(enum, filename)
	}
	return map[string]ContractSchemaEntry{
		flowdata.ToolName: {
			Category:        "flow_data",
			Description:     "Read a declared static reference-data file from the agent's owning flow data root.",
			GeneratedSchema: true,
			InputSchema: ObjectSchema(map[string]any{
				"filename": map[string]any{
					"type": "string",
					"enum": enum,
				},
			}, "filename"),
			OutputSchema: ObjectSchema(map[string]any{
				"content":      map[string]any{"type": "string"},
				"content_type": map[string]any{"type": "string", "enum": []any{"yaml", "json", "markdown", "text"}},
				"size_bytes":   map[string]any{"type": "integer", "minimum": 0},
			}, "content", "content_type", "size_bytes"),
		},
	}
}

func resolveEntityToolContract(source semanticview.Source, actor *models.AgentConfig) (entityruntime.Contract, bool) {
	if source == nil {
		return entityruntime.Contract{}, false
	}
	if actor != nil {
		if contract, ok := entityruntime.ResolveForActor(source, actor.ID); ok {
			return contract, true
		}
	}
	return entityruntime.ResolveForFlow(source, "")
}

func genericEntityRuntimeContractSchemas(readTargetSchema map[string]any) map[string]ContractSchemaEntry {
	anyValueSchema := map[string]any{}
	return map[string]ContractSchemaEntry{
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
		"create_entity": {
			Category:    "entity_persistence",
			Description: "Create a new entity_state row from the inferred flow-owned contract.",
			InputSchema: ObjectSchema(map[string]any{
				"flow_instance": map[string]any{"type": "string"},
				"name":          map[string]any{"type": "string"},
				"initial_state": map[string]any{"type": "string"},
				"fields": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{},
					"additionalProperties": true,
				},
			}, "flow_instance"),
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

func entityToolSchemaEntriesForContract(contract entityruntime.Contract, readContracts []entityruntime.Contract, readTargetSchema map[string]any) map[string]ContractSchemaEntry {
	topLevelFields := entityruntime.FieldNames(contract)
	writablePaths := entityToolWritablePathNames(contract)
	filterSelectors := entityToolReadLeafSelectorNames(readContracts)
	selectableSelectors := entityToolReadSelectableFieldNames(readContracts)
	filterProperties := make(map[string]any, len(filterSelectors))
	for _, name := range filterSelectors {
		filterProperties[name] = entityToolReadFilterPropertySchema(readContracts, name)
	}
	fieldProperties := make(map[string]any, len(topLevelFields))
	for _, name := range topLevelFields {
		decl, err := entityruntime.FieldDecl(contract, name)
		if err != nil {
			continue
		}
		fieldProperties[name] = entityContractJSONSchema(contract, decl.Type, map[string]struct{}{})
	}
	selectorEnum := make([]any, 0, len(selectableSelectors))
	for _, name := range selectableSelectors {
		selectorEnum = append(selectorEnum, name)
	}
	fieldEnum := make([]any, 0, len(writablePaths))
	for _, name := range writablePaths {
		fieldEnum = append(fieldEnum, name)
	}
	return map[string]ContractSchemaEntry{
		"create_entity": {
			Category:    "entity_persistence",
			Description: "Create a new entity_state row from the inferred flow-owned contract.",
			InputSchema: ObjectSchema(map[string]any{
				"flow_instance": map[string]any{"type": "string"},
				"name":          map[string]any{"type": "string"},
				"initial_state": map[string]any{"type": "string"},
				"fields": map[string]any{
					"type":                 "object",
					"properties":           fieldProperties,
					"additionalProperties": false,
				},
			}, "flow_instance"),
		},
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
				"field": map[string]any{
					"type": "string",
					"enum": fieldEnum,
				},
				"value": map[string]any{},
			}, "entity_id", "field", "value"),
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
			InputSchema: ObjectSchema(map[string]any{
				"entity_type":   deepCloneJSONValue(readTargetSchema),
				"flow_instance": existingEntityFlowInstanceSchema(),
				"filter":        map[string]any{"type": "string"},
				"select": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "string",
						"enum": selectorEnum,
					},
				},
				"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
				"group_by": map[string]any{
					"type": "string",
					"enum": selectorEnum,
				},
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
				"field": map[string]any{
					"type": "string",
					"enum": selectorEnum,
				},
				"group_by": map[string]any{
					"type": "string",
					"enum": selectorEnum,
				},
				"filter": map[string]any{"type": "string"},
			}, "metric"),
		},
	}
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
		return entityContractJSONSchema(contract, field.Type, map[string]struct{}{})
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
	named := contract.Types.Types[typeName]
	for fieldName, spec := range named.Fields {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			continue
		}
		collectEntityToolWritablePaths(contract, path+"."+fieldName, strings.TrimSpace(spec.Type), seen, visiting, out)
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
		named := contract.Types.Types[typeName]
		names := make([]string, 0, len(named.Fields))
		for name := range named.Fields {
			names = append(names, strings.TrimSpace(name))
		}
		sort.Strings(names)
		for _, name := range names {
			spec := named.Fields[name]
			collectEntityToolLeafSelectors(contract, path+"."+name, spec.Type, seen, visiting, out)
		}
		delete(visiting, typeName)
	}
}

func entityContractJSONSchema(contract entityruntime.Contract, typeRef string, seen map[string]struct{}) map[string]any {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case deliveryListType(typeRef):
		return map[string]any{
			"type":  "array",
			"items": entityContractJSONSchema(contract, deliveryListItemType(typeRef), seen),
		}
	case deliveryTextType(typeRef):
		return map[string]any{"type": "string"}
	case deliveryIntegerType(contract, typeRef):
		return map[string]any{"type": "integer"}
	case deliveryNumericType(contract, typeRef):
		return map[string]any{"type": "number"}
	case deliveryBooleanType(contract, typeRef):
		return map[string]any{"type": "boolean"}
	case deliveryTimestampType(contract, typeRef):
		return map[string]any{"type": "string", "format": "date-time"}
	case deliveryUUIDType(contract, typeRef):
		return map[string]any{"type": "string", "format": "uuid"}
	case deliveryEnumType(contract, typeRef):
		enum := contract.Types.Enums[deliveryTypeName(contract, typeRef)]
		values := make([]any, 0, len(enum.Values))
		for _, value := range enum.Values {
			values = append(values, strings.TrimSpace(value))
		}
		return map[string]any{"type": "string", "enum": values}
	case deliveryNamedType(contract, typeRef):
		typeName := deliveryTypeName(contract, typeRef)
		if _, ok := seen[typeName]; ok {
			return ObjectSchema(map[string]any{})
		}
		seen[typeName] = struct{}{}
		named := contract.Types.Types[typeName]
		props := make(map[string]any, len(named.Fields))
		required := make([]string, 0, len(named.Fields))
		names := make([]string, 0, len(named.Fields))
		for name := range named.Fields {
			names = append(names, strings.TrimSpace(name))
		}
		sort.Strings(names)
		for _, name := range names {
			spec := named.Fields[name]
			props[name] = entityContractJSONSchema(contract, spec.Type, seen)
			required = append(required, name)
		}
		delete(seen, typeName)
		schema := ObjectSchema(props, required...)
		schema["additionalProperties"] = false
		return schema
	default:
		return map[string]any{}
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

func deliveryListItemType(typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">"):
		return strings.TrimSpace(typeRef[len("list<") : len(typeRef)-1])
	case strings.HasSuffix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[:len(typeRef)-2])
	case strings.HasPrefix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[2:])
	default:
		return typeRef
	}
}
