package contracts

import (
	"fmt"
	"sort"
	"strings"
)

type EventSchema struct {
	Description string
	Schema      map[string]any
}

func EventSchemaRegistryFromCatalog(entries map[string]EventCatalogEntry) map[string]EventSchema {
	out := make(map[string]EventSchema, len(entries))
	for eventType, entry := range entries {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out[eventType] = eventSchemaFromCatalogEntry(eventType, entry, TypeCatalogDocument{})
	}
	return out
}

func eventSchemaFromCatalogEntry(eventType string, entry EventCatalogEntry, types TypeCatalogDocument) EventSchema {
	properties := make(map[string]any, len(entry.Payload.Properties))
	fieldNames := make([]string, 0, len(entry.Payload.Properties))
	for fieldName := range entry.Payload.Properties {
		fieldNames = append(fieldNames, strings.TrimSpace(fieldName))
	}
	sort.Strings(fieldNames)
	for _, fieldName := range fieldNames {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			continue
		}
		field := entry.Payload.Properties[fieldName]
		prop, typeDescription := eventSchemaForTypeRef(field.Type, types, map[string]struct{}{})
		description := strings.TrimSpace(field.Description)
		if description == "" {
			description = typeDescription
		} else if typeDescription != "" {
			description = typeDescription + ". " + description
		}
		if description != "" {
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

func EventSchemaRegistryFromBundle(bundle *WorkflowContractBundle) map[string]EventSchema {
	if bundle == nil {
		return map[string]EventSchema{}
	}
	out := map[string]EventSchema{}
	if bundle.FlowTree.Root != nil {
		appendEventSchemas(out, bundle, "", bundle.FlowTree.Root.Events, bundle.RootTypeCatalog())
	} else {
		appendEventSchemas(out, bundle, "", bundle.Events, bundle.RootTypeCatalog())
	}
	flowIDs := make([]string, 0, len(bundle.FlowTree.ByID))
	for flowID := range bundle.FlowTree.ByID {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		flowIDs = append(flowIDs, flowID)
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		view := bundle.FlowTree.ByID[flowID]
		if view == nil {
			continue
		}
		appendEventSchemas(out, bundle, flowID, view.Events, bundle.ResolvedTypeCatalogForFlow(flowID))
	}
	return out
}

func EventSchemaForFlowEvent(bundle *WorkflowContractBundle, flowID, eventType string) (EventSchema, string, bool) {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return EventSchema{}, "", false
	}
	entry, key, ok := bundle.ResolveFlowEventCatalogEntry(flowID, eventType)
	if !ok {
		return EventSchema{}, "", false
	}
	types, found := eventDeclarationTypesForCatalogKey(bundle, key)
	if !found {
		if flowID != "" {
			types = bundle.ResolvedTypeCatalogForFlow(flowID)
		} else {
			types = bundle.RootTypeCatalog()
		}
	}
	return eventSchemaFromCatalogEntry(key, entry, types), key, true
}

func normalizeEventFieldType(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	for _, base := range []string{"string", "integer", "number", "boolean", "object", "array"} {
		if raw == base {
			return base, ""
		}
		if strings.HasPrefix(raw, base+" ") || strings.HasPrefix(raw, base+"(") {
			desc := strings.TrimSpace(strings.TrimPrefix(raw, base))
			desc = strings.TrimLeft(desc, " -:\t")
			desc = strings.TrimSpace(strings.Trim(desc, "()"))
			return base, desc
		}
	}
	return raw, ""
}

func appendEventSchemas(out map[string]EventSchema, bundle *WorkflowContractBundle, flowID string, entries map[string]EventCatalogEntry, types TypeCatalogDocument) {
	for eventType, entry := range entries {
		if bundle != nil {
			if schema, key, ok := EventSchemaForFlowEvent(bundle, flowID, eventType); ok {
				out[key] = schema
				continue
			}
		}
		key := resolvedEventSchemaKey(bundle, flowID, eventType)
		if key == "" {
			continue
		}
		out[key] = eventSchemaFromCatalogEntry(key, entry, types)
	}
}

func resolvedEventSchemaKey(bundle *WorkflowContractBundle, flowID, eventType string) string {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return ""
	}
	if bundle == nil {
		return eventType
	}
	if resolved := strings.TrimSpace(bundle.ResolveFlowEventReference(flowID, eventType)); resolved != "" {
		return resolved
	}
	return eventType
}

func eventDeclarationTypesForCatalogKey(bundle *WorkflowContractBundle, catalogKey string) (TypeCatalogDocument, bool) {
	catalogKey = strings.TrimSpace(catalogKey)
	if bundle == nil || catalogKey == "" {
		return TypeCatalogDocument{}, false
	}
	if bundle.FlowTree.Root != nil {
		if hasResolvedEventSchemaKey(bundle, "", bundle.FlowTree.Root.Events, catalogKey) {
			return bundle.RootTypeCatalog(), true
		}
	} else if hasResolvedEventSchemaKey(bundle, "", bundle.Events, catalogKey) {
		return bundle.RootTypeCatalog(), true
	}
	flowIDs := make([]string, 0, len(bundle.FlowTree.ByID))
	for flowID := range bundle.FlowTree.ByID {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		flowIDs = append(flowIDs, flowID)
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		view := bundle.FlowTree.ByID[flowID]
		if view == nil {
			continue
		}
		if hasResolvedEventSchemaKey(bundle, flowID, view.Events, catalogKey) {
			return bundle.ResolvedTypeCatalogForFlow(flowID), true
		}
	}
	return TypeCatalogDocument{}, false
}

func hasResolvedEventSchemaKey(bundle *WorkflowContractBundle, flowID string, entries map[string]EventCatalogEntry, catalogKey string) bool {
	for eventType := range entries {
		if resolvedEventSchemaKey(bundle, flowID, eventType) == catalogKey {
			return true
		}
	}
	return false
}

func eventSchemaForTypeRef(raw string, types TypeCatalogDocument, seen map[string]struct{}) (map[string]any, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, ""
	}
	if normalized, typeDescription := normalizeEventFieldType(raw); normalized != "" && normalized != raw {
		return eventSchemaForResolvedType(normalized, types, seen), typeDescription
	}
	return eventSchemaForResolvedType(raw, types, seen), ""
}

func eventSchemaForResolvedType(typeRef string, types TypeCatalogDocument, seen map[string]struct{}) map[string]any {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return map[string]any{}
	}
	if isEventListType(typeRef) {
		return map[string]any{
			"type":  "array",
			"items": eventSchemaForResolvedType(eventListItemType(typeRef), types, seen),
		}
	}
	if enumName, ok := eventEnumTypeName(types, typeRef); ok {
		values := make([]any, 0, len(types.Enums[enumName].Values))
		for _, value := range types.Enums[enumName].Values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			values = append(values, value)
		}
		return map[string]any{
			"type": "string",
			"enum": values,
		}
	}
	if namedName, ok := eventNamedTypeName(types, typeRef); ok {
		if _, ok := seen[namedName]; ok {
			return map[string]any{"type": "object"}
		}
		seen[namedName] = struct{}{}
		defer delete(seen, namedName)
		named := types.Types[namedName]
		props := make(map[string]any, len(named.Fields))
		required := make([]string, 0, len(named.Fields))
		fieldNames := make([]string, 0, len(named.Fields))
		for fieldName := range named.Fields {
			fieldNames = append(fieldNames, strings.TrimSpace(fieldName))
		}
		sort.Strings(fieldNames)
		for _, fieldName := range fieldNames {
			if fieldName == "" {
				continue
			}
			spec := named.Fields[fieldName]
			props[fieldName] = eventSchemaForResolvedType(spec.Type, types, seen)
			required = append(required, fieldName)
		}
		return map[string]any{
			"type":                 "object",
			"properties":           props,
			"required":             required,
			"additionalProperties": false,
		}
	}
	switch normalized := strings.ToLower(strings.TrimSpace(eventTypeName(types, typeRef))); normalized {
	case "text", "string":
		return map[string]any{"type": "string"}
	case "integer":
		return map[string]any{"type": "integer"}
	case "numeric", "number":
		return map[string]any{"type": "number"}
	case "boolean":
		return map[string]any{"type": "boolean"}
	case "timestamp":
		return map[string]any{"type": "string", "format": "date-time"}
	case "uuid":
		return map[string]any{"type": "string", "format": "uuid"}
	case "object":
		return map[string]any{"type": "object"}
	case "array":
		return map[string]any{"type": "array"}
	default:
		return map[string]any{"type": typeRef}
	}
}

func eventTypeName(types TypeCatalogDocument, typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	if scalar, ok := types.Scalars[typeRef]; ok {
		return strings.TrimSpace(scalar.Base)
	}
	return typeRef
}

func eventNamedTypeName(types TypeCatalogDocument, typeRef string) (string, bool) {
	typeName := eventTypeName(types, typeRef)
	_, ok := types.Types[typeName]
	return typeName, ok
}

func eventEnumTypeName(types TypeCatalogDocument, typeRef string) (string, bool) {
	typeName := eventTypeName(types, typeRef)
	_, ok := types.Enums[typeName]
	return typeName, ok
}

func isEventListType(typeRef string) bool {
	typeRef = strings.TrimSpace(typeRef)
	return strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">") ||
		strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]") ||
		strings.HasSuffix(typeRef, "[]") ||
		strings.HasPrefix(typeRef, "[]")
}

func eventListItemType(typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">"):
		return strings.TrimSpace(typeRef[len("list<") : len(typeRef)-1])
	case strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]"):
		return strings.TrimSpace(typeRef[1 : len(typeRef)-1])
	case strings.HasSuffix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[:len(typeRef)-2])
	case strings.HasPrefix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[2:])
	default:
		return typeRef
	}
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
