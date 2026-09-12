package contracts

import (
	"fmt"
	"sort"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type EventSchema struct {
	Description    string
	Schema         map[string]any
	CitationFields map[string]CriteriaCitation
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
	citations := map[string]CriteriaCitation{}
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
		var prop map[string]any
		typeDescription := ""
		if field.ExactSchema != nil {
			var err error
			prop, err = field.ExactSchema.Project()
			if err != nil {
				panic(fmt.Sprintf("project admitted event schema %s.%s: %v", eventType, fieldName, err))
			}
		} else {
			prop, typeDescription = eventSchemaForTypeRef(field.Type, types, map[string]struct{}{})
		}
		description := strings.TrimSpace(field.Description)
		if description == "" {
			description = typeDescription
		} else if typeDescription != "" {
			description = typeDescription + ". " + description
		}
		if description != "" {
			prop["description"] = description
		}
		if field.ExactSchema == nil {
			applySchemaRefinements(prop, field.Refinements)
		}
		if strings.TrimSpace(field.Citation.Criteria) != "" || len(field.Citation.AllowedClasses) > 0 {
			citations[fieldName] = CriteriaCitation{
				Criteria:       strings.TrimSpace(field.Citation.Criteria),
				AllowedClasses: normalizeStrings(field.Citation.AllowedClasses),
			}
		}
		properties[fieldName] = prop
	}
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if required := normalizeStrings(entry.Payload.Required); len(required) > 0 {
		schema["required"] = required
	}
	return EventSchema{
		Description:    fmt.Sprintf("Emit %s event", eventType),
		Schema:         schema,
		CitationFields: citations,
	}
}

func EventSchemaRegistryFromBundle(bundle *WorkflowContractBundle) map[string]EventSchema {
	if bundle == nil {
		return map[string]EventSchema{}
	}
	out := map[string]EventSchema{}
	for _, scope := range bundle.compiledEventSchemas {
		for key, schema := range scope.bindings {
			if key == schema.EventName() {
				out[key] = schema.EventSchema()
			}
		}
	}
	appendPlatformEventSchemas(out, bundle.Platform)
	return out
}

func EventSchemaForFlowEvent(bundle *WorkflowContractBundle, flowID, eventType string) (EventSchema, string, bool) {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return EventSchema{}, "", false
	}
	if _, ok := bundle.exactFlowEventDeclarationView(flowID); !ok {
		return EventSchema{}, "", false
	}
	if compiled, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(flowID, eventType); err != nil {
		return EventSchema{}, "", false
	} else if ok {
		return compiled.EventSchema(), compiled.EventName(), true
	}
	entry, key, types, ok := effectiveEventDeclarationForFlowEvent(bundle, flowID, eventType)
	if !ok {
		return EventSchema{}, "", false
	}
	return eventSchemaFromCatalogEntry(key, entry, types), key, true
}

func normalizeEventFieldType(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	for _, base := range []string{"text", "string", "integer", "number", "numeric", "float", "double", "real", "boolean", "timestamp", "uuid", "object", "array"} {
		if raw == base {
			return base, ""
		}
		if strings.HasPrefix(raw, base+" ") || strings.HasPrefix(raw, base+"(") {
			desc := strings.TrimSpace(strings.TrimPrefix(raw, base))
			desc = strings.TrimLeft(desc, " -:\t")
			desc = strings.TrimSpace(strings.Trim(desc, "()"))
			if base == "numeric" && isNumericPrecisionModifier(desc) {
				return base, ""
			}
			return base, desc
		}
	}
	return raw, ""
}

func isNumericPrecisionModifier(value string) bool {
	left, right, ok := strings.Cut(strings.TrimSpace(value), ",")
	if !ok {
		return false
	}
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	for _, part := range []string{left, right} {
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func eventSchemaDeclarationForFlowEvent(bundle *WorkflowContractBundle, flowID, eventType string) (EventCatalogEntry, string, TypeCatalogDocument, bool) {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, false
	}
	entry, key, ok := bundle.resolveAuthoredFlowEventCatalogEntry(flowID, eventType)
	if !ok {
		if platformEntry, platformKey, platformOK := PlatformEventCatalogEntry(bundle.Platform, eventType); platformOK && len(platformEntry.Payload.Properties) > 0 {
			return platformEntry, platformKey, TypeCatalogDocument{}, true
		}
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, false
	}
	if flowID != "" {
		return entry, key, bundle.ResolvedTypeCatalogForFlow(flowID), true
	}
	return entry, key, bundle.RootTypeCatalog(), true
}

func appendPlatformEventSchemas(out map[string]EventSchema, platform PlatformSpecDocument) {
	for _, eventType := range PlatformEventCatalogNames(platform) {
		entry, key, ok := PlatformEventCatalogEntry(platform, eventType)
		if !ok || len(entry.Payload.Properties) == 0 {
			continue
		}
		out[key] = eventSchemaFromCatalogEntry(key, entry, TypeCatalogDocument{})
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

func uniqueNormalizedEventSchemaKeys(values ...string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = normalizedEventSchemaKey(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizedEventSchemaKey(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func eventSchemaForTypeRef(raw string, types TypeCatalogDocument, seen map[string]struct{}) (map[string]any, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, ""
	}
	if normalized, typeDescription := normalizeEventFieldType(raw); normalized != "" && normalized != raw {
		prop := eventSchemaForResolvedType(normalized, types, seen)
		return prop, typeDescription
	}
	return eventSchemaForResolvedType(raw, types, seen), ""
}

func eventSchemaForResolvedType(typeRef string, types TypeCatalogDocument, seen map[string]struct{}) map[string]any {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return map[string]any{}
	}
	if typeRef == runtimefailures.EnvelopeSchemaVersion+" envelope" {
		return runtimefailures.EnvelopeJSONSchema()
	}
	if isEventListType(typeRef) {
		return map[string]any{
			"type":  "array",
			"items": eventSchemaForTypeRefSchema(eventListItemType(typeRef), types, seen),
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
			return map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			}
		}
		seen[namedName] = struct{}{}
		defer delete(seen, namedName)
		resolved, err := (CatalogTypeReference{Type: namedName, Catalog: types}).Resolve()
		if err != nil {
			return map[string]any{}
		}
		props := make(map[string]any, len(resolved.Fields))
		required := make([]string, 0, len(resolved.Fields))
		for _, field := range resolved.Fields {
			prop := eventSchemaForTypeRefSchema(field.TypeRef, types, seen)
			applySchemaRefinements(prop, field.Refinements)
			props[field.Name] = prop
			if !field.IsOptional {
				required = append(required, field.Name)
			}
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
	case "numeric", "number", "float", "double", "real":
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
	case "json", "jsonb":
		return map[string]any{}
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

func eventSchemaForTypeRefSchema(raw string, types TypeCatalogDocument, seen map[string]struct{}) map[string]any {
	schema, _ := eventSchemaForTypeRef(raw, types, seen)
	return schema
}

func applySchemaRefinements(schema map[string]any, refinements SchemaRefinements) {
	if schema == nil || refinements.Empty() {
		return
	}
	if value := strings.TrimSpace(refinements.Pattern); value != "" {
		schema["pattern"] = value
	}
	if min := refinements.Length.Min; min != nil {
		switch strings.TrimSpace(asSchemaString(schema["type"])) {
		case "array":
			schema["minItems"] = *min
		default:
			schema["minLength"] = *min
		}
	}
	if max := refinements.Length.Max; max != nil {
		switch strings.TrimSpace(asSchemaString(schema["type"])) {
		case "array":
			schema["maxItems"] = *max
		default:
			schema["maxLength"] = *max
		}
	}
	if min := refinements.Range.Min; min != nil {
		schema["minimum"] = *min
	}
	if max := refinements.Range.Max; max != nil {
		schema["maximum"] = *max
	}
	if value := strings.TrimSpace(refinements.EqualTo); value != "" {
		schema["x-swarm-equalTo"] = value
	}
}

func asSchemaString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
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
	case strings.HasSuffix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[:len(typeRef)-2])
	case strings.HasPrefix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[2:])
	case strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]"):
		return strings.TrimSpace(typeRef[1 : len(typeRef)-1])
	default:
		return typeRef
	}
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
	case []string:
		return append([]string(nil), typed...)
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
