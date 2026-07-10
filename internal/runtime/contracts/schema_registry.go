package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
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
		applySchemaRefinements(prop, field.Refinements)
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
	if required := normalizeStrings(entry.Required); len(required) > 0 {
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
	for eventType, schema := range bundle.GeneratedActivityEventSchemas() {
		out[eventType] = schema
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
	entry, key, types, ok := eventSchemaDeclarationForFlowEvent(bundle, flowID, eventType)
	if !ok {
		if schema, generatedOK := bundle.GeneratedActivityEventSchemas()[eventidentity.Normalize(eventType)]; generatedOK {
			return schema, eventidentity.Normalize(eventType), true
		}
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

func eventSchemaDeclarationForFlowEvent(bundle *WorkflowContractBundle, flowID, eventType string) (EventCatalogEntry, string, TypeCatalogDocument, bool) {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, false
	}
	if bundle.FlowTree.Root == nil {
		entry, key, ok := bundle.ResolveFlowEventCatalogEntry(flowID, eventType)
		if !ok {
			if platformEntry, platformKey, platformOK := PlatformEventCatalogEntry(bundle.Platform, eventType); platformOK && len(platformEntry.Payload.Properties) > 0 {
				return platformEntry, platformKey, TypeCatalogDocument{}, true
			}
			return EventCatalogEntry{}, "", TypeCatalogDocument{}, false
		}
		return entry, key, bundle.RootTypeCatalog(), true
	}

	targetKeys := eventSchemaLookupKeys(bundle, flowID, eventType)
	for _, declaration := range eventSchemaDeclarationPath(bundle, flowID) {
		if entry, resolvedKey, ok := eventSchemaDeclarationEntry(bundle, declaration, targetKeys); ok {
			return entry, resolvedKey, declaration.types, true
		}
	}
	for _, declaration := range eventSchemaAllDeclarationScopes(bundle) {
		if entry, resolvedKey, ok := eventSchemaDeclarationEntry(bundle, declaration, targetKeys); ok {
			return entry, resolvedKey, declaration.types, true
		}
	}

	entry, key, ok := bundle.ResolveFlowEventCatalogEntry(flowID, eventType)
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

type eventSchemaDeclarationScope struct {
	flowID string
	view   *FlowContractView
	types  TypeCatalogDocument
}

func eventSchemaDeclarationPath(bundle *WorkflowContractBundle, flowID string) []eventSchemaDeclarationScope {
	if bundle == nil || bundle.FlowTree.Root == nil {
		return nil
	}
	var path []*FlowContractView
	if strings.TrimSpace(flowID) == "" {
		path = []*FlowContractView{bundle.FlowTree.Root}
	} else {
		path = flowmodel.CollectPathByID(bundle.FlowTree.Root, flowID, func(view *FlowContractView) string {
			if view == nil {
				return ""
			}
			return view.Paths.ID
		}, flowViewChildren)
	}
	if len(path) == 0 {
		return nil
	}
	out := make([]eventSchemaDeclarationScope, 0, len(path))
	for i, view := range path {
		if view == nil {
			continue
		}
		scopeFlowID := ""
		types := bundle.RootTypeCatalog()
		if i > 0 {
			scopeFlowID = strings.TrimSpace(view.Paths.ID)
			types = bundle.ResolvedTypeCatalogForFlow(scopeFlowID)
		}
		out = append(out, eventSchemaDeclarationScope{
			flowID: scopeFlowID,
			view:   view,
			types:  types,
		})
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func eventSchemaAllDeclarationScopes(bundle *WorkflowContractBundle) []eventSchemaDeclarationScope {
	if bundle == nil || bundle.FlowTree.Root == nil {
		return nil
	}
	out := []eventSchemaDeclarationScope{{
		view:  bundle.FlowTree.Root,
		types: bundle.RootTypeCatalog(),
	}}
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
		out = append(out, eventSchemaDeclarationScope{
			flowID: flowID,
			view:   view,
			types:  bundle.ResolvedTypeCatalogForFlow(flowID),
		})
	}
	return out
}

func eventSchemaDeclarationEntry(bundle *WorkflowContractBundle, declaration eventSchemaDeclarationScope, targetKeys []string) (EventCatalogEntry, string, bool) {
	if declaration.view == nil {
		return EventCatalogEntry{}, "", false
	}
	for localKey, entry := range declaration.view.Events {
		resolvedKey := resolvedEventSchemaKey(bundle, declaration.flowID, localKey)
		for _, targetKey := range targetKeys {
			if normalizedEventSchemaKey(resolvedKey) != normalizedEventSchemaKey(targetKey) {
				continue
			}
			return entry, resolvedKey, true
		}
	}
	return EventCatalogEntry{}, "", false
}

func eventSchemaLookupKeys(bundle *WorkflowContractBundle, flowID, eventType string) []string {
	keys := []string{
		eventType,
		resolvedEventSchemaKey(bundle, flowID, eventType),
	}
	if strings.TrimSpace(flowID) != "" {
		keys = append(keys, eventSchemaScopedLeafKey(bundle, flowID, eventType))
	}
	if key := instanceScopedEventSchemaKey(bundle, eventType); key != "" {
		keys = append(keys, key)
	}
	return uniqueNormalizedEventSchemaKeys(keys...)
}

func eventSchemaScopedLeafKey(bundle *WorkflowContractBundle, flowID, eventType string) string {
	eventType = normalizedEventSchemaKey(eventType)
	if eventType == "" || !strings.Contains(eventType, "/") {
		return ""
	}
	flowPath := ""
	if bundle != nil {
		flowPath = normalizedEventSchemaKey(bundle.FlowPath(flowID))
	}
	if flowPath == "" {
		flowPath = normalizedEventSchemaKey(flowID)
	}
	if flowPath == "" || !strings.HasPrefix(eventType, flowPath+"/") {
		return ""
	}
	if idx := strings.LastIndex(eventType, "/"); idx >= 0 && idx+1 < len(eventType) {
		return strings.TrimSpace(eventType[idx+1:])
	}
	return ""
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

func instanceScopedEventSchemaKey(bundle *WorkflowContractBundle, eventType string) string {
	if bundle == nil {
		return ""
	}
	eventType = normalizedEventSchemaKey(eventType)
	idx := strings.LastIndex(eventType, "/")
	if idx <= 0 || idx+1 >= len(eventType) {
		return ""
	}
	eventPath := normalizedEventSchemaKey(eventType[:idx])
	if eventPath == "" || eventSchemaFlowIDForPath(bundle, eventPath) != "" {
		return ""
	}
	semanticPath := eventSchemaSemanticScopeFromInstancePath(eventPath)
	if semanticPath == "" {
		return ""
	}
	flowID := eventSchemaFlowIDForPath(bundle, semanticPath)
	if flowID == "" {
		return ""
	}
	return resolvedEventSchemaKey(bundle, flowID, eventType[idx+1:])
}

func eventSchemaFlowIDForPath(bundle *WorkflowContractBundle, flowPath string) string {
	flowPath = normalizedEventSchemaKey(flowPath)
	if bundle == nil || flowPath == "" {
		return ""
	}
	if view := bundle.FlowTree.ByPath[flowPath]; view != nil {
		return strings.TrimSpace(view.Paths.ID)
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
		if normalizedEventSchemaKey(view.Path) == flowPath {
			return flowID
		}
	}
	return ""
}

func eventSchemaSemanticScopeFromInstancePath(instancePath string) string {
	instancePath = normalizedEventSchemaKey(instancePath)
	if instancePath == "" {
		return ""
	}
	idx := strings.LastIndex(instancePath, "/")
	if idx <= 0 {
		return ""
	}
	return normalizedEventSchemaKey(instancePath[:idx])
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
	nullable := eventFieldTypeAllowsNull(raw)
	if normalized, typeDescription := normalizeEventFieldType(raw); normalized != "" && normalized != raw {
		prop := eventSchemaForResolvedType(normalized, types, seen)
		if nullable {
			prop["nullable"] = true
		}
		return prop, typeDescription
	}
	prop := eventSchemaForResolvedType(raw, types, seen)
	if nullable {
		prop["nullable"] = true
	}
	return prop, ""
}

func eventFieldTypeAllowsNull(raw string) bool {
	lower := strings.ToLower(strings.TrimSpace(raw))
	return strings.Contains(lower, "nullable") || strings.Contains(lower, "null until")
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
			prop := eventSchemaForTypeRefSchema(spec.Type, types, seen)
			applySchemaRefinements(prop, spec.Refinements)
			props[fieldName] = prop
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
			Description:    strings.TrimSpace(schema.Description),
			Schema:         cloneEventSchemaMap(schema.Schema),
			CitationFields: cloneCriteriaCitationMap(schema.CitationFields),
		}
	}
	return out
}

func cloneCriteriaCitationMap(in map[string]CriteriaCitation) map[string]CriteriaCitation {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]CriteriaCitation, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = CriteriaCitation{
			Criteria:       strings.TrimSpace(value.Criteria),
			AllowedClasses: normalizeStrings(value.AllowedClasses),
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
