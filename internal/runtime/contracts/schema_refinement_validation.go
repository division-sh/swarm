package contracts

import (
	"fmt"
	"sort"
	"strings"
)

func validateWorkflowSchemaRefinements(bundle *WorkflowContractBundle) []error {
	if bundle == nil {
		return nil
	}
	errs := []error{}
	errs = append(errs, validateSchemaRefinementsInTypeCatalog("root types", bundle.RootTypeCatalog())...)
	errs = append(errs, validateSchemaRefinementsInEntities("root entities", bundle.RootTypeCatalog(), bundle.RootEntities)...)
	for _, flowID := range sortedFlowSchemaIDs(bundle.FlowSchemas) {
		types := bundle.ResolvedTypeCatalogForFlow(flowID)
		errs = append(errs, validateSchemaRefinementsInTypeCatalog("flow "+flowID+" types", types)...)
		errs = append(errs, validateSchemaRefinementsInEntities("flow "+flowID+" entities", types, bundle.flowEntities[flowID])...)
	}
	scopedKeys := make([]string, 0, len(bundle.scopedEvents))
	for scopedKey := range bundle.scopedEvents {
		scopedKey = strings.TrimSpace(scopedKey)
		if scopedKey != "" {
			scopedKeys = append(scopedKeys, scopedKey)
		}
	}
	sort.Strings(scopedKeys)
	for _, scopedKey := range scopedKeys {
		source := bundle.scopedEventSources[scopedKey]
		types := bundle.RootTypeCatalog()
		if strings.TrimSpace(source.FlowID) != "" {
			types = bundle.ResolvedTypeCatalogForFlow(source.FlowID)
		}
		entry := bundle.scopedEvents[scopedKey]
		fields := make(map[string]schemaRefinementField, len(entry.Payload.Properties))
		for name, spec := range entry.Payload.Properties {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			fields[name] = schemaRefinementField{
				Name:        name,
				TypeRef:     spec.Type,
				Refinements: spec.Refinements,
			}
		}
		errs = append(errs, validateSchemaRefinementFields("event "+scopedKey+" payload", types, fields)...)
	}
	if len(bundle.scopedEvents) == 0 {
		eventNames := make([]string, 0, len(bundle.Events))
		for name := range bundle.Events {
			name = strings.TrimSpace(name)
			if name != "" {
				eventNames = append(eventNames, name)
			}
		}
		sort.Strings(eventNames)
		for _, name := range eventNames {
			entry := bundle.Events[name]
			fields := make(map[string]schemaRefinementField, len(entry.Payload.Properties))
			for fieldName, spec := range entry.Payload.Properties {
				fieldName = strings.TrimSpace(fieldName)
				if fieldName == "" {
					continue
				}
				fields[fieldName] = schemaRefinementField{
					Name:        fieldName,
					TypeRef:     spec.Type,
					Refinements: spec.Refinements,
				}
			}
			errs = append(errs, validateSchemaRefinementFields("event "+name+" payload", bundle.RootTypeCatalog(), fields)...)
		}
	}
	return errs
}

func validateSchemaRefinementsInTypeCatalog(context string, types TypeCatalogDocument) []error {
	errs := []error{}
	typeNames := make([]string, 0, len(types.Types))
	for name := range types.Types {
		name = strings.TrimSpace(name)
		if name != "" {
			typeNames = append(typeNames, name)
		}
	}
	sort.Strings(typeNames)
	for _, typeName := range typeNames {
		named := types.Types[typeName]
		fields := make(map[string]schemaRefinementField, len(named.Fields))
		for fieldName, spec := range named.Fields {
			fieldName = strings.TrimSpace(fieldName)
			if fieldName == "" {
				continue
			}
			fields[fieldName] = schemaRefinementField{
				Name:        fieldName,
				TypeRef:     spec.Type,
				Refinements: spec.Refinements,
			}
		}
		errs = append(errs, validateSchemaRefinementFields(context+"."+typeName, types, fields)...)
	}
	return errs
}

func validateSchemaRefinementsInEntities(context string, types TypeCatalogDocument, entities EntityContractsDocument) []error {
	errs := []error{}
	entityNames := sortedEntityContractKeys(entities)
	for _, entityName := range entityNames {
		entity := entities[entityName]
		fields := make(map[string]schemaRefinementField, len(entity.Fields))
		for fieldName, spec := range entity.Fields {
			fieldName = strings.TrimSpace(fieldName)
			if fieldName == "" {
				continue
			}
			fields[fieldName] = schemaRefinementField{
				Name:        fieldName,
				TypeRef:     spec.Type,
				Refinements: spec.Refinements,
			}
		}
		errs = append(errs, validateSchemaRefinementFields(context+"."+entityName, types, fields)...)
	}
	return errs
}

type schemaRefinementField struct {
	Name        string
	TypeRef     string
	Refinements SchemaRefinements
}

func validateSchemaRefinementFields(context string, types TypeCatalogDocument, fields map[string]schemaRefinementField) []error {
	errs := []error{}
	names := make([]string, 0, len(fields))
	for name := range fields {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		field := fields[name]
		refs := field.Refinements
		if refs.Empty() {
			continue
		}
		fieldContext := context + "." + name
		kind := schemaRefinementKind(types, field.TypeRef)
		if kind == "unknown" {
			errs = append(errs, fmt.Errorf("%s refinements require a supported schema type, got %q", fieldContext, strings.TrimSpace(field.TypeRef)))
			continue
		}
		if strings.TrimSpace(refs.Pattern) != "" && kind != "string" {
			errs = append(errs, fmt.Errorf("%s pattern refinement requires string/text type, got %q", fieldContext, strings.TrimSpace(field.TypeRef)))
		}
		if !refs.Length.Empty() && kind != "string" && kind != "array" {
			errs = append(errs, fmt.Errorf("%s length refinement requires string/text or list type, got %q", fieldContext, strings.TrimSpace(field.TypeRef)))
		}
		if !refs.Range.Empty() && kind != "integer" && kind != "number" {
			errs = append(errs, fmt.Errorf("%s range refinement requires integer/numeric type, got %q", fieldContext, strings.TrimSpace(field.TypeRef)))
		}
		if target := strings.TrimSpace(refs.EqualTo); target != "" {
			if target == name {
				errs = append(errs, fmt.Errorf("%s equal_to must reference a different sibling field", fieldContext))
				continue
			}
			targetField, ok := fields[target]
			if !ok {
				errs = append(errs, fmt.Errorf("%s equal_to references undeclared sibling field %q", fieldContext, target))
				continue
			}
			left := schemaRefinementComparableType(types, field.TypeRef)
			right := schemaRefinementComparableType(types, targetField.TypeRef)
			if left == "" || right == "" || left != right {
				errs = append(errs, fmt.Errorf("%s equal_to target %q must have the same declared type", fieldContext, target))
			}
		}
	}
	return errs
}

func schemaRefinementKind(types TypeCatalogDocument, typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return "unknown"
	}
	if isEventListType(typeRef) {
		return "array"
	}
	if _, _, ok := parseWave1MapTypeRef(typeRef); ok {
		return "object"
	}
	typeName := eventTypeName(types, typeRef)
	if _, ok := types.Enums[typeName]; ok {
		return "string"
	}
	if _, ok := types.Types[typeName]; ok {
		return "object"
	}
	normalized, _ := normalizeEventFieldType(typeName)
	if normalized == "" {
		normalized = typeName
	}
	switch strings.ToLower(strings.TrimSpace(normalized)) {
	case "text", "string", "uuid":
		return "string"
	case "timestamp":
		return "timestamp"
	case "integer":
		return "integer"
	case "numeric", "number", "float", "double", "real":
		return "number"
	case "array":
		return "array"
	case "object", "json", "jsonb":
		return "object"
	case "boolean":
		return "boolean"
	default:
		return "unknown"
	}
}

func schemaRefinementComparableType(types TypeCatalogDocument, typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return ""
	}
	if isEventListType(typeRef) {
		return "list:" + schemaRefinementComparableType(types, eventListItemType(typeRef))
	}
	if keyType, valueType, ok := parseWave1MapTypeRef(typeRef); ok {
		return "map:" + schemaRefinementComparableType(types, keyType) + ":" + schemaRefinementComparableType(types, valueType)
	}
	typeName := eventTypeName(types, typeRef)
	if _, ok := types.Enums[typeName]; ok {
		return "enum:" + typeName
	}
	if _, ok := types.Types[typeName]; ok {
		return "type:" + typeName
	}
	normalized, _ := normalizeEventFieldType(typeName)
	if normalized == "" {
		normalized = typeName
	}
	switch strings.ToLower(strings.TrimSpace(normalized)) {
	case "text", "string":
		return "string"
	case "integer":
		return "integer"
	case "numeric", "number", "float", "double", "real":
		return "number"
	case "boolean":
		return "boolean"
	case "timestamp":
		return "timestamp"
	case "uuid":
		return "uuid"
	case "array":
		return "array"
	case "object", "json", "jsonb":
		return "object"
	default:
		return ""
	}
}
