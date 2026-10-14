package contracts

import (
	"fmt"
	"sort"
	"strings"
)

// ResolveJSONSchemaStructuralType lowers an already-admitted JSON schema into
// the same recursive field vocabulary used by catalog-backed declarations.
// The supplied name is diagnostic/type-provider identity, not value identity.
func ResolveJSONSchemaStructuralType(schema map[string]any, name string) (ResolvedCatalogType, error) {
	return resolveJSONSchemaStructuralType(schema, strings.TrimSpace(name), "")
}

func resolveJSONSchemaStructuralType(schema map[string]any, name, path string) (ResolvedCatalogType, error) {
	typeName, _ := schema["type"].(string)
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "", "json", "jsonb":
		return ResolvedCatalogType{Kind: CatalogTypeDynamic}, nil
	case "string":
		return ResolvedCatalogType{Kind: CatalogTypeText}, nil
	case "integer":
		return ResolvedCatalogType{Kind: CatalogTypeInteger}, nil
	case "number":
		return ResolvedCatalogType{Kind: CatalogTypeNumber}, nil
	case "boolean":
		return ResolvedCatalogType{Kind: CatalogTypeBoolean}, nil
	case "array":
		return resolveJSONSchemaArrayType(schema, name, path)
	case "object":
		return resolveJSONSchemaObjectType(schema, name, path)
	default:
		return ResolvedCatalogType{}, fmt.Errorf("unsupported structural schema type %q", typeName)
	}
}

func resolveJSONSchemaArrayType(schema map[string]any, name, path string) (ResolvedCatalogType, error) {
	items, ok := schema["items"].(map[string]any)
	if !ok {
		return ResolvedCatalogType{Kind: CatalogTypeList, Element: &ResolvedCatalogType{Kind: CatalogTypeDynamic}}, nil
	}
	element, err := resolveJSONSchemaStructuralType(items, name, path+"[]")
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	return ResolvedCatalogType{Kind: CatalogTypeList, Element: &element}, nil
}

func resolveJSONSchemaObjectType(schema map[string]any, name, path string) (ResolvedCatalogType, error) {
	properties, _ := schema["properties"].(map[string]any)
	if additional, ok := schema["additionalProperties"].(map[string]any); ok && len(properties) == 0 {
		value, err := resolveJSONSchemaStructuralType(additional, name, path+"{}")
		if err != nil {
			return ResolvedCatalogType{}, err
		}
		return ResolvedCatalogType{Kind: CatalogTypeMap, Key: &ResolvedCatalogType{Kind: CatalogTypeText}, Value: &value}, nil
	}
	if properties == nil {
		return ResolvedCatalogType{Kind: CatalogTypeDynamic}, nil
	}
	fields, err := resolveJSONSchemaFields(properties, compiledEventRequiredFields(schema), name, path)
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	objectName := name
	if path != "" {
		objectName += "." + path
	}
	return ResolvedCatalogType{Kind: CatalogTypeObject, Name: objectName, Fields: fields}, nil
}

func resolveJSONSchemaFields(properties map[string]any, required map[string]struct{}, name, path string) ([]ResolvedCatalogField, error) {
	fieldNames := make([]string, 0, len(properties))
	for fieldName := range properties {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)
	fields := make([]ResolvedCatalogField, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		fieldSchema, ok := properties[fieldName].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("structural schema field %s is %T, want object", fieldName, properties[fieldName])
		}
		fieldPath := fieldName
		if path != "" {
			fieldPath = path + "." + fieldName
		}
		fieldType, err := resolveJSONSchemaStructuralType(fieldSchema, name, fieldPath)
		if err != nil {
			return nil, fmt.Errorf("structural schema field %s: %w", fieldPath, err)
		}
		_, isRequired := required[fieldName]
		fields = append(fields, ResolvedCatalogField{Name: fieldName, Type: fieldType, IsOptional: !isRequired})
	}
	return fields, nil
}
