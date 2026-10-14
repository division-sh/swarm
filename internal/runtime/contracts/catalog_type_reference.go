package contracts

import (
	"fmt"
	"sort"
	"strings"
)

// CatalogTypeReference preserves the declaring catalog for a contract type.
// Consumers resolve through this owner instead of reducing named types to dyn.
type CatalogTypeReference struct {
	Type    string
	Catalog TypeCatalogDocument
}

func (r CatalogTypeReference) Empty() bool {
	return strings.TrimSpace(r.Type) == ""
}

func (r CatalogTypeReference) Resolve() (ResolvedCatalogType, error) {
	return r.ResolveReference(r.Type)
}

func (r CatalogTypeReference) ResolveReference(typeRef string) (ResolvedCatalogType, error) {
	return resolveCatalogTypeReference(strings.TrimSpace(typeRef), r.Catalog, map[string]struct{}{})
}

func resolveCatalogTypeReference(typeRef string, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	if typeRef == "" {
		return ResolvedCatalogType{}, catalogReferenceError("catalog type reference is required")
	}
	if err := retiredCatalogListSpelling(typeRef); err != nil {
		return ResolvedCatalogType{}, err
	}
	if isEventListType(typeRef) {
		return resolveCatalogListType(typeRef, catalog, resolving)
	}
	if keyRef, valueRef, ok := parseWave1MapTypeRef(typeRef); ok {
		return resolveCatalogMapType(keyRef, valueRef, catalog, resolving)
	}
	if scalar, ok := catalog.Scalars[typeRef]; ok {
		return resolveCatalogScalarAlias(typeRef, scalar, catalog, resolving)
	}
	if _, ok := catalog.Enums[typeRef]; ok {
		return ResolvedCatalogType{Kind: CatalogTypeText, Name: typeRef}, nil
	}
	if named, ok := catalog.Types[typeRef]; ok {
		return resolveCatalogNamedType(typeRef, named, catalog, resolving)
	}
	return resolveCatalogPrimitive(typeRef)
}

func resolveCatalogListType(typeRef string, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	element, err := resolveCatalogTypeReference(eventListItemType(typeRef), catalog, resolving)
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	return ResolvedCatalogType{Kind: CatalogTypeList, Element: &element}, nil
}

func resolveCatalogMapType(keyRef, valueRef string, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	key, err := resolveCatalogTypeReference(keyRef, catalog, resolving)
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	if err := validateCatalogMapKey(key, keyRef, catalog); err != nil {
		return ResolvedCatalogType{}, err
	}
	value, err := resolveCatalogTypeReference(valueRef, catalog, resolving)
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	return ResolvedCatalogType{Kind: CatalogTypeMap, Key: &key, Value: &value}, nil
}

func resolveCatalogScalarAlias(typeRef string, scalar ScalarTypeDecl, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	base := strings.TrimSpace(scalar.Base)
	if base == "" {
		return ResolvedCatalogType{}, catalogReferenceError(fmt.Sprintf("catalog scalar %q has empty base", typeRef))
	}
	if _, cycle := resolving[typeRef]; cycle {
		return ResolvedCatalogType{}, catalogReferenceError(fmt.Sprintf("catalog scalar alias cycle at %s", typeRef))
	}
	resolving[typeRef] = struct{}{}
	defer delete(resolving, typeRef)
	return resolveCatalogTypeReference(base, catalog, resolving)
}

func resolveCatalogNamedType(typeRef string, named NamedTypeDecl, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	if _, cycle := resolving[typeRef]; cycle {
		return ResolvedCatalogType{}, catalogReferenceError(fmt.Sprintf("recursive catalog type %q is unsupported", typeRef))
	}
	resolving[typeRef] = struct{}{}
	defer delete(resolving, typeRef)
	fieldNames := make([]string, 0, len(named.Fields))
	for fieldName := range named.Fields {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName != "" {
			fieldNames = append(fieldNames, fieldName)
		}
	}
	sort.Strings(fieldNames)
	fields := make([]ResolvedCatalogField, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		field := named.Fields[fieldName]
		resolved, err := resolveCatalogTypeReference(strings.TrimSpace(field.Type), catalog, resolving)
		if err != nil {
			return ResolvedCatalogType{}, fmt.Errorf("named type %s field %s: %w", typeRef, fieldName, err)
		}
		fields = append(fields, ResolvedCatalogField{
			Name:        fieldName,
			TypeRef:     strings.TrimSpace(field.Type),
			Type:        resolved,
			IsOptional:  field.IsOptional,
			Refinements: field.Refinements.Clone(),
		})
	}
	return ResolvedCatalogType{Kind: CatalogTypeObject, Name: typeRef, Fields: fields}, nil
}

func resolveCatalogPrimitive(typeRef string) (ResolvedCatalogType, error) {
	normalized, _ := normalizeEventFieldType(typeRef)
	if normalized == "" {
		normalized = typeRef
	}
	switch strings.ToLower(strings.TrimSpace(normalized)) {
	case "text", "string":
		return ResolvedCatalogType{Kind: CatalogTypeText}, nil
	case "uuid":
		return ResolvedCatalogType{Kind: CatalogTypeText, Format: "uuid"}, nil
	case "timestamp", "timestamptz":
		return ResolvedCatalogType{Kind: CatalogTypeText, Format: "date-time"}, nil
	case "integer", "int", "bigint":
		return ResolvedCatalogType{Kind: CatalogTypeInteger}, nil
	case "numeric", "number", "float", "double", "real":
		return ResolvedCatalogType{Kind: CatalogTypeNumber}, nil
	case "boolean", "bool":
		return ResolvedCatalogType{Kind: CatalogTypeBoolean}, nil
	case "array":
		return ResolvedCatalogType{Kind: CatalogTypeDynamic, Name: strings.ToLower(strings.TrimSpace(normalized)), primitive: "array"}, nil
	case "object":
		return ResolvedCatalogType{Kind: CatalogTypeDynamic, primitive: "object"}, nil
	case "json", "jsonb":
		return ResolvedCatalogType{Kind: CatalogTypeDynamic, primitive: "json"}, nil
	default:
		return ResolvedCatalogType{}, catalogReferenceError(fmt.Sprintf("unknown catalog type %q", typeRef))
	}
}
