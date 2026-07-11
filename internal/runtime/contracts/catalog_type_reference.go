package contracts

import (
	"fmt"
	"strings"
)

type CatalogTypeKind string

const (
	CatalogTypeDynamic CatalogTypeKind = "dynamic"
	CatalogTypeText    CatalogTypeKind = "text"
	CatalogTypeInteger CatalogTypeKind = "integer"
	CatalogTypeNumber  CatalogTypeKind = "number"
	CatalogTypeBoolean CatalogTypeKind = "boolean"
	CatalogTypeList    CatalogTypeKind = "list"
	CatalogTypeMap     CatalogTypeKind = "map"
	CatalogTypeObject  CatalogTypeKind = "object"
)

// CatalogTypeReference preserves the declaring catalog for a contract type.
// Consumers resolve through this owner instead of reducing named types to dyn.
type CatalogTypeReference struct {
	Type    string
	Catalog TypeCatalogDocument
}

type ResolvedCatalogType struct {
	Kind    CatalogTypeKind
	Name    string
	Element *ResolvedCatalogType
	Key     *ResolvedCatalogType
	Value   *ResolvedCatalogType
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

func (r CatalogTypeReference) NamedFields(name string) (map[string]TypeFieldSpec, bool) {
	decl, ok := r.Catalog.Types[strings.TrimSpace(name)]
	if !ok {
		return nil, false
	}
	return decl.Fields, true
}

func resolveCatalogTypeReference(typeRef string, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	if typeRef == "" {
		return ResolvedCatalogType{Kind: CatalogTypeDynamic}, nil
	}
	if isEventListType(typeRef) {
		element, err := resolveCatalogTypeReference(eventListItemType(typeRef), catalog, resolving)
		if err != nil {
			return ResolvedCatalogType{}, err
		}
		return ResolvedCatalogType{Kind: CatalogTypeList, Element: &element}, nil
	}
	if keyRef, valueRef, ok := parseWave1MapTypeRef(typeRef); ok {
		key, err := resolveCatalogTypeReference(keyRef, catalog, resolving)
		if err != nil {
			return ResolvedCatalogType{}, err
		}
		value, err := resolveCatalogTypeReference(valueRef, catalog, resolving)
		if err != nil {
			return ResolvedCatalogType{}, err
		}
		return ResolvedCatalogType{Kind: CatalogTypeMap, Key: &key, Value: &value}, nil
	}
	if scalar, ok := catalog.Scalars[typeRef]; ok {
		if _, cycle := resolving[typeRef]; cycle {
			return ResolvedCatalogType{}, fmt.Errorf("catalog scalar alias cycle at %s", typeRef)
		}
		resolving[typeRef] = struct{}{}
		defer delete(resolving, typeRef)
		return resolveCatalogTypeReference(strings.TrimSpace(scalar.Base), catalog, resolving)
	}
	if _, ok := catalog.Enums[typeRef]; ok {
		return ResolvedCatalogType{Kind: CatalogTypeText, Name: typeRef}, nil
	}
	if _, ok := catalog.Types[typeRef]; ok {
		return ResolvedCatalogType{Kind: CatalogTypeObject, Name: typeRef}, nil
	}
	normalized, _ := normalizeEventFieldType(typeRef)
	if normalized == "" {
		normalized = typeRef
	}
	switch strings.ToLower(strings.TrimSpace(normalized)) {
	case "text", "string", "uuid", "timestamp", "timestamptz":
		return ResolvedCatalogType{Kind: CatalogTypeText}, nil
	case "integer", "int", "bigint":
		return ResolvedCatalogType{Kind: CatalogTypeInteger}, nil
	case "numeric", "number", "float", "double", "real":
		return ResolvedCatalogType{Kind: CatalogTypeNumber}, nil
	case "boolean", "bool":
		return ResolvedCatalogType{Kind: CatalogTypeBoolean}, nil
	case "object", "json", "jsonb", "array":
		return ResolvedCatalogType{Kind: CatalogTypeDynamic}, nil
	default:
		return ResolvedCatalogType{}, fmt.Errorf("unknown catalog type %q", typeRef)
	}
}

func ResolveEventFieldType(bundle *WorkflowContractBundle, flowID, eventType, field string) (CatalogTypeReference, bool) {
	field = strings.TrimSpace(field)
	if field == "" {
		return CatalogTypeReference{}, false
	}
	entry, _, catalog, ok := eventSchemaDeclarationForFlowEvent(bundle, flowID, eventType)
	if !ok {
		return CatalogTypeReference{}, false
	}
	decl, ok := entry.Payload.Properties[field]
	if !ok || strings.TrimSpace(decl.Type) == "" {
		return CatalogTypeReference{}, false
	}
	return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(catalog)}, true
}
