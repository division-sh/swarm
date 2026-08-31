package contracts

import (
	"fmt"
	"sort"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
	Fields  []ResolvedCatalogField
}

// Clone returns a detached structural value. Callers may inspect and compose
// resolved types without mutating the compiled owner that produced them.
func (t ResolvedCatalogType) Clone() ResolvedCatalogType {
	out := t
	if t.Element != nil {
		value := t.Element.Clone()
		out.Element = &value
	}
	if t.Key != nil {
		value := t.Key.Clone()
		out.Key = &value
	}
	if t.Value != nil {
		value := t.Value.Clone()
		out.Value = &value
	}
	out.Fields = make([]ResolvedCatalogField, len(t.Fields))
	for index, field := range t.Fields {
		out.Fields[index] = field
		out.Fields[index].Type = field.Type.Clone()
	}
	return out
}

// StructuralCatalogTypesEqual compares executable value shape. Diagnostic and
// provider names are intentionally excluded; field-edge presence is semantic.
func StructuralCatalogTypesEqual(left, right ResolvedCatalogType) bool {
	if left.Kind != right.Kind {
		return false
	}
	switch left.Kind {
	case CatalogTypeList:
		return left.Element != nil && right.Element != nil && StructuralCatalogTypesEqual(*left.Element, *right.Element)
	case CatalogTypeMap:
		return left.Key != nil && right.Key != nil && left.Value != nil && right.Value != nil &&
			StructuralCatalogTypesEqual(*left.Key, *right.Key) && StructuralCatalogTypesEqual(*left.Value, *right.Value)
	case CatalogTypeObject:
		if len(left.Fields) != len(right.Fields) {
			return false
		}
		for _, leftField := range left.Fields {
			rightField, ok := right.Field(leftField.Name)
			if !ok || leftField.IsOptional != rightField.IsOptional || !StructuralCatalogTypesEqual(leftField.Type, rightField.Type) {
				return false
			}
		}
	}
	return true
}

// ResolvedCatalogField carries semantic type and presence together. Optionality
// belongs to the containing field edge, never to the field's value type.
type ResolvedCatalogField struct {
	Name        string
	TypeRef     string
	Type        ResolvedCatalogType
	IsOptional  bool
	Refinements SchemaRefinements
}

func (t ResolvedCatalogType) Field(name string) (ResolvedCatalogField, bool) {
	name = strings.TrimSpace(name)
	for _, field := range t.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return ResolvedCatalogField{}, false
}

// FieldPath resolves a dot-separated field path through the recursive
// structural owner. It never interprets map keys as named-record fields.
func (t ResolvedCatalogType) FieldPath(raw string) (ResolvedCatalogField, bool) {
	segments := strings.Split(strings.TrimSpace(raw), ".")
	current := t
	var field ResolvedCatalogField
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" || current.Kind != CatalogTypeObject {
			return ResolvedCatalogField{}, false
		}
		var ok bool
		field, ok = current.Field(segment)
		if !ok {
			return ResolvedCatalogField{}, false
		}
		current = field.Type
	}
	return field, strings.TrimSpace(raw) != ""
}

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
		items, ok := schema["items"].(map[string]any)
		if !ok {
			return ResolvedCatalogType{Kind: CatalogTypeList, Element: &ResolvedCatalogType{Kind: CatalogTypeDynamic}}, nil
		}
		element, err := resolveJSONSchemaStructuralType(items, name, path+"[]")
		if err != nil {
			return ResolvedCatalogType{}, err
		}
		return ResolvedCatalogType{Kind: CatalogTypeList, Element: &element}, nil
	case "object":
		properties, _ := schema["properties"].(map[string]any)
		if properties == nil {
			return ResolvedCatalogType{Kind: CatalogTypeDynamic}, nil
		}
		required := compiledEventRequiredFields(schema)
		fieldNames := make([]string, 0, len(properties))
		for fieldName := range properties {
			fieldNames = append(fieldNames, fieldName)
		}
		sort.Strings(fieldNames)
		fields := make([]ResolvedCatalogField, 0, len(fieldNames))
		for _, fieldName := range fieldNames {
			fieldSchema, ok := properties[fieldName].(map[string]any)
			if !ok {
				return ResolvedCatalogType{}, fmt.Errorf("structural schema field %s is %T, want object", fieldName, properties[fieldName])
			}
			fieldPath := fieldName
			if path != "" {
				fieldPath = path + "." + fieldName
			}
			fieldType, err := resolveJSONSchemaStructuralType(fieldSchema, name, fieldPath)
			if err != nil {
				return ResolvedCatalogType{}, fmt.Errorf("structural schema field %s: %w", fieldPath, err)
			}
			_, isRequired := required[fieldName]
			fields = append(fields, ResolvedCatalogField{
				Name:       fieldName,
				Type:       fieldType,
				IsOptional: !isRequired,
			})
		}
		objectName := name
		if path != "" {
			objectName += "." + path
		}
		return ResolvedCatalogType{Kind: CatalogTypeObject, Name: objectName, Fields: fields}, nil
	default:
		return ResolvedCatalogType{}, fmt.Errorf("unsupported structural schema type %q", typeName)
	}
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
	if named, ok := catalog.Types[typeRef]; ok {
		if _, cycle := resolving[typeRef]; cycle {
			return ResolvedCatalogType{Kind: CatalogTypeObject, Name: typeRef}, nil
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
				Refinements: field.Refinements,
			})
		}
		return ResolvedCatalogType{Kind: CatalogTypeObject, Name: typeRef, Fields: fields}, nil
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
	entry, _, catalog, ok := effectiveEventDeclarationForFlowEvent(bundle, flowID, eventType)
	if !ok {
		return CatalogTypeReference{}, false
	}
	decl, ok := entry.Payload.Properties[field]
	if !ok || strings.TrimSpace(decl.Type) == "" {
		return CatalogTypeReference{}, false
	}
	return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(catalog)}, true
}

func ResolveExecutableNodeEventFieldType(bundle *WorkflowContractBundle, node runtimeidentity.ExecutableNode, eventType, field string) (CatalogTypeReference, bool) {
	field = strings.TrimSpace(field)
	if bundle == nil || !node.Valid() || field == "" {
		return CatalogTypeReference{}, false
	}
	entry, _, catalog, ok := bundle.resolveEffectiveExecutableNodeEventDeclaration(node, eventType)
	if !ok {
		entry, _, ok = PlatformEventCatalogEntry(bundle.Platform, eventType)
		catalog = bundle.RootTypeCatalog()
	}
	if !ok {
		return CatalogTypeReference{}, false
	}
	decl, ok := entry.Payload.Properties[field]
	if !ok || strings.TrimSpace(decl.Type) == "" {
		return CatalogTypeReference{}, false
	}
	return CatalogTypeReference{
		Type:    strings.TrimSpace(decl.Type),
		Catalog: cloneTypeCatalogDocument(catalog),
	}, true
}
