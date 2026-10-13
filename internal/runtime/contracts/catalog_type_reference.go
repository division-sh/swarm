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
	Kind CatalogTypeKind
	Name string
	// Format preserves primitive string validation through scalar alias resolution.
	Format  string
	Element *ResolvedCatalogType
	Key     *ResolvedCatalogType
	Value   *ResolvedCatalogType
	Fields  []ResolvedCatalogField
	// primitive preserves opaque JSON versus object/array declaration policy
	// without changing the exported executable shape or diagnostic identity.
	primitive string
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
		out.Fields[index].Refinements = field.Refinements.Clone()
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

// StructuralCatalogTypeAssignable compares executable value shape while
// preserving the sole scalar widening accepted by the contract type system.
func StructuralCatalogTypeAssignable(source, target ResolvedCatalogType) bool {
	if source.Kind == CatalogTypeInteger && target.Kind == CatalogTypeNumber {
		return true
	}
	return StructuralCatalogTypesEqual(source, target)
}

// StructuralCatalogTypeSyntax renders one deterministic diagnostic spelling
// when an imported JSON schema has no authored catalog type reference.
func StructuralCatalogTypeSyntax(resolved ResolvedCatalogType) string {
	switch resolved.Kind {
	case CatalogTypeText:
		return "text"
	case CatalogTypeInteger:
		return "integer"
	case CatalogTypeNumber:
		return "numeric"
	case CatalogTypeBoolean:
		return "boolean"
	case CatalogTypeList:
		if resolved.Element == nil {
			return "[dynamic]"
		}
		return "[" + StructuralCatalogTypeSyntax(*resolved.Element) + "]"
	case CatalogTypeMap:
		if resolved.Key == nil || resolved.Value == nil {
			return "map<dynamic,dynamic>"
		}
		return "map<" + StructuralCatalogTypeSyntax(*resolved.Key) + "," + StructuralCatalogTypeSyntax(*resolved.Value) + ">"
	case CatalogTypeObject:
		fields := append([]ResolvedCatalogField(nil), resolved.Fields...)
		sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
		parts := make([]string, 0, len(fields))
		for _, field := range fields {
			suffix := ""
			if field.IsOptional {
				suffix = "?"
			}
			parts = append(parts, field.Name+":"+StructuralCatalogTypeSyntax(field.Type)+suffix)
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return "dynamic"
	}
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

// ResolveMapKey retains the validation domain carried by a string-like key.
func (r CatalogTypeReference) ResolveMapKey() (ResolvedCatalogType, error) {
	resolved, err := r.Resolve()
	if err != nil {
		return ResolvedCatalogType{}, err
	}
	if err := validateCatalogMapKey(resolved, r.Type, r.Catalog); err != nil {
		return ResolvedCatalogType{}, err
	}
	return resolved, nil
}

func validateCatalogMapKey(key ResolvedCatalogType, ref string, catalog TypeCatalogDocument) error {
	_, enum := catalog.Enums[key.Name]
	if key.Kind == CatalogTypeText && (key.PlainText() || key.Format == "uuid" || enum) {
		return nil
	}
	return catalogReferenceError(fmt.Sprintf("map key type %q is unsupported; map keys must be text, enum or uuid", strings.TrimSpace(ref)))
}

func catalogReferenceError(problem string) error {
	return NewExpectedShapeDiagnostic("contract_loader.invalid_type_reference", "", problem, "Declare a supported, nonrecursive catalog type.", ErrInvalidField)
}

func retiredCatalogListSpelling(typeRef string) error {
	var item string
	switch {
	case strings.HasSuffix(typeRef, "[]"):
		item = strings.TrimSpace(strings.TrimSuffix(typeRef, "[]"))
	case strings.HasPrefix(typeRef, "[]"):
		item = strings.TrimSpace(strings.TrimPrefix(typeRef, "[]"))
	default:
		return nil
	}
	if item == "" {
		item = "T"
	}
	return catalogReferenceError(fmt.Sprintf("type reference %q is retired; use [%s]", typeRef, item))
}

// PlainText excludes the UUID, timestamp and enum domains despite their string
// representation. Alias spelling does not change the resolved domain.
func (t ResolvedCatalogType) PlainText() bool {
	return t.Kind == CatalogTypeText && t.Name == "" && t.Format == ""
}

// ComparableDomain preserves declared domains, unlike structural assignment.
func (r CatalogTypeReference) ComparableDomain() string {
	resolved, err := r.Resolve()
	if err != nil {
		return ""
	}
	return catalogComparableDomain(resolved)
}

func catalogComparableDomain(resolved ResolvedCatalogType) string {
	switch resolved.Kind {
	case CatalogTypeList:
		return "list:" + catalogComparableDomain(*resolved.Element)
	case CatalogTypeMap:
		return "map:" + catalogComparableDomain(*resolved.Key) + ":" + catalogComparableDomain(*resolved.Value)
	case CatalogTypeObject:
		return "type:" + resolved.Name
	case CatalogTypeText:
		if resolved.Name != "" {
			return "enum:" + resolved.Name
		}
		switch resolved.Format {
		case "uuid":
			return "uuid"
		case "date-time":
			return "timestamp"
		default:
			return "string"
		}
	case CatalogTypeInteger:
		return "integer"
	case CatalogTypeNumber:
		return "number"
	case CatalogTypeBoolean:
		return "boolean"
	case CatalogTypeDynamic:
		return resolved.primitive
	}
	return ""
}

func resolveCatalogTypeReference(typeRef string, catalog TypeCatalogDocument, resolving map[string]struct{}) (ResolvedCatalogType, error) {
	if typeRef == "" {
		return ResolvedCatalogType{}, catalogReferenceError("catalog type reference is required")
	}
	if err := retiredCatalogListSpelling(typeRef); err != nil {
		return ResolvedCatalogType{}, err
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
		if err := validateCatalogMapKey(key, keyRef, catalog); err != nil {
			return ResolvedCatalogType{}, err
		}
		value, err := resolveCatalogTypeReference(valueRef, catalog, resolving)
		if err != nil {
			return ResolvedCatalogType{}, err
		}
		return ResolvedCatalogType{Kind: CatalogTypeMap, Key: &key, Value: &value}, nil
	}
	if scalar, ok := catalog.Scalars[typeRef]; ok {
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
	if _, ok := catalog.Enums[typeRef]; ok {
		return ResolvedCatalogType{Kind: CatalogTypeText, Name: typeRef}, nil
	}
	if named, ok := catalog.Types[typeRef]; ok {
		if _, cycle := resolving[typeRef]; cycle {
			return ResolvedCatalogType{}, catalogReferenceError(fmt.Sprintf("recursive catalog type %q is unsupported in Wave 1", typeRef))
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
