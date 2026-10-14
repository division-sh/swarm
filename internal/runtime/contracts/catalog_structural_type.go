package contracts

import (
	"sort"
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

// ResolvedCatalogField carries semantic type and presence together. Optionality
// belongs to the containing field edge, never to the field's value type.
type ResolvedCatalogField struct {
	Name        string
	TypeRef     string
	Type        ResolvedCatalogType
	IsOptional  bool
	Refinements SchemaRefinements
}

// Clone returns a detached structural value. Callers may inspect and compose
// resolved types without mutating the compiled owner that produced them.
func (t ResolvedCatalogType) Clone() ResolvedCatalogType {
	out := t
	out.Element = cloneCatalogTypeChild(t.Element)
	out.Key = cloneCatalogTypeChild(t.Key)
	out.Value = cloneCatalogTypeChild(t.Value)
	out.Fields = make([]ResolvedCatalogField, len(t.Fields))
	for index, field := range t.Fields {
		out.Fields[index] = field
		out.Fields[index].Type = field.Type.Clone()
		out.Fields[index].Refinements = field.Refinements.Clone()
	}
	return out
}

func cloneCatalogTypeChild(child *ResolvedCatalogType) *ResolvedCatalogType {
	if child == nil {
		return nil
	}
	value := child.Clone()
	return &value
}

// StructuralCatalogTypesEqual compares executable value shape. Diagnostic and
// provider names are intentionally excluded; field-edge presence is semantic.
func StructuralCatalogTypesEqual(left, right ResolvedCatalogType) bool {
	if left.Kind != right.Kind {
		return false
	}
	switch left.Kind {
	case CatalogTypeList:
		return catalogTypeChildrenEqual(left.Element, right.Element)
	case CatalogTypeMap:
		return left.Key != nil && right.Key != nil && left.Value != nil && right.Value != nil &&
			StructuralCatalogTypesEqual(*left.Key, *right.Key) && StructuralCatalogTypesEqual(*left.Value, *right.Value)
	case CatalogTypeObject:
		return catalogTypeFieldsEqual(left.Fields, right)
	}
	return true
}

func catalogTypeChildrenEqual(left, right *ResolvedCatalogType) bool {
	return left != nil && right != nil && StructuralCatalogTypesEqual(*left, *right)
}

func catalogTypeFieldsEqual(fields []ResolvedCatalogField, right ResolvedCatalogType) bool {
	if len(fields) != len(right.Fields) {
		return false
	}
	for _, leftField := range fields {
		rightField, ok := right.Field(leftField.Name)
		if !ok || leftField.IsOptional != rightField.IsOptional || !StructuralCatalogTypesEqual(leftField.Type, rightField.Type) {
			return false
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
		return catalogTypeObjectSyntax(resolved.Fields)
	default:
		return "dynamic"
	}
}

func catalogTypeObjectSyntax(source []ResolvedCatalogField) string {
	fields := append([]ResolvedCatalogField(nil), source...)
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
