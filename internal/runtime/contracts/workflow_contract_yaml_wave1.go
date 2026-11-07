package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

var builtinWave1ScalarTypes = map[string]struct{}{
	"text": {}, "integer": {}, "numeric": {}, "boolean": {}, "timestamp": {}, "uuid": {},
}

var flowConnectFieldOptions = map[string]struct{}{
	"replies_to": {}, "correlation_key": {}, "resolution": {}, "key_from": {},
	"event": {}, "from": {}, "to": {}, "rename": {},
}

var declaredFieldMappingKeys = map[string]struct{}{"type": {}}

func (d *TypeCatalogDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = TypeCatalogDocument{}
		return nil
	}
	doc, err := projectTypeCatalogDocument(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*d = doc
	return nil
}

func projectTypeCatalogDocument(root yamlsource.Value) (TypeCatalogDocument, error) {
	if root.Presence() != yamlsource.PresenceMapping && root.Presence() != yamlsource.PresenceEmptyMapping {
		return TypeCatalogDocument{}, fmt.Errorf("type catalog must be a mapping")
	}
	fields, err := uniqueYAMLMappingFields(root, "type catalog")
	if err != nil {
		return TypeCatalogDocument{}, err
	}
	doc := TypeCatalogDocument{
		Enums: map[string]EnumTypeDecl{},
		Types: map[string]NamedTypeDecl{},
	}
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return TypeCatalogDocument{}, nodeValueError(field.Value, fmt.Errorf("type catalog declares a type with an empty name"))
		}
		if name != field.Name {
			return TypeCatalogDocument{}, nodeValueError(field.Value, fmt.Errorf("type catalog name %q must not have surrounding whitespace", field.Name))
		}
		switch field.Value.Presence() {
		case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
			decl, err := projectNamedTypeDeclaration(field.Value)
			if err != nil {
				return TypeCatalogDocument{}, fmt.Errorf("record %s: %w", name, err)
			}
			decl.definition = field.IntroductionLocation()
			doc.Types[name] = decl
		case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
			decl, err := projectEnumDeclaration(field.Value)
			if err != nil {
				return TypeCatalogDocument{}, fmt.Errorf("enum %s: %w", name, err)
			}
			doc.Enums[name] = decl
		default:
			return TypeCatalogDocument{}, nodeValueError(field.Value, fmt.Errorf("type %s must be a record mapping or an enum sequence; scalar aliases are not supported, use the built-in type directly", name))
		}
	}
	return doc, nil
}

func (e *EnumTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	decl, err := projectEnumDeclaration(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*e = decl
	return nil
}

func projectEnumDeclaration(value yamlsource.Value) (EnumTypeDecl, error) {
	items, err := value.Sequence()
	if err != nil {
		return EnumTypeDecl{}, nodeValueError(value, fmt.Errorf("enum declaration must be a sequence of quoted text literals; set initial values in state.yaml"))
	}
	decl := EnumTypeDecl{Values: make([]string, 0, len(items))}
	for _, item := range items {
		scalar, err := item.Scalar()
		if err != nil || scalar.Tag != "!!str" || scalar.Style&(yamlsource.SingleQuotedStyle|yamlsource.DoubleQuotedStyle) == 0 {
			return EnumTypeDecl{}, nodeValueError(item, fmt.Errorf("enum members are literals; quote them; types.yaml declares no list types, declare lists on fields instead"))
		}
		decl.Values = append(decl.Values, scalar.Value)
	}
	if err := decl.Validate(""); err != nil {
		return EnumTypeDecl{}, nodeValueError(value, err)
	}
	return decl, nil
}

func (n *NamedTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if n == nil {
		return nil
	}
	decl, err := projectNamedTypeDeclaration(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*n = decl
	return nil
}

func projectNamedTypeDeclaration(value yamlsource.Value) (NamedTypeDecl, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return NamedTypeDecl{}, fmt.Errorf("named type declaration must be a mapping")
	}
	fields, err := uniqueYAMLMappingFields(value, "named type declaration")
	if err != nil {
		return NamedTypeDecl{}, err
	}
	decl := NamedTypeDecl{Fields: map[string]TypeFieldSpec{}}
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return NamedTypeDecl{}, fmt.Errorf("named type declaration has an empty field name")
		}
		if name != field.Name {
			return NamedTypeDecl{}, fmt.Errorf("named type field %q must not have surrounding whitespace", field.Name)
		}
		if strings.HasPrefix(name, "_") {
			return NamedTypeDecl{}, nodeValueError(field.Value, fmt.Errorf("%s is not a type field; put descriptions in YAML comments", name))
		}
		spec, err := projectTypeFieldSpec(field.Value)
		if err != nil {
			return NamedTypeDecl{}, fmt.Errorf("field %s: %w", name, err)
		}
		spec.origin.definition = field.IntroductionLocation()
		decl.Fields[name] = spec
	}
	return decl, nil
}

func (f *TypeFieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	spec, err := projectTypeFieldSpec(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*f = spec
	return nil
}

func projectTypeFieldSpec(value yamlsource.Value) (TypeFieldSpec, error) {
	return projectDeclaredFieldType(value, "type field")
}

func projectDeclaredFieldType(value yamlsource.Value, context string) (TypeFieldSpec, error) {
	allowQuoted := false
	if value.Presence() == yamlsource.PresenceMapping || value.Presence() == yamlsource.PresenceEmptyMapping {
		fields, err := uniqueYAMLMappingFields(value, context)
		if err != nil {
			return TypeFieldSpec{}, err
		}
		if err := rejectLongListField(fields, context); err != nil {
			return TypeFieldSpec{}, err
		}
		var typeValue yamlsource.Value
		for _, field := range fields {
			if field.Name != "type" {
				diagnostic := NewUndefinedFieldDiagnostic(context, field.Name, declaredFieldMappingKeys, field)
				diagnostic.Remediation = "Declare the field as its type; only state fields support {type: T, indexed: true}. Put descriptions in YAML comments."
				return TypeFieldSpec{}, diagnostic
			}
			typeValue = field.Value
		}
		value, allowQuoted = typeValue, true
	}
	var typeName string
	var optional bool
	var err error
	switch value.Presence() {
	case yamlsource.PresenceScalar:
		var raw string
		raw, err = listDeclarationScalar(value, context, allowQuoted)
		if err == nil {
			typeName, optional, err = admitEventFieldTypeMarker(raw, context)
		}
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		typeName, err = listDeclarationSequence(value, context)
		if err == nil {
			err = rejectEventTypeOptionalMarker(typeName, context)
		}
	default:
		err = fmt.Errorf("%s type is required", context)
	}
	if err != nil {
		return TypeFieldSpec{}, err
	}
	if err := validateWave1TypeRef(typeName, context); err != nil {
		return TypeFieldSpec{}, err
	}
	return TypeFieldSpec{Type: typeName, IsOptional: optional}, nil
}

func (d *EntityContractsDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = EntityContractsDocument{}
		return nil
	}
	document, err := projectEntityContractsDocument(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*d = document
	return nil
}

func projectEntityContractsDocument(root yamlsource.Value) (EntityContractsDocument, error) {
	return projectStateDeclarations(root, TypeCatalogDocument{})
}

func (e *EntityContract) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	decl, err := projectEntityContract(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*e = decl
	return nil
}

func projectEntityContract(value yamlsource.Value) (EntityContract, error) {
	return projectStateContract(value, TypeCatalogDocument{})
}

func (f *EntityFieldDecl) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	spec, err := projectEntityFieldDecl(yamlsource.ValueFromNode(node))
	if err != nil {
		return err
	}
	*f = spec
	return nil
}

func projectEntityFieldDecl(value yamlsource.Value) (EntityFieldDecl, error) {
	return projectStateField(value, TypeCatalogDocument{})
}

func validateWave1TypeRef(raw, context string) error {
	raw = strings.TrimSpace(raw)
	if err := retiredCatalogListSpelling(raw); err != nil {
		return fmt.Errorf("%s: %w", context, err)
	}
	if err := rejectAlternateListReference(raw, context); err != nil {
		return err
	}
	if raw == "" {
		return fmt.Errorf("%s type is required", context)
	}
	switch strings.ToLower(raw) {
	case "jsonb", "object":
		return fmt.Errorf("%s type %q is unsupported", context, raw)
	}
	if strings.HasPrefix(raw, "Optional<") {
		return fmt.Errorf("%s type %q is not supported by the current type system", context, raw)
	}
	if keyType, valueType, ok := parseWave1MapTypeRef(raw); ok {
		if keyType == "" || valueType == "" {
			return fmt.Errorf("%s map type requires key and value types", context)
		}
		if strings.HasPrefix(keyType, "[") || strings.HasPrefix(strings.ToLower(keyType), "map[") {
			return fmt.Errorf("%s map key type %q is unsupported; map keys must be text, enum or uuid", context, keyType)
		}
		if strings.EqualFold(keyType, "object") || strings.EqualFold(keyType, "jsonb") {
			return fmt.Errorf("%s map key type %q is unsupported; map keys must be text, enum or uuid", context, keyType)
		}
		if strings.EqualFold(valueType, "object") || strings.EqualFold(valueType, "jsonb") {
			return fmt.Errorf("%s map value type %q is unsupported", context, valueType)
		}
		return nil
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]"))
		if inner == "" {
			return fmt.Errorf("%s list type requires an element type", context)
		}
		if strings.EqualFold(inner, "object") || strings.EqualFold(inner, "jsonb") {
			return fmt.Errorf("%s type %q is unsupported", context, raw)
		}
		return nil
	}
	return nil
}

func parseWave1MapTypeRef(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToLower(raw), "map[") {
		return "", "", false
	}
	closeIdx := strings.Index(raw, "]")
	if closeIdx < len("map[]")-1 {
		return "", "", true
	}
	keyType := strings.TrimSpace(raw[len("map["):closeIdx])
	valueType := strings.TrimSpace(raw[closeIdx+1:])
	return keyType, valueType, true
}
