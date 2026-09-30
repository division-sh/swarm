package contracts

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

var builtinWave1ScalarTypes = map[string]struct{}{
	"text":      {},
	"integer":   {},
	"numeric":   {},
	"boolean":   {},
	"timestamp": {},
	"uuid":      {},
}

var projectFlowIngressFields = map[string]struct{}{
	"alias":     {},
	"providers": {},
}

var projectFlowIngressProviderFields = map[string]struct{}{
	"provider":       {},
	"signing_secret": {},
	"admission":      {},
}

var projectFlowIngressAdmissionFields = map[string]struct{}{
	"kind":           {},
	"pack":           {},
	"acknowledge":    {},
	"authentication": {},
	"event":          {},
	"delivery_id":    {},
	"payload":        {},
}

var projectFlowIngressAdmissionPackFields = map[string]struct{}{"id": {}}

var projectFlowIngressAuthenticationFields = map[string]struct{}{
	"kind": {}, "header": {}, "prefix": {}, "encoding": {},
}

var projectFlowIngressDeliveryIDFields = map[string]struct{}{
	"source": {}, "header": {}, "json_path": {},
}

func yamlMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return node.Content[i+1]
		}
	}
	return nil
}

var connectorPackFieldOptions = map[string]struct{}{
	"imports": {},
}

var connectorPackImportFieldOptions = map[string]struct{}{
	"provider": {},
	"tool":     {},
}

var providerTriggerEventFieldOptions = map[string]struct{}{
	"imports": {},
}

var providerTriggerEventImportFieldOptions = map[string]struct{}{
	"provider": {},
	"event":    {},
}

var flowConnectFieldOptions = map[string]struct{}{
	"event":  {},
	"from":   {},
	"to":     {},
	"rename": {},
}

var typeCatalogFieldOptions = map[string]struct{}{
	"scalars": {},
	"enums":   {},
	"types":   {},
}

var typeMetadataFieldOptions = map[string]struct{}{
	"_description": {},
}

var entityMetadataFieldOptions = map[string]struct{}{
	"_description": {},
	"_owner":       {},
}

var schemaLengthRefinementFieldOptions = map[string]struct{}{
	"min": {},
	"max": {},
}

var schemaRangeRefinementFieldOptions = map[string]struct{}{
	"min": {},
	"max": {},
}

func (c ConnectorPackImports) normalized() ConnectorPackImports {
	out := ConnectorPackImports{Imports: make([]ConnectorPackImport, 0, len(c.Imports))}
	for _, item := range c.Imports {
		item = item.normalized()
		if item.Provider == "" && item.Tool == "" {
			continue
		}
		out.Imports = append(out.Imports, item)
	}
	return out
}

func (i ConnectorPackImport) normalized() ConnectorPackImport {
	return ConnectorPackImport{
		Provider: normalizeConnectorPackToken(i.Provider),
		Tool:     strings.TrimSpace(i.Tool),
	}
}

func (p ProviderTriggerEventImports) normalized() ProviderTriggerEventImports {
	out := ProviderTriggerEventImports{Imports: make([]ProviderTriggerEventImport, len(p.Imports))}
	for index, item := range p.Imports {
		out.Imports[index] = item.normalized()
	}
	return out
}

func (i ProviderTriggerEventImport) normalized() ProviderTriggerEventImport {
	return ProviderTriggerEventImport{
		Provider: normalizeConnectorPackToken(i.Provider),
		Event:    strings.TrimSpace(i.Event),
	}
}

func normalizeConnectorPackToken(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = strings.ReplaceAll(raw, " ", "_")
	return strings.Trim(raw, "_")
}

func normalizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

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
	fields, err := uniqueYAMLMappingFields(root, "entity contracts document")
	if err != nil {
		return TypeCatalogDocument{}, err
	}
	doc := TypeCatalogDocument{
		Scalars: map[string]ScalarTypeDecl{},
		Enums:   map[string]EnumTypeDecl{},
		Types:   map[string]NamedTypeDecl{},
	}
	for _, field := range fields {
		key := strings.TrimSpace(field.Name)
		switch key {
		case "":
			continue
		case "scalars":
			if err := field.Value.Project(&doc.Scalars); err != nil {
				return TypeCatalogDocument{}, err
			}
		case "enums":
			if err := field.Value.Project(&doc.Enums); err != nil {
				return TypeCatalogDocument{}, err
			}
		case "types":
			types, err := projectNamedTypeDeclarations(field.Value)
			if err != nil {
				return TypeCatalogDocument{}, err
			}
			doc.Types = types
		default:
			return TypeCatalogDocument{}, NewUndefinedFieldDiagnostic("type catalog", key, typeCatalogFieldOptions)
		}
	}
	enumNames := make([]string, 0, len(doc.Enums))
	for name := range doc.Enums {
		enumNames = append(enumNames, name)
	}
	sort.Strings(enumNames)
	for _, name := range enumNames {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return TypeCatalogDocument{}, fmt.Errorf("type catalog declares an enum with an empty name")
		}
		if trimmed != name {
			return TypeCatalogDocument{}, fmt.Errorf("type catalog enum name %q must not have surrounding whitespace", name)
		}
		if err := doc.Enums[name].Validate(trimmed); err != nil {
			return TypeCatalogDocument{}, err
		}
	}
	return doc, nil
}

func (s *ScalarTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	base, err := decodeScalarStringNode(node)
	if err != nil {
		return err
	}
	if err := validateWave1TypeRef(base, "scalar alias"); err != nil {
		return err
	}
	if !isBuiltinWave1Scalar(base) {
		return fmt.Errorf("RETIRED: scalar alias %q must resolve to a supported built-in scalar", strings.TrimSpace(base))
	}
	s.Base = base
	return nil
}

func (e *EnumTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	values, defaultValue, err := decodeEnumDeclaration(node)
	if err != nil {
		return err
	}
	e.Values = values
	e.Default = defaultValue
	return nil
}

// decodeEnumDeclaration parses the canonical enum mapping form
// (`{values: [...], default: <member>}`). The retired sequence form and the
// scalar shorthand are rejected with teaching errors carrying the
// behavior-preserving codemod: enum defaults are explicit, never implied by
// member order (#1532).
func decodeEnumDeclaration(node *yaml.Node) ([]string, string, error) {
	if node == nil || node.Kind == 0 {
		return nil, "", fmt.Errorf("enum declaration requires the mapping form {values: [...], default: <member>}")
	}
	if node.Kind == yaml.SequenceNode {
		values, err := decodeStringListNode(node)
		if err != nil {
			return nil, "", err
		}
		if len(values) == 0 {
			return nil, "", fmt.Errorf("enum declaration requires at least one value")
		}
		return nil, "", fmt.Errorf("RETIRED: enum declaration uses the sequence form; convert to the mapping form and add default: %s to preserve behavior (e.g. {values: [...], default: %s})", values[0], values[0])
	}
	if node.Kind == yaml.ScalarNode {
		member, _ := decodeScalarStringNode(node)
		if member == "" {
			return nil, "", fmt.Errorf("enum declaration requires the mapping form {values: [...], default: <member>}")
		}
		return nil, "", fmt.Errorf("RETIRED: enum declaration uses the scalar shorthand; convert to the mapping form and add default: %s to preserve behavior (e.g. {values: [%s], default: %s})", member, member, member)
	}
	if node.Kind != yaml.MappingNode {
		return nil, "", fmt.Errorf("enum declaration must be a mapping {values: [...], default: <member>}")
	}
	var values []string
	var defaultValue string
	seen := map[string]struct{}{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if _, duplicate := seen[key]; duplicate {
			return nil, "", fmt.Errorf("enum declaration repeats key %q; each of values and default may appear once", key)
		}
		seen[key] = struct{}{}
		switch key {
		case "values":
			decoded, err := decodeStringListNode(value)
			if err != nil {
				return nil, "", fmt.Errorf("enum declaration values: %w", err)
			}
			values = decoded
		case "default":
			if value.Kind != yaml.ScalarNode {
				return nil, "", fmt.Errorf("enum declaration default must be a scalar member")
			}
			decoded, _ := decodeScalarStringNode(value)
			defaultValue = decoded
		default:
			return nil, "", NewUndefinedFieldDiagnostic("enum declaration", key, enumDeclarationFields)
		}
	}
	if len(values) == 0 {
		return nil, "", fmt.Errorf("enum declaration requires values with at least one member")
	}
	if defaultValue == "" {
		return nil, "", fmt.Errorf("enum declaration requires default; add default: %s to preserve current behavior", values[0])
	}
	if slices.Contains(values, defaultValue) {
		return values, defaultValue, nil
	}
	return nil, "", fmt.Errorf("enum declaration default %q is not a declared member; declared members: %s", defaultValue, strings.Join(values, ", "))
}

var enumDeclarationFields = map[string]struct{}{
	"values":  {},
	"default": {},
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

func projectNamedTypeDeclarations(value yamlsource.Value) (map[string]NamedTypeDecl, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull, yamlsource.PresenceEmptyMapping:
		return map[string]NamedTypeDecl{}, nil
	case yamlsource.PresenceMapping:
	default:
		return nil, fmt.Errorf("types catalog must be a mapping")
	}
	fields, err := uniqueYAMLMappingFields(value, "types catalog")
	if err != nil {
		return nil, err
	}
	out := make(map[string]NamedTypeDecl, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return nil, fmt.Errorf("types catalog declares a named type with an empty name")
		}
		if name != field.Name {
			return nil, fmt.Errorf("types catalog named type %q must not have surrounding whitespace", field.Name)
		}
		decl, err := projectNamedTypeDeclaration(field.Value)
		if err != nil {
			return nil, fmt.Errorf("named type %s: %w", name, err)
		}
		out[name] = decl
	}
	return out, nil
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
			switch name {
			case "_description":
				decl.Description, err = optionalScalarString(field.Value, "type description")
				if err != nil {
					return NamedTypeDecl{}, err
				}
			default:
				return NamedTypeDecl{}, NewUndefinedFieldDiagnostic("type metadata", name, typeMetadataFieldOptions)
			}
			continue
		}
		spec, err := projectTypeFieldSpec(field.Value)
		if err != nil {
			return NamedTypeDecl{}, fmt.Errorf("field %s: %w", name, err)
		}
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
	parsed, err := decodeWave1FieldValue(value, wave1FieldNodeOptions{
		Context:           "type field",
		AllowInitial:      false,
		AllowImmutable:    false,
		AllowUnusedReason: false,
	})
	if err != nil {
		return TypeFieldSpec{}, err
	}
	return TypeFieldSpec{
		Type: parsed.Type, IsOptional: parsed.IsOptional,
		Description: parsed.Description, Refinements: parsed.Refinements,
	}, nil
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
	if root.Presence() != yamlsource.PresenceMapping && root.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("entity contracts document must be a mapping")
	}
	fields, err := root.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(EntityContractsDocument, len(fields))
	for _, field := range fields {
		key := strings.TrimSpace(field.Name)
		if key == "" {
			continue
		}
		if key != field.Name {
			return nil, fmt.Errorf("entity name %q must not have surrounding whitespace", field.Name)
		}
		entity, err := projectEntityContract(field.Value)
		if err != nil {
			return nil, err
		}
		out[key] = entity
	}
	return out, nil
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
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return EntityContract{}, fmt.Errorf("entity contract must be a mapping")
	}
	fields, err := uniqueYAMLMappingFields(value, "entity contract")
	if err != nil {
		return EntityContract{}, err
	}
	decl := EntityContract{Fields: map[string]EntityFieldDecl{}}
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return EntityContract{}, fmt.Errorf("entity contract has an empty field name")
		}
		if name != field.Name {
			return EntityContract{}, fmt.Errorf("entity field %q must not have surrounding whitespace", field.Name)
		}
		if strings.HasPrefix(name, "_") {
			switch name {
			case "_description":
				decl.Description, err = optionalScalarString(field.Value, "entity description")
			case "_owner":
				decl.Owner, err = optionalScalarString(field.Value, "entity owner")
			case "_state_model":
				return EntityContract{}, fmt.Errorf("RETIRED: entity field %q is retired; state authority is implicit from schema.yaml", name)
			default:
				return EntityContract{}, NewUndefinedFieldDiagnostic("entity metadata", name, entityMetadataFieldOptions)
			}
			if err != nil {
				return EntityContract{}, err
			}
			continue
		}
		if name == "state_field" {
			return EntityContract{}, fmt.Errorf("RETIRED: entity field %q is retired; state authority is implicit from schema.yaml", name)
		}
		spec, err := projectEntityFieldDecl(field.Value)
		if err != nil {
			return EntityContract{}, fmt.Errorf("field %s: %w", name, err)
		}
		decl.Fields[name] = spec
	}
	return decl, nil
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
	parsed, err := decodeWave1FieldValue(value, wave1FieldNodeOptions{
		Context:                 "entity field",
		AllowInitial:            true,
		AllowImmutable:          true,
		AllowIndexed:            true,
		AllowUnusedReason:       true,
		AllowUnusedReaderReason: true,
		AllowMaterializeFrom:    true,
		AllowProject:            true,
	})
	if err != nil {
		return EntityFieldDecl{}, err
	}
	return EntityFieldDecl{
		Type: parsed.Type, IsOptional: parsed.IsOptional, Initial: parsed.Initial, Indexed: parsed.Indexed,
		Immutable: parsed.Immutable, Description: parsed.Description,
		Refinements: parsed.Refinements, MaterializeFrom: parsed.MaterializeFrom,
		Project: parsed.Project, UnusedReason: parsed.UnusedReason,
		UnusedReaderReason: parsed.UnusedReaderReason,
	}, nil
}

func decodeWave1FieldValue(value yamlsource.Value, opts wave1FieldNodeOptions) (wave1ParsedFieldNode, error) {
	var field wave1ParsedFieldNode
	switch value.Presence() {
	case yamlsource.PresenceScalar:
		raw, err := requiredLiteralString(value, opts.Context+" type")
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		field.Type, field.IsOptional, err = admitEventFieldTypeMarker(raw, opts.Context)
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if err := validateWave1TypeRef(field.Type, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		return field, nil
	case yamlsource.PresenceSequence:
		values, err := value.Sequence()
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if len(values) != 1 {
			return wave1ParsedFieldNode{}, fmt.Errorf("%s list shorthand requires exactly one element type", opts.Context)
		}
		element, err := requiredLiteralString(values[0], opts.Context+" element type")
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		field.Type = "[" + strings.TrimSpace(element) + "]"
		if err := rejectEventTypeOptionalMarker(field.Type, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if err := validateWave1TypeRef(field.Type, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		return field, nil
	case yamlsource.PresenceMapping:
	case yamlsource.PresenceEmptyMapping:
		return wave1ParsedFieldNode{}, fmt.Errorf("%s type is required", opts.Context)
	default:
		return wave1ParsedFieldNode{}, fmt.Errorf("%s type is required", opts.Context)
	}

	fields, err := uniqueYAMLMappingFields(value, opts.Context)
	if err != nil {
		return wave1ParsedFieldNode{}, err
	}
	allowed := wave1FieldNodeAllowedKeys(opts)
	byName := make(map[string]yamlsource.MappingField, len(fields))
	for _, candidate := range fields {
		key := strings.TrimSpace(candidate.Name)
		if key == "" {
			return wave1ParsedFieldNode{}, fmt.Errorf("%s has an empty field name", opts.Context)
		}
		if key != candidate.Name {
			return wave1ParsedFieldNode{}, fmt.Errorf("%s field %q must not have surrounding whitespace", opts.Context, candidate.Name)
		}
		if _, ok := allowed[key]; !ok && key != "of" {
			if key == "properties" || key == "fields" || key == "shape" {
				return wave1ParsedFieldNode{}, fmt.Errorf("RETIRED: %s inline object declarations are retired; declare a named type in types.yaml", opts.Context)
			}
			return wave1ParsedFieldNode{}, NewUndefinedFieldDiagnostic(opts.Context, key, allowed)
		}
		byName[key] = candidate
	}

	typeField, ok := byName["type"]
	if !ok {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s type is required", opts.Context)
	}
	rawType, err := requiredLiteralString(typeField.Value, opts.Context+" type")
	if err != nil {
		return wave1ParsedFieldNode{}, err
	}
	field.Type, field.IsOptional, err = admitEventFieldTypeMarker(rawType, opts.Context)
	if err != nil {
		return wave1ParsedFieldNode{}, err
	}
	if strings.EqualFold(field.Type, "list") {
		ofField, exists := byName["of"]
		if !exists {
			return wave1ParsedFieldNode{}, fmt.Errorf("RETIRED: %s list declarations require an of: element type", opts.Context)
		}
		element, err := requiredLiteralString(ofField.Value, opts.Context+" list element type")
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		field.Type = "[" + strings.TrimSpace(element) + "]"
	} else if _, exists := byName["of"]; exists {
		return wave1ParsedFieldNode{}, NewUndefinedFieldDiagnostic(opts.Context, "of", allowed)
	}
	if err := rejectEventTypeOptionalMarker(field.Type, opts.Context); err != nil {
		return wave1ParsedFieldNode{}, err
	}
	if err := validateWave1TypeRef(field.Type, opts.Context); err != nil {
		return wave1ParsedFieldNode{}, err
	}

	if candidate, ok := byName["description"]; ok {
		if opts.AllowDefault {
			field.Description, err = schemaValueText(candidate.Value, false)
		} else {
			field.Description, err = optionalScalarString(candidate.Value, opts.Context+" description")
		}
	}
	if err == nil {
		if candidate, ok := byName["pattern"]; ok {
			field.Refinements.Pattern, err = decodeSchemaRefinementPatternValue(candidate.Value)
		}
	}
	if err == nil {
		if candidate, ok := byName["length"]; ok {
			field.Refinements.Length, err = decodeSchemaLengthRefinementValue(candidate.Value)
		}
	}
	if err == nil {
		if candidate, ok := byName["range"]; ok {
			field.Refinements.Range, err = decodeSchemaRangeRefinementValue(candidate.Value)
		}
	}
	if err == nil {
		if candidate, ok := byName["equal_to"]; ok {
			field.Refinements.EqualTo, err = requiredLiteralString(candidate.Value, opts.Context+" equal_to field")
			field.Refinements.EqualTo = strings.TrimSpace(field.Refinements.EqualTo)
		}
	}
	if err != nil {
		return wave1ParsedFieldNode{}, err
	}
	if candidate, ok := byName["citation"]; ok {
		if !opts.AllowCitation {
			return wave1ParsedFieldNode{}, NewUndefinedFieldDiagnostic(opts.Context, "citation", allowed)
		}
		if err := candidate.Value.ValidateUniqueMappings(); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if err := candidate.Value.Project(&field.Citation); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		field.Citation.Criteria = strings.TrimSpace(field.Citation.Criteria)
		field.Citation.AllowedClasses = normalizeStrings(field.Citation.AllowedClasses)
	}
	if candidate, ok := byName["initial"]; ok {
		if candidate.Value.Presence() == yamlsource.PresenceNull {
			return wave1ParsedFieldNode{}, fmt.Errorf("%s initial cannot be null; omit initial for an unassigned field", opts.Context)
		}
		if err := candidate.Value.Project(&field.Initial); err != nil {
			return wave1ParsedFieldNode{}, err
		}
	}
	if candidate, ok := byName["default"]; ok {
		field.HasDefault = true
		if err := candidate.Value.ValidateUniqueMappings(); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if err := candidate.Value.Project(&field.Default); err != nil {
			return wave1ParsedFieldNode{}, err
		}
	}
	if candidate, ok := byName["immutable"]; ok {
		field.Immutable, err = decodeBoolValue(candidate.Value, opts.Context+" immutable")
	}
	if err == nil {
		if candidate, ok := byName["indexed"]; ok {
			field.Indexed, err = decodeBoolValue(candidate.Value, opts.Context+" indexed")
		}
	}
	if err != nil {
		return wave1ParsedFieldNode{}, err
	}
	for name, target := range map[string]*string{
		"_unused_reason": &field.UnusedReason, "_unused_reader_reason": &field.UnusedReaderReason,
		"materialize_from": &field.MaterializeFrom,
	} {
		if candidate, ok := byName[name]; ok {
			*target, err = optionalScalarString(candidate.Value, opts.Context+" "+name)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
		}
	}
	if candidate, ok := byName["project"]; ok {
		field.Project, err = decodeProjectionMapValue(candidate.Value)
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
	}
	if opts.AllowUnusedReason && field.UnusedReason != "" && len(field.UnusedReason) < 10 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s _unused_reason must be at least 10 characters", opts.Context)
	}
	if opts.AllowUnusedReaderReason && field.UnusedReaderReason != "" && len(field.UnusedReaderReason) < 10 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s _unused_reader_reason must be at least 10 characters", opts.Context)
	}
	return field, nil
}

func wave1FieldNodeAllowedKeys(opts wave1FieldNodeOptions) map[string]struct{} {
	allowed := map[string]struct{}{
		"type":        {},
		"description": {},
		"pattern":     {},
		"length":      {},
		"range":       {},
		"equal_to":    {},
	}
	if opts.AllowInitial {
		allowed["initial"] = struct{}{}
	}
	if opts.AllowDefault {
		allowed["default"] = struct{}{}
	}
	if opts.AllowImmutable {
		allowed["immutable"] = struct{}{}
	}
	if opts.AllowIndexed {
		allowed["indexed"] = struct{}{}
	}
	if opts.AllowUnusedReason {
		allowed["_unused_reason"] = struct{}{}
	}
	if opts.AllowUnusedReaderReason {
		allowed["_unused_reader_reason"] = struct{}{}
	}
	if opts.AllowMaterializeFrom {
		allowed["materialize_from"] = struct{}{}
	}
	if opts.AllowProject {
		allowed["project"] = struct{}{}
	}
	if opts.AllowCitation {
		allowed["citation"] = struct{}{}
	}
	return allowed
}

func decodeBoolValue(value yamlsource.Value, context string) (bool, error) {
	if value.Presence() != yamlsource.PresenceScalar {
		return false, fmt.Errorf("%s at %s is %s, want boolean scalar", context, value.Location(), value.Presence())
	}
	scalar, err := value.Scalar()
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(scalar.Value)) {
	case "true", "yes", "on", "conditional":
		return true, nil
	case "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported bool value %q", scalar.Value)
	}
}

func decodeSchemaRefinementPatternValue(value yamlsource.Value) (string, error) {
	pattern, err := requiredLiteralString(value, "pattern")
	if err != nil {
		return "", err
	}
	pattern = strings.TrimSpace(pattern)
	if _, err := regexp.Compile(pattern); err != nil {
		return "", fmt.Errorf("pattern must compile as a regular expression: %w", err)
	}
	return pattern, nil
}

func decodeSchemaLengthRefinementValue(value yamlsource.Value) (SchemaLengthRefinement, error) {
	if value.Presence() != yamlsource.PresenceMapping {
		return SchemaLengthRefinement{}, fmt.Errorf("length must be a mapping with min and/or max")
	}
	fields, err := uniqueYAMLMappingFields(value, "length")
	if err != nil {
		return SchemaLengthRefinement{}, err
	}
	var out SchemaLengthRefinement
	for _, field := range fields {
		if field.Name != "min" && field.Name != "max" {
			return SchemaLengthRefinement{}, nodeValueError(field.Value, NewUndefinedFieldDiagnostic("length", field.Name, schemaLengthRefinementFieldOptions))
		}
		scalar, err := field.Value.Scalar()
		if err != nil || scalar.Tag != "!!int" {
			return SchemaLengthRefinement{}, nodeValueError(field.Value, fmt.Errorf("length bound must be a nonnegative YAML integer"))
		}
		var bound int
		if err := field.Value.Project(&bound); err != nil {
			return SchemaLengthRefinement{}, nodeValueError(field.Value, fmt.Errorf("length bound must fit its integer carrier: %v", err))
		}
		if bound < 0 {
			return SchemaLengthRefinement{}, nodeValueError(field.Value, fmt.Errorf("length bound must be >= 0"))
		}
		switch field.Name {
		case "min":
			out.Min = &bound
		case "max":
			out.Max = &bound
		default:
			return SchemaLengthRefinement{}, NewUndefinedFieldDiagnostic("length", field.Name, schemaLengthRefinementFieldOptions)
		}
	}
	if out.Min == nil && out.Max == nil {
		return SchemaLengthRefinement{}, fmt.Errorf("length must declare min and/or max")
	}
	if out.Min != nil && *out.Min < 0 {
		return SchemaLengthRefinement{}, fmt.Errorf("length min must be >= 0")
	}
	if out.Max != nil && *out.Max < 0 {
		return SchemaLengthRefinement{}, fmt.Errorf("length max must be >= 0")
	}
	if out.Min != nil && out.Max != nil && *out.Min > *out.Max {
		return SchemaLengthRefinement{}, fmt.Errorf("length min must be <= max")
	}
	return out, nil
}

func decodeSchemaRangeRefinementValue(value yamlsource.Value) (SchemaRangeRefinement, error) {
	if value.Presence() != yamlsource.PresenceMapping {
		return SchemaRangeRefinement{}, fmt.Errorf("range must be a mapping with min and/or max")
	}
	fields, err := uniqueYAMLMappingFields(value, "range")
	if err != nil {
		return SchemaRangeRefinement{}, err
	}
	var out SchemaRangeRefinement
	for _, field := range fields {
		if field.Name != "min" && field.Name != "max" {
			return SchemaRangeRefinement{}, nodeValueError(field.Value, NewUndefinedFieldDiagnostic("range", field.Name, schemaRangeRefinementFieldOptions))
		}
		scalar, err := field.Value.Scalar()
		if err != nil || (scalar.Tag != "!!int" && scalar.Tag != "!!float") {
			return SchemaRangeRefinement{}, nodeValueError(field.Value, fmt.Errorf("range bound must be a finite YAML number"))
		}
		var bound float64
		if err := field.Value.Project(&bound); err != nil {
			return SchemaRangeRefinement{}, nodeValueError(field.Value, err)
		}
		if math.IsNaN(bound) || math.IsInf(bound, 0) {
			return SchemaRangeRefinement{}, nodeValueError(field.Value, fmt.Errorf("range bound must be finite"))
		}
		switch field.Name {
		case "min":
			out.Min = &bound
		case "max":
			out.Max = &bound
		default:
			return SchemaRangeRefinement{}, NewUndefinedFieldDiagnostic("range", field.Name, schemaRangeRefinementFieldOptions)
		}
	}
	if out.Min == nil && out.Max == nil {
		return SchemaRangeRefinement{}, fmt.Errorf("range must declare min and/or max")
	}
	if out.Min != nil && out.Max != nil && *out.Min > *out.Max {
		return SchemaRangeRefinement{}, fmt.Errorf("range min must be <= max")
	}
	return out, nil
}

func decodeProjectionMapValue(value yamlsource.Value) (map[string]any, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("entity field project must be a mapping")
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return nil, err
	}
	var out map[string]any
	if err := value.Project(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeProjectionMapNode(node *yaml.Node) (map[string]any, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("entity field project must be a mapping")
	}
	out := make(map[string]any, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		var value any
		switch node.Content[i+1].Kind {
		case yaml.ScalarNode:
			switch strings.TrimSpace(node.Content[i+1].Tag) {
			case "!!int", "!!float", "!!bool":
				if err := node.Content[i+1].Decode(&value); err != nil {
					return nil, err
				}
			default:
				text, err := decodeScalarStringNode(node.Content[i+1])
				if err != nil {
					return nil, err
				}
				value = text
			}
		default:
			if err := node.Content[i+1].Decode(&value); err != nil {
				return nil, err
			}
		}
		out[key] = value
	}
	return out, nil
}

func validateWave1TypeRef(raw, context string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("%s type is required", context)
	}
	switch strings.ToLower(raw) {
	case "jsonb":
		return fmt.Errorf("RETIRED: %s type %q is retired; declare a named type in types.yaml", context, raw)
	case "object":
		return fmt.Errorf("RETIRED: %s type %q is retired; declare a named type in types.yaml", context, raw)
	}
	if strings.HasPrefix(raw, "Optional<") {
		return fmt.Errorf("RETIRED: %s type %q is not supported by the current type system", context, raw)
	}
	if keyType, valueType, ok := parseWave1MapTypeRef(raw); ok {
		if keyType == "" || valueType == "" {
			return fmt.Errorf("%s map type requires key and value types", context)
		}
		if strings.HasPrefix(keyType, "[") || strings.HasPrefix(strings.ToLower(keyType), "map[") {
			return fmt.Errorf("%s map key type %q must be scalar or enum", context, keyType)
		}
		if strings.EqualFold(keyType, "object") || strings.EqualFold(keyType, "jsonb") {
			return fmt.Errorf("RETIRED: %s map key type %q is retired", context, keyType)
		}
		if strings.EqualFold(valueType, "object") || strings.EqualFold(valueType, "jsonb") {
			return fmt.Errorf("RETIRED: %s map value type %q is retired; declare a named type in types.yaml", context, valueType)
		}
		return nil
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]"))
		if inner == "" {
			return fmt.Errorf("%s list type requires an element type", context)
		}
		if strings.EqualFold(inner, "object") || strings.EqualFold(inner, "jsonb") {
			return fmt.Errorf("RETIRED: %s type %q is retired; declare a named type in types.yaml", context, raw)
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

func isBuiltinWave1Scalar(raw string) bool {
	_, ok := builtinWave1ScalarTypes[strings.TrimSpace(raw)]
	return ok
}

type wave1FieldNodeOptions struct {
	Context                 string
	AllowDefault            bool
	AllowInitial            bool
	AllowImmutable          bool
	AllowIndexed            bool
	AllowUnusedReason       bool
	AllowUnusedReaderReason bool
	AllowMaterializeFrom    bool
	AllowProject            bool
	AllowCitation           bool
}

type wave1ParsedFieldNode struct {
	Type               string
	Default            any
	HasDefault         bool
	IsOptional         bool
	Initial            any
	Indexed            bool
	Immutable          bool
	Description        string
	Refinements        SchemaRefinements
	Citation           CriteriaCitation
	MaterializeFrom    string
	Project            map[string]any
	UnusedReason       string
	UnusedReaderReason string
}
