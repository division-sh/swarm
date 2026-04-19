package contracts

import (
	"fmt"
	"strings"

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

func (p *ProjectPackageDocument) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	var aux struct {
		Name            string              `yaml:"name"`
		Version         string              `yaml:"version"`
		PlatformVersion string              `yaml:"platform_version"`
		Author          string              `yaml:"author"`
		Description     string              `yaml:"description"`
		Flows           []ProjectFlowRef    `yaml:"flows"`
		Packages        []ProjectPackageRef `yaml:"packages"`
		Children        []ProjectPackageRef `yaml:"children"`
		Subpackages     []ProjectPackageRef `yaml:"subpackages"`
		Handoffs        []ProjectHandoff    `yaml:"handoffs"`
		EntitySchema    EntitySchema        `yaml:"entity_schema"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*p = ProjectPackageDocument{
		Name:            aux.Name,
		Version:         aux.Version,
		PlatformVersion: aux.PlatformVersion,
		Author:          aux.Author,
		Description:     aux.Description,
		Flows:           append([]ProjectFlowRef(nil), aux.Flows...),
		Packages:        append([]ProjectPackageRef(nil), aux.Packages...),
		Children:        append([]ProjectPackageRef(nil), aux.Children...),
		Subpackages:     append([]ProjectPackageRef(nil), aux.Subpackages...),
		Handoffs:        append([]ProjectHandoff(nil), aux.Handoffs...),
		EntitySchema:    aux.EntitySchema,
	}
	p.UsesLegacyEntitySchema = hasYAMLMappingKey(node, "entity_schema")
	return nil
}

func (d *TypeCatalogDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = TypeCatalogDocument{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("type catalog must be a mapping")
	}
	doc := TypeCatalogDocument{
		Scalars: map[string]ScalarTypeDecl{},
		Enums:   map[string]EnumTypeDecl{},
		Types:   map[string]NamedTypeDecl{},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "scalars":
			if err := value.Decode(&doc.Scalars); err != nil {
				return err
			}
		case "enums":
			if err := value.Decode(&doc.Enums); err != nil {
				return err
			}
		case "types":
			if err := value.Decode(&doc.Types); err != nil {
				return err
			}
		default:
			return fmt.Errorf("UNDEFINED-FIELD: type catalog field %q not in platform spec", key)
		}
	}
	*d = doc
	return nil
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
		return fmt.Errorf("RETIRED: scalar alias %q must resolve to a built-in Wave 1 scalar", strings.TrimSpace(base))
	}
	s.Base = base
	return nil
}

func (e *EnumTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	values, err := decodeStringListNode(node)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return fmt.Errorf("enum declaration requires at least one value")
	}
	e.Values = values
	return nil
}

func (n *NamedTypeDecl) UnmarshalYAML(node *yaml.Node) error {
	if n == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*n = NamedTypeDecl{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("named type declaration must be a mapping")
	}
	decl := NamedTypeDecl{Fields: map[string]TypeFieldSpec{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" {
			continue
		}
		if strings.HasPrefix(key, "_") {
			switch key {
			case "_description":
				text, err := decodeScalarStringNode(value)
				if err != nil {
					return err
				}
				decl.Description = text
			default:
				return fmt.Errorf("UNDEFINED-FIELD: type metadata field %q not in platform spec", key)
			}
			continue
		}
		var field TypeFieldSpec
		if err := value.Decode(&field); err != nil {
			return err
		}
		decl.Fields[key] = field
	}
	*n = decl
	return nil
}

func (f *TypeFieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	parsed, err := decodeWave1FieldNode(node, wave1FieldNodeOptions{
		Context:           "type field",
		AllowInitial:      false,
		AllowImmutable:    false,
		AllowUnusedReason: false,
	})
	if err != nil {
		return err
	}
	f.Type = parsed.Type
	f.Description = parsed.Description
	return nil
}

func (d *EntityContractsDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = EntityContractsDocument{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("entity contracts document must be a mapping")
	}
	out := make(EntityContractsDocument, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		var entity EntityContract
		if err := node.Content[i+1].Decode(&entity); err != nil {
			return err
		}
		out[key] = entity
	}
	*d = out
	return nil
}

func (e *EntityContract) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*e = EntityContract{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("entity contract must be a mapping")
	}
	decl := EntityContract{Fields: map[string]EntityFieldDecl{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" {
			continue
		}
		if strings.HasPrefix(key, "_") {
			switch key {
			case "_description":
				text, err := decodeScalarStringNode(value)
				if err != nil {
					return err
				}
				decl.Description = text
			case "_owner":
				text, err := decodeScalarStringNode(value)
				if err != nil {
					return err
				}
				decl.Owner = text
			case "_state_model":
				return fmt.Errorf("RETIRED: entity field %q is retired; state authority is implicit from schema.yaml", key)
			default:
				return fmt.Errorf("UNDEFINED-FIELD: entity metadata field %q not in platform spec", key)
			}
			continue
		}
		if key == "state_field" {
			return fmt.Errorf("RETIRED: entity field %q is retired; state authority is implicit from schema.yaml", key)
		}
		var field EntityFieldDecl
		if err := value.Decode(&field); err != nil {
			return err
		}
		decl.Fields[key] = field
	}
	*e = decl
	return nil
}

func (f *EntityFieldDecl) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	parsed, err := decodeWave1FieldNode(node, wave1FieldNodeOptions{
		Context:           "entity field",
		AllowInitial:      true,
		AllowImmutable:    true,
		AllowUnusedReason: true,
	})
	if err != nil {
		return err
	}
	f.Type = parsed.Type
	f.Initial = parsed.Initial
	f.Immutable = parsed.Immutable
	f.Description = parsed.Description
	f.UnusedReason = parsed.UnusedReason
	return nil
}

func decodeWave1FieldNode(node *yaml.Node, opts wave1FieldNodeOptions) (wave1ParsedFieldNode, error) {
	if node == nil || node.Kind == 0 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s type is required", opts.Context)
	}
	switch node.Kind {
	case yaml.ScalarNode:
		typ, err := decodeScalarStringNode(node)
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if err := validateWave1TypeRef(typ, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		return wave1ParsedFieldNode{Type: typ}, nil
	case yaml.SequenceNode:
		values, err := decodeStringListNode(node)
		if err != nil {
			return wave1ParsedFieldNode{}, err
		}
		if len(values) != 1 {
			return wave1ParsedFieldNode{}, fmt.Errorf("%s list shorthand requires exactly one element type", opts.Context)
		}
		typ := "[" + strings.TrimSpace(values[0]) + "]"
		if err := validateWave1TypeRef(typ, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		return wave1ParsedFieldNode{Type: typ}, nil
	case yaml.MappingNode:
	default:
		return wave1ParsedFieldNode{}, fmt.Errorf("unsupported %s yaml node kind %d", opts.Context, node.Kind)
	}

	allowed := map[string]struct{}{
		"type":        {},
		"description": {},
	}
	if opts.AllowInitial {
		allowed["initial"] = struct{}{}
	}
	if opts.AllowImmutable {
		allowed["immutable"] = struct{}{}
	}
	if opts.AllowUnusedReason {
		allowed["_unused_reason"] = struct{}{}
	}

	var field wave1ParsedFieldNode
	var listOf string
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			switch key {
			case "properties", "fields", "shape":
				return wave1ParsedFieldNode{}, fmt.Errorf("RETIRED: %s inline object declarations are retired; declare a named type in types.yaml", opts.Context)
			case "of":
				listValue, err := decodeScalarStringNode(value)
				if err != nil {
					return wave1ParsedFieldNode{}, err
				}
				listOf = listValue
				continue
			case "initial", "immutable", "_unused_reason":
				return wave1ParsedFieldNode{}, fmt.Errorf("UNDEFINED-FIELD: %s field %q not in platform spec", opts.Context, key)
			default:
				return wave1ParsedFieldNode{}, fmt.Errorf("UNDEFINED-FIELD: %s field %q not in platform spec", opts.Context, key)
			}
		}
		switch key {
		case "type":
			typ, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Type = typ
		case "description":
			text, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Description = text
		case "initial":
			var initial any
			if err := value.Decode(&initial); err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Initial = initial
		case "immutable":
			immutable, err := decodeBoolNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.Immutable = immutable
		case "_unused_reason":
			text, err := decodeScalarStringNode(value)
			if err != nil {
				return wave1ParsedFieldNode{}, err
			}
			field.UnusedReason = text
		}
	}
	if strings.EqualFold(strings.TrimSpace(field.Type), "list") {
		if strings.TrimSpace(listOf) == "" {
			return wave1ParsedFieldNode{}, fmt.Errorf("RETIRED: %s list declarations require an of: element type", opts.Context)
		}
		if err := validateWave1TypeRef(listOf, opts.Context); err != nil {
			return wave1ParsedFieldNode{}, err
		}
		field.Type = fmt.Sprintf("[%s]", strings.TrimSpace(listOf))
	}
	if err := validateWave1TypeRef(field.Type, opts.Context); err != nil {
		return wave1ParsedFieldNode{}, err
	}
	if opts.AllowUnusedReason && strings.TrimSpace(field.UnusedReason) != "" && len(strings.TrimSpace(field.UnusedReason)) < 10 {
		return wave1ParsedFieldNode{}, fmt.Errorf("%s _unused_reason must be at least 10 characters", opts.Context)
	}
	return field, nil
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
		return fmt.Errorf("RETIRED: %s type %q is not part of the Wave 1 type system", context, raw)
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

func isBuiltinWave1Scalar(raw string) bool {
	_, ok := builtinWave1ScalarTypes[strings.TrimSpace(raw)]
	return ok
}

func buildFlatEventPayloadSpec(node *yaml.Node) (EventPayloadSpec, error) {
	spec := EventPayloadSpec{Properties: map[string]EventFieldSpec{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key == "" || strings.HasPrefix(key, "_") {
			continue
		}
		switch key {
		case "description", "emitter", "emitter_type", "producer", "_producer", "alternate_emitters", "consumer", "_consumer", "consumer_type", "_consumer_type", "_source", "_status", "_note", "intercepted", "passthrough", "runtime_handling", "owning_node", "delivery_channel", "payload", "required":
			continue
		}
		var field EventFieldSpec
		if err := value.Decode(&field); err != nil {
			return EventPayloadSpec{}, err
		}
		spec.Properties[key] = field
	}
	return spec, nil
}

type wave1FieldNodeOptions struct {
	Context           string
	AllowInitial      bool
	AllowImmutable    bool
	AllowUnusedReason bool
}

type wave1ParsedFieldNode struct {
	Type         string
	Initial      any
	Immutable    bool
	Description  string
	UnusedReason string
}
