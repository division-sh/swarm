package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func (s *EntitySchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		type alias EntitySchema
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*s = EntitySchema(aux)
		return nil
	}
	if hasYAMLMappingKey(node, "groups") {
		type alias EntitySchema
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*s = EntitySchema(aux)
		return nil
	}
	if looksLikeEntitySchemaFieldMap(node) {
		fields, err := decodeEntitySchemaFields(node)
		if err != nil {
			return err
		}
		s.Groups = []EntitySchemaGroup{{Name: "default", Fields: fields}}
		return nil
	}
	groups := make([]EntitySchemaGroup, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		groupName := strings.TrimSpace(node.Content[i].Value)
		if groupName == "" || groupName == "description" {
			continue
		}
		if node.Content[i+1].Kind == yaml.ScalarNode {
			continue
		}
		fields, err := decodeEntitySchemaFields(node.Content[i+1])
		if err != nil {
			return err
		}
		groups = append(groups, EntitySchemaGroup{Name: groupName, Fields: fields})
	}
	s.Groups = groups
	return nil
}

func (p *EventPayloadSpec) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	type alias EventPayloadSpec
	if node.Kind != yaml.MappingNode {
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*p = EventPayloadSpec(aux)
		return nil
	}
	if hasYAMLMappingKey(node, "properties") || hasYAMLMappingKey(node, "required") {
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*p = EventPayloadSpec(aux)
		return nil
	}
	spec := EventPayloadSpec{Properties: map[string]EventFieldSpec{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		switch key {
		case "type":
			spec.Type = strings.TrimSpace(node.Content[i+1].Value)
		case "required":
			var required []string
			if err := node.Content[i+1].Decode(&required); err != nil {
				return err
			}
			spec.Required = normalizeStrings(required)
		default:
			var field EventFieldSpec
			if err := node.Content[i+1].Decode(&field); err != nil {
				return err
			}
			spec.Properties[key] = field
		}
	}
	*p = spec
	return nil
}

func (d *PackInterfaceDefinition) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if err := rejectUnknownYAMLFields(node, "pack interface", "kind", "schemas", "operations", "events"); err != nil {
		return err
	}
	var decoded struct {
		Kind       string                            `yaml:"kind"`
		Schemas    map[string]map[string]any         `yaml:"schemas"`
		Operations map[string]PackInterfaceOperation `yaml:"operations"`
		Events     map[string]PackInterfaceEvent     `yaml:"events"`
	}
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*d = PackInterfaceDefinition{Kind: decoded.Kind, Operations: decoded.Operations, Events: decoded.Events}
	if decoded.Schemas != nil {
		d.Schemas = make(map[string]ToolInputSchema, len(decoded.Schemas))
		for _, name := range sortedContractKeys(decoded.Schemas) {
			schema, err := AdmitToolInputSchemaMap(decoded.Schemas[name])
			if err != nil {
				return fmt.Errorf("pack interface schemas.%s: %w", name, err)
			}
			d.Schemas[name] = schema
		}
	}
	return nil
}

func (o *PackInterfaceOperation) UnmarshalYAML(node *yaml.Node) error {
	if o == nil {
		return nil
	}
	if err := rejectUnknownYAMLFields(node, "pack interface operation", "effect_class", "input", "context", "output"); err != nil {
		return err
	}
	type alias PackInterfaceOperation
	var decoded alias
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*o = PackInterfaceOperation(decoded)
	return nil
}

func (e *PackInterfaceEvent) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	if err := rejectUnknownYAMLFields(node, "pack interface event", "required_fields"); err != nil {
		return err
	}
	type alias PackInterfaceEvent
	var decoded alias
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*e = PackInterfaceEvent(decoded)
	return nil
}

func (f *PackInterfaceField) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	if err := rejectUnknownYAMLFields(node, "pack interface field", "schema", "opaque"); err != nil {
		return err
	}
	type alias PackInterfaceField
	var decoded alias
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*f = PackInterfaceField(decoded)
	return nil
}

func rejectUnknownYAMLFields(node *yaml.Node, subject string, allowed ...string) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", subject)
	}
	known := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		known[field] = struct{}{}
	}
	for index := 0; index < len(node.Content); index += 2 {
		field := strings.TrimSpace(node.Content[index].Value)
		if _, ok := known[field]; !ok {
			return fmt.Errorf("%s field %q is unsupported", subject, field)
		}
	}
	return nil
}

func looksLikeEntitySchemaFieldMap(node *yaml.Node) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	if len(node.Content) == 0 {
		return true
	}
	for i := 1; i < len(node.Content); i += 2 {
		value := node.Content[i]
		switch value.Kind {
		case yaml.ScalarNode:
			continue
		case yaml.MappingNode:
			if !hasAnyYAMLMappingKey(value, "type", "primary", "indexed", "nullable") {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func decodeEntitySchemaFields(node *yaml.Node) ([]EntitySchemaField, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var fields []EntitySchemaField
		if err := node.Decode(&fields); err != nil {
			return nil, err
		}
		return fields, nil
	case yaml.MappingNode:
		fields := make([]EntitySchemaField, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := strings.TrimSpace(node.Content[i].Value)
			if name == "" {
				continue
			}
			field, err := decodeEntitySchemaField(name, node.Content[i+1])
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("unsupported entity schema fields yaml node kind %d", node.Kind)
	}
}

func decodeEntitySchemaField(name string, node *yaml.Node) (EntitySchemaField, error) {
	field := EntitySchemaField{Name: strings.TrimSpace(name)}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.Contains(strings.ToLower(node.Value), " initial ") {
			return EntitySchemaField{}, fmt.Errorf("entity schema field %s: scalar form cannot declare initial values; use mapping form with type and initial", field.Name)
		}
		parsed := parseTypedFieldString(node.Value)
		field.Type = parsed.Type
		field.Primary = parsed.Primary
		field.Indexed = parsed.Indexed
		field.Nullable = parsed.Nullable
		if err := validateWave1TypeRef(field.Type, fmt.Sprintf("entity schema field %s", field.Name)); err != nil {
			return EntitySchemaField{}, err
		}
		return field, nil
	case yaml.SequenceNode:
		var items []string
		if err := node.Decode(&items); err != nil {
			return EntitySchemaField{}, err
		}
		if len(items) != 1 {
			return EntitySchemaField{}, fmt.Errorf("entity schema field %s: list shorthand requires exactly one item type", field.Name)
		}
		itemType := strings.TrimSpace(items[0])
		if itemType == "" {
			return EntitySchemaField{}, fmt.Errorf("entity schema field %s: list shorthand requires a non-empty item type", field.Name)
		}
		if err := validateWave1TypeRef(itemType, fmt.Sprintf("entity schema field %s list item", field.Name)); err != nil {
			return EntitySchemaField{}, err
		}
		field.Type = fmt.Sprintf("list<%s>", itemType)
		return field, nil
	case yaml.MappingNode:
		type alias EntitySchemaField
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return EntitySchemaField{}, err
		}
		field.Type = aux.Type
		field.Initial = aux.Initial
		field.Primary = aux.Primary
		field.Indexed = aux.Indexed
		field.Nullable = aux.Nullable
		field.Description = aux.Description
		if strings.TrimSpace(field.Type) == "" {
			return EntitySchemaField{}, fmt.Errorf("entity schema field %s: type is required", field.Name)
		}
		if err := validateWave1TypeRef(field.Type, fmt.Sprintf("entity schema field %s", field.Name)); err != nil {
			return EntitySchemaField{}, err
		}
		return field, nil
	default:
		return EntitySchemaField{}, fmt.Errorf("unsupported entity schema field yaml node kind %d", node.Kind)
	}
}

type parsedTypedField struct {
	Type     string
	Primary  bool
	Indexed  bool
	Nullable bool
	Default  any
}

func parseTypedFieldString(value string) parsedTypedField {
	value = strings.TrimSpace(value)
	if value == "" {
		return parsedTypedField{}
	}
	out := parsedTypedField{Type: value}
	lower := strings.ToLower(value)
	if idx := strings.Index(lower, " default "); idx >= 0 {
		out.Type = strings.TrimSpace(value[:idx])
		out.Default = strings.TrimSpace(value[idx+len(" default "):])
		lower = strings.ToLower(out.Type)
	}
	if strings.Contains(lower, "primary key") {
		out.Primary = true
		out.Type = strings.TrimSpace(strings.ReplaceAll(strings.ToLower(out.Type), "(primary key)", ""))
	}
	if strings.Contains(lower, "nullable") || strings.Contains(lower, "null until") {
		out.Nullable = true
	}
	if strings.Contains(lower, "indexed") {
		out.Indexed = true
	}
	out.Type = strings.TrimSpace(strings.TrimSuffix(out.Type, ","))
	return out
}
