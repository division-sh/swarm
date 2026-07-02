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

func (s *NodeStateSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	var aux struct {
		Description string    `yaml:"description"`
		Fields      yaml.Node `yaml:"fields"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	s.Description = strings.TrimSpace(aux.Description)
	fields, err := decodeNodeStateFields(&aux.Fields)
	if err != nil {
		return err
	}
	s.Fields = fields
	return nil
}

func (s *NodeGateStateSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*s = NodeGateStateSchema{}
		return nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		gates, err := decodeNodeGateFields(node)
		if err != nil {
			return err
		}
		s.Gates = gates
		return nil
	case yaml.MappingNode:
		if hasYAMLMappingKey(node, "description") || hasYAMLMappingKey(node, "gates") || hasYAMLMappingKey(node, "storage") {
			var aux struct {
				Description string    `yaml:"description"`
				Gates       yaml.Node `yaml:"gates"`
				Storage     string    `yaml:"storage"`
			}
			if err := node.Decode(&aux); err != nil {
				return err
			}
			gates, err := decodeNodeGateFields(&aux.Gates)
			if err != nil {
				return err
			}
			s.Description = strings.TrimSpace(aux.Description)
			s.Gates = gates
			s.Storage = strings.TrimSpace(aux.Storage)
			return nil
		}
		gates, err := decodeNodeGateFields(node)
		if err != nil {
			return err
		}
		s.Gates = gates
		return nil
	default:
		return fmt.Errorf("unsupported node gate state yaml node kind %d", node.Kind)
	}
}

func (s *EventFieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	parsed, err := decodeWave1FieldNode(node, wave1FieldNodeOptions{
		Context:           "event payload field",
		AllowInitial:      false,
		AllowImmutable:    false,
		AllowUnusedReason: false,
	})
	if err != nil {
		return err
	}
	*s = EventFieldSpec{
		Type:        parsed.Type,
		Description: parsed.Description,
	}
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

func (v *SchemaLiteral) UnmarshalYAML(node *yaml.Node) error {
	if v == nil || node == nil {
		return nil
	}
	v.Node = *node
	return nil
}

func (a *ToolAdditionalProperties) UnmarshalYAML(node *yaml.Node) error {
	if a == nil || node == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*a = ToolAdditionalProperties{}
			return nil
		}
		var allowed bool
		if err := node.Decode(&allowed); err != nil {
			return err
		}
		a.Allowed = &allowed
		a.Schema = nil
		return nil
	case yaml.MappingNode:
		var schema ToolInputSchema
		if err := node.Decode(&schema); err != nil {
			return err
		}
		a.Allowed = nil
		a.Schema = &schema
		return nil
	default:
		return fmt.Errorf("unsupported additionalProperties yaml node kind %d", node.Kind)
	}
}

func (s *ToolInputSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias ToolInputSchema
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ToolInputSchema(aux)
	return nil
}

func (t *ToolSchemaEntry) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	if hasYAMLMappingKey(node, "parameters") {
		return fmt.Errorf("RETIRED: tool field %q is retired; use input_schema", "parameters")
	}
	if hasYAMLMappingKey(node, "returns") {
		return fmt.Errorf("RETIRED: tool field %q is retired; use output_schema", "returns")
	}
	type alias ToolSchemaEntry
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*t = ToolSchemaEntry(aux)
	t.HandlerType = strings.TrimSpace(t.HandlerType)
	t.Permission = strings.TrimSpace(t.Permission)
	t.RequiredPermission = strings.TrimSpace(t.RequiredPermission)
	t.Credentials = normalizeStrings(t.Credentials)
	if t.HTTP != nil {
		t.HTTP.Method = strings.TrimSpace(t.HTTP.Method)
		t.HTTP.URL = strings.TrimSpace(t.HTTP.URL)
		t.HTTP.Retry.Backoff = strings.TrimSpace(t.HTTP.Retry.Backoff)
		headers := make(map[string]string, len(t.HTTP.Headers))
		for key, value := range t.HTTP.Headers {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			headers[key] = value
		}
		t.HTTP.Headers = headers
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

func decodeNodeStateFields(node *yaml.Node) ([]NodeStateField, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var fields []NodeStateField
		if err := node.Decode(&fields); err != nil {
			return nil, err
		}
		for i := range fields {
			fields[i].Name = strings.TrimSpace(fields[i].Name)
			normalizedType, err := NormalizeNodeStateFieldType(fields[i].Type)
			if err != nil {
				return nil, fmt.Errorf("node state field %s: %w", fields[i].Name, err)
			}
			fields[i].Type = normalizedType
		}
		return fields, nil
	case yaml.MappingNode:
		fields := make([]NodeStateField, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := strings.TrimSpace(node.Content[i].Value)
			if name == "" {
				continue
			}
			field, err := decodeNodeStateField(name, node.Content[i+1])
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("unsupported node state fields yaml node kind %d", node.Kind)
	}
}

func decodeNodeStateField(name string, node *yaml.Node) (NodeStateField, error) {
	field := NodeStateField{Name: strings.TrimSpace(name)}
	switch node.Kind {
	case yaml.ScalarNode:
		field.Type = strings.TrimSpace(node.Value)
		normalizedType, err := NormalizeNodeStateFieldType(field.Type)
		if err != nil {
			return NodeStateField{}, fmt.Errorf("node state field %s: %w", field.Name, err)
		}
		field.Type = normalizedType
		return field, nil
	case yaml.MappingNode:
		type alias NodeStateField
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return NodeStateField{}, err
		}
		normalizedType, err := NormalizeNodeStateFieldType(aux.Type)
		if err != nil {
			return NodeStateField{}, fmt.Errorf("node state field %s: %w", field.Name, err)
		}
		field.Type = normalizedType
		field.Default = aux.Default
		return field, nil
	default:
		return NodeStateField{}, fmt.Errorf("unsupported node state field yaml node kind %d", node.Kind)
	}
}

func decodeNodeGateFields(node *yaml.Node) ([]NodeGateField, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		gates := make([]NodeGateField, 0, len(node.Content))
		for _, item := range node.Content {
			switch item.Kind {
			case yaml.ScalarNode:
				name := strings.TrimSpace(item.Value)
				if name == "" {
					continue
				}
				gates = append(gates, NodeGateField{Name: name})
			case yaml.MappingNode:
				var field NodeGateField
				if err := item.Decode(&field); err != nil {
					return nil, err
				}
				field.Name = strings.TrimSpace(field.Name)
				field.Description = strings.TrimSpace(field.Description)
				if field.Name == "" {
					return nil, fmt.Errorf("node gate field entry missing name")
				}
				gates = append(gates, field)
			default:
				return nil, fmt.Errorf("unsupported node gate fields yaml node kind %d", item.Kind)
			}
		}
		return gates, nil
	case yaml.MappingNode:
		gates := make([]NodeGateField, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := strings.TrimSpace(node.Content[i].Value)
			if name == "" {
				continue
			}
			field := NodeGateField{Name: name}
			switch node.Content[i+1].Kind {
			case yaml.ScalarNode:
				field.Description = strings.TrimSpace(node.Content[i+1].Value)
			case yaml.MappingNode:
				var aux NodeGateField
				if err := node.Content[i+1].Decode(&aux); err != nil {
					return nil, err
				}
				if strings.TrimSpace(aux.Name) != "" {
					field.Name = strings.TrimSpace(aux.Name)
				}
				field.Description = strings.TrimSpace(aux.Description)
			default:
				return nil, fmt.Errorf("unsupported node gate field yaml node kind %d", node.Content[i+1].Kind)
			}
			gates = append(gates, field)
		}
		return gates, nil
	default:
		return nil, fmt.Errorf("unsupported node gate fields yaml node kind %d", node.Kind)
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
