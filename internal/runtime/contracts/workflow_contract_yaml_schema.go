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

func (s *ToolInputSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("tool schema must be a mapping")
	}
	allowed := map[string]struct{}{
		"type": {}, "description": {}, "properties": {}, "required": {}, "items": {}, "enum": {},
		"additionalProperties": {}, "minimum": {}, "maximum": {}, "pattern": {},
		"format": {}, "x-swarm-equalTo": {}, "minLength": {}, "maxLength": {}, "minItems": {}, "maxItems": {},
	}
	for index := 0; index < len(node.Content); index += 2 {
		key := strings.TrimSpace(node.Content[index].Value)
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("tool schema field %q is unsupported", key)
		}
		value, err := resolveToolSchemaYAMLValue(node.Content[index+1])
		if err != nil {
			return fmt.Errorf("tool schema field %q: %w", key, err)
		}
		if value.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(value.Tag), "!!null") {
			return fmt.Errorf("tool schema field %q must not be null", key)
		}
	}
	type wire struct {
		Type        string               `yaml:"type"`
		Description string               `yaml:"description"`
		Properties  map[string]yaml.Node `yaml:"properties"`
		Required    []string             `yaml:"required"`
		Items       *ToolInputSchema     `yaml:"items"`
		Minimum     *float64             `yaml:"minimum"`
		Maximum     *float64             `yaml:"maximum"`
		Pattern     string               `yaml:"pattern"`
		Format      string               `yaml:"format"`
		EqualTo     string               `yaml:"x-swarm-equalTo"`
		MinLength   *int                 `yaml:"minLength"`
		MaxLength   *int                 `yaml:"maxLength"`
		MinItems    *int                 `yaml:"minItems"`
		MaxItems    *int                 `yaml:"maxItems"`
	}
	var decoded wire
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	options := []ToolInputSchemaOption{ToolSchemaDescription(decoded.Description)}
	if decoded.Properties != nil {
		properties := make(map[string]ToolInputSchema, len(decoded.Properties))
		equalities := make(map[string]string)
		for name, propertyNode := range decoded.Properties {
			property, equalTo, err := decodeToolSchemaProperty(propertyNode)
			if err != nil {
				return fmt.Errorf("tool schema property %q: %w", name, err)
			}
			properties[name] = property
			if equalTo != "" {
				equalities[name] = equalTo
			}
		}
		options = append(options, ToolSchemaProperties(properties))
		if len(equalities) > 0 {
			options = append(options, toolSchemaPropertyEqualities(equalities))
		}
	}
	if decoded.Required != nil {
		options = append(options, ToolSchemaRequired(decoded.Required...))
	}
	if decoded.Items != nil {
		options = append(options, ToolSchemaItems(*decoded.Items))
	}
	if decoded.Minimum != nil {
		options = append(options, ToolSchemaMinimum(*decoded.Minimum))
	}
	if decoded.Maximum != nil {
		options = append(options, ToolSchemaMaximum(*decoded.Maximum))
	}
	if decoded.Pattern != "" {
		options = append(options, ToolSchemaPattern(decoded.Pattern))
	}
	if decoded.Format != "" {
		options = append(options, ToolSchemaFormat(decoded.Format))
	}
	if decoded.EqualTo != "" {
		options = append(options, ToolSchemaEqualTo(decoded.EqualTo))
	}
	if decoded.MinLength != nil {
		options = append(options, ToolSchemaMinLength(*decoded.MinLength))
	}
	if decoded.MaxLength != nil {
		options = append(options, ToolSchemaMaxLength(*decoded.MaxLength))
	}
	if decoded.MinItems != nil {
		options = append(options, ToolSchemaMinItems(*decoded.MinItems))
	}
	if decoded.MaxItems != nil {
		options = append(options, ToolSchemaMaxItems(*decoded.MaxItems))
	}
	for index := 0; index < len(node.Content); index += 2 {
		switch strings.TrimSpace(node.Content[index].Value) {
		case "enum":
			enumNode, err := resolveToolSchemaYAMLValue(node.Content[index+1])
			if err != nil {
				return fmt.Errorf("tool schema enum: %w", err)
			}
			if enumNode.Kind != yaml.SequenceNode {
				return fmt.Errorf("tool schema enum must be a sequence")
			}
			values := make([]any, 0, len(enumNode.Content))
			for itemIndex, item := range enumNode.Content {
				value, err := toolInputSchemaEnumLiteralValue(item)
				if err != nil {
					return fmt.Errorf("tool schema enum[%d]: %w", itemIndex, err)
				}
				values = append(values, value)
			}
			options = append(options, ToolSchemaEnum(values...))
		case "additionalProperties":
			additionalNode, err := resolveToolSchemaYAMLValue(node.Content[index+1])
			if err != nil {
				return fmt.Errorf("tool schema additionalProperties: %w", err)
			}
			switch additionalNode.Kind {
			case yaml.ScalarNode:
				var allowed bool
				if err := additionalNode.Decode(&allowed); err != nil {
					return fmt.Errorf("tool schema additionalProperties: %w", err)
				}
				options = append(options, ToolSchemaAdditionalPropertiesAllowed(allowed))
			case yaml.MappingNode:
				var additional ToolInputSchema
				if err := additionalNode.Decode(&additional); err != nil {
					return fmt.Errorf("tool schema additionalProperties: %w", err)
				}
				options = append(options, ToolSchemaAdditionalPropertiesSchema(additional))
			default:
				return fmt.Errorf("unsupported additionalProperties yaml node kind %d", additionalNode.Kind)
			}
		}
	}
	kind := ToolSchemaKind(decoded.Type)
	if kind == "" && len(node.Content) == 0 {
		kind = ToolSchemaAny
	}
	admitted, err := NewToolInputSchema(kind, options...)
	if err != nil {
		return fmt.Errorf("tool schema: %w", err)
	}
	*s = admitted
	return nil
}

func decodeToolSchemaProperty(node yaml.Node) (ToolInputSchema, string, error) {
	resolved, err := resolveToolSchemaYAMLValue(&node)
	if err != nil {
		return ToolInputSchema{}, "", err
	}
	if resolved.Kind != yaml.MappingNode {
		return ToolInputSchema{}, "", fmt.Errorf("must be a mapping")
	}
	copyNode := *resolved
	copyNode.Content = make([]*yaml.Node, 0, len(resolved.Content))
	equalTo := ""
	for index := 0; index < len(resolved.Content); index += 2 {
		key := strings.TrimSpace(resolved.Content[index].Value)
		if key != "x-swarm-equalTo" {
			copyNode.Content = append(copyNode.Content, resolved.Content[index], resolved.Content[index+1])
			continue
		}
		value, err := resolveToolSchemaYAMLValue(resolved.Content[index+1])
		if err != nil {
			return ToolInputSchema{}, "", err
		}
		if err := value.Decode(&equalTo); err != nil {
			return ToolInputSchema{}, "", err
		}
		equalTo = strings.TrimSpace(equalTo)
		if equalTo == "" {
			return ToolInputSchema{}, "", fmt.Errorf("x-swarm-equalTo requires a valid field name")
		}
	}
	var property ToolInputSchema
	if err := copyNode.Decode(&property); err != nil {
		return ToolInputSchema{}, "", err
	}
	return property, equalTo, nil
}

func resolveToolSchemaYAMLValue(node *yaml.Node) (*yaml.Node, error) {
	seen := make(map[*yaml.Node]struct{})
	for depth := 0; node != nil && node.Kind == yaml.AliasNode; depth++ {
		if depth >= MaxToolInputSchemaDepth {
			return nil, fmt.Errorf("YAML alias chain exceeds maximum depth %d", MaxToolInputSchemaDepth)
		}
		if _, exists := seen[node]; exists {
			return nil, fmt.Errorf("YAML alias cycle")
		}
		seen[node] = struct{}{}
		if node.Alias == nil {
			return nil, fmt.Errorf("YAML alias has no target")
		}
		node = node.Alias
	}
	if node == nil {
		return nil, fmt.Errorf("YAML value is missing")
	}
	return node, nil
}

func (d *PackInterfaceDefinition) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if err := rejectUnknownYAMLFields(node, "pack interface", "kind", "schemas", "operations", "events"); err != nil {
		return err
	}
	type alias PackInterfaceDefinition
	var decoded alias
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*d = PackInterfaceDefinition(decoded)
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
	if hasYAMLMappingKey(node, "endpoint") {
		return fmt.Errorf("RETIRED: tool field %q is not accepted; use http.url", "endpoint")
	}
	if hasYAMLMappingKey(node, "type") {
		return fmt.Errorf("RETIRED: tool field %q is not accepted; use handler_type", "type")
	}
	if hasYAMLMappingKey(node, "required_permission") {
		return fmt.Errorf("RETIRED: tool field %q is not accepted; use permission", "required_permission")
	}
	var aux struct {
		Category          string                `yaml:"category,omitempty"`
		Description       string                `yaml:"description,omitempty"`
		HandlerType       string                `yaml:"handler_type,omitempty"`
		EffectClass       string                `yaml:"effect_class,omitempty"`
		Permission        string                `yaml:"permission,omitempty"`
		RateLimit         string                `yaml:"rate_limit,omitempty"`
		RateLimitMaxWait  string                `yaml:"rate_limit_max_wait,omitempty"`
		InputSchema       ToolInputSchema       `yaml:"input_schema,omitempty"`
		OutputSchema      ToolInputSchema       `yaml:"output_schema,omitempty"`
		HTTP              *HTTPToolSpec         `yaml:"http,omitempty"`
		ResponseMapping   map[string]any        `yaml:"response_mapping,omitempty"`
		ResponseSuccess   *HTTPResponseSuccess  `yaml:"response_success,omitempty"`
		Credentials       []string              `yaml:"credentials,omitempty"`
		ManagedCredential *ManagedCredentialRef `yaml:"managed_credential,omitempty"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	handler, err := ParseToolHandlerKind(aux.HandlerType)
	if err != nil {
		return err
	}
	if aux.InputSchema.IsZero() {
		aux.InputSchema = MustToolInputSchema(ToolSchemaObject)
	}
	if aux.OutputSchema.IsZero() {
		aux.OutputSchema = MustToolInputSchema(ToolSchemaObject)
	}
	options := []ToolSchemaEntryOption{
		WithToolCategory(aux.Category),
		WithToolDescription(aux.Description),
		WithToolHandler(handler),
		WithToolEffect(ActivityEffectClass(aux.EffectClass)),
		WithToolPermission(aux.Permission),
		WithToolRateLimit(aux.RateLimit, aux.RateLimitMaxWait),
		WithToolSchemas(aux.InputSchema, aux.OutputSchema),
	}
	if aux.HTTP != nil {
		options = append(options, WithToolHTTP(*aux.HTTP))
	}
	if aux.ResponseMapping != nil {
		options = append(options, WithToolResponseMapping(aux.ResponseMapping))
	}
	if aux.ResponseSuccess != nil {
		options = append(options, WithToolResponseSuccess(*aux.ResponseSuccess))
	}
	if len(aux.Credentials) > 0 {
		options = append(options, WithToolCredentials(aux.Credentials...))
	}
	if aux.ManagedCredential != nil {
		options = append(options, WithToolManagedCredential(*aux.ManagedCredential))
	}
	admitted, err := NewToolSchemaEntry(options...)
	if err != nil {
		return err
	}
	*t = admitted
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
