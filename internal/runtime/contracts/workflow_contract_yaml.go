package contracts

import (
	"empireai/internal/runtime/core/paths"
	"fmt"
	"gopkg.in/yaml.v3"
	"regexp"
	"strings"
)

var legacyComputeExpressionPattern = regexp.MustCompile(`^\s*weighted_average\s*\(\s*accumulated\.([a-zA-Z0-9_]+)\s*,\s*accumulated\.([a-zA-Z0-9_]+)\s*\)\s*$`)

func (r *HandlerRuleEntry) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		r.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias HandlerRuleEntry
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = HandlerRuleEntry(aux)
	return nil
}

func (s *ComputeSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	var aux struct {
		Operation   ComputeOperation `yaml:"operation"`
		Tiers       []ComputeTier    `yaml:"tiers"`
		Keys        ComputeKeyConfig `yaml:"keys"`
		StoreAs     string           `yaml:"store_as"`
		ItemsFrom   string           `yaml:"items_from"`
		ValueField  string           `yaml:"value_field"`
		WeightField string           `yaml:"weight_field"`
		OutputField string           `yaml:"output_field"`
		Expression  string           `yaml:"expression"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ComputeSpec{
		Operation:   aux.Operation,
		Tiers:       aux.Tiers,
		Keys:        aux.Keys,
		StoreAs:     strings.TrimSpace(aux.StoreAs),
		ItemsFrom:   strings.TrimSpace(aux.ItemsFrom),
		ValueField:  strings.TrimSpace(aux.ValueField),
		WeightField: strings.TrimSpace(aux.WeightField),
	}
	if s.StoreAs == "" {
		if outputField := strings.TrimSpace(aux.OutputField); outputField != "" {
			if strings.HasPrefix(outputField, "entity.") || strings.HasPrefix(outputField, "metadata.") {
				s.StoreAs = outputField
			} else {
				s.StoreAs = "entity." + outputField
			}
		}
	}
	if s.Operation == ComputeOpUnknown {
		if valueField, weightField, ok := parseLegacyComputeExpression(strings.TrimSpace(aux.Expression)); ok {
			s.Operation = ComputeOpWeightedAverage
			if s.ItemsFrom == "" {
				s.ItemsFrom = "accumulated.items"
			}
			if s.ValueField == "" {
				s.ValueField = valueField
			}
			if s.WeightField == "" {
				s.WeightField = weightField
			}
		}
	}
	return nil
}

func parseLegacyComputeExpression(expression string) (string, string, bool) {
	match := legacyComputeExpressionPattern.FindStringSubmatch(expression)
	if len(match) != 3 {
		return "", "", false
	}
	return singularizeLegacyComputeField(match[1]), singularizeLegacyComputeField(match[2]), true
}

func singularizeLegacyComputeField(name string) string {
	name = strings.TrimSpace(name)
	// Legacy fixtures only use simple plural payload field names like
	// values/weights in weighted_average(accumulated.values, accumulated.weights).
	// This is a narrow compatibility shim, not a general singularization rule.
	if len(name) > 1 && strings.HasSuffix(name, "s") {
		return strings.TrimSpace(strings.TrimSuffix(name, "s"))
	}
	return name
}
func (g *GuardSpec) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*g = GuardSpec{}
			return nil
		}
		*g = GuardSpec{ID: strings.TrimSpace(node.Value)}
		return nil
	case yaml.MappingNode:
		type alias GuardSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*g = GuardSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported guard yaml node kind %d", node.Kind)
	}
}
func (a *AccumulateSpec) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	var aux struct {
		ExpectedFrom string    `yaml:"expected_from"`
		DedupBy      string    `yaml:"dedup_by"`
		Threshold    int       `yaml:"threshold"`
		TimeoutMS    int       `yaml:"timeout_ms"`
		Completion   string    `yaml:"completion"`
		OnComplete   yaml.Node `yaml:"on_complete"`
		OnTimeout    yaml.Node `yaml:"on_timeout"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*a = AccumulateSpec{
		ExpectedFrom: strings.TrimSpace(aux.ExpectedFrom),
		ExpectedPath: paths.Parse(aux.ExpectedFrom),
		DedupBy:      strings.TrimSpace(aux.DedupBy),
		DedupPath:    paths.Parse(aux.DedupBy),
		Threshold:    aux.Threshold,
		TimeoutMS:    aux.TimeoutMS,
		Completion:   ParseAccumulateCompletion(aux.Completion),
	}
	var err error
	if a.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete); err != nil {
		return err
	}
	if a.OnTimeout, err = decodeHandlerRuleEntryNode(&aux.OnTimeout); err != nil {
		return err
	}
	return nil
}
func (f *FanOutSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	var aux struct {
		ItemsFrom   string    `yaml:"items_from"`
		Target      string    `yaml:"target"`
		EmitPerItem string    `yaml:"emit_per_item"`
		EmitMapping yaml.Node `yaml:"emit_mapping"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*f = FanOutSpec{
		ItemsFrom:   strings.TrimSpace(aux.ItemsFrom),
		Target:      strings.TrimSpace(aux.Target),
		EmitPerItem: strings.TrimSpace(aux.EmitPerItem),
	}
	f.ItemsFrom = strings.TrimSpace(f.ItemsFrom)
	f.ItemsPath = paths.Parse(f.ItemsFrom)
	f.Target = strings.TrimSpace(f.Target)
	f.EmitPerItem = strings.TrimSpace(f.EmitPerItem)
	if aux.EmitMapping.Kind != 0 {
		mapping, keyField, err := decodeFanOutEmitMappingNode(&aux.EmitMapping)
		if err != nil {
			return err
		}
		f.EmitMapping = mapping
		f.EmitMappingKey = keyField
	}
	return nil
}

func decodeFanOutEmitMappingNode(node *yaml.Node) (map[string]string, string, error) {
	if node == nil || node.Kind == 0 {
		return nil, "", nil
	}
	var plain map[string]string
	if err := node.Decode(&plain); err == nil && len(plain) > 0 {
		return plain, "", nil
	}
	var structured struct {
		KeyField string            `yaml:"key_field"`
		Mapping  map[string]string `yaml:"mapping"`
	}
	if err := node.Decode(&structured); err != nil {
		return nil, "", err
	}
	return structured.Mapping, strings.TrimSpace(structured.KeyField), nil
}

func (g *GroupBySpec) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	type alias GroupBySpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*g = GroupBySpec(aux)
	g.ItemsFrom = strings.TrimSpace(g.ItemsFrom)
	g.ItemsPath = paths.Parse(g.ItemsFrom)
	g.Key = strings.TrimSpace(g.Key)
	g.KeyPath = paths.Parse(g.Key)
	g.StoreAs = strings.TrimSpace(g.StoreAs)
	g.StorePath = paths.Parse(g.StoreAs)
	return nil
}
func (p *PayloadTransformSpec) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	type alias PayloadTransformSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*p = PayloadTransformSpec(aux)
	p.Entries = p.TransformEntries()
	return nil
}
func (g *GateSpec) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*g = GateSpec{}
			return nil
		}
		*g = GateSpec{Name: strings.TrimSpace(node.Value), Value: true}
		return nil
	case yaml.MappingNode:
		type alias GateSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*g = GateSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported gate yaml node kind %d", node.Kind)
	}
}
func (w *WorkflowDataWrite) UnmarshalYAML(node *yaml.Node) error {
	if w == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*w = WorkflowDataWrite{}
			return nil
		}
		w.Field = strings.TrimSpace(node.Value)
		w.SourcePath = paths.Parse(w.Field)
		w.TargetPath = paths.Parse(w.Field)
		return nil
	case yaml.MappingNode:
	default:
		return fmt.Errorf("unsupported workflow data write yaml node kind %d", node.Kind)
	}
	var aux struct {
		Field       string    `yaml:"field"`
		SourceField string    `yaml:"source_field"`
		TargetField string    `yaml:"target_field"`
		Value       yaml.Node `yaml:"value"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*w = WorkflowDataWrite{
		Field:       strings.TrimSpace(aux.Field),
		SourceField: strings.TrimSpace(aux.SourceField),
		TargetField: strings.TrimSpace(aux.TargetField),
	}
	switch aux.Value.Kind {
	case 0:
		// no-op
	case yaml.MappingNode:
		var expr ExpressionValue
		if err := aux.Value.Decode(&expr); err != nil {
			return err
		}
		w.Value = expr
	default:
		var literal any
		if err := aux.Value.Decode(&literal); err != nil {
			return err
		}
		w.Value = ExpressionValue{Literal: literal}
	}
	w.SourcePath = paths.Parse(w.Source())
	w.TargetPath = paths.Parse(w.Target())
	return nil
}
func (a *WorkflowDataAccumulation) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*a = WorkflowDataAccumulation{}
		return nil
	}
	switch node.Kind {
	case yaml.MappingNode:
	default:
		type alias WorkflowDataAccumulation
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*a = WorkflowDataAccumulation(aux)
		return nil
	}

	var aux struct {
		Writes      []WorkflowDataWrite `yaml:"writes"`
		Source      string              `yaml:"source"`
		SourceEvent string              `yaml:"source_event"`
		Value       ExpressionValue     `yaml:"value"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}

	spec := WorkflowDataAccumulation{
		Writes:      append([]WorkflowDataWrite(nil), aux.Writes...),
		SourceEvent: strings.TrimSpace(aux.SourceEvent),
		Value:       aux.Value,
	}
	commonSource := strings.TrimSpace(aux.Source)
	if commonSource != "" {
		for i := range spec.Writes {
			if strings.TrimSpace(spec.Writes[i].SourceField) != "" {
				continue
			}
			spec.Writes[i].SourceField = commonSource
			spec.Writes[i].SourcePath = paths.Parse(spec.Writes[i].Source())
			spec.Writes[i].TargetPath = paths.Parse(spec.Writes[i].Target())
		}
	}

	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]
		key := strings.TrimSpace(keyNode.Value)
		switch key {
		case "", "writes", "source", "source_event", "value":
			continue
		}
		write, err := decodeWorkflowDataAccumulationShorthandWrite(key, valueNode)
		if err != nil {
			return err
		}
		spec.Writes = append(spec.Writes, write)
	}

	*a = spec
	return nil
}

func decodeWorkflowDataAccumulationShorthandWrite(target string, node *yaml.Node) (WorkflowDataWrite, error) {
	write := WorkflowDataWrite{TargetField: strings.TrimSpace(target)}
	if node == nil || node.Kind == 0 {
		write.TargetPath = paths.Parse(write.Target())
		return write, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		switch {
		case looksLikeWorkflowDataCEL(value):
			write.Value = ExpressionValue{CEL: value}
		case looksLikeWorkflowDataSourceRef(value):
			write.SourceField = value
		default:
			var literal any
			if err := node.Decode(&literal); err != nil {
				return WorkflowDataWrite{}, err
			}
			write.Value = ExpressionValue{Literal: literal}
		}
	case yaml.MappingNode:
		if hasAnyYAMLMappingKey(node, "source", "source_field", "value", "literal", "cel") {
			var aux struct {
				Source      string     `yaml:"source"`
				SourceField string     `yaml:"source_field"`
				Value       *yaml.Node `yaml:"value"`
				Literal     *yaml.Node `yaml:"literal"`
				CEL         string     `yaml:"cel"`
			}
			if err := node.Decode(&aux); err != nil {
				return WorkflowDataWrite{}, err
			}
			if sourceField := strings.TrimSpace(aux.SourceField); sourceField != "" {
				write.SourceField = sourceField
			} else {
				write.SourceField = strings.TrimSpace(aux.Source)
			}
			switch {
			case strings.TrimSpace(aux.CEL) != "":
				write.Value = ExpressionValue{CEL: strings.TrimSpace(aux.CEL)}
			case aux.Value != nil:
				var literal any
				if err := aux.Value.Decode(&literal); err != nil {
					return WorkflowDataWrite{}, err
				}
				write.Value = ExpressionValue{Literal: literal}
			case aux.Literal != nil:
				var literal any
				if err := aux.Literal.Decode(&literal); err != nil {
					return WorkflowDataWrite{}, err
				}
				write.Value = ExpressionValue{Literal: literal}
			}
		} else {
			var literal any
			if err := node.Decode(&literal); err != nil {
				return WorkflowDataWrite{}, err
			}
			write.Value = ExpressionValue{Literal: literal}
		}
	default:
		var literal any
		if err := node.Decode(&literal); err != nil {
			return WorkflowDataWrite{}, err
		}
		write.Value = ExpressionValue{Literal: literal}
	}
	write.SourcePath = paths.Parse(write.Source())
	write.TargetPath = paths.Parse(write.Target())
	return write, nil
}

func looksLikeWorkflowDataSourceRef(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, prefix := range []string{
		"entity.",
		"payload.",
		"policy.",
		"metadata.",
		"gates.",
		"accumulated.",
		"fan_out.",
		"computed.",
	} {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func looksLikeWorkflowDataCEL(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, needle := range []string{
		" + ", " - ", " * ", " / ", " % ",
		"==", "!=", ">=", "<=", " > ", " < ",
		"&&", "||", " AND ", " OR ",
		"(", ")",
	} {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
func (s *FilterSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias FilterSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = FilterSpec(aux)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.ItemsFrom = strings.TrimSpace(s.ItemsFrom)
	s.ItemsPath = paths.Parse(s.ItemsFrom)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.Predicate = strings.TrimSpace(s.Predicate)
	s.Condition = strings.TrimSpace(s.Condition)
	return nil
}
func (s *ReduceSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias ReduceSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ReduceSpec(aux)
	s.Operation = strings.TrimSpace(s.Operation)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.ItemsFrom = strings.TrimSpace(s.ItemsFrom)
	s.ItemsPath = paths.Parse(s.ItemsFrom)
	return nil
}
func (s *CountSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias CountSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = CountSpec(aux)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.ItemsFrom = strings.TrimSpace(s.ItemsFrom)
	s.ItemsPath = paths.Parse(s.ItemsFrom)
	s.Condition = strings.TrimSpace(s.Condition)
	return nil
}
func (s *QuerySpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias QuerySpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = QuerySpec(aux)
	s.hydratePaths()
	return nil
}
func (v *FlowVariable) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		v.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias FlowVariable
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*v = FlowVariable(aux)
	return nil
}
func (t *WorkflowTransitionContract) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	// Decode all fields except From using a shadow type that replaces From with a yaml.Node.
	type shadow struct {
		ID                string                   `yaml:"id"`
		From              yaml.Node                `yaml:"from"`
		To                string                   `yaml:"to"`
		Trigger           string                   `yaml:"trigger"`
		Node              string                   `yaml:"node"`
		Guards            []string                 `yaml:"guards"`
		Actions           []string                 `yaml:"actions"`
		DataAccumulation  WorkflowDataAccumulation `yaml:"data_accumulation"`
		AllowTerminalExit bool                     `yaml:"allow_terminal_exit"`
	}
	var aux shadow
	if err := node.Decode(&aux); err != nil {
		return err
	}
	t.ID = aux.ID
	t.To = aux.To
	t.Trigger = aux.Trigger
	t.Node = aux.Node
	t.Guards = aux.Guards
	t.Actions = aux.Actions
	t.DataAccumulation = aux.DataAccumulation
	t.AllowTerminalExit = aux.AllowTerminalExit
	from, err := decodeStringListNode(&aux.From)
	if err != nil {
		return err
	}
	t.From = from
	return nil
}
func (e *ExpressionValue) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = ExpressionValue{}
			return nil
		}
		switch strings.TrimSpace(node.Tag) {
		case "!!str", "":
			*e = ExpressionValue{CEL: strings.TrimSpace(node.Value)}
		default:
			var literal any
			if err := node.Decode(&literal); err != nil {
				return err
			}
			*e = ExpressionValue{Literal: literal}
		}
		return nil
	case yaml.MappingNode:
		type alias ExpressionValue
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*e = ExpressionValue(aux)
		return nil
	case yaml.SequenceNode:
		var literal any
		if err := node.Decode(&literal); err != nil {
			return err
		}
		*e = ExpressionValue{Literal: literal}
		return nil
	default:
		return fmt.Errorf("unsupported expression value yaml node kind %d", node.Kind)
	}
}
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
		// Skip scalar values (e.g. description text) — groups are mappings.
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
func (s *EventFieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		*s = EventFieldSpec{Type: strings.TrimSpace(node.Value)}
		return nil
	case yaml.MappingNode:
		type alias EventFieldSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*s = EventFieldSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported event field yaml node kind %d", node.Kind)
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
func (t *WorkflowTimerContract) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*t = WorkflowTimerContract{}
			return nil
		}
		t.ID = strings.TrimSpace(node.Value)
		return nil
	case yaml.MappingNode:
		type alias WorkflowTimerContract
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*t = WorkflowTimerContract(aux)
		return nil
	default:
		return fmt.Errorf("unsupported workflow timer yaml node kind %d", node.Kind)
	}
}
func (e *EventEmission) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = EventEmission{}
			return nil
		}
		e.Single = strings.TrimSpace(node.Value)
		return nil
	case yaml.SequenceNode:
		var many []string
		if err := node.Decode(&many); err != nil {
			return err
		}
		e.Many = normalizeStrings(many)
		return nil
	default:
		return fmt.Errorf("unsupported event emission yaml node kind %d", node.Kind)
	}
}
func (h *SystemNodeEventHandler) UnmarshalYAML(node *yaml.Node) error {
	if h == nil {
		return nil
	}
	if err := validateHandlerFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Action           yaml.Node                `yaml:"action"`
		Description      string                   `yaml:"description"`
		Emits            EventEmission            `yaml:"emits"`
		Guard            yaml.Node                `yaml:"guard"`
		AdvancesTo       yaml.Node                `yaml:"advances_to"`
		SetsGate         yaml.Node                `yaml:"sets_gate"`
		ClearGates       yaml.Node                `yaml:"clear_gates"`
		DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
		Condition        string                   `yaml:"condition"`
		CompletionRule   string                   `yaml:"completion_rule"`
		Logic            string                   `yaml:"logic"`
		PolicyRef        string                   `yaml:"policy_ref"`
		OnComplete       yaml.Node                `yaml:"on_complete"`
		Rules            yaml.Node                `yaml:"rules"`
		Accumulate       *AccumulateSpec          `yaml:"accumulate"`
		Compute          *ComputeSpec             `yaml:"compute"`
		Query            yaml.Node                `yaml:"query"`
		FanOut           *FanOutSpec              `yaml:"fan_out"`
		GroupBy          *GroupBySpec             `yaml:"group_by"`
		Filter           *FilterSpec              `yaml:"filter"`
		Reduce           *ReduceSpec              `yaml:"reduce"`
		Count            *CountSpec               `yaml:"count"`
		Clear            yaml.Node                `yaml:"clear"`
		PayloadTransform *PayloadTransformSpec    `yaml:"payload_transform"`
		Branch           yaml.Node                `yaml:"branch"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		Description:      strings.TrimSpace(aux.Description),
		Emits:            aux.Emits,
		DataAccumulation: aux.DataAccumulation,
		Condition:        strings.TrimSpace(aux.Condition),
		CompletionRule:   strings.TrimSpace(aux.CompletionRule),
		Logic:            strings.TrimSpace(aux.Logic),
		PolicyRef:        strings.TrimSpace(aux.PolicyRef),
		Accumulate:       aux.Accumulate,
		Compute:          aux.Compute,
		FanOut:           aux.FanOut,
		GroupBy:          aux.GroupBy,
		Filter:           aux.Filter,
		Reduce:           aux.Reduce,
		Count:            aux.Count,
		PayloadTransform: aux.PayloadTransform,
	}
	var err error
	if h.Action, err = decodeActionSpecNode(&aux.Action); err != nil {
		return err
	}
	if h.Guard, err = decodeGuardSpecNode(&aux.Guard); err != nil {
		return err
	}
	if h.AdvancesTo, err = decodeAdvancesToNode(&aux.AdvancesTo); err != nil {
		return err
	}
	if h.SetsGate, err = decodeGateSpecNode(&aux.SetsGate); err != nil {
		return err
	}
	if h.ClearGates, err = decodeClearGatesNode(&aux.ClearGates); err != nil {
		return err
	}
	if h.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete); err != nil {
		return err
	}
	if h.Rules, err = decodeHandlerRuleEntriesNode(&aux.Rules); err != nil {
		return err
	}
	if h.Query, err = decodeQuerySpecNode(&aux.Query); err != nil {
		return err
	}
	if h.Clear, err = decodeClearSpecNode(&aux.Clear); err != nil {
		return err
	}
	if h.Branch, err = decodeBranchSpecsNode(&aux.Branch); err != nil {
		return err
	}
	return nil
}
func (e *EventCatalogEntry) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	var aux struct {
		Emitter           yaml.Node `yaml:"emitter"`
		EmitterType       string    `yaml:"emitter_type"`
		AlternateEmitters []string  `yaml:"alternate_emitters"`
		Consumer          yaml.Node `yaml:"consumer"`
		ConsumerType      yaml.Node `yaml:"consumer_type"`
		Source            string    `yaml:"_source"`
		Status            string    `yaml:"_status"`
		Intercepted       yaml.Node `yaml:"intercepted"`
		Passthrough       yaml.Node `yaml:"passthrough"`
		RuntimeHandling   string    `yaml:"runtime_handling"`
		OwningNode        string    `yaml:"owning_node"`
		DeliveryChannel   yaml.Node `yaml:"delivery_channel"`
		Payload           yaml.Node `yaml:"payload"`
		Required          []string  `yaml:"required"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	emitter, alternates, err := decodeEventEmitterNode(&aux.Emitter)
	if err != nil {
		return err
	}
	consumer, err := decodeStringListNode(&aux.Consumer)
	if err != nil {
		return err
	}
	consumerType, err := decodeStringListNode(&aux.ConsumerType)
	if err != nil {
		return err
	}
	intercepted, err := decodeBoolNode(&aux.Intercepted)
	if err != nil {
		return err
	}
	passthrough, err := decodeBoolNode(&aux.Passthrough)
	if err != nil {
		return err
	}
	deliveryChannel, err := decodeScalarStringNode(&aux.DeliveryChannel)
	if err != nil {
		return err
	}
	payload, err := decodeEventPayloadSpecNode(&aux.Payload)
	if err != nil {
		return err
	}
	e.Emitter = emitter
	e.EmitterType = strings.TrimSpace(aux.EmitterType)
	e.AlternateEmitters = mergeStringLists(aux.AlternateEmitters, alternates)
	e.Consumer = consumer
	e.ConsumerType = consumerType
	e.Source = strings.TrimSpace(aux.Source)
	e.Status = strings.TrimSpace(aux.Status)
	e.Intercepted = intercepted
	e.Passthrough = passthrough
	e.RuntimeHandling = strings.TrimSpace(aux.RuntimeHandling)
	e.OwningNode = strings.TrimSpace(aux.OwningNode)
	e.DeliveryChannel = deliveryChannel
	e.Payload = payload
	e.Required = normalizeStrings(aux.Required)
	return nil
}
func hasYAMLMappingKey(node *yaml.Node, key string) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return true
		}
	}
	return false
}
func decodeStringListNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		var values []string
		if err := node.Decode(&values); err != nil {
			return nil, err
		}
		return normalizeStrings(values), nil
	default:
		return nil, fmt.Errorf("unsupported string list yaml node kind %d", node.Kind)
	}
}
func decodeScalarStringNode(node *yaml.Node) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind != yaml.ScalarNode {
		return "", fmt.Errorf("unsupported scalar string yaml node kind %d", node.Kind)
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return "", nil
	}
	return strings.TrimSpace(node.Value), nil
}
func decodeBoolNode(node *yaml.Node) (bool, error) {
	if node == nil || node.Kind == 0 {
		return false, nil
	}
	if node.Kind != yaml.ScalarNode {
		return false, fmt.Errorf("unsupported bool yaml node kind %d", node.Kind)
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
		return false, nil
	}
	var value bool
	if err := node.Decode(&value); err == nil {
		return value, nil
	}
	switch strings.ToLower(strings.TrimSpace(node.Value)) {
	case "true", "yes", "on", "conditional":
		return true, nil
	case "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported bool value %q", node.Value)
	}
}
func decodeGuardSpecNode(node *yaml.Node) (*GuardSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind == yaml.ScalarNode && !strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") && strings.TrimSpace(node.Value) != "" {
		return nil, fmt.Errorf("DIALECT-GUARD: guard is string, must be {id, check}")
	}
	var spec GuardSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.ID) == "" && strings.TrimSpace(spec.Check) == "" && len(spec.Checks) == 0 && strings.TrimSpace(spec.OnFail) == "" && strings.TrimSpace(spec.PolicyRef) == "" {
		return nil, nil
	}
	return &spec, nil
}
func decodeAdvancesToNode(node *yaml.Node) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind == yaml.SequenceNode {
		return "", fmt.Errorf("DIALECT-ADV-LIST: advances_to is list, must be string")
	}
	return decodeScalarStringNode(node)
}
func validateHandlerFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	allowed := map[string]struct{}{
		"action":            {},
		"description":       {},
		"_note":             {},
		"emits":             {},
		"guard":             {},
		"advances_to":       {},
		"sets_gate":         {},
		"clear_gates":       {},
		"data_accumulation": {},
		"condition":         {},
		"completion_rule":   {},
		"logic":             {},
		"policy_ref":        {},
		"on_complete":       {},
		"rules":             {},
		"accumulate":        {},
		"compute":           {},
		"query":             {},
		"fan_out":           {},
		"group_by":          {},
		"filter":            {},
		"reduce":            {},
		"count":             {},
		"clear":             {},
		"template":          {},
		"instance_id_from":  {},
		"config_from":       {},
		"from":              {},
		"payload_transform": {},
		"branch":            {},
		"dedup_by":          {},
	}
	deprecated := map[string]struct{}{
		"condition":          {},
		"logic":              {},
		"on_below_threshold": {},
		"on_dedup":           {},
		"on_pass":            {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := deprecated[key]; ok {
			return fmt.Errorf("DEPRECATED: handler uses deprecated field %q", key)
		}
		if key == "on_complete" && node.Content[i+1].Kind == yaml.MappingNode {
			return fmt.Errorf("DIALECT-OC-ORDER: on_complete is dict, must be ordered list")
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: handler field %q not in platform spec", key)
		}
	}
	return nil
}
func decodeGateSpecNode(node *yaml.Node) (*GateSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var spec GateSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Name) == "" && spec.Value == nil {
		return nil, nil
	}
	return &spec, nil
}
func decodeClearGatesNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		var all bool
		if err := node.Decode(&all); err == nil {
			if all {
				return []string{"*"}, nil
			}
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		return decodeStringListNode(node)
	default:
		return nil, fmt.Errorf("unsupported clear_gates yaml node kind %d", node.Kind)
	}
}
func decodeHandlerRuleEntryNode(node *yaml.Node) (*HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var rule HandlerRuleEntry
	if err := node.Decode(&rule); err != nil {
		return nil, err
	}
	if strings.TrimSpace(rule.ID) == "" && strings.TrimSpace(rule.Description) == "" && strings.TrimSpace(rule.Condition) == "" && strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emits.Empty() && !rule.DataAccumulation.HasWrites() && rule.DataAccumulation.Value.IsZero() && rule.Compute == nil && rule.FanOut == nil {
		return nil, nil
	}
	return &rule, nil
}
func decodeHandlerRuleEntriesNode(node *yaml.Node) ([]HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var rules []HandlerRuleEntry
		if err := node.Decode(&rules); err != nil {
			return nil, err
		}
		return rules, nil
	case yaml.MappingNode:
		if hasAnyYAMLMappingKey(node, "condition", "advances_to", "emits", "data_accumulation", "compute", "fan_out") {
			rule, err := decodeHandlerRuleEntryNode(node)
			if err != nil || rule == nil {
				return nil, err
			}
			return []HandlerRuleEntry{*rule}, nil
		}
		rules := make([]HandlerRuleEntry, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			id := strings.TrimSpace(node.Content[i].Value)
			if id == "" {
				continue
			}
			var rule HandlerRuleEntry
			if err := node.Content[i+1].Decode(&rule); err != nil {
				return nil, err
			}
			if strings.TrimSpace(rule.ID) == "" {
				rule.ID = id
			}
			rules = append(rules, rule)
		}
		return rules, nil
	default:
		return nil, fmt.Errorf("unsupported rules yaml node kind %d", node.Kind)
	}
}
func decodeQuerySpecNode(node *yaml.Node) (*QuerySpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.MappingNode:
		var spec QuerySpec
		if err := node.Decode(&spec); err != nil {
			return nil, err
		}
		spec.hydratePaths()
		return &spec, nil
	case yaml.SequenceNode:
		var queries []QuerySpec
		if err := node.Decode(&queries); err != nil {
			return nil, err
		}
		for i := range queries {
			queries[i].hydratePaths()
		}
		return &QuerySpec{Queries: queries}, nil
	default:
		return nil, fmt.Errorf("unsupported query yaml node kind %d", node.Kind)
	}
}
func decodeActionSpecNode(node *yaml.Node) (ActionSpec, error) {
	if node == nil || node.Kind == 0 {
		return ActionSpec{}, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return ActionSpec{}, nil
		}
		return ActionSpec{ID: strings.TrimSpace(node.Value)}, nil
	case yaml.MappingNode:
		var aux struct {
			ID             string    `yaml:"id"`
			Template       string    `yaml:"template"`
			InstanceIDFrom string    `yaml:"instance_id_from"`
			ConfigFrom     yaml.Node `yaml:"config_from"`
		}
		if err := node.Decode(&aux); err != nil {
			return ActionSpec{}, err
		}
		configFrom, err := decodeConfigFromSpecNode(&aux.ConfigFrom)
		if err != nil {
			return ActionSpec{}, err
		}
		return ActionSpec{
			ID:             strings.TrimSpace(aux.ID),
			Template:       strings.TrimSpace(aux.Template),
			InstanceIDFrom: strings.TrimSpace(aux.InstanceIDFrom),
			InstanceIDPath: paths.Parse(aux.InstanceIDFrom),
			ConfigFrom:     configFrom,
		}, nil
	default:
		return ActionSpec{}, fmt.Errorf("unsupported action yaml node kind %d", node.Kind)
	}
}
func decodeClearSpecNode(node *yaml.Node) (*ClearSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var spec ClearSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Target) == "" && len(spec.Targets) == 0 {
		return nil, nil
	}
	return &spec, nil
}
func decodeConfigFromSpecNode(node *yaml.Node) (*ConfigFromSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("unsupported config_from yaml node kind %d", node.Kind)
	}
	spec := &ConfigFromSpec{Bindings: map[string]string{}}
	if hasYAMLMappingKey(node, "policy_keys") {
		type alias ConfigFromSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return nil, err
		}
		spec.PolicyKeys = normalizeStrings(aux.PolicyKeys)
		for key, value := range aux.Bindings {
			spec.Bindings[key] = value
		}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" || key == "policy_keys" {
			continue
		}
		spec.Bindings[key] = strings.TrimSpace(node.Content[i+1].Value)
	}
	if len(spec.PolicyKeys) == 0 && len(spec.Bindings) == 0 {
		return nil, nil
	}
	spec.Entries = spec.ConfigEntries()
	return spec, nil
}
func decodeBranchSpecsNode(node *yaml.Node) ([]BranchSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var specs []BranchSpec
	if err := node.Decode(&specs); err != nil {
		return nil, err
	}
	return specs, nil
}
func decodeEventEmitterNode(node *yaml.Node) (EventEmitterRef, []string, error) {
	if node == nil || node.Kind == 0 {
		return EventEmitterRef{}, nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		if value == "" {
			return EventEmitterRef{}, nil, nil
		}
		return EventEmitterRef{AgentID: value}, nil, nil
	case yaml.SequenceNode:
		values, err := decodeStringListNode(node)
		if err != nil || len(values) == 0 {
			return EventEmitterRef{}, nil, err
		}
		return EventEmitterRef{AgentID: values[0]}, values[1:], nil
	case yaml.MappingNode:
		var ref EventEmitterRef
		if err := node.Decode(&ref); err != nil {
			return EventEmitterRef{}, nil, err
		}
		return ref, nil, nil
	default:
		return EventEmitterRef{}, nil, fmt.Errorf("unsupported emitter yaml node kind %d", node.Kind)
	}
}
func decodeEventPayloadSpecNode(node *yaml.Node) (EventPayloadSpec, error) {
	if node == nil || node.Kind == 0 {
		return EventPayloadSpec{}, nil
	}
	var spec EventPayloadSpec
	if err := node.Decode(&spec); err != nil {
		return EventPayloadSpec{}, err
	}
	return spec, nil
}
func hasAnyYAMLMappingKey(node *yaml.Node, keys ...string) bool {
	for _, key := range keys {
		if hasYAMLMappingKey(node, key) {
			return true
		}
	}
	return false
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
		parsed := parseTypedFieldString(node.Value)
		field.Type = parsed.Type
		field.Primary = parsed.Primary
		field.Indexed = parsed.Indexed
		field.Nullable = parsed.Nullable
		return field, nil
	case yaml.MappingNode:
		type alias EntitySchemaField
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return EntitySchemaField{}, err
		}
		field.Type = aux.Type
		field.Primary = aux.Primary
		field.Indexed = aux.Indexed
		field.Nullable = aux.Nullable
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
		parsed := parseTypedFieldString(node.Value)
		field.Type = parsed.Type
		field.Default = parsed.Default
		return field, nil
	case yaml.MappingNode:
		type alias NodeStateField
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return NodeStateField{}, err
		}
		field.Type = aux.Type
		field.Default = aux.Default
		return field, nil
	default:
		return NodeStateField{}, fmt.Errorf("unsupported node state field yaml node kind %d", node.Kind)
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
