package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
	"swarm/internal/runtime/core/paths"
)

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
	if node == nil || node.Kind == 0 {
		*p = PayloadTransformSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("unsupported payload transform yaml node kind %d", node.Kind)
	}
	type shadow struct {
		Mappings map[string]string `yaml:"mappings"`
		Fields   map[string]string `yaml:"fields"`
		Entries  []TransformSpec   `yaml:"entries"`
	}
	var aux shadow
	if err := node.Decode(&aux); err != nil {
		return err
	}
	spec := PayloadTransformSpec{
		Mappings: cloneStringMap(aux.Mappings),
		Fields:   cloneStringMap(aux.Fields),
		Entries:  append([]TransformSpec(nil), aux.Entries...),
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		switch key {
		case "", "mappings", "fields", "entries":
			continue
		}
		entry, err := decodePayloadTransformShorthandEntry(key, node.Content[i+1])
		if err != nil {
			return err
		}
		spec.Entries = append(spec.Entries, entry)
	}
	spec.Entries = spec.TransformEntries()
	for i := range spec.Entries {
		if err := hydrateTransformSpec(&spec.Entries[i]); err != nil {
			return err
		}
	}
	*p = spec
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		switch strings.TrimSpace(node.Content[i].Value) {
		case "", "field", "source_field", "target_field", "expression", "value":
		default:
			return fmt.Errorf("unsupported workflow data write field %q", strings.TrimSpace(node.Content[i].Value))
		}
	}
	var aux struct {
		Field       string    `yaml:"field"`
		SourceField string    `yaml:"source_field"`
		TargetField string    `yaml:"target_field"`
		Expression  string    `yaml:"expression"`
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
		switch {
		case strings.TrimSpace(aux.Expression) != "":
			w.Value = CELExpression(aux.Expression)
		}
	default:
		if strings.TrimSpace(aux.Expression) != "" {
			return fmt.Errorf("workflow data write cannot declare both value and expression")
		}
		expr, err := decodeWorkflowDataWriteValueNode(&aux.Value)
		if err != nil {
			return err
		}
		w.Value = expr
	}
	return hydrateWorkflowDataWrite(w)
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
		SourceEvent string              `yaml:"source_event"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		switch strings.TrimSpace(node.Content[i].Value) {
		case "", "writes", "source_event":
		default:
			return fmt.Errorf("unsupported workflow data accumulation field %q", strings.TrimSpace(node.Content[i].Value))
		}
	}

	spec := WorkflowDataAccumulation{
		Writes:      append([]WorkflowDataWrite(nil), aux.Writes...),
		SourceEvent: strings.TrimSpace(aux.SourceEvent),
	}

	for i := range spec.Writes {
		if err := hydrateWorkflowDataWrite(&spec.Writes[i]); err != nil {
			return err
		}
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
		return WorkflowDataWrite{}, fmt.Errorf("workflow data accumulation shorthand writes are not supported; use writes list")
	case yaml.MappingNode:
		return WorkflowDataWrite{}, fmt.Errorf("workflow data accumulation shorthand writes are not supported; use writes list")
	default:
		return WorkflowDataWrite{}, fmt.Errorf("workflow data accumulation shorthand writes are not supported; use writes list")
	}
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
		expr, err := decodeLiteralExpressionNode(node)
		if err != nil {
			return err
		}
		*e = expr
		return nil
	case yaml.MappingNode:
		var aux struct {
			Kind       ExpressionKind `yaml:"kind"`
			Literal    any            `yaml:"literal"`
			Ref        string         `yaml:"ref"`
			CEL        string         `yaml:"cel"`
			Expression string         `yaml:"expression"`
		}
		if err := node.Decode(&aux); err != nil {
			return err
		}
		fields := 0
		if strings.TrimSpace(aux.Ref) != "" {
			fields++
		}
		if strings.TrimSpace(aux.CEL) != "" {
			fields++
		}
		if strings.TrimSpace(aux.Expression) != "" {
			fields++
		}
		if aux.Literal != nil {
			fields++
		}
		if fields > 1 {
			return fmt.Errorf("expression value must declare exactly one semantic field")
		}
		switch {
		case strings.TrimSpace(aux.Ref) != "":
			*e = RefExpression(aux.Ref)
		case strings.TrimSpace(aux.CEL) != "":
			*e = CELExpression(aux.CEL)
		case strings.TrimSpace(aux.Expression) != "":
			*e = CELExpression(aux.Expression)
		case aux.Literal != nil:
			*e = LiteralExpression(aux.Literal)
		case aux.Kind != "":
			switch aux.Kind {
			case ExpressionKindLiteral, ExpressionKindRef, ExpressionKindCEL:
				e.Kind = aux.Kind
			default:
				return fmt.Errorf("unsupported expression kind %q", aux.Kind)
			}
		default:
			expr, err := decodeLiteralExpressionNode(node)
			if err != nil {
				return err
			}
			*e = expr
		}
		e.hydrate()
		if err := validateExpressionValue(*e); err != nil {
			return err
		}
		return nil
	case yaml.SequenceNode:
		expr, err := decodeLiteralExpressionNode(node)
		if err != nil {
			return err
		}
		*e = expr
		return nil
	default:
		return fmt.Errorf("unsupported expression value yaml node kind %d", node.Kind)
	}
}

func (t *TransformSpec) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*t = TransformSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("unsupported payload transform entry yaml node kind %d", node.Kind)
	}
	var aux struct {
		Target     string `yaml:"target"`
		Expression string `yaml:"expression"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*t = TransformSpec{Target: strings.TrimSpace(aux.Target)}
	if strings.TrimSpace(aux.Expression) != "" {
		t.Value = CELExpression(aux.Expression)
	}
	return hydrateTransformSpec(t)
}

func decodeLiteralExpressionNode(node *yaml.Node) (ExpressionValue, error) {
	var literal any
	if err := node.Decode(&literal); err != nil {
		return ExpressionValue{}, err
	}
	return LiteralExpression(literal), nil
}

func validateExplicitScopedPath(raw, context string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed := paths.Parse(raw)
	if !parsed.HasExplicitRoot() {
		return fmt.Errorf("%s %q must use an explicit scope", context, raw)
	}
	return nil
}

func validateExpressionValue(expr ExpressionValue) error {
	if expr.IsZero() {
		return nil
	}
	expr.hydrate()
	switch expr.Kind {
	case ExpressionKindLiteral:
		return nil
	case ExpressionKindRef:
		if strings.TrimSpace(expr.Ref) == "" {
			return fmt.Errorf("ref expression requires a ref value")
		}
		return validateExplicitScopedPath(expr.Ref, "ref expression")
	case ExpressionKindCEL:
		if strings.TrimSpace(expr.CEL) == "" {
			return fmt.Errorf("cel expression requires a cel value")
		}
		return nil
	default:
		return fmt.Errorf("unsupported expression kind %q", expr.Kind)
	}
}

func hydrateWorkflowDataWrite(w *WorkflowDataWrite) error {
	if w == nil {
		return nil
	}
	w.Field = strings.TrimSpace(w.Field)
	w.SourceField = strings.TrimSpace(w.SourceField)
	w.TargetField = strings.TrimSpace(w.TargetField)
	w.Value.hydrate()
	if err := validateExpressionValue(w.Value); err != nil {
		return err
	}
	switch {
	case w.Field != "":
		if w.SourceField != "" || w.TargetField != "" || !w.Value.IsZero() {
			return fmt.Errorf("workflow data write %q must use bare direct form only", w.Field)
		}
	case w.SourceField != "":
		if w.TargetField == "" || !w.Value.IsZero() {
			return fmt.Errorf("workflow data write with source_field %q must also declare target_field and no value/expression", w.SourceField)
		}
		if paths.Parse(w.SourceField).HasExplicitRoot() {
			return fmt.Errorf("workflow data source_field %q must be payload-local, not scoped", w.SourceField)
		}
	case !w.Value.IsZero():
		if w.TargetField == "" {
			return fmt.Errorf("workflow data write with value/expression must declare target_field")
		}
		if w.Value.HasRefValue() {
			return fmt.Errorf("workflow data write %q cannot use ref expressions", w.TargetField)
		}
	default:
		return fmt.Errorf("workflow data write must use one canonical form")
	}
	w.SourcePath = paths.Parse(w.Source())
	w.TargetPath = paths.Parse(w.Target())
	return nil
}

func hydrateTransformSpec(t *TransformSpec) error {
	if t == nil {
		return nil
	}
	t.Target = strings.TrimSpace(t.Target)
	t.TargetPath = paths.Parse(t.Target)
	t.Value.hydrate()
	if err := validateExpressionValue(t.Value); err != nil {
		return err
	}
	if !t.Value.HasCELValue() {
		return fmt.Errorf("payload transform target %q must use a CEL expression", t.Target)
	}
	return nil
}

func decodePayloadTransformShorthandEntry(target string, node *yaml.Node) (TransformSpec, error) {
	entry := TransformSpec{Target: strings.TrimSpace(target)}
	if node == nil || node.Kind == 0 {
		entry.TargetPath = paths.Parse(entry.Target)
		return entry, nil
	}
	if node.Kind == yaml.ScalarNode {
		entry.Value = CELExpression(strings.TrimSpace(node.Value))
		return entry, hydrateTransformSpec(&entry)
	}
	return TransformSpec{}, fmt.Errorf("payload transform target %q must be a CEL expression string", target)
}

func decodeWorkflowDataWriteValueNode(node *yaml.Node) (ExpressionValue, error) {
	if node == nil || node.Kind == 0 {
		return ExpressionValue{}, nil
	}
	var literal any
	if err := node.Decode(&literal); err != nil {
		return ExpressionValue{}, err
	}
	return LiteralExpression(literal), nil
}
