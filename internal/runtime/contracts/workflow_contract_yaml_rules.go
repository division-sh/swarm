package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"gopkg.in/yaml.v3"
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
	if err := validateAccumulateFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Into         string    `yaml:"into"`
		ExpectedFrom string    `yaml:"expected_from"`
		From         string    `yaml:"from"`
		Description  string    `yaml:"description"`
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
		Into:         strings.TrimSpace(aux.Into),
		ExpectedFrom: strings.TrimSpace(aux.ExpectedFrom),
		From:         strings.TrimSpace(aux.From),
		Description:  strings.TrimSpace(aux.Description),
		ExpectedPath: paths.Parse(aux.ExpectedFrom),
		DedupBy:      strings.TrimSpace(aux.DedupBy),
		DedupPath:    paths.Parse(aux.DedupBy),
		Threshold:    aux.Threshold,
		TimeoutMS:    aux.TimeoutMS,
		Completion:   ParseAccumulateCompletion(aux.Completion),
	}
	var err error
	if a.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete, handlerRuleDecodeContextAccumulateOnComplete); err != nil {
		return err
	}
	if a.OnTimeout, err = decodeHandlerRuleEntryNode(&aux.OnTimeout, handlerRuleDecodeContextAccumulateOnTimeout); err != nil {
		return err
	}
	return nil
}

func validateAccumulateFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	allowed := map[string]struct{}{
		"into":          {},
		"expected_from": {},
		"from":          {},
		"description":   {},
		"dedup_by":      {},
		"threshold":     {},
		"timeout_ms":    {},
		"completion":    {},
		"on_complete":   {},
		"on_timeout":    {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: accumulate field %q not in platform spec", key)
		}
	}
	return nil
}

func (f *FanOutSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	if err := validateFanOutFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		ItemsFrom string   `yaml:"items_from"`
		Target    string   `yaml:"target"`
		Emit      EmitSpec `yaml:"emit"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*f = FanOutSpec{
		ItemsFrom: strings.TrimSpace(aux.ItemsFrom),
		Target:    strings.TrimSpace(aux.Target),
		Emit:      aux.Emit,
	}
	f.ItemsFrom = strings.TrimSpace(f.ItemsFrom)
	f.ItemsPath = paths.Parse(f.ItemsFrom)
	f.Target = strings.TrimSpace(f.Target)
	return nil
}

func validateFanOutFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	allowed := map[string]struct{}{
		"items_from": {},
		"target":     {},
		"emit":       {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		switch key {
		case "emit_per_item":
			return fmt.Errorf("RETIRED: fan_out field %q is retired; use emit: <event> or emit: {event, fields}", key)
		case "emit_mapping":
			return fmt.Errorf("RETIRED: fan_out field %q is retired; move per-item payload ownership into fan_out.emit", key)
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: fan_out field %q not in platform spec", key)
		}
	}
	return nil
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
		case "", "field", "source_field", "target_field", "target_path", "expression", "value":
		default:
			return fmt.Errorf("unsupported workflow data write field %q", strings.TrimSpace(node.Content[i].Value))
		}
	}
	var aux struct {
		Field       string    `yaml:"field"`
		SourceField string    `yaml:"source_field"`
		TargetField string    `yaml:"target_field"`
		TargetPath  string    `yaml:"target_path"`
		Expression  string    `yaml:"expression"`
		Value       yaml.Node `yaml:"value"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*w = WorkflowDataWrite{
		Field:         strings.TrimSpace(aux.Field),
		SourceField:   strings.TrimSpace(aux.SourceField),
		TargetField:   strings.TrimSpace(aux.TargetField),
		TargetPathRef: strings.TrimSpace(aux.TargetPath),
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
	w.TargetPathRef = strings.TrimSpace(w.TargetPathRef)
	w.Value.hydrate()
	if err := validateExpressionValue(w.Value); err != nil {
		return err
	}
	if w.TargetField != "" && w.TargetPathRef != "" {
		return fmt.Errorf("workflow data write must not declare both target_field and target_path")
	}
	switch {
	case w.Field != "":
		if w.SourceField != "" || w.TargetField != "" || w.TargetPathRef != "" || !w.Value.IsZero() {
			return fmt.Errorf("workflow data write %q must use bare direct form only", w.Field)
		}
	case w.SourceField != "":
		if (w.TargetField == "" && w.TargetPathRef == "") || !w.Value.IsZero() {
			return fmt.Errorf("workflow data write with source_field %q must also declare target_field or target_path and no value/expression", w.SourceField)
		}
		if paths.Parse(w.SourceField).HasExplicitRoot() {
			return fmt.Errorf("workflow data source_field %q must be payload-local, not scoped", w.SourceField)
		}
	case !w.Value.IsZero():
		if w.TargetField == "" && w.TargetPathRef == "" {
			return fmt.Errorf("workflow data write with value/expression must declare target_field or target_path")
		}
		if w.Value.HasRefValue() {
			return fmt.Errorf("workflow data write %q cannot use ref expressions", w.Target())
		}
	default:
		return fmt.Errorf("workflow data write must use one canonical form")
	}
	w.SourcePath = paths.Parse(w.Source())
	w.TargetPath = paths.Parse(w.Target())
	return nil
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
