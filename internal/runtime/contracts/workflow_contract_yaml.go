package contracts

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"regexp"
	"strings"
	"swarm/internal/runtime/core/paths"
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

func (p *FlowInputPins) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	var aux struct {
		Events yaml.Node `yaml:"events"`
		Reads  yaml.Node `yaml:"reads"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	events, err := decodeFlowPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	reads, err := decodeFlowPinFieldNamesNode(&aux.Reads)
	if err != nil {
		return err
	}
	*p = FlowInputPins{
		Events: events,
		Reads:  reads,
	}
	return nil
}

func (p *FlowOutputPins) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	var aux struct {
		Events yaml.Node `yaml:"events"`
		Writes yaml.Node `yaml:"writes"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	events, err := decodeFlowPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	writes, err := decodeFlowPinFieldNamesNode(&aux.Writes)
	if err != nil {
		return err
	}
	*p = FlowOutputPins{
		Events: events,
		Writes: writes,
	}
	return nil
}

func decodeFlowPinEventsNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var legacy []string
	if err := node.Decode(&legacy); err == nil {
		return append([]string{}, legacy...), nil
	}

	var structured []struct {
		Name string `yaml:"name"`
	}
	if err := node.Decode(&structured); err != nil {
		return nil, err
	}
	events := make([]string, 0, len(structured))
	for _, entry := range structured {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			continue
		}
		events = append(events, name)
	}
	return events, nil
}

func decodeFlowPinFieldNamesNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var legacy []string
	if err := node.Decode(&legacy); err == nil {
		return append([]string{}, legacy...), nil
	}

	var structured []struct {
		Field string `yaml:"field"`
	}
	if err := node.Decode(&structured); err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(structured))
	for _, entry := range structured {
		field := strings.TrimSpace(entry.Field)
		if field == "" {
			continue
		}
		fields = append(fields, field)
	}
	return fields, nil
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
	if err := validateTieredWeightedAverageSpec(*s); err != nil {
		return err
	}
	return nil
}

func validateTieredWeightedAverageSpec(spec ComputeSpec) error {
	if spec.Operation != ComputeOpWeightedAverage || len(spec.Tiers) == 0 {
		return nil
	}
	if strings.TrimSpace(spec.Keys.DimensionKey) == "" {
		return fmt.Errorf("invalid compute spec: weighted_average with tiers requires keys.dimension_key")
	}
	if len(normalizeStrings(spec.Keys.ScoreKeys)) == 0 {
		return fmt.Errorf("invalid compute spec: weighted_average with tiers requires keys.score_keys")
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
	return write, nil
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
		expr, err := decodeLiteralExpressionNode(node)
		if err != nil {
			return err
		}
		*e = expr
		return nil
	case yaml.MappingNode:
	var aux struct {
		Kind       ExpressionKind `yaml:"kind"`
		Literal    any    `yaml:"literal"`
		Ref        string `yaml:"ref"`
		CEL        string `yaml:"cel"`
			Expression string `yaml:"expression"`
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
		Target     string     `yaml:"target"`
		Expression string     `yaml:"expression"`
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
func (t *ToolSchemaEntry) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	type alias ToolSchemaEntry
	var aux struct {
		alias      `yaml:",inline"`
		Parameters *ToolInputSchema `yaml:"parameters"`
		Returns    *ToolInputSchema `yaml:"returns"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*t = ToolSchemaEntry(aux.alias)
	if aux.Parameters != nil && t.InputSchema.Type == "" && len(t.InputSchema.Properties) == 0 && len(t.InputSchema.Required) == 0 {
		t.InputSchema = *aux.Parameters
	}
	if aux.Returns != nil && t.OutputSchema.Type == "" && len(t.OutputSchema.Properties) == 0 && len(t.OutputSchema.Required) == 0 {
		t.OutputSchema = *aux.Returns
	}
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
		CreateEntity     bool                     `yaml:"create_entity"`
		Template         string                   `yaml:"template"`
		InstanceIDFrom   string                   `yaml:"instance_id_from"`
		ConfigFrom       yaml.Node                `yaml:"config_from"`
		EvidenceTarget   string                   `yaml:"evidence_target"`
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
		CreateEntity:     aux.CreateEntity,
		EvidenceTarget:   strings.TrimSpace(aux.EvidenceTarget),
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
	if strings.TrimSpace(h.Action.ID) != "" {
		if strings.TrimSpace(h.Action.Template) == "" {
			h.Action.Template = strings.TrimSpace(aux.Template)
		}
		if strings.TrimSpace(h.Action.InstanceIDFrom) == "" {
			h.Action.InstanceIDFrom = strings.TrimSpace(aux.InstanceIDFrom)
			h.Action.InstanceIDPath = paths.Parse(aux.InstanceIDFrom)
		}
		if h.Action.ConfigFrom == nil {
			if h.Action.ConfigFrom, err = decodeConfigFromSpecNode(&aux.ConfigFrom); err != nil {
				return err
			}
		}
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
		Emitter            yaml.Node `yaml:"emitter"`
		EmitterType        string    `yaml:"emitter_type"`
		Producer           yaml.Node `yaml:"producer"`
		ProducerLegacy     yaml.Node `yaml:"_producer"`
		AlternateEmitters  []string  `yaml:"alternate_emitters"`
		Consumer           yaml.Node `yaml:"consumer"`
		ConsumerLegacy     yaml.Node `yaml:"_consumer"`
		ConsumerType       yaml.Node `yaml:"consumer_type"`
		ConsumerTypeLegacy yaml.Node `yaml:"_consumer_type"`
		Source             string    `yaml:"_source"`
		Status             string    `yaml:"_status"`
		Intercepted        yaml.Node `yaml:"intercepted"`
		Passthrough        yaml.Node `yaml:"passthrough"`
		RuntimeHandling    string    `yaml:"runtime_handling"`
		OwningNode         string    `yaml:"owning_node"`
		DeliveryChannel    yaml.Node `yaml:"delivery_channel"`
		Payload            yaml.Node `yaml:"payload"`
		Required           []string  `yaml:"required"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	emitter, alternates, err := decodeEventEmitterNode(&aux.Emitter)
	if err != nil {
		return err
	}
	producer, err := decodeStringListNode(&aux.Producer)
	if err != nil {
		return err
	}
	legacyProducer, err := decodeStringListNode(&aux.ProducerLegacy)
	if err != nil {
		return err
	}
	consumer, err := decodeStringListNode(&aux.Consumer)
	if err != nil {
		return err
	}
	legacyConsumer, err := decodeStringListNode(&aux.ConsumerLegacy)
	if err != nil {
		return err
	}
	consumerType, err := decodeStringListNode(&aux.ConsumerType)
	if err != nil {
		return err
	}
	legacyConsumerType, err := decodeStringListNode(&aux.ConsumerTypeLegacy)
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
	e.Producer = mergeStringLists(producer, legacyProducer)
	e.AlternateEmitters = mergeStringLists(aux.AlternateEmitters, alternates)
	e.Consumer = mergeStringLists(consumer, legacyConsumer)
	e.ConsumerType = mergeStringLists(consumerType, legacyConsumerType)
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
		"evidence_target":   {},
		"create_entity":     {},
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
