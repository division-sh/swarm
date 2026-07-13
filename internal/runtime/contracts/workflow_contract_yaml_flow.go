package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func (r *HandlerRuleEntry) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		r.Description = strings.TrimSpace(node.Value)
		return nil
	}
	if err := validateRuleFieldNodes(node); err != nil {
		return err
	}
	type alias HandlerRuleEntry
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = HandlerRuleEntry(aux)
	if err := lowerPolicySheetRuleNode(node, r); err != nil {
		return err
	}
	return nil
}

func validateRuleFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		switch key {
		case "emits":
			return fmt.Errorf("RETIRED: rule field %q is retired; use emit: <event> or emit: {event, fields}", key)
		case "payload_transform":
			return fmt.Errorf("RETIRED: rule field %q is retired; move payload ownership into rule-local emit.fields", key)
		case "switch", "threshold":
			return fmt.Errorf("UNSUPPORTED-POLICY-SHEET-ROW: rule field %q is not a standalone row type; use rules when/case/range selection rows or split value lookup to compute", key)
		case "policy":
			return fmt.Errorf("UNSUPPORTED-POLICY-SHEET-ROW: rule field %q would create a second policy-sheet authoring owner; enhance rules in place", key)
		case "temporal", "join", "loop", "collection", "schedule":
			return fmt.Errorf("UNSUPPORTED-POLICY-SHEET-ROW: rule field %q is outside the promoted selection-row scope", key)
		}
		if _, ok := ruleFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("rule", key, ruleFieldOptions)
		}
	}
	return nil
}

var ruleFieldOptions = map[string]struct{}{
	"id":                {},
	"description":       {},
	"condition":         {},
	"when":              {},
	"case":              {},
	"range":             {},
	"lookup":            {},
	"validate":          {},
	"compute_module":    {},
	"else":              {},
	"default":           {},
	"advances_to":       {},
	"emit":              {},
	"action":            {},
	"activity":          {},
	"data_accumulation": {},
	"compute":           {},
	"fan_out":           {},
}

var templateInstanceFieldOptions = map[string]struct{}{
	"by":          {},
	"on_missing":  {},
	"on_conflict": {},
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
	events, eventPins, err := decodeFlowInputPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	reads, err := decodeFlowPinFieldNamesNode(&aux.Reads)
	if err != nil {
		return err
	}
	*p = FlowInputPins{
		Events:    events,
		EventPins: eventPins,
		Reads:     reads,
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
	events, eventPins, err := decodeFlowOutputPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	writes, err := decodeFlowPinFieldNamesNode(&aux.Writes)
	if err != nil {
		return err
	}
	*p = FlowOutputPins{
		Events:    events,
		EventPins: eventPins,
		Writes:    writes,
	}
	return nil
}

func (i *FlowTemplateInstanceDeclaration) UnmarshalYAML(node *yaml.Node) error {
	if i == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("template instance must be a mapping")
	}
	out := FlowTemplateInstanceDeclaration{Declared: true}
	for idx := 0; idx+1 < len(node.Content); idx += 2 {
		key := strings.TrimSpace(node.Content[idx].Value)
		value := node.Content[idx+1]
		switch key {
		case "":
			continue
		case "by":
			by, err := decodeTemplateInstanceByNode(value)
			if err != nil {
				return err
			}
			out.By = by
		case "on_missing":
			out.OnMissingDeclared = true
			if err := value.Decode(&out.OnMissing); err != nil {
				return fmt.Errorf("template instance on_missing: %w", err)
			}
			out.OnMissing = strings.TrimSpace(out.OnMissing)
		case "on_conflict":
			out.OnConflictDeclared = true
			if err := value.Decode(&out.OnConflict); err != nil {
				return fmt.Errorf("template instance on_conflict: %w", err)
			}
			out.OnConflict = strings.TrimSpace(out.OnConflict)
		default:
			return NewUndefinedFieldDiagnostic("template instance", key, templateInstanceFieldOptions)
		}
	}
	*i = out
	return nil
}

func decodeTemplateInstanceByNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		out := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			if item == nil || item.Kind != yaml.ScalarNode {
				return nil, fmt.Errorf("template instance by entries must be strings")
			}
			out = append(out, strings.TrimSpace(item.Value))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("template instance by must be a string or sequence")
	}
}

func decodeFlowInputPinEventsNode(node *yaml.Node) ([]string, []FlowInputEventPin, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil, nil
	}
	if node.Kind != yaml.SequenceNode {
		return nil, nil, fmt.Errorf("flow pin events must be a sequence")
	}
	events := make([]string, 0, len(node.Content))
	pins := make([]FlowInputEventPin, 0, len(node.Content))
	for _, entry := range node.Content {
		pin, err := decodeFlowInputPinEventNode(entry)
		if err != nil {
			return nil, nil, err
		}
		pin = pin.normalized()
		if pin.PinName() == "" {
			continue
		}
		events = append(events, pin.EventType())
		pins = append(pins, pin)
	}
	return events, pins, nil
}

func decodeFlowOutputPinEventsNode(node *yaml.Node) ([]string, []FlowOutputEventPin, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil, nil
	}
	if node.Kind != yaml.SequenceNode {
		return nil, nil, fmt.Errorf("flow pin events must be a sequence")
	}
	events := make([]string, 0, len(node.Content))
	pins := make([]FlowOutputEventPin, 0, len(node.Content))
	for _, entry := range node.Content {
		pin, err := decodeFlowOutputPinEventNode(entry)
		if err != nil {
			return nil, nil, err
		}
		pin = pin.normalized()
		if pin.PinName() == "" {
			continue
		}
		events = append(events, pin.EventType())
		pins = append(pins, pin)
	}
	return events, pins, nil
}

var inputEventPinFieldOptions = map[string]struct{}{
	"name":       {},
	"event":      {},
	"source":     {},
	"address":    {},
	"resolution": {},
	"carries":    {},
}

var outputEventPinFieldOptions = map[string]struct{}{
	"name":    {},
	"event":   {},
	"key":     {},
	"carries": {},
}

var inputEventPinCarryFieldOptions = map[string]struct{}{
	"from":     {},
	"type":     {},
	"optional": {},
	"convert":  {},
}

var inputEventPinResolutionFieldOptions = map[string]struct{}{
	"mode":            {},
	"instance_key":    {},
	"aggregation":     {},
	"window":          {},
	"dedup_by":        {},
	"singleton":       {},
	"replies_to":      {},
	"correlation_key": {},
}

var inputEventPinResolutionInstanceKeyFieldOptions = map[string]struct{}{
	"from": {},
	"mint": {},
	"as":   {},
}

var inputEventPinAddressFieldOptions = map[string]struct{}{
	"by":          {},
	"source":      {},
	"target":      {},
	"cardinality": {},
	"mode":        {},
}

var computeFieldOptions = map[string]struct{}{
	"operation":   {},
	"tiers":       {},
	"keys":        {},
	"params":      {},
	"store_as":    {},
	"description": {},
}

func decodeFlowInputPinEventNode(node *yaml.Node) (FlowInputEventPin, error) {
	if node == nil || node.Kind == 0 {
		return FlowInputEventPin{}, nil
	}
	if node.Kind == yaml.ScalarNode {
		value := strings.TrimSpace(node.Value)
		return FlowInputEventPin{Name: value, Event: value}, nil
	}
	if node.Kind != yaml.MappingNode {
		return FlowInputEventPin{}, fmt.Errorf("flow input event pin must be a string or mapping")
	}
	var out FlowInputEventPin
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "name":
			if err := value.Decode(&out.Name); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin name: %w", err)
			}
		case "event":
			if err := value.Decode(&out.Event); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin event: %w", err)
			}
		case "source":
			if err := value.Decode(&out.Source); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin source: %w", err)
			}
			if source := strings.ToLower(strings.TrimSpace(out.Source)); source != "" && source != "external" {
				return FlowInputEventPin{}, fmt.Errorf("input event pin source must be external")
			}
		case "address":
			var address FlowInputPinAddress
			if err := value.Decode(&address); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin address: %w", err)
			}
			out.Address = &address
		case "resolution":
			if err := value.Decode(&out.Resolution); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin resolution: %w", err)
			}
		case "carries":
			if err := value.Decode(&out.Carries); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin carries: %w", err)
			}
		default:
			return FlowInputEventPin{}, NewUndefinedFieldDiagnostic("input event pin", key, inputEventPinFieldOptions)
		}
	}
	out = out.normalized()
	if out.PinName() == "" {
		return FlowInputEventPin{}, NewExpectedShapeDiagnostic(
			"contract_loader.input_event_pin_name_required",
			"schema.yaml.pins.inputs.events",
			"input event pins must name the pin or use a scalar event name.",
			"Use `events: [item.received]` or `events: [{name: item_received, event: item.received, source: external}]`.",
			nil,
		)
	}
	return out, nil
}

func decodeFlowOutputPinEventNode(node *yaml.Node) (FlowOutputEventPin, error) {
	if node == nil || node.Kind == 0 {
		return FlowOutputEventPin{}, nil
	}
	if node.Kind == yaml.ScalarNode {
		value := strings.TrimSpace(node.Value)
		return FlowOutputEventPin{Name: value, Event: value}, nil
	}
	if node.Kind != yaml.MappingNode {
		return FlowOutputEventPin{}, fmt.Errorf("flow output event pin must be a string or mapping")
	}
	var out FlowOutputEventPin
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "name":
			if err := value.Decode(&out.Name); err != nil {
				return FlowOutputEventPin{}, fmt.Errorf("output event pin name: %w", err)
			}
		case "event":
			if err := value.Decode(&out.Event); err != nil {
				return FlowOutputEventPin{}, fmt.Errorf("output event pin event: %w", err)
			}
		case "key":
			if err := value.Decode(&out.Key); err != nil {
				return FlowOutputEventPin{}, fmt.Errorf("output event pin key: %w", err)
			}
		case "carries":
			carries, err := decodeFlowOutputPinCarriesNode(value)
			if err != nil {
				return FlowOutputEventPin{}, fmt.Errorf("output event pin carries: %w", err)
			}
			out.Carries = carries
		default:
			return FlowOutputEventPin{}, NewUndefinedFieldDiagnostic("output event pin", key, outputEventPinFieldOptions)
		}
	}
	out = out.normalized()
	if out.PinName() == "" {
		return FlowOutputEventPin{}, NewOutputEventPinNameRequiredDiagnostic(nil)
	}
	return out, nil
}

func decodeFlowOutputPinCarriesNode(node *yaml.Node) ([]string, error) {
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
		out := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			if item == nil || item.Kind != yaml.ScalarNode {
				return nil, fmt.Errorf("carries entries must be strings")
			}
			out = append(out, strings.TrimSpace(item.Value))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("carries must be a string or sequence")
	}
}

func (c *FlowInputPinCarries) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*c = nil
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("input pin carries must be a mapping")
	}
	out := FlowInputPinCarries{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		name := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if name == "" {
			continue
		}
		var carry FlowInputPinCarry
		if err := value.Decode(&carry); err != nil {
			return fmt.Errorf("carry %s: %w", name, err)
		}
		out[name] = carry.normalized()
	}
	*c = out.normalized()
	return nil
}

func (c *FlowInputPinCarry) UnmarshalYAML(node *yaml.Node) error {
	if c == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*c = FlowInputPinCarry{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("input pin carry must be a mapping")
	}
	var out FlowInputPinCarry
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "from":
			if err := value.Decode(&out.From); err != nil {
				return fmt.Errorf("carry.from: %w", err)
			}
		case "type":
			if err := value.Decode(&out.Type); err != nil {
				return fmt.Errorf("carry.type: %w", err)
			}
		case "optional":
			if err := value.Decode(&out.Optional); err != nil {
				return fmt.Errorf("carry.optional: %w", err)
			}
		case "convert":
			if err := value.Decode(&out.Convert); err != nil {
				return fmt.Errorf("carry.convert: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("input event pin carry", key, inputEventPinCarryFieldOptions)
		}
	}
	*c = out.normalized()
	return nil
}

func (r *FlowInputPinResolution) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*r = FlowInputPinResolution{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("input pin resolution must be a mapping")
	}
	var out FlowInputPinResolution
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "mode":
			if err := value.Decode(&out.Mode); err != nil {
				return fmt.Errorf("resolution.mode: %w", err)
			}
		case "instance_key":
			if err := value.Decode(&out.InstanceKey); err != nil {
				return fmt.Errorf("resolution.instance_key: %w", err)
			}
		case "aggregation":
			if err := value.Decode(&out.Aggregation); err != nil {
				return fmt.Errorf("resolution.aggregation: %w", err)
			}
		case "window":
			if err := value.Decode(&out.Window); err != nil {
				return fmt.Errorf("resolution.window: %w", err)
			}
		case "dedup_by":
			dedup, err := decodeFlowOutputPinCarriesNode(value)
			if err != nil {
				return fmt.Errorf("resolution.dedup_by: %w", err)
			}
			out.DedupBy = dedup
		case "singleton":
			if err := value.Decode(&out.Singleton); err != nil {
				return fmt.Errorf("resolution.singleton: %w", err)
			}
		case "replies_to":
			if err := value.Decode(&out.RepliesTo); err != nil {
				return fmt.Errorf("resolution.replies_to: %w", err)
			}
		case "correlation_key":
			if err := value.Decode(&out.CorrelationKey); err != nil {
				return fmt.Errorf("resolution.correlation_key: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("input event pin resolution", key, inputEventPinResolutionFieldOptions)
		}
	}
	*r = out.normalized()
	return nil
}

func (k *FlowInputPinResolutionInstanceKey) UnmarshalYAML(node *yaml.Node) error {
	if k == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*k = FlowInputPinResolutionInstanceKey{}
		return nil
	}
	if node.Kind == yaml.ScalarNode {
		*k = (FlowInputPinResolutionInstanceKey{From: strings.TrimSpace(node.Value)}).normalized()
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("resolution.instance_key must be a string or mapping")
	}
	var out FlowInputPinResolutionInstanceKey
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "from":
			if err := value.Decode(&out.From); err != nil {
				return fmt.Errorf("instance_key.from: %w", err)
			}
		case "mint":
			if err := value.Decode(&out.Mint); err != nil {
				return fmt.Errorf("instance_key.mint: %w", err)
			}
		case "as":
			if err := value.Decode(&out.As); err != nil {
				return fmt.Errorf("instance_key.as: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("input event pin resolution.instance_key", key, inputEventPinResolutionInstanceKeyFieldOptions)
		}
	}
	*k = out.normalized()
	return nil
}

func (a *FlowInputPinAddress) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*a = FlowInputPinAddress{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("input event pin address must be a mapping")
	}
	var out FlowInputPinAddress
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "":
			continue
		case "by":
			if err := value.Decode(&out.By); err != nil {
				return fmt.Errorf("address.by: %w", err)
			}
		case "source":
			if err := value.Decode(&out.Source); err != nil {
				return fmt.Errorf("address.source: %w", err)
			}
		case "target":
			if err := value.Decode(&out.Target); err != nil {
				return fmt.Errorf("address.target: %w", err)
			}
		case "cardinality":
			if err := value.Decode(&out.Cardinality); err != nil {
				return fmt.Errorf("address.cardinality: %w", err)
			}
		case "mode":
			if err := value.Decode(&out.Mode); err != nil {
				return fmt.Errorf("address.mode: %w", err)
			}
		default:
			return NewUndefinedFieldDiagnostic("input event pin address", key, inputEventPinAddressFieldOptions)
		}
	}
	*a = out.normalized()
	return nil
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
	if err := validateComputeFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Operation   ComputeOperation `yaml:"operation"`
		Tiers       []ComputeTier    `yaml:"tiers"`
		Keys        ComputeKeyConfig `yaml:"keys"`
		Params      map[string]any   `yaml:"params"`
		StoreAs     string           `yaml:"store_as"`
		Description string           `yaml:"description"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ComputeSpec{
		Operation:   aux.Operation,
		Tiers:       aux.Tiers,
		Keys:        aux.Keys,
		Params:      aux.Params,
		StoreAs:     strings.TrimSpace(aux.StoreAs),
		Description: strings.TrimSpace(aux.Description),
	}
	if err := validateTieredWeightedAverageSpec(*s); err != nil {
		return err
	}
	return nil
}

func validateComputeFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := computeFieldOptions[key]; ok {
			continue
		}
		return NewUndefinedFieldDiagnostic("compute", key, computeFieldOptions)
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
