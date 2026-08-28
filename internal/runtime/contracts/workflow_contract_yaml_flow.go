package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func (r *HandlerRuleEntry) UnmarshalYAML(node *yaml.Node) error {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	if resolved == nil || resolved.Kind != yaml.MappingNode {
		return fmt.Errorf("handler rule must be a mapping with element_id; run `swarm mint-element-ids --contracts <path>`")
	}
	if len(resolved.Content) == 0 {
		return fmt.Errorf("EMPTY-AUTHORED-RULE: authored handler rule mapping must not be empty")
	}
	if err := validateUniqueNormalizedMappingKeys(resolved, "authored handler rule"); err != nil {
		return err
	}
	if err := validateRuleFieldNodes(resolved); err != nil {
		return err
	}
	type alias HandlerRuleEntry
	var aux alias
	if err := resolved.Decode(&aux); err != nil {
		return err
	}
	*r = HandlerRuleEntry(aux)
	r.authored = true
	if err := lowerPolicySheetRuleNode(resolved, r); err != nil {
		return err
	}
	return nil
}

// resolveHandlerRuleYAMLNode makes aliases presentation-only for rule grammar.
// The graph walk rejects recursive aliases before yaml.v3 can recurse through
// them while decoding a semantic row.
func resolveHandlerRuleYAMLNode(node *yaml.Node) (*yaml.Node, error) {
	if node == nil {
		return nil, nil
	}
	if err := validateHandlerRuleYAMLAliasGraph(node, map[*yaml.Node]bool{}, map[*yaml.Node]bool{}); err != nil {
		return nil, err
	}
	for node.Kind == yaml.AliasNode {
		if node.Alias == nil {
			return nil, fmt.Errorf("YAML-ALIAS: handler rule alias has no target")
		}
		node = node.Alias
	}
	return node, nil
}

func validateHandlerRuleYAMLAliasGraph(node *yaml.Node, visiting, visited map[*yaml.Node]bool) error {
	if node == nil || visited[node] {
		return nil
	}
	if visiting[node] {
		return fmt.Errorf("YAML-ALIAS-CYCLE: handler rule aliases must not be recursive")
	}
	visiting[node] = true
	if node.Kind == yaml.AliasNode {
		if node.Alias == nil {
			return fmt.Errorf("YAML-ALIAS: handler rule alias has no target")
		}
		if err := validateHandlerRuleYAMLAliasGraph(node.Alias, visiting, visited); err != nil {
			return err
		}
	} else {
		for _, child := range node.Content {
			if err := validateHandlerRuleYAMLAliasGraph(child, visiting, visited); err != nil {
				return err
			}
		}
	}
	delete(visiting, node)
	visited[node] = true
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
	"element_id":        {},
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

func (p *FlowInputPins) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	if err := validateFlowPinDirectionNode(node, "input", map[string]struct{}{"events": {}, "reads": {}}); err != nil {
		return err
	}
	var aux struct {
		Events yaml.Node `yaml:"events"`
		Reads  yaml.Node `yaml:"reads"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	eventPins, err := decodeFlowInputPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	reads, err := decodeFlowPinFieldNamesNode(&aux.Reads)
	if err != nil {
		return err
	}
	*p = FlowInputPins{
		EventPins: eventPins,
		Reads:     reads,
	}
	return nil
}

func (p *FlowOutputPins) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	if err := validateFlowPinDirectionNode(node, "output", map[string]struct{}{"events": {}, "writes": {}}); err != nil {
		return err
	}
	var aux struct {
		Events yaml.Node `yaml:"events"`
		Writes yaml.Node `yaml:"writes"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	eventPins, err := decodeFlowOutputPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	writes, err := decodeFlowPinFieldNamesNode(&aux.Writes)
	if err != nil {
		return err
	}
	*p = FlowOutputPins{
		EventPins: eventPins,
		Writes:    writes,
	}
	return nil
}

func validateFlowPinsNode(node *yaml.Node) error {
	presence := yamlsource.ValueFromNode(node).Presence()
	if presence == yamlsource.PresenceNull || presence == yamlsource.PresenceEmptyMapping {
		return fmt.Errorf("flow pins are explicitly %s; omit pins when no boundary is declared", presence)
	}
	if presence != yamlsource.PresenceMapping {
		return fmt.Errorf("flow pins must be a non-empty mapping")
	}
	if err := validateExactW2MappingKeys(node, "flow pins"); err != nil {
		return err
	}
	allowed := map[string]struct{}{"inputs": {}, "outputs": {}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if _, ok := allowed[key]; !ok {
			return NewUndefinedFieldDiagnostic("flow pins", key, allowed)
		}
		switch key {
		case "inputs":
			if err := validateFlowPinDirectionNode(node.Content[i+1], "input", map[string]struct{}{"events": {}, "reads": {}}); err != nil {
				return err
			}
		case "outputs":
			if err := validateFlowPinDirectionNode(node.Content[i+1], "output", map[string]struct{}{"events": {}, "writes": {}}); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFlowPinDirectionNode(node *yaml.Node, direction string, allowed map[string]struct{}) error {
	presence := yamlsource.ValueFromNode(node).Presence()
	if presence == yamlsource.PresenceNull || presence == yamlsource.PresenceEmptyMapping {
		return fmt.Errorf("flow %s pins are explicitly %s; omit %ss when no facts are declared", direction, presence, direction)
	}
	if presence != yamlsource.PresenceMapping {
		return fmt.Errorf("flow %s pins must be a non-empty mapping", direction)
	}
	if err := validateExactW2MappingKeys(node, "flow "+direction+" pins"); err != nil {
		return err
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if _, ok := allowed[key]; !ok {
			return NewUndefinedFieldDiagnostic("flow "+direction+" pins", key, allowed)
		}
	}
	return nil
}

func (i *TemplateInstanceField) UnmarshalYAML(node *yaml.Node) error {
	if i == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		return nil
	}
	if node.Kind != yaml.ScalarNode || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return fmt.Errorf("retired template instance form; use `instance: <field>` with one non-empty scalar identity field")
	}
	field, err := ParseTemplateInstanceField(node.Value)
	if err != nil {
		return err
	}
	*i = field
	return nil
}

func decodeFlowInputPinEventsNode(node *yaml.Node) ([]FlowInputEventPin, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	presence := yamlsource.ValueFromNode(node).Presence()
	if presence == yamlsource.PresenceNull || presence == yamlsource.PresenceEmptySequence {
		return nil, fmt.Errorf("flow input pin events are explicitly %s; omit events when no inputs are declared", presence)
	}
	if presence != yamlsource.PresenceSequence {
		return nil, fmt.Errorf("flow input pin events must be a non-empty sequence")
	}
	pins := make([]FlowInputEventPin, 0, len(node.Content))
	seen := map[string]struct{}{}
	for _, entry := range node.Content {
		pin, err := decodeFlowInputPinEventNode(entry)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[pin.EventType()]; duplicate {
			return nil, fmt.Errorf("flow input pin event %q is declared more than once", pin.EventType())
		}
		seen[pin.EventType()] = struct{}{}
		pins = append(pins, pin)
	}
	return pins, nil
}

func decodeFlowOutputPinEventsNode(node *yaml.Node) ([]FlowOutputEventPin, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	presence := yamlsource.ValueFromNode(node).Presence()
	if presence == yamlsource.PresenceNull || presence == yamlsource.PresenceEmptySequence {
		return nil, fmt.Errorf("flow output pin events are explicitly %s; omit events when no outputs are declared", presence)
	}
	if presence != yamlsource.PresenceSequence {
		return nil, fmt.Errorf("flow output pin events must be a non-empty sequence")
	}
	pins := make([]FlowOutputEventPin, 0, len(node.Content))
	seen := map[string]struct{}{}
	for _, entry := range node.Content {
		pin, err := decodeFlowOutputPinEventNode(entry)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[pin.EventType()]; duplicate {
			return nil, fmt.Errorf("flow output pin event %q is declared more than once", pin.EventType())
		}
		seen[pin.EventType()] = struct{}{}
		pins = append(pins, pin)
	}
	return pins, nil
}

var inputEventPinFieldOptions = map[string]struct{}{
	"event":      {},
	"source":     {},
	"address":    {},
	"resolution": {},
}

var outputEventPinFieldOptions = map[string]struct{}{
	"event": {},
	"sink":  {},
}

var inputEventPinResolutionFieldOptions = map[string]struct{}{
	"mode":            {},
	"from":            {},
	"aggregation":     {},
	"window":          {},
	"dedup_by":        {},
	"singleton":       {},
	"replies_to":      {},
	"correlation_key": {},
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
	presence := yamlsource.ValueFromNode(node).Presence()
	if node.Kind == yaml.ScalarNode {
		eventType, err := decodeExactFlowPinEvent(node, "input")
		if err != nil {
			return FlowInputEventPin{}, err
		}
		out := FlowInputEventPin{Event: eventType, sourceLine: node.Line, sourceCol: node.Column}
		return out, validateAuthoredFlowInputPin(out)
	}
	if presence != yamlsource.PresenceMapping {
		return FlowInputEventPin{}, fmt.Errorf("flow input event pin must be a string or mapping")
	}
	if err := validateExactW2MappingKeys(node, "flow input event pin"); err != nil {
		return FlowInputEventPin{}, err
	}
	out := FlowInputEventPin{sourceLine: node.Line, sourceCol: node.Column}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		value := node.Content[i+1]
		switch key {
		case "name":
			return FlowInputEventPin{}, fmt.Errorf("RETIRED: input event pin name is unsupported; use the exact local event identity")
		case "event":
			eventType, err := decodeExactFlowPinEvent(value, "input")
			if err != nil {
				return FlowInputEventPin{}, err
			}
			out.Event = eventType
		case "source":
			if yamlsource.ValueFromNode(value).Presence() != yamlsource.PresenceScalar {
				return FlowInputEventPin{}, fmt.Errorf("input event pin source must be an exact non-empty scalar")
			}
			source, err := ParseFlowInputPinSource(value.Value)
			if err != nil {
				return FlowInputEventPin{}, err
			}
			out.Source = source
		case "address":
			return FlowInputEventPin{}, fmt.Errorf("RETIRED: input pin address is unsupported; declare instance plus resolution")
		case "resolution":
			if err := value.Decode(&out.Resolution); err != nil {
				return FlowInputEventPin{}, fmt.Errorf("input event pin resolution: %w", err)
			}
		case "carries":
			return FlowInputEventPin{}, fmt.Errorf("RETIRED: input event pin carries are unsupported; use instance plus resolution.from/window/dedup_by")
		default:
			return FlowInputEventPin{}, NewUndefinedFieldDiagnostic("input event pin", key, inputEventPinFieldOptions)
		}
	}
	if out.Event == "" {
		return FlowInputEventPin{}, fmt.Errorf("input event pin mapping requires event; use a scalar event when no options are needed")
	}
	if out.Source == FlowInputPinSourceNone && out.Resolution.Empty() {
		return FlowInputEventPin{}, fmt.Errorf("input event pin mapping requires a non-default source or resolution; use a scalar event when no options are needed")
	}
	if err := validateAuthoredFlowInputPin(out); err != nil {
		return FlowInputEventPin{}, err
	}
	return out, nil
}

func decodeFlowOutputPinEventNode(node *yaml.Node) (FlowOutputEventPin, error) {
	presence := yamlsource.ValueFromNode(node).Presence()
	if node.Kind == yaml.ScalarNode {
		eventType, err := decodeExactFlowPinEvent(node, "output")
		if err != nil {
			return FlowOutputEventPin{}, err
		}
		out := FlowOutputEventPin{Event: eventType, sourceLine: node.Line, sourceCol: node.Column}
		return out, validateAuthoredFlowOutputPin(out)
	}
	if presence != yamlsource.PresenceMapping {
		return FlowOutputEventPin{}, fmt.Errorf("flow output event pin must be a string or mapping")
	}
	if err := validateExactW2MappingKeys(node, "flow output event pin"); err != nil {
		return FlowOutputEventPin{}, err
	}
	out := FlowOutputEventPin{sourceLine: node.Line, sourceCol: node.Column}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		value := node.Content[i+1]
		switch key {
		case "name":
			return FlowOutputEventPin{}, fmt.Errorf("RETIRED: output event pin name is unsupported; use the exact local event identity")
		case "event":
			eventType, err := decodeExactFlowPinEvent(value, "output")
			if err != nil {
				return FlowOutputEventPin{}, err
			}
			out.Event = eventType
		case "sink":
			if value.Kind != yaml.ScalarNode || strings.EqualFold(strings.TrimSpace(value.Tag), "!!null") {
				return FlowOutputEventPin{}, fmt.Errorf("output event pin sink must be %q", FlowOutputSinkCode(FlowOutputSinkHarness))
			}
			var err error
			out.Sink, err = ParseFlowOutputSink(value.Value)
			if err != nil {
				return FlowOutputEventPin{}, err
			}
		case "key":
			return FlowOutputEventPin{}, fmt.Errorf("RETIRED: output event pin key is unsupported; use the producer event business key")
		case "carries":
			return FlowOutputEventPin{}, fmt.Errorf("RETIRED: output event pin carries are unsupported; route evidence is compiled from event schema and receiver resolution")
		default:
			return FlowOutputEventPin{}, NewUndefinedFieldDiagnostic("output event pin", key, outputEventPinFieldOptions)
		}
	}
	if out.Event == "" {
		return FlowOutputEventPin{}, fmt.Errorf("output event pin mapping requires event; use a scalar event when no options are needed")
	}
	if out.Sink == FlowOutputSinkNone {
		return FlowOutputEventPin{}, fmt.Errorf("output event pin mapping requires a non-default sink; use a scalar event when no options are needed")
	}
	if err := validateAuthoredFlowOutputPin(out); err != nil {
		return FlowOutputEventPin{}, err
	}
	return out, nil
}

func decodeExactFlowPinEvent(node *yaml.Node, direction string) (string, error) {
	value := yamlsource.ValueFromNode(node)
	if value.Presence() != yamlsource.PresenceScalar {
		return "", fmt.Errorf("flow %s pin event must be an exact non-empty scalar", direction)
	}
	eventType := node.Value
	if eventType != strings.TrimSpace(eventType) || !eventidentity.IsValidName(eventType) || strings.ContainsAny(eventType, "/*") {
		return "", fmt.Errorf("flow %s pin event %q must be an exact local canonical event identity", direction, eventType)
	}
	return eventType, nil
}

func (r *FlowInputPinResolution) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*r = FlowInputPinResolution{}
		return nil
	}
	if yamlsource.ValueFromNode(node).Presence() == yamlsource.PresenceEmptyMapping {
		return fmt.Errorf("input pin resolution is explicitly empty; omit resolution when no options are declared")
	}
	if yamlsource.ValueFromNode(node).Presence() != yamlsource.PresenceMapping {
		return fmt.Errorf("input pin resolution must be a non-empty mapping")
	}
	if err := validateExactW2MappingKeys(node, "input event pin resolution"); err != nil {
		return err
	}
	var out FlowInputPinResolution
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		value := node.Content[i+1]
		switch key {
		case "mode":
			raw, err := decodeExactNonEmptyFlowPinScalar(value, "resolution.mode")
			if err != nil {
				return err
			}
			mode, err := ParseFlowInputResolutionMode(raw)
			if err != nil {
				return fmt.Errorf("resolution.mode: %w", err)
			}
			out.Mode = mode
		case "from":
			if yamlsource.ValueFromNode(value).Presence() != yamlsource.PresenceScalar {
				return fmt.Errorf("resolution.from must be an exact non-empty scalar")
			}
			if value.Value != strings.TrimSpace(value.Value) {
				return fmt.Errorf("resolution.from must not contain surrounding whitespace")
			}
			out.From = value.Value
		case "instance_key":
			return NewRetiredResolutionInstanceKeyDiagnostic()
		case "aggregation":
			decoded, err := decodeExactNonEmptyFlowPinScalar(value, "resolution.aggregation")
			if err != nil {
				return err
			}
			out.Aggregation = decoded
		case "window":
			decoded, err := decodeExactNonEmptyFlowPinScalar(value, "resolution.window")
			if err != nil {
				return err
			}
			out.Window = decoded
		case "dedup_by":
			dedup, err := decodeExactFlowPinFieldSequence(value, "resolution.dedup_by")
			if err != nil {
				return fmt.Errorf("resolution.dedup_by: %w", err)
			}
			out.DedupBy = dedup
		case "singleton":
			decoded, err := decodeExactNonEmptyFlowPinScalar(value, "resolution.singleton")
			if err != nil {
				return err
			}
			out.Singleton = decoded
		case "replies_to":
			decoded, err := decodeExactNonEmptyFlowPinScalar(value, "resolution.replies_to")
			if err != nil {
				return err
			}
			out.RepliesTo = decoded
		case "correlation_key":
			decoded, err := decodeExactNonEmptyFlowPinScalar(value, "resolution.correlation_key")
			if err != nil {
				return err
			}
			out.CorrelationKey = decoded
		default:
			return NewUndefinedFieldDiagnostic("input event pin resolution", key, inputEventPinResolutionFieldOptions)
		}
	}
	*r = out
	return nil
}

func validateExactW2MappingKeys(node *yaml.Node, owner string) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	seen := make(map[string]struct{}, len(node.Content)/2)
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := node.Content[index].Value
		if key == "" || key != strings.TrimSpace(key) {
			return fmt.Errorf("%s key %q must be one exact non-empty canonical spelling", owner, key)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%s repeats key %q", owner, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func decodeExactNonEmptyFlowPinScalar(node *yaml.Node, owner string) (string, error) {
	if yamlsource.ValueFromNode(node).Presence() != yamlsource.PresenceScalar {
		return "", fmt.Errorf("%s must be an exact non-empty scalar", owner)
	}
	if node.Value != strings.TrimSpace(node.Value) {
		return "", fmt.Errorf("%s must not contain surrounding whitespace", owner)
	}
	if node.Value == "" {
		return "", fmt.Errorf("%s must be an exact non-empty scalar", owner)
	}
	return node.Value, nil
}

func decodeFlowPinFieldNamesNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	presence := yamlsource.ValueFromNode(node).Presence()
	if presence == yamlsource.PresenceNull || presence == yamlsource.PresenceEmptySequence {
		return nil, fmt.Errorf("flow pin entity fields are explicitly %s; omit the field when no permissions are declared", presence)
	}
	if presence != yamlsource.PresenceSequence {
		return nil, fmt.Errorf("flow pin entity fields must be a non-empty scalar sequence")
	}
	return decodeExactFlowPinFieldSequence(node, "flow pin entity fields")
}

func decodeExactFlowPinFieldSequence(node *yaml.Node, owner string) ([]string, error) {
	if yamlsource.ValueFromNode(node).Presence() != yamlsource.PresenceSequence {
		return nil, fmt.Errorf("%s must be a non-empty scalar sequence", owner)
	}
	fields := make([]string, 0, len(node.Content))
	seen := map[string]struct{}{}
	for _, entry := range node.Content {
		if yamlsource.ValueFromNode(entry).Presence() != yamlsource.PresenceScalar {
			return nil, fmt.Errorf("%s entries must be exact non-empty scalars", owner)
		}
		field := entry.Value
		if field == "" || field != strings.TrimSpace(field) {
			return nil, fmt.Errorf("%s field %q must not contain surrounding whitespace", owner, field)
		}
		if _, duplicate := seen[field]; duplicate {
			return nil, fmt.Errorf("%s field %q is declared more than once", owner, field)
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	sort.Strings(fields)
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
