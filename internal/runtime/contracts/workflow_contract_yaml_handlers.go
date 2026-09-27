package contracts

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

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
		if err := rejectWorkflowTimerRetiredDurationAliases(node); err != nil {
			return err
		}
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

func rejectWorkflowTimerRetiredDurationAliases(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	hasDelay, retired := workflowTimerDurationKeys(node, map[*yaml.Node]bool{})
	if len(retired) == 0 {
		return nil
	}
	if hasDelay {
		return fmt.Errorf("RETIRED timer duration fields %s cannot be combined with canonical delay; use delay only", strings.Join(retired, ", "))
	}
	if len(retired) == 1 {
		return fmt.Errorf("RETIRED timer duration field %s is not accepted; use delay", retired[0])
	}
	return fmt.Errorf("RETIRED timer duration fields %s are not accepted; use delay", strings.Join(retired, ", "))
}

func workflowTimerDurationKeys(node *yaml.Node, seen map[*yaml.Node]bool) (bool, []string) {
	if node == nil {
		return false, nil
	}
	if seen[node] {
		return false, nil
	}
	seen[node] = true
	switch node.Kind {
	case yaml.AliasNode:
		return workflowTimerDurationKeys(node.Alias, seen)
	case yaml.SequenceNode:
		hasDelay := false
		retired := []string{}
		for _, child := range node.Content {
			childHasDelay, childRetired := workflowTimerDurationKeys(child, seen)
			hasDelay = hasDelay || childHasDelay
			retired = appendRetiredTimerDurationFields(retired, childRetired...)
		}
		return hasDelay, retired
	case yaml.MappingNode:
		hasDelay := false
		retired := []string{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if key == "<<" || strings.TrimSpace(node.Content[i].Tag) == "!!merge" {
				mergeHasDelay, mergeRetired := workflowTimerDurationKeys(node.Content[i+1], seen)
				hasDelay = hasDelay || mergeHasDelay
				retired = appendRetiredTimerDurationFields(retired, mergeRetired...)
				continue
			}
			switch key {
			case "delay":
				hasDelay = true
			case "delay_seconds", "delay_minutes", "delay_hours", "delay_days":
				retired = appendRetiredTimerDurationFields(retired, key)
			}
		}
		return hasDelay, retired
	default:
		return false, nil
	}
}

func appendRetiredTimerDurationFields(fields []string, candidates ...string) []string {
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		seen := false
		for _, existing := range fields {
			if existing == candidate {
				seen = true
				break
			}
		}
		if !seen {
			fields = append(fields, candidate)
		}
	}
	return fields
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

func (e *EmitSpec) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = EmitSpec{}
			return nil
		}
		*e = EmitSpec{Event: strings.TrimSpace(node.Value)}
		return nil
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if key == "" {
				continue
			}
			if _, ok := emitFieldOptions[key]; !ok {
				return NewUndefinedFieldDiagnostic("emit", key, emitFieldOptions)
			}
		}
		var event string
		var from string
		fields := map[string]ExpressionValue{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "event":
				if err := value.Decode(&event); err != nil {
					return err
				}
			case "from":
				if err := value.Decode(&from); err != nil {
					return err
				}
				if err := validateEmitFromSource(from); err != nil {
					return err
				}
			case "fields":
				decoded, err := decodeEmitFieldsNode(value)
				if err != nil {
					return err
				}
				fields = decoded
			case "target":
				return fmt.Errorf("RETIRED-EMIT-ROUTING: emit.target is not accepted; use same-flow subscriptions, package connect with receiver select/reply, or an accepted external consumer")
			case "broadcast":
				return fmt.Errorf("RETIRED-EMIT-ROUTING: emit.broadcast is not accepted; use same-flow subscriptions, package connect with receiver select/reply, or a structured output pin with sink: harness for validation-only observation")
			}
		}
		*e = EmitSpec{
			Event:  strings.TrimSpace(event),
			From:   strings.TrimSpace(from),
			Fields: fields,
		}
		return nil
	default:
		return fmt.Errorf("unsupported emit yaml node kind %d", node.Kind)
	}
}

var emitFieldOptions = map[string]struct{}{
	"event":     {},
	"from":      {},
	"fields":    {},
	"target":    {},
	"broadcast": {},
}

var onSuccessFieldOptions = map[string]struct{}{
	"emit": {},
}

var activityFieldOptions = map[string]struct{}{
	"id":       {},
	"tool":     {},
	"input":    {},
	"approval": {},
}

var activityApprovalFieldOptions = map[string]struct{}{
	"decision": {},
}

func (s *HandlerOnSuccessSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*s = HandlerOnSuccessSpec{}
		return nil
	}
	if err := validateRetiredHandlerActionFields(node, "on_success"); err != nil {
		return err
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("unsupported on_success yaml node kind %d", node.Kind)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := onSuccessFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("on_success", key, onSuccessFieldOptions)
		}
	}
	var aux struct {
		Emit EmitSpec `yaml:"emit"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = HandlerOnSuccessSpec{Emit: aux.Emit}
	if !s.Empty() && s.Emit.EventType() == "" {
		return fmt.Errorf("INVALID-EMIT: on_success.emit.event is required")
	}
	return nil
}

func (a *ActivitySpec) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*a = ActivitySpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ACTIVITY: activity must be a mapping with tool and input")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := activityFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("activity", key, activityFieldOptions)
		}
	}
	var out ActivitySpec
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "id":
			if err := value.Decode(&out.ID); err != nil {
				return err
			}
		case "tool":
			if err := value.Decode(&out.Tool); err != nil {
				return err
			}
		case "input":
			input, err := decodeActivityInputNode(value)
			if err != nil {
				return err
			}
			out.Input = input
		case "approval":
			approval, err := decodeActivityApprovalNode(value)
			if err != nil {
				return err
			}
			out.Approval = approval
		}
	}
	out.ID = strings.TrimSpace(out.ID)
	out.Tool = strings.TrimSpace(out.Tool)
	*a = out
	return nil
}

func decodeActivityApprovalNode(node *yaml.Node) (*ActivityApprovalSpec, error) {
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval must be a mapping with decision")
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval must be a mapping with decision")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := activityApprovalFieldOptions[key]; !ok {
			return nil, NewUndefinedFieldDiagnostic("activity.approval", key, activityApprovalFieldOptions)
		}
	}
	var out ActivityApprovalSpec
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == "decision" {
			if err := node.Content[i+1].Decode(&out.Decision); err != nil {
				return nil, err
			}
		}
	}
	rawDecision := out.Decision
	out.Decision = strings.TrimSpace(rawDecision)
	if out.Decision == "" {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval.decision is required; use the activity id when it is the intended stable approval class")
	}
	if rawDecision != out.Decision {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval.decision %q is not canonical; use %q", rawDecision, out.Decision)
	}
	return &out, nil
}

func decodeActivityInputNode(node *yaml.Node) (map[string]ExpressionValue, error) {
	if node == nil || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-ACTIVITY: activity.input must be a mapping")
	}
	if err := validateUniqueNormalizedMappingKeys(node, "activity.input"); err != nil {
		return nil, fmt.Errorf("INVALID-ACTIVITY: %w", err)
	}
	fields := make(map[string]ExpressionValue, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		target := strings.TrimSpace(node.Content[i].Value)
		if target == "" {
			return nil, fmt.Errorf("INVALID-ACTIVITY: activity.input field name is empty")
		}
		value, err := decodeExpressionValueNode(node.Content[i+1])
		if err != nil {
			return nil, fmt.Errorf("INVALID-ACTIVITY: activity.input.%s: %w", target, err)
		}
		fields[target] = value
	}
	return fields, nil
}

func decodeEmitFieldsNode(node *yaml.Node) (map[string]ExpressionValue, error) {
	return decodeExpressionValueMapNode(node, "emit.fields")
}

func decodeExpressionValueMapNode(node *yaml.Node, label string) (map[string]ExpressionValue, error) {
	if node == nil {
		return nil, nil
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-EMIT: %s must be a mapping", label)
	}
	if err := validateUniqueNormalizedMappingKeys(node, label); err != nil {
		return nil, fmt.Errorf("INVALID-EMIT: %w", err)
	}
	fields := make(map[string]ExpressionValue, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		target := strings.TrimSpace(node.Content[i].Value)
		if target == "" {
			return nil, fmt.Errorf("INVALID-EMIT: %s field name is empty", label)
		}
		value, err := decodeExpressionValueNode(node.Content[i+1])
		if err != nil {
			return nil, fmt.Errorf("INVALID-EMIT: %s.%s: %w", label, target, err)
		}
		fields[target] = value
	}
	return fields, nil
}

// decodeExpressionValueNode is the authoring boundary for expression-bearing
// values. Dynamic containers lower to one CEL value, so runtime consumers do
// not acquire another interpretation of nested leaves.
func decodeExpressionValueNode(node *yaml.Node) (ExpressionValue, error) {
	if node == nil || node.Kind == 0 {
		return ExpressionValue{}, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag == "!!str" || (node.Tag == "" && node.Style == yaml.DoubleQuotedStyle) {
			return decodeInterpolatedScalar(node.Value)
		}
		return decodeLiteralExpressionNode(node)
	case yaml.MappingNode:
		if err := validateUniqueNormalizedMappingKeys(node, "expression value"); err != nil {
			return ExpressionValue{}, err
		}
		if len(node.Content) == 2 {
			switch node.Content[0].Value {
			case "literal":
				return decodeLiteralExpressionNode(node.Content[1])
			case "cel", "expression", "ref", "kind":
				return ExpressionValue{}, fmt.Errorf("retired expression value form %q; use ${...} or {literal: ...}", node.Content[0].Value)
			}
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == "kind" {
				for j := 0; j+1 < len(node.Content); j += 2 {
					switch node.Content[j].Value {
					case "cel", "expression", "ref", "literal":
						return ExpressionValue{}, fmt.Errorf("retired expression value kind form; use ${...} or {literal: ...}")
					}
				}
			}
		}
		return decodeExpressionContainer(node)
	case yaml.SequenceNode:
		return decodeExpressionContainer(node)
	default:
		return ExpressionValue{}, fmt.Errorf("unsupported expression value yaml node kind %d", node.Kind)
	}
}

func decodeExpressionContainer(node *yaml.Node) (ExpressionValue, error) {
	if node.Kind == yaml.SequenceNode {
		items := make([]string, 0, len(node.Content))
		literals := make([]any, 0, len(node.Content))
		dynamic := false
		for _, child := range node.Content {
			value, err := decodeExpressionValueNode(child)
			if err != nil {
				return ExpressionValue{}, err
			}
			code, err := expressionValueCELSource(value)
			if err != nil {
				return ExpressionValue{}, err
			}
			items = append(items, code)
			dynamic = dynamic || value.HasCELValue()
			literals = append(literals, value.Literal)
		}
		if !dynamic {
			return LiteralExpression(literals), nil
		}
		return CELExpression("[" + strings.Join(items, ", ") + "]"), nil
	}
	values := make(map[string]string, len(node.Content)/2)
	literals := make(map[string]any, len(node.Content)/2)
	dynamic := false
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return ExpressionValue{}, fmt.Errorf("expression value object keys must be strings")
		}
		value, err := decodeExpressionValueNode(node.Content[i+1])
		if err != nil {
			return ExpressionValue{}, fmt.Errorf("expression value field %s: %w", key.Value, err)
		}
		code, err := expressionValueCELSource(value)
		if err != nil {
			return ExpressionValue{}, err
		}
		values[key.Value] = code
		literals[key.Value] = value.Literal
		dynamic = dynamic || value.HasCELValue()
	}
	if !dynamic {
		return LiteralExpression(literals), nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]string, 0, len(keys))
	for _, key := range keys {
		items = append(items, strconv.Quote(key)+": "+values[key])
	}
	return CELExpression("{" + strings.Join(items, ", ") + "}"), nil
}

func (h *SystemNodeEventHandler) UnmarshalYAML(node *yaml.Node) error {
	if h == nil {
		return nil
	}
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	node = resolved
	if err := validateHandlerFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Activity         ActivitySpec             `yaml:"activity"`
		CreateEntity     bool                     `yaml:"create_entity"`
		Description      string                   `yaml:"description"`
		Emit             EmitSpec                 `yaml:"emit"`
		OnSuccess        HandlerOnSuccessSpec     `yaml:"on_success"`
		Guard            yaml.Node                `yaml:"guard"`
		AdvancesTo       yaml.Node                `yaml:"advances_to"`
		SetsGate         yaml.Node                `yaml:"sets_gate"`
		ClearGates       yaml.Node                `yaml:"clear_gates"`
		DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
		Condition        string                   `yaml:"condition"`
		Logic            string                   `yaml:"logic"`
		Loop             *LoopOperationSpec       `yaml:"loop"`
		OnComplete       yaml.Node                `yaml:"on_complete"`
		Rules            yaml.Node                `yaml:"rules"`
		Accumulate       *AccumulateSpec          `yaml:"accumulate"`
		Join             *JoinSpec                `yaml:"join"`
		Compute          *ComputeSpec             `yaml:"compute"`
		Query            yaml.Node                `yaml:"query"`
		FanOut           *FanOutSpec              `yaml:"fan_out"`
		GroupBy          *GroupBySpec             `yaml:"group_by"`
		Filter           *FilterSpec              `yaml:"filter"`
		Reduce           *ReduceSpec              `yaml:"reduce"`
		Count            *CountSpec               `yaml:"count"`
		Clear            yaml.Node                `yaml:"clear"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		Activity:         aux.Activity,
		CreateEntity:     aux.CreateEntity,
		Description:      strings.TrimSpace(aux.Description),
		Emit:             aux.Emit,
		OnSuccess:        aux.OnSuccess,
		DataAccumulation: aux.DataAccumulation,
		Condition:        strings.TrimSpace(aux.Condition),
		Logic:            strings.TrimSpace(aux.Logic),
		Loop:             aux.Loop,
		Accumulate:       aux.Accumulate,
		Join:             aux.Join,
		Compute:          aux.Compute,
		FanOut:           aux.FanOut,
		GroupBy:          aux.GroupBy,
		Filter:           aux.Filter,
		Reduce:           aux.Reduce,
		Count:            aux.Count,
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
	if h.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete, handlerRuleDecodeContextOnComplete); err != nil {
		return err
	}
	if h.Rules, err = decodeHandlerRuleEntriesNode(&aux.Rules, handlerRuleDecodeContextRules); err != nil {
		return err
	}
	if h.Query, err = decodeQuerySpecNode(&aux.Query); err != nil {
		return err
	}
	if h.Clear, err = decodeClearSpecNode(&aux.Clear); err != nil {
		return err
	}
	if err := HandlerEmitSiteOwnershipError(*h); err != nil {
		return err
	}
	return nil
}

func mergeCanonicalLegacyString(canonical, legacy, canonicalKey, legacyKey string) (string, error) {
	canonical = strings.TrimSpace(canonical)
	legacy = strings.TrimSpace(legacy)
	switch {
	case canonical == "":
		return legacy, nil
	case legacy == "":
		return canonical, nil
	case canonical == legacy:
		return canonical, nil
	default:
		return "", fmt.Errorf("event metadata fields %s and %s conflict: %q != %q", canonicalKey, legacyKey, canonical, legacy)
	}
}

func mergeCanonicalLegacyStringLists(canonical, legacy []string, canonicalKey, legacyKey string) ([]string, error) {
	canonical = normalizeStrings(canonical)
	legacy = normalizeStrings(legacy)
	switch {
	case len(canonical) == 0:
		return legacy, nil
	case len(legacy) == 0:
		return canonical, nil
	case sameStringSet(canonical, legacy):
		return canonical, nil
	default:
		return nil, fmt.Errorf("event metadata fields %s and %s conflict: %v != %v", canonicalKey, legacyKey, canonical, legacy)
	}
}

func sameStringSet(a, b []string) bool {
	a = normalizeStrings(a)
	b = normalizeStrings(b)
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		if seen[value] == 0 {
			return false
		}
		seen[value]--
	}
	return true
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

var handlerFieldOptions = map[string]struct{}{
	"activity":          {},
	"description":       {},
	"_note":             {},
	"create_entity":     {},
	"emit":              {},
	"on_success":        {},
	"guard":             {},
	"advances_to":       {},
	"sets_gate":         {},
	"clear_gates":       {},
	"data_accumulation": {},
	"condition":         {},
	"logic":             {},
	"loop":              {},
	"on_complete":       {},
	"rules":             {},
	"accumulate":        {},
	"join":              {},
	"compute":           {},
	"query":             {},
	"fan_out":           {},
	"group_by":          {},
	"filter":            {},
	"reduce":            {},
	"count":             {},
	"clear":             {},
	"from":              {},
	"dedup_by":          {},
}

func validateHandlerFieldNodes(node *yaml.Node) error {
	if err := validateRetiredHandlerActionFields(node, "handler"); err != nil {
		return err
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
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
		switch key {
		case "select_entity", "select_or_create_entity":
			return fmt.Errorf("RETIRED: handler field %q is retired; declare the receiver key once with flow instance and select resolution at the input/composition boundary; handlers operate on that admitted instance", key)
		case "branch":
			return fmt.Errorf("RETIRED: handler field %q is retired; use rules for branch selection", key)
		case "emits":
			return fmt.Errorf("RETIRED: handler field %q is retired; use emit: <event> or emit: {event, fields}", key)
		case "payload_transform":
			return fmt.Errorf("RETIRED: handler field %q is retired; move payload ownership into emit.fields at the active emit site", key)
		}
		if key == "on_complete" {
			if _, err := resolveHandlerRuleCollectionNode(node.Content[i+1], handlerRuleDecodeContextOnComplete); err != nil {
				return err
			}
		}
		if _, ok := handlerFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("handler", key, handlerFieldOptions)
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

type handlerRuleDecodeContext string

const (
	handlerRuleDecodeContextRules          handlerRuleDecodeContext = "rules"
	handlerRuleDecodeContextOnComplete     handlerRuleDecodeContext = "on_complete"
	handlerRuleDecodeContextJoinOnComplete handlerRuleDecodeContext = "join.on_complete"
	handlerRuleDecodeContextJoinTimeout    handlerRuleDecodeContext = "join.timeout"
)

func decodeHandlerRuleEntryNode(node *yaml.Node, context handlerRuleDecodeContext) (*HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return nil, err
	}
	if err := validateRetiredHandlerActionFields(resolved, string(context)); err != nil {
		return nil, err
	}
	var rule HandlerRuleEntry
	if err := resolved.Decode(&rule); err != nil {
		return nil, err
	}
	if strings.TrimSpace(rule.ID) == "" && strings.TrimSpace(rule.Description) == "" && strings.TrimSpace(rule.Condition) == "" && strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emit.Empty() && rule.Activity.Empty() && !rule.DataAccumulation.HasWrites() && rule.Compute == nil && rule.FanOut == nil {
		return nil, nil
	}
	return &rule, nil
}

func decodeHandlerRuleEntriesNode(node *yaml.Node, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	resolved, err := resolveHandlerRuleCollectionNode(node, context)
	if err != nil {
		return nil, err
	}
	node = resolved
	switch node.Kind {
	case yaml.SequenceNode:
		for _, row := range node.Content {
			if err := validateRetiredHandlerActionFields(row, string(context)); err != nil {
				return nil, err
			}
		}
		var rules []HandlerRuleEntry
		if err := node.Decode(&rules); err != nil {
			return nil, err
		}
		if err := validatePolicySheetRows(rules, context); err != nil {
			return nil, err
		}
		return rules, nil
	case yaml.MappingNode:
		shape, err := classifyHandlerRuleMapping(node)
		if err != nil {
			return nil, err
		}
		if shape == handlerRuleMappingSingleton {
			rule, err := decodeHandlerRuleEntryNode(node, context)
			if err != nil || rule == nil {
				return nil, err
			}
			rules := []HandlerRuleEntry{*rule}
			if err := validatePolicySheetRows(rules, context); err != nil {
				return nil, err
			}
			return rules, nil
		}
		rules := make([]HandlerRuleEntry, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			id := strings.TrimSpace(node.Content[i].Value)
			row, err := resolveHandlerRuleYAMLNode(node.Content[i+1])
			if err != nil {
				return nil, err
			}
			if err := validateRetiredHandlerActionFields(row, string(context)); err != nil {
				return nil, err
			}
			var rule HandlerRuleEntry
			if err := row.Decode(&rule); err != nil {
				return nil, err
			}
			if strings.TrimSpace(rule.ID) == "" {
				rule.ID = id
			}
			rules = append(rules, rule)
		}
		if err := validatePolicySheetRows(rules, context); err != nil {
			return nil, err
		}
		return rules, nil
	default:
		return nil, fmt.Errorf("unsupported rules yaml node kind %d", node.Kind)
	}
}

func resolveHandlerRuleCollectionNode(node *yaml.Node, context handlerRuleDecodeContext) (*yaml.Node, error) {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return nil, err
	}
	if resolved != nil && yamlNodeIsNull(resolved) {
		return nil, fmt.Errorf("%s handler rule collection must not be null", context)
	}
	if context == handlerRuleDecodeContextOnComplete && resolved != nil && resolved.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("DIALECT-OC-ORDER: on_complete is dict, must be ordered list")
	}
	return resolved, nil
}

type handlerRuleMappingShape uint8

const (
	handlerRuleMappingSingleton handlerRuleMappingShape = iota + 1
	handlerRuleMappingKeyed
)

// classifyHandlerRuleMapping resolves grammar from both the outer field names
// and child row shape. Keyed display labels are never reserved grammar tokens.
func classifyHandlerRuleMapping(node *yaml.Node) (handlerRuleMappingShape, error) {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return 0, err
	}
	if resolved == nil || resolved.Kind != yaml.MappingNode {
		return 0, fmt.Errorf("rule mapping must be a mapping")
	}
	node = resolved
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == "" {
			return 0, fmt.Errorf("keyed handler rule label must not be empty")
		}
	}

	singletonErr := decodeSingletonHandlerRuleShape(node)
	keyedStructureErr := validateKeyedHandlerRuleStructure(node)
	keyedErr := keyedStructureErr
	if keyedStructureErr == nil {
		keyedErr = decodeKeyedHandlerRuleShape(node)
	}
	switch {
	case singletonErr == nil && keyedErr == nil:
		return 0, fmt.Errorf("AMBIGUOUS-RULE-GRAMMAR: mapping is valid as both one handler rule and keyed handler rules; use sequence form to state the row boundary explicitly")
	case singletonErr == nil:
		return handlerRuleMappingSingleton, nil
	case keyedStructureErr == nil:
		if keyedErr != nil {
			if err := validateRetiredHandlerActionFields(node, "rule"); err != nil {
				return 0, err
			}
		}
		return handlerRuleMappingKeyed, nil
	default:
		return 0, fmt.Errorf("invalid handler rule mapping (singleton: %v; keyed: %v)", singletonErr, keyedErr)
	}
}

func decodeSingletonHandlerRuleShape(node *yaml.Node) error {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	if err := validateRetiredHandlerActionFields(resolved, "rule"); err != nil {
		return err
	}
	var rule HandlerRuleEntry
	return resolved.Decode(&rule)
}

func decodeKeyedHandlerRuleShape(node *yaml.Node) error {
	if err := validateKeyedHandlerRuleStructure(node); err != nil {
		return err
	}
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(resolved.Content); i += 2 {
		label := strings.TrimSpace(resolved.Content[i].Value)
		row, err := resolveHandlerRuleYAMLNode(resolved.Content[i+1])
		if err != nil {
			return err
		}
		var rule HandlerRuleEntry
		if err := row.Decode(&rule); err != nil {
			return fmt.Errorf("keyed handler rule %q: %w", label, err)
		}
	}
	return nil
}

func validateKeyedHandlerRuleStructure(node *yaml.Node) error {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	if resolved == nil || resolved.Kind != yaml.MappingNode {
		return fmt.Errorf("keyed handler rules must be a mapping")
	}
	node = resolved
	if err := validateUniqueNormalizedMappingKeys(node, "keyed handler rules"); err != nil {
		return err
	}
	if len(node.Content) == 0 {
		return fmt.Errorf("keyed handler rules must contain at least one row")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		label := strings.TrimSpace(node.Content[i].Value)
		if label == "" {
			return fmt.Errorf("keyed handler rule label must not be empty")
		}
		row, err := resolveHandlerRuleYAMLNode(node.Content[i+1])
		if err != nil {
			return fmt.Errorf("keyed handler rule %q: %w", label, err)
		}
		if row.Kind != yaml.MappingNode {
			return fmt.Errorf("keyed handler rule %q must be a mapping", label)
		}
	}
	return nil
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

func decodeClearSpecNode(node *yaml.Node) (*ClearSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if hasYAMLMappingKey(node, "target") {
		return nil, fmt.Errorf("RETIRED: clear field target is retired; use targets")
	}
	var spec ClearSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if len(spec.Targets) == 0 {
		return nil, nil
	}
	return &spec, nil
}
