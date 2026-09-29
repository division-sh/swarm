package contracts

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

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
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return ExpressionValue{}, err
	}
	node = resolved
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

type handlerRuleDecodeContext string

const (
	handlerRuleDecodeContextRules          handlerRuleDecodeContext = "rules"
	handlerRuleDecodeContextOnComplete     handlerRuleDecodeContext = "on_complete"
	handlerRuleDecodeContextJoinOnComplete handlerRuleDecodeContext = "join.on_complete"
	handlerRuleDecodeContextJoinTimeout    handlerRuleDecodeContext = "join.timeout"
)
