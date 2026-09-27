package contracts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

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

func decodeInterpolatedScalar(value string) (ExpressionValue, error) {
	parts, expressions, err := splitExpressionInterpolation(value)
	if err != nil {
		return ExpressionValue{}, err
	}
	if len(expressions) == 0 {
		return LiteralExpression(value), nil
	}
	if len(expressions) == 1 && parts[0] == "" && parts[1] == "" {
		return CELExpression(expressions[0]), nil
	}
	terms := make([]string, 0, len(expressions)*2+1)
	for i, expr := range expressions {
		if parts[i] != "" {
			terms = append(terms, strconv.Quote(parts[i]))
		}
		terms = append(terms, "string(("+expr+"))")
	}
	if parts[len(parts)-1] != "" {
		terms = append(terms, strconv.Quote(parts[len(parts)-1]))
	}
	return CELExpression(strings.Join(terms, " + ")), nil
}

func splitExpressionInterpolation(value string) ([]string, []string, error) {
	parts := []string{}
	expressions := []string{}
	for offset := 0; ; {
		start := strings.Index(value[offset:], "${")
		if start < 0 {
			parts = append(parts, value[offset:])
			break
		}
		start += offset
		parts = append(parts, value[offset:start])
		depth, quote, escaped := 1, byte(0), false
		end := -1
		for i := start + 2; i < len(value); i++ {
			c := value[i]
			if quote != 0 {
				if escaped {
					escaped = false
				} else if c == '\\' {
					escaped = true
				} else if c == quote {
					quote = 0
				}
				continue
			}
			switch c {
			case '\'', '"':
				quote = c
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					end = i
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			return nil, nil, fmt.Errorf("unterminated ${...} expression")
		}
		expr := strings.TrimSpace(value[start+2 : end])
		if expr == "" {
			return nil, nil, fmt.Errorf("empty ${...} expression")
		}
		expressions = append(expressions, expr)
		offset = end + 1
	}
	return parts, expressions, nil
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

func expressionValueCELSource(value ExpressionValue) (string, error) {
	if value.HasCELValue() {
		return "(" + value.CEL + ")", nil
	}
	if !value.HasLiteralValue() {
		return "", fmt.Errorf("unsupported nested expression value kind %q", value.Kind)
	}
	return literalCELSource(value.Literal)
}

func literalCELSource(value any) (string, error) {
	switch typed := value.(type) {
	case float64:
		text := strconv.FormatFloat(typed, 'g', -1, 64)
		if !strings.ContainsAny(text, ".eE") {
			text += ".0"
		}
		return text, nil
	case float32:
		text := strconv.FormatFloat(float64(typed), 'g', -1, 32)
		if !strings.ContainsAny(text, ".eE") {
			text += ".0"
		}
		return text, nil
	case []any:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			encoded, err := literalCELSource(item)
			if err != nil {
				return "", err
			}
			items = append(items, encoded)
		}
		return "[" + strings.Join(items, ", ") + "]", nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		items := make([]string, 0, len(keys))
		for _, key := range keys {
			encoded, err := literalCELSource(typed[key])
			if err != nil {
				return "", err
			}
			items = append(items, strconv.Quote(key)+": "+encoded)
		}
		return "{" + strings.Join(items, ", ") + "}", nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode nested expression literal: %w", err)
	}
	return string(raw), nil
}
