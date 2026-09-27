package contracts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

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
