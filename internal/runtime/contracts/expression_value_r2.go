package contracts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/google/cel-go/parser/gen"
)

const R2FormatFunction = "__swarm_r2_format"

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
		terms = append(terms, R2FormatFunction+"(("+expr+"))")
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
		end := interpolationEnd(value[start+2:])
		if end < 0 {
			return nil, nil, fmt.Errorf("unterminated ${...} expression")
		}
		end += start + 2
		expr := strings.TrimSpace(value[start+2 : end])
		if expr == "" {
			return nil, nil, fmt.Errorf("empty ${...} expression")
		}
		expressions = append(expressions, expr)
		offset = end + 1
	}
	return parts, expressions, nil
}

func interpolationEnd(source string) int {
	lexer := gen.NewCELLexer(antlr.NewInputStream(source))
	depth := 0
	for token := lexer.NextToken(); token.GetTokenType() != antlr.TokenEOF; token = lexer.NextToken() {
		switch token.GetTokenType() {
		case gen.CELLexerLBRACE:
			depth++
		case gen.CELLexerRBRACE:
			if depth == 0 {
				return len(string([]rune(source)[:token.GetStart()]))
			}
			depth--
		}
	}
	return -1
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
