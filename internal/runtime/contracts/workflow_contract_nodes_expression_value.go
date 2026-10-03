package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeExpressionValue(value yamlsource.Value) (ExpressionValue, error) {
	switch value.Presence() {
	case yamlsource.PresenceMissing:
		return ExpressionValue{}, nil
	case yamlsource.PresenceNull:
		return projectNodeLiteralValue(value)
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		scalar, err := value.Scalar()
		if err != nil {
			return ExpressionValue{}, err
		}
		if scalar.Tag == "!!str" || scalar.Tag == "" && scalar.Style == yamlsource.DoubleQuotedStyle {
			return decodeInterpolatedScalar(scalar.Value)
		}
		return projectNodeLiteralValue(value)
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		return projectNodeExpressionSequenceValue(value)
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		return projectNodeExpressionObjectValue(value)
	default:
		return ExpressionValue{}, fmt.Errorf("unsupported expression value at %s: %s", value.Location(), value.Presence())
	}
}

func projectNodeExpressionSequenceValue(value yamlsource.Value) (ExpressionValue, error) {
	items, err := value.Sequence()
	if err != nil {
		return ExpressionValue{}, err
	}
	codes := make([]string, 0, len(items))
	literals := make([]any, 0, len(items))
	dynamic := false
	for _, item := range items {
		projected, err := projectNodeExpressionValue(item)
		if err != nil {
			return ExpressionValue{}, err
		}
		code, err := expressionValueCELSource(projected)
		if err != nil {
			return ExpressionValue{}, err
		}
		codes = append(codes, code)
		literals = append(literals, projected.Literal)
		dynamic = dynamic || projected.HasCELValue()
	}
	if !dynamic {
		return LiteralExpression(literals), nil
	}
	return CELExpression("[" + strings.Join(codes, ", ") + "]"), nil
}

func projectNodeExpressionObjectValue(value yamlsource.Value) (ExpressionValue, error) {
	if err := value.ValidateUniqueMappings(); err != nil {
		return ExpressionValue{}, err
	}
	fields, err := value.Mapping()
	if err != nil {
		return ExpressionValue{}, err
	}
	if len(fields) == 1 && fields[0].Name == "literal" {
		return projectNodeLiteralValue(fields[0].Value)
	}
	if err := rejectNodeReservedExpressionShape(fields); err != nil {
		return ExpressionValue{}, err
	}
	codes := make(map[string]string, len(fields))
	literals := make(map[string]any, len(fields))
	dynamic := false
	for _, field := range fields {
		if field.KeyTag != "!!str" {
			return ExpressionValue{}, fmt.Errorf("expression value object key %q at %s must be a string", field.Name, field.KeyLocation)
		}
		projected, err := projectNodeExpressionValue(field.Value)
		if err != nil {
			return ExpressionValue{}, err
		}
		code, err := expressionValueCELSource(projected)
		if err != nil {
			return ExpressionValue{}, err
		}
		codes[field.Name] = code
		literals[field.Name] = projected.Literal
		dynamic = dynamic || projected.HasCELValue()
	}
	if !dynamic {
		return LiteralExpression(literals), nil
	}
	items := make([]string, 0, len(codes))
	for _, key := range sortedContractKeys(codes) {
		items = append(items, strconv.Quote(key)+": "+codes[key])
	}
	return CELExpression("{" + strings.Join(items, ", ") + "}"), nil
}

func rejectNodeReservedExpressionShape(fields []yamlsource.MappingField) error {
	if len(fields) == 1 {
		switch fields[0].Name {
		case "cel", "expression", "ref", "kind":
			return fmt.Errorf("unsupported expression value form %q at %s", fields[0].Name, fields[0].IntroductionLocation())
		}
	}
	for _, field := range fields {
		if field.Name != "kind" {
			continue
		}
		for _, other := range fields {
			switch other.Name {
			case "cel", "expression", "ref", "literal":
				return fmt.Errorf("unsupported expression value kind form at %s", field.IntroductionLocation())
			}
		}
	}
	return nil
}

func projectNodeLiteralValue(value yamlsource.Value) (ExpressionValue, error) {
	var literal any
	if err := value.Project(&literal); err != nil {
		return ExpressionValue{}, fmt.Errorf("%s at %s: %w", value.SemanticPath(), value.Location(), err)
	}
	return LiteralExpression(literal), nil
}

func projectNodeExpressionFields(value yamlsource.Value, owner string) (map[string]ExpressionValue, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return nil, nil
	}
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("%s at %s must be a mapping", value.SemanticPath(), value.Location())
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return nil, err
	}
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]ExpressionValue, len(fields))
	for _, field := range fields {
		target := strings.TrimSpace(field.Name)
		if target == "" || target != field.Name {
			return nil, fmt.Errorf("%s field name %q at %s is invalid", owner, field.Name, field.KeyLocation)
		}
		projected, err := projectNodeExpressionValue(field.Value)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", owner, target, err)
		}
		out[target] = projected
	}
	return out, nil
}
