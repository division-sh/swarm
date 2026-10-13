package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func listDeclarationScalar(value yamlsource.Value, context string, explicitType bool) (string, error) {
	raw, err := requiredLiteralString(value, context+" type")
	if err != nil {
		return "", err
	}
	scalar, err := value.Scalar()
	if err != nil {
		return "", err
	}
	if !explicitType && scalar.Style != 0 && strings.HasPrefix(strings.TrimSpace(raw), "[") {
		return "", nodeValueError(value, fmt.Errorf("%w: %s styled list reference is not a declaration; use [T] unquoted, or an explicit type: scalar for metadata", ErrInvalidField, context))
	}
	return raw, nil
}

func listDeclarationSequence(value yamlsource.Value, context string) (string, error) {
	items, err := value.Sequence()
	if err != nil {
		return "", err
	}
	if len(items) != 1 {
		return "", nodeValueError(value, fmt.Errorf("%w: %s list declaration requires exactly one plain unquoted element type; use [T]; for numeric(p,s), use a one-item block sequence", ErrInvalidField, context))
	}
	element, err := items[0].Scalar()
	if err != nil || element.Tag != "!!str" || element.Style != 0 || strings.TrimSpace(element.Value) == "" {
		return "", nodeValueError(items[0], fmt.Errorf("%w: %s list declaration requires a plain unquoted type token; quoted literals are data, not declarations; use [T]", ErrInvalidField, context))
	}
	return "[" + strings.TrimSpace(element.Value) + "]", nil
}
