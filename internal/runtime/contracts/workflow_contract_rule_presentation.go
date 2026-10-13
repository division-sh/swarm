package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// DisplayLabel is presentation only. DeclarationIdentity and explicit activity
// IDs own execution identity; neither may consume this projection.
func (r HandlerRuleEntry) DisplayLabel(context string, index int) string {
	position := fmt.Sprintf("%s[%d]", context, index)
	if strings.HasPrefix(context, "join.") {
		position = context
	}
	predicate := r.predicateDisplay
	if predicate == "" {
		condition := strings.TrimSpace(r.Condition)
		switch {
		case r.PolicyRow.Kind == PolicySheetRowKindDefault || strings.EqualFold(condition, "else"):
			predicate = "else"
		case r.PolicyRow.Kind == PolicySheetRowKindCase || r.PolicyRow.Kind == PolicySheetRowKindRange:
			// Direct typed fixtures have no authored predicate text. Keep their
			// admitted kind; do not reverse their lowered execution expression.
			predicate = string(r.PolicyRow.Kind)
		case condition != "":
			field := "when"
			if context == "on_complete" {
				field = "condition"
			}
			predicate = field + " " + condition
		default:
			predicate = string(r.PolicyRow.Kind)
		}
	}
	if predicate == "" {
		return position
	}
	return position + ": " + predicate
}

// Capture the already-admitted case/range spelling once, before it is lowered
// to CEL. Renderers consume this text, not YAML or the lowered expression.
func admittedRulePredicateDisplay(kind string, value yamlsource.Value) (string, error) {
	fields, err := value.Mapping()
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		text, err := admittedRulePredicateFieldDisplay(field.Value)
		if err != nil {
			return "", err
		}
		parts = append(parts, field.Name+": "+text)
	}
	return kind + " {" + strings.Join(parts, ", ") + "}", nil
}

func admittedRulePredicateFieldDisplay(value yamlsource.Value) (string, error) {
	if value.Presence() == yamlsource.PresenceSequence || value.Presence() == yamlsource.PresenceEmptySequence {
		items, err := value.Sequence()
		if err != nil {
			return "", err
		}
		parts := make([]string, 0, len(items))
		for _, item := range items {
			text, err := admittedRulePredicateScalarDisplay(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, text)
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	}
	return admittedRulePredicateScalarDisplay(value)
}

func admittedRulePredicateScalarDisplay(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return "", err
	}
	switch {
	case scalar.Style&yamlsource.SingleQuotedStyle != 0:
		return "'" + strings.ReplaceAll(scalar.Value, "'", "''") + "'", nil
	case scalar.Style&yamlsource.DoubleQuotedStyle != 0:
		return strconv.Quote(scalar.Value), nil
	default:
		return scalar.Value, nil
	}
}
