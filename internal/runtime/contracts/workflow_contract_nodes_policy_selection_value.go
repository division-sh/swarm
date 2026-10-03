package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodePolicyCaseValue(value yamlsource.Value) (string, PolicySheetRowMetadata, error) {
	fields, err := nodeValueFields(value, "case", map[string]struct{}{
		"selector": {}, "selectors": {}, "equals": {},
	})
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	if _, singular := fields["selector"]; singular {
		if _, plural := fields["selectors"]; plural {
			return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s declares both selector and selectors", value.Location())
		}
	}
	var selectors []string
	if singular, present := fields["selector"]; present {
		text, err := nodeValueText(singular, "case.selector")
		if err != nil {
			return "", PolicySheetRowMetadata{}, err
		}
		selectors = []string{strings.TrimSpace(text)}
	}
	if plural, present := fields["selectors"]; present {
		selectors, err = projectNodePolicyStringListValue(plural, "case.selectors")
		if err != nil {
			return "", PolicySheetRowMetadata{}, err
		}
	}
	if len(selectors) == 0 {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s requires selector or selectors", value.Location())
	}
	for _, selector := range selectors {
		if err := validatePolicySheetSelector(selector, "case.selector"); err != nil {
			return "", PolicySheetRowMetadata{}, err
		}
	}
	equals, present := fields["equals"]
	if !present {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s requires equals", value.Location())
	}
	var values, literals []string
	if equals.Presence() == yamlsource.PresenceSequence || equals.Presence() == yamlsource.PresenceEmptySequence {
		items, err := equals.Sequence()
		if err != nil {
			return "", PolicySheetRowMetadata{}, err
		}
		for _, item := range items {
			literalValue, literal, err := projectNodePolicyLiteralValue(item, "case.equals")
			if err != nil {
				return "", PolicySheetRowMetadata{}, err
			}
			values, literals = append(values, literalValue), append(literals, literal)
		}
	} else {
		literalValue, literal, err := projectNodePolicyLiteralValue(equals, "case.equals")
		if err != nil {
			return "", PolicySheetRowMetadata{}, err
		}
		values, literals = []string{literalValue}, []string{literal}
	}
	if len(values) != len(selectors) {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s selector count %d does not match equals count %d", value.Location(), len(selectors), len(values))
	}
	parts := make([]string, 0, len(selectors))
	for i := range selectors {
		parts = append(parts, selectors[i]+" == "+literals[i])
	}
	return strings.Join(parts, " && "), PolicySheetRowMetadata{
		Kind: PolicySheetRowKindCase, Selectors: selectors, CaseValues: values,
	}, nil
}

func projectNodePolicyRangeValue(value yamlsource.Value) (string, PolicySheetRowMetadata, error) {
	fields, err := nodeValueFields(value, "range", map[string]struct{}{
		"value": {}, "gt": {}, "gte": {}, "lt": {}, "lte": {}, "monotonicity": {},
	})
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	selected, present := fields["value"]
	if !present {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range.value is required at %s", value.Location())
	}
	selector, err := nodeValueText(selected, "range.value")
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	selector = strings.TrimSpace(selector)
	if err := validatePolicySheetSelector(selector, "range.value"); err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	if _, gt := fields["gt"]; gt {
		if _, gte := fields["gte"]; gte {
			return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range at %s declares both gt and gte", value.Location())
		}
	}
	if _, lt := fields["lt"]; lt {
		if _, lte := fields["lte"]; lte {
			return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range at %s declares both lt and lte", value.Location())
		}
	}
	var lower, upper PolicySheetRangeBound
	for _, entry := range []struct {
		key      string
		operator string
		target   *PolicySheetRangeBound
	}{
		{"gt", ">", &lower}, {"gte", ">=", &lower}, {"lt", "<", &upper}, {"lte", "<=", &upper},
	} {
		if bound, present := fields[entry.key]; present {
			*entry.target, err = projectNodePolicyRangeBoundValue(bound, entry.key, entry.operator)
			if err != nil {
				return "", PolicySheetRowMetadata{}, err
			}
		}
	}
	if lower.Value == "" && upper.Value == "" {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: range at %s requires at least one bound", value.Location())
	}
	var monotonicity []string
	if list, present := fields["monotonicity"]; present {
		monotonicity, err = projectNodePolicyStringListValue(list, "range.monotonicity")
		if err != nil {
			return "", PolicySheetRowMetadata{}, err
		}
	}
	parts := make([]string, 0, 2)
	if lower.Value != "" {
		parts = append(parts, selector+" "+lower.Operator+" "+lower.Value)
	}
	if upper.Value != "" {
		parts = append(parts, selector+" "+upper.Operator+" "+upper.Value)
	}
	return strings.Join(parts, " && "), PolicySheetRowMetadata{
		Kind: PolicySheetRowKindRange, RangeValue: selector, RangeLower: lower,
		RangeUpper: upper, Monotonicity: monotonicity,
	}, nil
}

func projectNodePolicyRangeBoundValue(value yamlsource.Value, key, operator string) (PolicySheetRangeBound, error) {
	raw, err := nodeValueRequiredText(value, "range."+key)
	if err != nil {
		return PolicySheetRangeBound{}, err
	}
	raw = strings.TrimSpace(raw)
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return PolicySheetRangeBound{Operator: operator, Value: raw, Kind: "literal"}, nil
	}
	if isPolicySheetPolicyConstantExpression(raw) {
		return PolicySheetRangeBound{Operator: operator, Value: raw, Kind: "policy"}, nil
	}
	if policySheetRoot(raw) != "" {
		return PolicySheetRangeBound{}, fmt.Errorf("POLICY-SHEET-ROW: range.%s dynamic bound %q at %s is forbidden", key, raw, value.Location())
	}
	return PolicySheetRangeBound{}, fmt.Errorf("POLICY-SHEET-ROW: range.%s bound %q at %s must be numeric or a policy constant", key, raw, value.Location())
}

func projectNodePolicyStringListValue(value yamlsource.Value, label string) ([]string, error) {
	switch value.Presence() {
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		text, err := nodeValueText(value, label)
		if err != nil || strings.TrimSpace(text) == "" {
			return nil, err
		}
		return []string{strings.TrimSpace(text)}, nil
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(items))
		for _, item := range items {
			text, err := nodeValueText(item, label)
			if err != nil {
				return nil, err
			}
			if trimmed := strings.TrimSpace(text); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("POLICY-SHEET-ROW: %s at %s must be a scalar or sequence", label, value.Location())
	}
}

func projectNodePolicyLiteralValue(value yamlsource.Value, label string) (string, string, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s at %s must be a scalar: %w", label, value.Location(), err)
	}
	raw := strings.TrimSpace(scalar.Value)
	switch scalar.Tag {
	case "!!int", "!!float":
		if _, err := strconv.ParseFloat(raw, 64); err != nil {
			return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s numeric literal %q at %s is invalid", label, raw, value.Location())
		}
		return raw, raw, nil
	case "!!bool":
		if raw != "true" && raw != "false" {
			return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s bool literal %q at %s is invalid", label, raw, value.Location())
		}
		return raw, raw, nil
	case "!!str", "":
		return raw, strconv.Quote(raw), nil
	default:
		return "", "", fmt.Errorf("POLICY-SHEET-ROW: %s at %s has unsupported tag %s", label, value.Location(), scalar.Tag)
	}
}
