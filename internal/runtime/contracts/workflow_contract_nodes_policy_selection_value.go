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
	selectors, plural, err := projectNodePolicyCaseSelectors(value, fields)
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	equals, present := fields["equals"]
	if !present {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s requires equals", value.Location())
	}
	values, literals, err := projectNodePolicyCaseEquals(equals, plural)
	if err != nil {
		return "", PolicySheetRowMetadata{}, err
	}
	if len(values) != len(selectors) {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s selector count %d does not match equals count %d", value.Location(), len(selectors), len(values))
	}
	if plural && len(selectors) == 1 {
		return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: case at %s: one selector: write `selector:` and a scalar `equals:`", value.Location())
	}
	parts := make([]string, 0, len(selectors))
	seen := make(map[[2]string]int, len(selectors))
	for i := range selectors {
		// The lowered literal preserves text/bool/number distinctions without
		// inventing a separate numeric equivalence or changing authored order.
		pair := [2]string{selectors[i], literals[i]}
		if previous, duplicate := seen[pair]; duplicate {
			return "", PolicySheetRowMetadata{}, fmt.Errorf("POLICY-SHEET-ROW: duplicate case pair at positions %d and %d at %s", previous+1, i+1, value.Location())
		}
		seen[pair] = i
		parts = append(parts, selectors[i]+" == "+literals[i])
	}
	return strings.Join(parts, " && "), PolicySheetRowMetadata{
		Kind: PolicySheetRowKindCase, Selectors: selectors, CaseValues: values,
	}, nil
}

func projectNodePolicyCaseSelectors(value yamlsource.Value, fields map[string]yamlsource.Value) ([]string, bool, error) {
	singular, hasSingular := fields["selector"]
	plural, hasPlural := fields["selectors"]
	if hasSingular && hasPlural {
		return nil, false, fmt.Errorf("POLICY-SHEET-ROW: case at %s declares both selector and selectors", value.Location())
	}
	var items []yamlsource.Value
	switch {
	case hasSingular:
		items = []yamlsource.Value{singular}
	case hasPlural:
		if plural.Presence() == yamlsource.PresenceScalar || plural.Presence() == yamlsource.PresenceEmptyScalar {
			return nil, false, fmt.Errorf("POLICY-SHEET-ROW: case.selectors at %s: `selectors` is a list; for one selector write `selector`", plural.Location())
		}
		var err error
		items, err = plural.Sequence()
		if err != nil {
			return nil, false, nodeValueError(plural, err)
		}
	default:
		return nil, false, fmt.Errorf("POLICY-SHEET-ROW: case at %s requires selector or selectors", value.Location())
	}
	if len(items) == 0 {
		return nil, false, fmt.Errorf("POLICY-SHEET-ROW: case.selectors at %s requires a non-empty list", plural.Location())
	}
	selectors := make([]string, len(items))
	for i, item := range items {
		text, err := nodeValueText(item, "case.selector")
		if err != nil {
			return nil, false, err
		}
		selectors[i] = strings.TrimSpace(text)
		if err := validatePolicySheetSelector(selectors[i], "case.selector"); err != nil {
			return nil, false, nodeValueError(item, err)
		}
	}
	return selectors, hasPlural, nil
}

func projectNodePolicyCaseEquals(value yamlsource.Value, plural bool) ([]string, []string, error) {
	sequence := value.Presence() == yamlsource.PresenceSequence || value.Presence() == yamlsource.PresenceEmptySequence
	if sequence != plural {
		return nil, nil, fmt.Errorf("POLICY-SHEET-ROW: case.equals at %s: case is either singular (selector/equals) or plural (selectors/equals), not mixed", value.Location())
	}
	items := []yamlsource.Value{value}
	if sequence {
		var err error
		items, err = value.Sequence()
		if err != nil {
			return nil, nil, err
		}
	}
	values, literals := make([]string, 0, len(items)), make([]string, 0, len(items))
	for _, item := range items {
		literalValue, literal, err := projectNodePolicyLiteralValue(item, "case.equals")
		if err != nil {
			return nil, nil, err
		}
		values, literals = append(values, literalValue), append(literals, literal)
	}
	return values, literals, nil
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
