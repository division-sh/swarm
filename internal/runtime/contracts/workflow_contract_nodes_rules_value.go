package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

var retiredNodeRuleFields = map[string]string{
	"element_id":        "rule identity derives from its declaration site",
	"emits":             "use rule-local emit",
	"payload_transform": "use rule-local emit.fields",
	"switch":            "use when/case/range selection rows",
	"threshold":         "use when/case/range selection rows",
	"policy":            "enhance rules in place rather than creating a second policy-sheet owner",
	"temporal":          "outside the selection-row grammar",
	"join":              "outside the selection-row grammar",
	"loop":              "outside the selection-row grammar",
	"collection":        "outside the selection-row grammar",
	"schedule":          "outside the selection-row grammar",
}

func projectNodeRuleRowsValue(value yamlsource.Value, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return nil, fmt.Errorf("%s handler rule collection at %s must not be null", context, value.Location())
	}
	var rows []HandlerRuleEntry
	switch value.Presence() {
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			row, err := projectNodeRuleEntryValue(item, context)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		if context == handlerRuleDecodeContextOnComplete {
			return nil, fmt.Errorf("DIALECT-OC-ORDER: on_complete at %s must be an ordered list", value.Location())
		}
		singleton, singletonErr := projectNodeRuleEntryValue(value, context)
		keyed, keyedErr := projectNodeKeyedRulesValue(value, context)
		switch {
		case singletonErr == nil && keyedErr == nil:
			return nil, fmt.Errorf("AMBIGUOUS-RULE-GRAMMAR: mapping at %s is valid both as one rule and keyed rules; use a sequence", value.Location())
		case singletonErr == nil:
			rows = []HandlerRuleEntry{singleton}
		case keyedErr == nil:
			rows = keyed
		default:
			return nil, fmt.Errorf("invalid rules at %s (singleton: %v; keyed: %v)", value.Location(), singletonErr, keyedErr)
		}
	default:
		return nil, fmt.Errorf("%s at %s must be a rule sequence or mapping", context, value.Location())
	}
	if err := validatePolicySheetRows(rows, context); err != nil {
		return nil, err
	}
	return rows, nil
}

func projectNodeKeyedRulesValue(value yamlsource.Value, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("keyed rule mapping at %s must not be empty", value.Location())
	}
	rows := make([]HandlerRuleEntry, 0, len(fields))
	seen := map[string]struct{}{}
	for _, field := range fields {
		label := strings.TrimSpace(field.Name)
		if label == "" {
			return nil, fmt.Errorf("keyed rule label at %s must not be empty", field.KeyLocation)
		}
		if _, duplicate := seen[label]; duplicate {
			return nil, fmt.Errorf("duplicate keyed rule label %q at %s", label, field.KeyLocation)
		}
		seen[label] = struct{}{}
		row, err := projectNodeRuleEntryValue(field.Value, context)
		if err != nil {
			return nil, fmt.Errorf("keyed rule %q: %w", label, err)
		}
		if row.ID == "" {
			row.ID = label
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func projectNodeRuleEntryValue(value yamlsource.Value, context handlerRuleDecodeContext) (HandlerRuleEntry, error) {
	fields, err := nodeValueFields(value, "rule", ruleFieldOptions, retiredNodeRuleFields)
	if err != nil {
		return HandlerRuleEntry{}, err
	}
	if len(fields) == 0 {
		return HandlerRuleEntry{}, fmt.Errorf("EMPTY-AUTHORED-RULE: rule at %s must not be empty", value.Location())
	}
	var out HandlerRuleEntry
	out.authored = true
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"id", &out.ID}, {"description", &out.Description}, {"advances_to", &out.AdvancesTo},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "rule."+entry.key)
			if err != nil {
				return HandlerRuleEntry{}, err
			}
		}
	}
	if condition, present := fields["condition"]; present {
		if context != handlerRuleDecodeContextOnComplete {
			return HandlerRuleEntry{}, fmt.Errorf("rule.condition at %s is not accepted in %s", condition.Location(), context)
		}
		out.Condition, err = nodeValueText(condition, "rule.condition")
		if err != nil {
			return HandlerRuleEntry{}, err
		}
		out.Condition = strings.TrimSpace(out.Condition)
	}
	for _, entry := range []struct {
		key     string
		project func(yamlsource.Value) error
	}{
		{"emit", func(v yamlsource.Value) error { out.Emit, err = projectNodeEmitValue(v); return err }},
		{"activity", func(v yamlsource.Value) error { out.Activity, err = projectNodeActivityValue(v); return err }},
		{"data_accumulation", func(v yamlsource.Value) error {
			out.DataAccumulation, err = projectNodeDataAccumulationValue(v)
			return err
		}},
		{"compute", func(v yamlsource.Value) error { out.Compute, err = projectNodeComputeValue(v); return err }},
		{"fan_out", func(v yamlsource.Value) error { out.FanOut, err = projectNodeFanOutValue(v); return err }},
	} {
		if field, present := fields[entry.key]; present {
			if err := entry.project(field); err != nil {
				return HandlerRuleEntry{}, err
			}
		}
	}
	rowKeys := []string{"when", "case", "range", "lookup", "validate", "compute_module", "else", "default"}
	var selected string
	for _, key := range rowKeys {
		if _, present := fields[key]; present {
			if selected != "" {
				return HandlerRuleEntry{}, fmt.Errorf("POLICY-SHEET-ROW: rule at %s declares multiple row types", value.Location())
			}
			selected = key
		}
	}
	if selected != "" && context != handlerRuleDecodeContextRules {
		return HandlerRuleEntry{}, fmt.Errorf("POLICY-SHEET-ROW: %s at %s only supports typed rows under handler.rules", selected, fields[selected].Location())
	}
	switch selected {
	case "when":
		when, err := nodeValueRequiredText(fields["when"], "rule.when")
		if err != nil {
			return HandlerRuleEntry{}, err
		}
		if strings.EqualFold(strings.TrimSpace(when), "else") {
			return HandlerRuleEntry{}, fmt.Errorf("POLICY-SHEET-ROW: when at %s must be a predicate; use else: true", fields["when"].Location())
		}
		out.Condition = strings.TrimSpace(when)
		out.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindWhen}
	case "else", "default":
		allowed, err := nodeValueBool(fields[selected], "rule."+selected)
		if err != nil || !allowed {
			return HandlerRuleEntry{}, fmt.Errorf("POLICY-SHEET-ROW: %s at %s must be true", selected, fields[selected].Location())
		}
		out.Condition = "else"
		out.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindDefault}
	case "case":
		out.Condition, out.PolicyRow, err = projectNodePolicyCaseValue(fields[selected])
		if err != nil {
			return HandlerRuleEntry{}, err
		}
	case "range":
		out.Condition, out.PolicyRow, err = projectNodePolicyRangeValue(fields[selected])
		if err != nil {
			return HandlerRuleEntry{}, err
		}
	case "lookup", "validate", "compute_module":
		return HandlerRuleEntry{}, fmt.Errorf("source-aware %s policy row projection is not yet implemented at %s", selected, fields[selected].Location())
	}
	return out, nil
}
