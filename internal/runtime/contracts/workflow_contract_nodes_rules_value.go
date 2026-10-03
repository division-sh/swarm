package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeRuleRowsValue(value yamlsource.Value, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return nil, fmt.Errorf("%s handler rule collection must not be null at %s", context, value.Location())
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
			return nil, nodeRuleMappingDiagnostic(value, singletonErr, keyedErr)
		}
	default:
		return nil, fmt.Errorf("%s at %s must be a rule sequence or mapping", context, value.Location())
	}
	if err := validatePolicySheetRows(rows, context); err != nil {
		return nil, nodeValueError(value, err)
	}
	return rows, nil
}

func nodeRuleMappingDiagnostic(value yamlsource.Value, singletonErr, keyedErr error) error {
	fields, err := value.Mapping()
	if err != nil {
		return singletonErr
	}
	if len(fields) == 0 {
		return keyedErr
	}
	for _, entry := range fields {
		if _, known := ruleFieldOptions[entry.Name]; known && entry.Value.Presence() != yamlsource.PresenceMapping && entry.Value.Presence() != yamlsource.PresenceEmptyMapping {
			return singletonErr
		}
	}
	for _, entry := range fields {
		if entry.Name == "condition" && (entry.Value.Presence() == yamlsource.PresenceMapping || entry.Value.Presence() == yamlsource.PresenceEmptyMapping) {
			return keyedErr
		}
		if _, known := ruleFieldOptions[entry.Name]; known {
			continue
		}
		if entry.Value.Presence() != yamlsource.PresenceMapping && entry.Value.Presence() != yamlsource.PresenceEmptyMapping {
			return singletonErr
		}
		return keyedErr
	}
	return singletonErr
}

func projectNodeKeyedRulesValue(value yamlsource.Value, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("keyed rule mapping must contain at least one row at %s", value.Location())
	}
	rows := make([]HandlerRuleEntry, 0, len(fields))
	seen := map[string]struct{}{}
	for _, field := range fields {
		label := strings.TrimSpace(field.Name)
		if label == "" {
			return nil, fmt.Errorf("keyed rule label must not be empty at %s", field.KeyLocation)
		}
		if _, duplicate := seen[label]; duplicate {
			return nil, fmt.Errorf("duplicate normalized key %q in keyed rules at %s", label, field.KeyLocation)
		}
		seen[label] = struct{}{}
		row, err := projectNodeRuleEntryValue(field.Value, context)
		if err != nil {
			return nil, fmt.Errorf("keyed rule %q: %w", label, err)
		}
		if row.ID == "" {
			row.ID = label
			if row.Compute != nil {
				switch {
				case row.Compute.Lookup != nil:
					row.Compute.Lookup.RowID = label
				case row.Compute.Validation != nil:
					row.Compute.Validation.RowID = label
				case row.Compute.Module != nil:
					row.Compute.Module.RowID = label
				}
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func projectNodeRuleEntryValue(value yamlsource.Value, context handlerRuleDecodeContext) (HandlerRuleEntry, error) {
	allowed := ruleFieldOptions
	if context != handlerRuleDecodeContextOnComplete {
		allowed = make(map[string]struct{}, len(ruleFieldOptions)-1)
		for key := range ruleFieldOptions {
			if key != "condition" {
				allowed[key] = struct{}{}
			}
		}
	}
	fields, err := nodeValueFields(value, "rule", allowed)
	if err != nil {
		return HandlerRuleEntry{}, err
	}
	if len(fields) == 0 {
		return HandlerRuleEntry{}, fmt.Errorf("EMPTY-AUTHORED-RULE: rule at %s must not be empty", value.Location())
	}
	if activity, present := fields["activity"]; present && context == handlerRuleDecodeContextOnComplete {
		return HandlerRuleEntry{}, nodeValueError(activity, fmt.Errorf("on_complete.activity is unsupported; declare activity on a handler or selection rule"))
	}
	out := HandlerRuleEntry{authored: true}
	if err := nodeValueTexts(fields, map[string]*string{
		"id": &out.ID, "description": &out.Description, "advances_to": &out.AdvancesTo,
	}, false); err != nil {
		return HandlerRuleEntry{}, err
	}
	if condition, present := fields["condition"]; present {
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
	if err := projectNodeRulePolicyValue(value, fields, context, &out); err != nil {
		return HandlerRuleEntry{}, err
	}
	return out, nil
}

func projectNodeRulePolicyValue(value yamlsource.Value, fields map[string]yamlsource.Value, context handlerRuleDecodeContext, out *HandlerRuleEntry) error {
	var selected string
	for _, key := range []string{"when", "case", "range", "lookup", "validate", "compute_module", "else", "default"} {
		if _, present := fields[key]; present {
			if selected != "" {
				return fmt.Errorf("POLICY-SHEET-ROW: rule at %s declares multiple row types", value.Location())
			}
			selected = key
		}
	}
	if selected != "" && context != handlerRuleDecodeContextRules {
		return fmt.Errorf("POLICY-SHEET-ROW: %s is only supported under handler.rules at %s", selected, fields[selected].Location())
	}
	var err error
	switch selected {
	case "when":
		when, err := nodeValueRequiredText(fields["when"], "rule.when")
		if err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(when), "else") {
			return nodeValueError(fields["when"], fmt.Errorf("POLICY-SHEET-ROW: when must be a CEL predicate; use else: true"))
		}
		out.Condition = strings.TrimSpace(when)
		out.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindWhen}
	case "else", "default":
		allowed, err := nodeValueBool(fields[selected], "rule."+selected)
		if err != nil || !allowed {
			return fmt.Errorf("POLICY-SHEET-ROW: %s at %s must be true", selected, fields[selected].Location())
		}
		out.Condition = "else"
		out.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindDefault}
	case "case":
		out.Condition, out.PolicyRow, err = projectNodePolicyCaseValue(fields[selected])
	case "range":
		out.Condition, out.PolicyRow, err = projectNodePolicyRangeValue(fields[selected])
	case "lookup", "validate", "compute_module":
		if !out.Emit.Empty() || out.AdvancesTo != "" || !out.Activity.Empty() || out.DataAccumulation.HasWrites() || out.FanOut != nil || out.Compute != nil {
			return fmt.Errorf("POLICY-SHEET-ROW: %s row at %s derives a value only and cannot declare branch outputs", selected, fields[selected].Location())
		}
		out.PolicyRow, out.Compute, err = projectNodePolicyValueRow(fields[selected], selected, out.ID)
	}
	if err != nil {
		return nodeValueError(fields[selected], err)
	}
	return nil
}
