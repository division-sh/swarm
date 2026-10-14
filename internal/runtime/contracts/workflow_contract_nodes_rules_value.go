package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeRuleRowsValue(value yamlsource.Value, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	if value.Presence() != yamlsource.PresenceSequence && value.Presence() != yamlsource.PresenceEmptySequence {
		if context == handlerRuleDecodeContextOnComplete {
			return nil, nodeValueError(value, fmt.Errorf("DIALECT-OC-ORDER: on_complete at %s must be an ordered list of rule rows; write on_complete: [{condition: payload.ready, emit: done}]", value.Location()))
		}
		return nil, nodeValueError(value, fmt.Errorf("DIALECT-RULES-ORDER: %s at %s must be an ordered list of rule rows, evaluated top to bottom.\n  %s:\n    - when: payload.kind == 'a'\n      emit: routed.a\n    - emit: routed.other\nA mapping keyed by rule name, a single row without \"- \", or a row \"id:\" is not accepted; rows are identified by position. The last row without a predicate is the default branch; a block that matches nothing already succeeds and records no_match.", context, value.Location(), context))
	}
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	var rows []HandlerRuleEntry
	for index, item := range items {
		row, err := projectNodeRuleEntryValue(item, context)
		if err != nil {
			return nil, err
		}
		if context == handlerRuleDecodeContextRules && row.Condition == "" &&
			row.PolicyRow.Kind != PolicySheetRowKindLookup && row.PolicyRow.Kind != PolicySheetRowKindValidate && row.PolicyRow.Kind != PolicySheetRowKindModule {
			if index != len(items)-1 {
				return nil, nodeValueError(item, fmt.Errorf("DIALECT-RULES-DEFAULT: rules[%d] at %s has no predicate but is not the last row; rows after it are unreachable. Add a when/case/range, or move it last to make it the default branch.", index, item.Location()))
			}
			if strings.TrimSpace(row.AdvancesTo) == "" && row.Emit.EventType() == "" && !row.Emit.HasFields() && row.Activity.Tool == "" && !row.DataAccumulation.HasWrites() &&
				(row.Compute == nil || row.Compute.Operation == ComputeOpUnknown) && row.FanOut == nil {
				return nil, nodeValueError(item, fmt.Errorf("DIALECT-RULES-DEFAULT: rules[%d] at %s is empty; a default row must advance, emit, set data or gates. A block that matches nothing already succeeds and records no_match.", index, item.Location()))
			}
			row.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindDefault}
		}
		rows = append(rows, row)
	}
	if err := validatePolicySheetRows(rows, context); err != nil {
		return nil, nodeValueError(value, err)
	}
	for index, row := range rows {
		if row.Compute == nil {
			continue
		}
		display := row.DisplayLabel(string(context), index)
		switch {
		case row.Compute.Lookup != nil:
			row.Compute.Lookup.RowID = display
		case row.Compute.Validation != nil:
			row.Compute.Validation.RowID = display
		case row.Compute.Module != nil:
			row.Compute.Module.RowID = display
		}
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
		if context == handlerRuleDecodeContextRules {
			return HandlerRuleEntry{}, nodeValueError(value, fmt.Errorf("DIALECT-RULES-DEFAULT: %s at %s is empty; a default row must advance, emit, set data or gates. A block that matches nothing already succeeds and records no_match.", value.SemanticPath(), value.Location()))
		}
		return HandlerRuleEntry{}, fmt.Errorf("EMPTY-AUTHORED-RULE: rule at %s must not be empty", value.Location())
	}
	if activity, present := fields["activity"]; present && context == handlerRuleDecodeContextOnComplete {
		return HandlerRuleEntry{}, nodeValueError(activity, fmt.Errorf("on_complete.activity is unsupported; declare activity on a handler or selection rule"))
	}
	out := HandlerRuleEntry{authored: true}
	if err := nodeValueTexts(fields, map[string]*string{
		"advances_to": &out.AdvancesTo,
	}, false); err != nil {
		return HandlerRuleEntry{}, err
	}
	if condition, present := fields["condition"]; present {
		out.Condition, err = projectNodeScalarExpression(condition, "on_complete.condition", true)
		if err != nil {
			return HandlerRuleEntry{}, err
		}
		out.Condition = strings.TrimSpace(out.Condition)
		predicate, err := condition.Scalar()
		if err != nil {
			return HandlerRuleEntry{}, err
		}
		out.predicateDisplay = "condition " + strings.TrimSpace(predicate.Value)
	}
	out.Terminate, err = projectTransitionTerminate(fields, out.AdvancesTo)
	if err != nil {
		return HandlerRuleEntry{}, err
	}
	for _, entry := range []struct {
		key     string
		project func(yamlsource.Value) error
	}{
		{"emit", func(v yamlsource.Value) error { out.Emit, err = projectNodeEmitValue(v); return err }},
		{"activity", func(v yamlsource.Value) error {
			out.Activity, err = projectNodeActivityValue(v)
			if err == nil && !out.Activity.Empty() && strings.TrimSpace(out.Activity.ID) == "" {
				return nodeValueError(v, fmt.Errorf("rule activity.id is required; declare an explicit activity.id"))
			}
			return err
		}},
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
	out.admissionProvenance = make(map[string]EffectiveValueProvenance)
	if err := collectNodeValueProvenance(value, "", out.admissionProvenance, nil); err != nil {
		return HandlerRuleEntry{}, err
	}
	return out, nil
}

func projectNodeRulePolicyValue(value yamlsource.Value, fields map[string]yamlsource.Value, context handlerRuleDecodeContext, out *HandlerRuleEntry) error {
	var selected string
	for _, key := range []string{"when", "case", "range", "lookup", "validate", "compute_module"} {
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
		when, err := projectNodeScalarExpression(fields["when"], "rule.when", true)
		if err != nil {
			return err
		}
		out.Condition = strings.TrimSpace(when)
		predicate, err := fields["when"].Scalar()
		if err != nil {
			return err
		}
		out.predicateDisplay = "when " + strings.TrimSpace(predicate.Value)
		out.PolicyRow = PolicySheetRowMetadata{Kind: PolicySheetRowKindWhen}
	case "case":
		out.Condition, out.PolicyRow, err = projectNodePolicyCaseValue(fields[selected])
	case "range":
		out.Condition, out.PolicyRow, err = projectNodePolicyRangeValue(fields[selected])
	case "lookup", "validate", "compute_module":
		if !out.Emit.Empty() || out.AdvancesTo != "" || !out.Activity.Empty() || out.DataAccumulation.HasWrites() || out.FanOut != nil || out.Compute != nil {
			return fmt.Errorf("POLICY-SHEET-ROW: %s row at %s derives a value only and cannot declare branch outputs", selected, fields[selected].Location())
		}
		out.PolicyRow, out.Compute, err = projectNodePolicyValueRow(fields[selected], selected)
	}
	if err != nil {
		return nodeValueError(fields[selected], err)
	}
	if selected == "case" || selected == "range" {
		out.predicateDisplay, err = admittedRulePredicateDisplay(selected, fields[selected])
		if err != nil {
			return nodeValueError(fields[selected], err)
		}
	}
	return nil
}
