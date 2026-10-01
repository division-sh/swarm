package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectPolicyDeclarationsValue(root yamlsource.Value) (PolicyDocument, error) {
	if err := root.ValidateExpansion(); err != nil {
		return PolicyDocument{}, err
	}
	if err := root.ValidateUniqueMappings(); err != nil {
		return PolicyDocument{}, err
	}
	fields, err := uniqueYAMLMappingFields(root, "policy.yaml values")
	if err != nil {
		return PolicyDocument{}, err
	}
	out := PolicyDocument{Values: make(map[string]PolicyValue, len(fields))}
	for _, field := range fields {
		if field.KeyTag != "!!str" {
			return PolicyDocument{}, fmt.Errorf("policy key %q at %s must be text", field.Name, field.KeyLocation)
		}
		var literal any
		if err := field.Value.Project(&literal); err != nil {
			return PolicyDocument{}, nodeValueError(field.Value, err)
		}
		out.Values[field.Name] = PolicyValue{Value: literal}
	}
	return out, nil
}

func projectRulesDeclarationsValue(root yamlsource.Value) (RulesDocument, error) {
	if err := root.ValidateExpansion(); err != nil {
		return nil, err
	}
	fields, err := uniqueYAMLMappingFields(root, "rules.yaml declarations")
	if err != nil {
		return nil, err
	}
	if err := validateExactDeclarationNames(fields, "rules.yaml declaration"); err != nil {
		return nil, err
	}
	out := make(RulesDocument, len(fields))
	for _, field := range fields {
		set, err := projectRuleSetValue(field.Value)
		if err != nil {
			return nil, nodeValueError(field.Value, fmt.Errorf("rule set %q: %w", field.Name, err))
		}
		out[field.Name] = set
	}
	return out, nil
}

func projectRuleSetValue(value yamlsource.Value) (RuleSet, error) {
	fields, err := nodeValueFields(value, "rule set", map[string]struct{}{"classes": {}, "rules": {}, "inputs": {}}, nil)
	if err != nil {
		return RuleSet{}, err
	}
	classes, err := projectRuleClassesValue(fields["classes"])
	if err != nil {
		return RuleSet{}, err
	}
	rows, err := fields["rules"].Sequence()
	if err != nil || len(rows) == 0 {
		return RuleSet{}, fmt.Errorf("rules must be a nonempty sequence at %s", value.Location())
	}
	_, machine := fields["inputs"]
	for _, row := range rows {
		keys, err := row.Mapping()
		if err != nil {
			return RuleSet{}, err
		}
		for _, key := range keys {
			if key.Name == "check" || key.Name == "pin_candidate" {
				machine = true
			}
		}
	}
	var inputs map[string]string
	if machine {
		inputs, err = projectRuleInputsValue(fields["inputs"])
		if err != nil {
			return RuleSet{}, err
		}
	}
	var agentRows []PolicyCriteriaRule
	var machineRows []PolicyValidationRule
	for _, row := range rows {
		rule, err := projectRuleRowValue(row, machine)
		if err != nil {
			return RuleSet{}, nodeValueError(row, err)
		}
		if machine {
			machineRows = append(machineRows, rule)
		} else {
			agentRows = append(agentRows, PolicyCriteriaRule{ID: rule.ID, Class: rule.Class, Text: rule.Text, Params: rule.Params})
		}
	}
	if !machine {
		return RuleSet{Criteria: &PolicyCriteriaSet{Classes: classes, Rules: agentRows}}, nil
	}
	machineClasses := make(map[string]PolicyValidationClass, len(classes))
	for name, class := range classes {
		machineClasses[name] = PolicyValidationClass{Disposition: class.Disposition}
	}
	return RuleSet{Validation: &PolicyValidationSet{Classes: machineClasses, Inputs: inputs, Rules: machineRows}}, nil
}

func projectRuleInputsValue(value yamlsource.Value) (map[string]string, error) {
	fields, err := uniqueYAMLMappingFields(value, "machine rule inputs")
	if err != nil || len(fields) == 0 {
		return nil, fmt.Errorf("machine inputs must be a nonempty mapping at %s", value.Location())
	}
	if err := validateExactDeclarationNames(fields, "rule input"); err != nil {
		return nil, err
	}
	inputs := make(map[string]string, len(fields))
	for _, input := range fields {
		text, err := requiredRuleText(input.Value)
		if err != nil {
			return nil, err
		}
		inputs[input.Name] = text
	}
	return inputs, nil
}

func projectRuleClassesValue(value yamlsource.Value) (map[string]PolicyCriteriaClass, error) {
	fields, err := uniqueYAMLMappingFields(value, "rule classes")
	if err != nil || len(fields) == 0 {
		return nil, fmt.Errorf("classes must be a nonempty mapping at %s", value.Location())
	}
	if err := validateExactDeclarationNames(fields, "rule class"); err != nil {
		return nil, err
	}
	out := make(map[string]PolicyCriteriaClass, len(fields))
	for _, field := range fields {
		properties, err := nodeValueFields(field.Value, "rule class", map[string]struct{}{"disposition": {}}, nil)
		if err != nil {
			return nil, err
		}
		disposition, err := requiredRuleText(properties["disposition"])
		if err != nil {
			return nil, err
		}
		out[field.Name] = PolicyCriteriaClass{Disposition: disposition}
	}
	return out, nil
}

func projectRuleRowValue(value yamlsource.Value, machine bool) (PolicyValidationRule, error) {
	allowed := map[string]struct{}{"id": {}, "class": {}, "text": {}, "params": {}}
	if machine {
		allowed["check"] = struct{}{}
		allowed["pin_candidate"] = struct{}{}
	}
	fields, err := nodeValueFields(value, "rule", allowed, nil)
	if err != nil {
		return PolicyValidationRule{}, err
	}
	var out PolicyValidationRule
	for name, target := range map[string]*string{"id": &out.ID, "class": &out.Class, "text": &out.Text} {
		*target, err = requiredRuleText(fields[name])
		if err != nil {
			return out, fmt.Errorf("%s: %w", name, err)
		}
	}
	if params, present := fields["params"]; present {
		entries, err := uniqueYAMLMappingFields(params, "rule params")
		if err != nil {
			return out, err
		}
		if err := validateExactDeclarationNames(entries, "rule param"); err != nil {
			return out, err
		}
		out.Params = make(map[string]PolicyCriteriaParam, len(entries))
		for _, entry := range entries {
			if entry.Value.Presence() != yamlsource.PresenceScalar && entry.Value.Presence() != yamlsource.PresenceEmptyScalar {
				return out, nodeValueError(entry.Value, fmt.Errorf("rule parameter must be an inert scalar"))
			}
			var literal any
			if err := entry.Value.Project(&literal); err != nil {
				return out, err
			}
			if !criteriaParamScalar(literal) {
				return out, nodeValueError(entry.Value, fmt.Errorf("rule parameter must be an inert finite scalar"))
			}
			out.Params[entry.Name] = PolicyCriteriaParam{Value: literal}
		}
	}
	if !machine {
		return out, nil
	}
	candidate, present := fields["pin_candidate"]
	if !present {
		return out, fmt.Errorf("machine rule requires explicit pin_candidate")
	}
	flag, err := agentValueBool(candidate)
	if err != nil {
		return out, err
	}
	out.PinCandidate = &flag
	checks, err := nodeValueFields(fields["check"], "rule check", map[string]struct{}{"equal": {}}, nil)
	if err != nil {
		return out, err
	}
	operands, err := nodeValueFields(checks["equal"], "rule equal", map[string]struct{}{"left": {}, "right": {}}, nil)
	if err != nil {
		return out, err
	}
	left, err := requiredRuleText(operands["left"])
	if err != nil {
		return out, err
	}
	right, err := requiredRuleText(operands["right"])
	if err != nil {
		return out, err
	}
	out.Check.Equal = &PolicyValidationEqualCheck{Left: left, Right: right}
	return out, nil
}

func requiredRuleText(value yamlsource.Value) (string, error) {
	text, err := agentValueText(value)
	if err != nil {
		return "", nodeValueError(value, err)
	}
	if strings.TrimSpace(text) == "" {
		return "", nodeValueError(value, fmt.Errorf("requires nonempty text"))
	}
	return text, nil
}
