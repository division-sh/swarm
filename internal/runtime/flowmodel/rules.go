package flowmodel

import (
	"encoding/json"
	"fmt"
	"strings"
)

// RulesDocument is one namespace; resolution precedes variant selection.
type RulesDocument map[string]RuleSet

type RuleSet struct {
	Criteria   *PolicyCriteriaSet
	Validation *PolicyValidationSet
}

func (r RuleSet) MarshalYAML() (any, error) {
	if r.Criteria != nil && r.Validation == nil {
		return r.Criteria, nil
	}
	if r.Validation != nil && r.Criteria == nil {
		return r.Validation, nil
	}
	return nil, fmt.Errorf("rule set must have exactly one variant")
}

func (r RuleSet) MarshalJSON() ([]byte, error) {
	value, err := r.MarshalYAML()
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func (d RulesDocument) Criteria(name string) (PolicyCriteriaSet, bool) {
	set, ok := d[name]
	if !ok || set.Criteria == nil || set.Validation != nil {
		return PolicyCriteriaSet{}, false
	}
	return clonePolicyCriteriaSet(*set.Criteria), true
}
func (d RulesDocument) Validation(name string) (PolicyValidationSet, bool) {
	set, ok := d[name]
	if !ok || set.Validation == nil || set.Criteria != nil {
		return PolicyValidationSet{}, false
	}
	return clonePolicyValidationSet(*set.Validation), true
}
func (d RulesDocument) CriteriaSets() map[string]PolicyCriteriaSet {
	out := map[string]PolicyCriteriaSet{}
	for name := range d {
		if set, ok := d.Criteria(name); ok {
			out[name] = set
		}
	}
	return out
}
func (d RulesDocument) ValidationSets() map[string]PolicyValidationSet {
	out := map[string]PolicyValidationSet{}
	for name := range d {
		if set, ok := d.Validation(name); ok {
			out[name] = set
		}
	}
	return out
}
func CloneRulesDocument(in RulesDocument) RulesDocument {
	out := make(RulesDocument, len(in))
	for name, set := range in {
		cloned := RuleSet{}
		if set.Criteria != nil {
			v := clonePolicyCriteriaSet(*set.Criteria)
			cloned.Criteria = &v
		}
		if set.Validation != nil {
			v := clonePolicyValidationSet(*set.Validation)
			cloned.Validation = &v
		}
		out[name] = cloned
	}
	return out
}
func ResolveRulesByID[T any](base RulesDocument, tree Tree[T], flowID string, id func(*T) string, rules func(*T) RulesDocument, children func(*T) []*T) RulesDocument {
	out := CloneRulesDocument(base)
	if tree.Root == nil {
		return out
	}
	var chain []*T
	if strings.TrimSpace(flowID) == "" {
		chain = []*T{tree.Root}
	} else {
		chain = CollectPathByID(tree.Root, flowID, id, children)
	}
	for _, view := range chain {
		if view == nil {
			continue
		}
		for name, set := range CloneRulesDocument(rules(view)) {
			out[name] = set
		}
	}
	return out
}
