package pipeline

// typed_adapter_shims.go — TEMPORARY shims bridging typed contract structs
// to legacy map[string]any matcher functions. CP2-B (handler engine) will
// delete this entire file when the 10-step engine replaces matchWorkflowRules.

import (
	"fmt"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

// matchTypedRules adapts []HandlerRuleEntry to the legacy matchWorkflowRules path.
func (pc *FactoryPipelineCoordinator) matchTypedRules(triggerCtx workflowTriggerContext, rules []runtimecontracts.HandlerRuleEntry) (workflowRuleMatch, bool) {
	return pc.matchTypedRulesWithVars(triggerCtx, rules, nil)
}

// matchTypedRulesWithVars adapts []HandlerRuleEntry to legacy matchWorkflowRulesWithVars.
func (pc *FactoryPipelineCoordinator) matchTypedRulesWithVars(triggerCtx workflowTriggerContext, rules []runtimecontracts.HandlerRuleEntry, extraVars map[string]any) (workflowRuleMatch, bool) {
	if len(rules) == 0 {
		return workflowRuleMatch{}, false
	}
	m := handlerRuleEntriesToMap(rules)
	return pc.matchWorkflowRulesWithVars(triggerCtx, m, extraVars)
}

// matchTypedOnComplete adapts *HandlerRuleEntry (on_complete) to legacy matcher.
func (pc *FactoryPipelineCoordinator) matchTypedOnComplete(triggerCtx workflowTriggerContext, onComplete *runtimecontracts.HandlerRuleEntry, extraVars map[string]any) (workflowRuleMatch, bool) {
	if onComplete == nil {
		return workflowRuleMatch{}, false
	}
	m := handlerRuleEntryToMap(onComplete)
	return pc.matchWorkflowRulesWithVars(triggerCtx, m, extraVars)
}

func handlerRuleEntriesToMap(rules []runtimecontracts.HandlerRuleEntry) map[string]any {
	m := make(map[string]any, len(rules))
	for i, r := range rules {
		id := strings.TrimSpace(r.ID)
		if id == "" {
			id = strings.TrimSpace(r.Description)
		}
		if id == "" {
			id = fmt.Sprintf("rule_%d", i)
		}
		m[id] = handlerRuleEntryValue(r)
	}
	return m
}

func handlerRuleEntryToMap(r *runtimecontracts.HandlerRuleEntry) map[string]any {
	id := strings.TrimSpace(r.ID)
	if id == "" {
		id = "on_complete"
	}
	return map[string]any{id: handlerRuleEntryValue(*r)}
}

func handlerRuleEntryValue(r runtimecontracts.HandlerRuleEntry) map[string]any {
	v := map[string]any{}
	if r.Condition != "" {
		v["condition"] = r.Condition
	}
	if r.AdvancesTo != "" {
		v["advances_to"] = r.AdvancesTo
	}
	if !r.Emits.Empty() {
		v["emits"] = r.Emits.Values()
	}
	if r.DataAccumulation.HasWrites() || r.DataAccumulation.SourceEvent != "" {
		acc := map[string]any{}
		if r.DataAccumulation.SourceEvent != "" {
			acc["source_event"] = r.DataAccumulation.SourceEvent
		}
		if len(r.DataAccumulation.Writes) > 0 {
			writes := make([]any, 0, len(r.DataAccumulation.Writes))
			for _, w := range r.DataAccumulation.Writes {
				wm := map[string]any{}
				if w.SourceField != "" {
					wm["source_field"] = w.SourceField
				}
				writes = append(writes, wm)
			}
			acc["writes"] = writes
		}
		v["data_accumulation"] = acc
	}
	return v
}

func handlerRuleEntryToMapOrNil(r *runtimecontracts.HandlerRuleEntry) map[string]any {
	if r == nil {
		return nil
	}
	return handlerRuleEntryToMap(r)
}

func configFromSpecToMap(spec *runtimecontracts.ConfigFromSpec) map[string]any {
	if spec == nil {
		return nil
	}
	m := map[string]any{}
	for _, k := range spec.PolicyKeys {
		m[k] = true
	}
	for k, v := range spec.Bindings {
		m[k] = v
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func gateSpecString(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
}

func policyDocumentToMap(doc runtimecontracts.PolicyDocument) map[string]any {
	if len(doc.Values) == 0 {
		return nil
	}
	m := make(map[string]any, len(doc.Values))
	for k, v := range doc.Values {
		m[k] = v.Value
	}
	return m
}

func stateSchemaGateNames(schema runtimecontracts.NodeStateSchema) map[string]struct{} {
	gates := map[string]struct{}{}
	for _, f := range schema.Fields {
		if strings.TrimSpace(f.Name) != "" {
			gates[strings.TrimSpace(f.Name)] = struct{}{}
		}
	}
	return gates
}

// computeSpecToMap adapts *ComputeSpec to map[string]any for legacy scoringExpressionVars.
func computeSpecToMap(spec *runtimecontracts.ComputeSpec) map[string]any {
	if spec == nil {
		return nil
	}
	m := map[string]any{
		"operation": spec.Operation,
		"store_as":  spec.StoreAs,
	}
	if len(spec.Tiers) > 0 {
		tiers := make([]any, 0, len(spec.Tiers))
		for _, t := range spec.Tiers {
			tm := map[string]any{
				"weight": t.Weight,
			}
			dims := make([]any, 0, len(t.Dimensions))
			for _, d := range t.Dimensions {
				dims = append(dims, d)
			}
			tm["dimensions"] = dims
			tiers = append(tiers, tm)
		}
		m["tiers"] = tiers
	}
	return m
}

