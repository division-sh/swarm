package pipeline

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func (pc *FactoryPipelineCoordinator) matchTypedRules(triggerCtx workflowTriggerContext, rules []runtimecontracts.HandlerRuleEntry) (workflowRuleMatch, bool) {
	return pc.matchWorkflowRulesWithVars(triggerCtx, rules, nil)
}

func (pc *FactoryPipelineCoordinator) matchTypedRulesWithVars(triggerCtx workflowTriggerContext, rules []runtimecontracts.HandlerRuleEntry, extraVars map[string]any) (workflowRuleMatch, bool) {
	return pc.matchWorkflowRulesWithVars(triggerCtx, rules, extraVars)
}

func (pc *FactoryPipelineCoordinator) matchTypedOnComplete(triggerCtx workflowTriggerContext, onComplete []runtimecontracts.HandlerRuleEntry, extraVars map[string]any) (workflowRuleMatch, bool) {
	if len(onComplete) == 0 {
		return workflowRuleMatch{}, false
	}
	return pc.matchWorkflowRulesWithVars(triggerCtx, onComplete, extraVars)
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
		"operation": spec.Operation.String(),
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
