package contracts

import (
	"fmt"
	"sort"
	"strings"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
)

// AssembleAgentPrompt combines immutable authored intent with separately owned
// criteria without creating another authored prompt surface.
func AssembleAgentPrompt(bundle *WorkflowContractBundle, flowID string, entry AgentRegistryEntry, runtimeCriteria []string) (runtimeagentintent.DerivedPrompt, error) {
	if err := entry.ResolvedIntent.Validate(); err != nil {
		return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("resolved agent intent: %w", err)
	}
	refs := normalizeStrings(entry.Criteria)
	runtimeRefs := normalizeStrings(runtimeCriteria)
	if len(runtimeRefs) > 0 {
		switch {
		case len(refs) == 0:
			return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("criteria delivery requires contract agent criteria; runtime criteria refs are not authoritative: %s", strings.Join(runtimeRefs, ", "))
		case !sameStringSet(refs, runtimeRefs):
			return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("criteria delivery runtime refs must match contract agent criteria; contract refs: %s; runtime refs: %s", strings.Join(refs, ", "), strings.Join(runtimeRefs, ", "))
		}
	}
	if len(refs) == 0 {
		return runtimeagentintent.IntentOnlyPrompt(entry.ResolvedIntent)
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("criteria delivery requires a flow-scoped agent")
	}
	if bundle == nil {
		return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("criteria delivery requires a workflow bundle")
	}
	policy := bundle.ResolvedPolicyForFlow(flowID)
	var section strings.Builder
	section.WriteString("\n\n## Contract Criteria\n\n")
	for i, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		set, ok := policy.Criteria[ref]
		if !ok {
			return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("criteria set %q does not resolve in flow %s", ref, flowID)
		}
		if i > 0 {
			section.WriteString("\n")
		}
		writeCriteriaSetPromptSection(&section, ref, set)
	}
	return runtimeagentintent.NewDerivedPrompt(entry.ResolvedIntent, refs, section.String())
}

func writeCriteriaSetPromptSection(out *strings.Builder, name string, set PolicyCriteriaSet) {
	out.WriteString("### ")
	out.WriteString(strings.TrimSpace(name))
	out.WriteString("\n\nClasses:\n")
	for _, className := range sortedCriteriaClassNames(set.Classes) {
		out.WriteString("- ")
		out.WriteString(className)
		disposition := strings.TrimSpace(set.Classes[className].Disposition)
		if disposition != "" {
			out.WriteString(": ")
			out.WriteString(disposition)
		}
		out.WriteString("\n")
	}
	out.WriteString("\nRules:\n")
	for _, rule := range sortedCriteriaRules(set.Rules) {
		out.WriteString("- ")
		out.WriteString(strings.TrimSpace(rule.ID))
		if className := strings.TrimSpace(rule.Class); className != "" {
			out.WriteString(" [")
			out.WriteString(className)
			out.WriteString("]")
		}
		out.WriteString(": ")
		out.WriteString(strings.TrimSpace(rule.Text))
		out.WriteString("\n")
		if len(rule.Params) > 0 {
			paramNames := make([]string, 0, len(rule.Params))
			for name := range rule.Params {
				if name = strings.TrimSpace(name); name != "" {
					paramNames = append(paramNames, name)
				}
			}
			sort.Strings(paramNames)
			for _, paramName := range paramNames {
				out.WriteString("  - ")
				out.WriteString(paramName)
				out.WriteString(": ")
				out.WriteString(renderCriteriaParamValue(rule.Params[paramName].Value))
				out.WriteString("\n")
			}
		}
	}
}

func sortedCriteriaRules(in []PolicyCriteriaRule) []PolicyCriteriaRule {
	out := append([]PolicyCriteriaRule{}, in...)
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.TrimSpace(out[i].ID)
		right := strings.TrimSpace(out[j].ID)
		if left == right {
			return strings.TrimSpace(out[i].Class) < strings.TrimSpace(out[j].Class)
		}
		return left < right
	})
	return out
}

func renderCriteriaParamValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return typed
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", typed)
	}
}
