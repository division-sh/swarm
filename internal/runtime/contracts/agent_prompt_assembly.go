package contracts

import (
	"fmt"
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
	selected := make(map[string]PolicyCriteriaSet, len(refs))
	for _, ref := range refs {
		set, ok := policy.Criteria[ref]
		if !ok {
			return runtimeagentintent.DerivedPrompt{}, fmt.Errorf("criteria set %q does not resolve in flow %s", ref, flowID)
		}
		selected[ref] = set
	}
	return runtimeagentintent.ContractCriteriaPrompt(entry.ResolvedIntent, refs, selected)
}
