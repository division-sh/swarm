package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// SourceBootEffectReachability is the boot-time projection of canonical actor
// selection and exact mock responder admission. Its zero value fails closed.
type SourceBootEffectReachability struct {
	resolved                 bool
	liveAgentIDs             []string
	unreachableOutboundTools map[string]struct{}
}

// ResolveSourceBootEffectReachability derives which outbound tool transports
// can still execute for one effective source. Any live-selected agent keeps all
// outbound tools reachable; an all-mock source waives only exact responders.
func ResolveSourceBootEffectReachability(source semanticview.Source, configuredDefault llmselection.Profile, plan *providerconnectors.MockResponsePlan) (SourceBootEffectReachability, error) {
	if source == nil {
		return SourceBootEffectReachability{}, fmt.Errorf("semantic source is required")
	}

	agents := source.AgentEntries()
	rawAgentIDs := make([]string, 0, len(agents))
	for rawID := range agents {
		rawAgentIDs = append(rawAgentIDs, rawID)
	}
	sort.Strings(rawAgentIDs)

	fact := SourceBootEffectReachability{resolved: true}
	for _, rawAgentID := range rawAgentIDs {
		agentID := strings.TrimSpace(rawAgentID)
		entry := agents[rawAgentID]
		selection, err := llmselection.ResolveAgentExecutionSelection(configuredDefault, entry.Mock.Configured())
		if err != nil {
			return SourceBootEffectReachability{}, fmt.Errorf("resolve effective execution selection for agent %q: %w", agentID, err)
		}
		if selection.Mode != executionmode.Mock {
			fact.liveAgentIDs = append(fact.liveAgentIDs, agentID)
		}
	}
	if len(rawAgentIDs) == 0 || len(fact.liveAgentIDs) > 0 {
		return fact, nil
	}

	fact.unreachableOutboundTools = map[string]struct{}{}
	for toolID, entries := range sourceToolEntriesByID(source) {
		if exactMockResponderAdmitsEveryEntry(plan, toolID, entries) {
			fact.unreachableOutboundTools[toolID] = struct{}{}
		}
	}
	return fact, nil
}

// ToolCredentialRequired reports whether boot must retain a credential
// requirement for the named tool. Unknown and unresolved tools fail closed.
func (r SourceBootEffectReachability) ToolCredentialRequired(toolID string) bool {
	if !r.resolved {
		return true
	}
	_, unreachable := r.unreachableOutboundTools[strings.TrimSpace(toolID)]
	return !unreachable
}

// LiveAgentIDs returns the sorted actors that keep outbound effects reachable.
func (r SourceBootEffectReachability) LiveAgentIDs() []string {
	return append([]string(nil), r.liveAgentIDs...)
}

func sourceToolEntriesByID(source semanticview.Source) map[string][]runtimecontracts.ToolSchemaEntry {
	out := map[string][]runtimecontracts.ToolSchemaEntry{}
	appendEntries := func(entries map[string]runtimecontracts.ToolSchemaEntry) {
		for rawID, entry := range entries {
			toolID := strings.TrimSpace(rawID)
			if toolID != "" {
				out[toolID] = append(out[toolID], entry)
			}
		}
	}
	appendEntries(source.ToolEntries())
	for _, scope := range source.ProjectScopes() {
		appendEntries(scope.Tools)
	}
	for _, scope := range source.FlowScopes() {
		appendEntries(scope.Tools)
	}
	return out
}

func exactMockResponderAdmitsEveryEntry(plan *providerconnectors.MockResponsePlan, toolID string, entries []runtimecontracts.ToolSchemaEntry) bool {
	if plan == nil || len(entries) == 0 {
		return false
	}
	for _, entry := range entries {
		if _, err := plan.Admit(toolID, entry); err != nil {
			return false
		}
	}
	return true
}
