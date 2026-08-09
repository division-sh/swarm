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
	resolved                  bool
	liveAgentIDs              []string
	liveWorkflowActivitySites map[string][]string
	unreachableOutboundTools  map[string]struct{}
}

// ResolveSourceBootEffectReachability derives which outbound tool transports
// can still execute for one effective source. Any live-selected agent or
// declarative workflow activity keeps its outbound tools reachable; an
// all-mock source waives only exact responders with no live workflow entrance.
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

	fact := SourceBootEffectReachability{
		resolved:                  true,
		liveWorkflowActivitySites: sourceLiveWorkflowActivitySites(source),
	}
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
		if exactMockResponderAdmitsEveryEntry(plan, toolID, entries) && len(fact.liveWorkflowActivitySites[toolID]) == 0 {
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

// LiveWorkflowActivitySites returns the sorted declarative activity entrances
// that can execute the tool from live non-agent ingress.
func (r SourceBootEffectReachability) LiveWorkflowActivitySites(toolID string) []string {
	return append([]string(nil), r.liveWorkflowActivitySites[strings.TrimSpace(toolID)]...)
}

func sourceLiveWorkflowActivitySites(source semanticview.Source) map[string][]string {
	sitesByTool := map[string][]string{}
	seen := map[string]map[string]struct{}{}
	for _, site := range source.ActivitySites() {
		toolID := strings.TrimSpace(site.Spec.Tool)
		if toolID == "" {
			continue
		}
		label := workflowActivitySiteLabel(site)
		if seen[toolID] == nil {
			seen[toolID] = map[string]struct{}{}
		}
		if _, duplicate := seen[toolID][label]; duplicate {
			continue
		}
		seen[toolID][label] = struct{}{}
		sitesByTool[toolID] = append(sitesByTool[toolID], label)
	}
	for toolID := range sitesByTool {
		sort.Strings(sitesByTool[toolID])
	}
	return sitesByTool
}

func workflowActivitySiteLabel(site runtimecontracts.ActivitySite) string {
	parts := []string{}
	if flowID := strings.TrimSpace(site.FlowID); flowID != "" {
		parts = append(parts, "flow "+flowID)
	}
	parts = append(parts, "node "+strings.TrimSpace(site.NodeID), "handler "+strings.TrimSpace(site.HandlerEventKey))
	if source := strings.TrimSpace(site.Source); source != "" {
		parts = append(parts, source)
	}
	return strings.Join(parts, " ")
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
