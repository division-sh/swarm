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

type sourceAgentSelectionEntry struct {
	scopeKind string
	scopeID   string
	localID   string
	entry     runtimecontracts.AgentRegistryEntry
}

// ResolveSourceBootEffectReachability derives which outbound tool transports
// can still execute for one effective source. Any live-selected agent or
// declarative workflow activity keeps its outbound tools reachable; an
// all-mock source waives only exact responders with no live workflow entrance.
func ResolveSourceBootEffectReachability(source semanticview.Source, configuredDefault llmselection.Profile, plan *providerconnectors.MockResponsePlan) (SourceBootEffectReachability, error) {
	if source == nil {
		return SourceBootEffectReachability{}, fmt.Errorf("semantic source is required")
	}

	agents := sourceAgentSelectionEntries(source)

	fact := SourceBootEffectReachability{
		resolved:                  true,
		liveWorkflowActivitySites: sourceLiveWorkflowActivitySites(source),
	}
	localIDCounts := map[string]int{}
	for _, agent := range agents {
		localIDCounts[agent.localID]++
	}
	liveAgentLabels := map[string]struct{}{}
	for _, agent := range agents {
		selection, err := llmselection.ResolveAgentExecutionSelection(configuredDefault, agent.entry.Mock.Configured())
		if err != nil {
			return SourceBootEffectReachability{}, fmt.Errorf("resolve effective execution selection for agent %q: %w", sourceAgentSelectionLabel(agent, localIDCounts[agent.localID] > 1), err)
		}
		if selection.Mode != executionmode.Mock {
			liveAgentLabels[sourceAgentSelectionLabel(agent, localIDCounts[agent.localID] > 1)] = struct{}{}
		}
	}
	fact.liveAgentIDs = sortedSetKeysLocal(liveAgentLabels)
	if len(agents) == 0 || len(fact.liveAgentIDs) > 0 {
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

// LiveAgentIDs returns the sorted declarations that keep outbound effects
// reachable. Ambiguous local IDs are scope-qualified.
func (r SourceBootEffectReachability) LiveAgentIDs() []string {
	return append([]string(nil), r.liveAgentIDs...)
}

// LiveWorkflowActivitySites returns the sorted declarative activity entrances
// that can execute the tool from live non-agent ingress.
func (r SourceBootEffectReachability) LiveWorkflowActivitySites(toolID string) []string {
	return append([]string(nil), r.liveWorkflowActivitySites[strings.TrimSpace(toolID)]...)
}

func sourceAgentSelectionEntries(source semanticview.Source) []sourceAgentSelectionEntry {
	entries := []sourceAgentSelectionEntry{}
	scopedLocalIDs := map[string]struct{}{}
	appendScoped := func(scopeKind, scopeID string, agents map[string]runtimecontracts.AgentRegistryEntry) {
		for _, rawID := range sortedSetKeysLocal(agents) {
			localID := strings.TrimSpace(rawID)
			if localID == "" {
				continue
			}
			scopedLocalIDs[localID] = struct{}{}
			entries = append(entries, sourceAgentSelectionEntry{
				scopeKind: scopeKind,
				scopeID:   strings.TrimSpace(scopeID),
				localID:   localID,
				entry:     agents[rawID],
			})
		}
	}
	for _, scope := range source.ProjectScopes() {
		appendScoped("project", scope.Key, scope.Agents)
	}
	for _, scope := range source.FlowScopes() {
		appendScoped("flow", scope.ID, scope.Agents)
	}
	aliases := source.AgentEntries()
	for _, rawID := range sortedSetKeysLocal(aliases) {
		localID := strings.TrimSpace(rawID)
		if localID == "" {
			continue
		}
		if _, represented := scopedLocalIDs[localID]; represented {
			continue
		}
		entries = append(entries, sourceAgentSelectionEntry{
			localID: localID,
			entry:   aliases[rawID],
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		left := sourceAgentSelectionLabel(entries[i], true)
		right := sourceAgentSelectionLabel(entries[j], true)
		return left < right
	})
	return entries
}

func sourceAgentSelectionLabel(agent sourceAgentSelectionEntry, qualify bool) string {
	if !qualify || strings.TrimSpace(agent.scopeKind) == "" {
		return agent.localID
	}
	scopeID := strings.TrimSpace(agent.scopeID)
	if scopeID == "" || scopeID == "." {
		scopeID = "root"
	}
	return strings.Join([]string{agent.scopeKind, scopeID, "agent", agent.localID}, " ")
}

func sourceLiveWorkflowActivitySites(source semanticview.Source) map[string][]string {
	sitesByTool := map[string][]string{}
	seen := map[string]map[string]struct{}{}
	scopedLocalNodeIDs := map[string]struct{}{}
	appendNodes := func(scopeLabel, flowID string, nodes map[string]runtimecontracts.SystemNodeContract) {
		for _, rawNodeID := range sortedSetKeysLocal(nodes) {
			nodeID := strings.TrimSpace(rawNodeID)
			if nodeID == "" {
				continue
			}
			scopedLocalNodeIDs[nodeID] = struct{}{}
			node := nodes[rawNodeID]
			appendWorkflowActivitySites(sitesByTool, seen, scopeLabel, runtimecontracts.ActivitySitesForNode(flowID, nodeID, node.EventHandlers))
		}
	}
	for _, scope := range source.ProjectScopes() {
		projectID := strings.TrimSpace(scope.Key)
		if projectID == "" || projectID == "." {
			projectID = "root"
		}
		appendNodes("project "+projectID, scope.OwningFlowID, scope.Nodes)
	}
	for _, scope := range source.FlowScopes() {
		appendNodes("", scope.ID, scope.Nodes)
	}
	aliases := source.NodeEntries()
	for _, rawNodeID := range sortedSetKeysLocal(aliases) {
		nodeID := strings.TrimSpace(rawNodeID)
		if nodeID == "" {
			continue
		}
		if _, represented := scopedLocalNodeIDs[nodeID]; represented {
			continue
		}
		flowID := ""
		if contractSource, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(contractSource.FlowID)
		}
		node := aliases[rawNodeID]
		appendWorkflowActivitySites(sitesByTool, seen, "", runtimecontracts.ActivitySitesForNode(flowID, nodeID, node.EventHandlers))
	}
	for toolID := range sitesByTool {
		sort.Strings(sitesByTool[toolID])
	}
	return sitesByTool
}

func appendWorkflowActivitySites(sitesByTool map[string][]string, seen map[string]map[string]struct{}, scopeLabel string, sites []runtimecontracts.ActivitySite) {
	for _, site := range sites {
		toolID := strings.TrimSpace(site.Spec.Tool)
		if toolID == "" {
			continue
		}
		label := workflowActivitySiteLabel(scopeLabel, site)
		if seen[toolID] == nil {
			seen[toolID] = map[string]struct{}{}
		}
		if _, duplicate := seen[toolID][label]; duplicate {
			continue
		}
		seen[toolID][label] = struct{}{}
		sitesByTool[toolID] = append(sitesByTool[toolID], label)
	}
}

func workflowActivitySiteLabel(scopeLabel string, site runtimecontracts.ActivitySite) string {
	parts := []string{}
	if scopeLabel = strings.TrimSpace(scopeLabel); scopeLabel != "" {
		parts = append(parts, scopeLabel)
	}
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
