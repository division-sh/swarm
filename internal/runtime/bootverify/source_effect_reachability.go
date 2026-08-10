package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
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

// SourceBootEffectContext is the canonical boot projection shared by Builder,
// verify/serve validation, and local preflight.
type SourceBootEffectContext struct {
	Source                 semanticview.Source
	MockConnectorResponses *providerconnectors.MockResponsePlan
	Reachability           SourceBootEffectReachability
}

// PrepareSourceBootEffectContext imports connector packs, compiles the exact
// mock response plan, and derives effect reachability as one operation.
func PrepareSourceBootEffectContext(source semanticview.Source, configuredDefault llmselection.Profile, posture executionposture.Posture) (SourceBootEffectContext, error) {
	effectiveSource, err := providerconnectors.SourceWithConnectorPackImports(source)
	if err != nil {
		return SourceBootEffectContext{}, fmt.Errorf("provider connector pack import failed: %w", err)
	}
	plan, err := providerconnectors.CompileMockResponsePlan(effectiveSource)
	if err != nil {
		return SourceBootEffectContext{}, fmt.Errorf("provider connector mock response compilation failed: %w", err)
	}
	reachability, err := ResolveSourceBootEffectReachability(effectiveSource, configuredDefault, plan, posture)
	if err != nil {
		return SourceBootEffectContext{}, fmt.Errorf("resolve source boot effect reachability: %w", err)
	}
	return SourceBootEffectContext{
		Source:                 effectiveSource,
		MockConnectorResponses: plan,
		Reachability:           reachability,
	}, nil
}

// ResolveSourceBootEffectReachability derives which outbound tool transports
// can still execute for one effective source. Any live-selected agent or
// declarative workflow activity keeps its outbound tools reachable; an
// all-mock source waives only exact responders with no live workflow entrance.
func ResolveSourceBootEffectReachability(source semanticview.Source, configuredDefault llmselection.Profile, plan *providerconnectors.MockResponsePlan, posture executionposture.Posture) (SourceBootEffectReachability, error) {
	if source == nil {
		return SourceBootEffectReachability{}, fmt.Errorf("semantic source is required")
	}
	if !posture.Valid() {
		return SourceBootEffectReachability{}, fmt.Errorf("runtime execution posture is required")
	}

	agents := semanticview.AgentDeclarations(source)

	fact := SourceBootEffectReachability{
		resolved:                  true,
		liveWorkflowActivitySites: sourceLiveWorkflowActivitySites(source),
	}
	localIDCounts := map[string]int{}
	for _, agent := range agents {
		localIDCounts[agent.LocalID]++
	}
	liveAgentLabels := map[string]struct{}{}
	for _, agent := range agents {
		selection, err := llmselection.ResolveAgentExecutionSelection(llmselection.AgentExecutionSelectionInput{
			ConfiguredDefault: configuredDefault,
			MockConfigured:    agent.Entry.Mock.Configured(),
		})
		if err != nil {
			return SourceBootEffectReachability{}, fmt.Errorf("resolve effective execution selection for agent %q: %w", agent.Label(localIDCounts[agent.LocalID] > 1), err)
		}
		if selection.Mode != executionmode.Mock {
			liveAgentLabels[agent.Label(localIDCounts[agent.LocalID] > 1)] = struct{}{}
		}
	}
	fact.liveAgentIDs = sortedSetKeysLocal(liveAgentLabels)
	if posture == executionposture.MockOnly && len(fact.liveAgentIDs) > 0 {
		return SourceBootEffectReachability{}, fmt.Errorf("runtime.execution_posture=mock_only requires every effective agent to select mock execution; live agents: %s", strings.Join(fact.liveAgentIDs, ", "))
	}
	if posture == executionposture.MockOnly {
		for toolID, sites := range fact.liveWorkflowActivitySites {
			entries := sourceToolEntriesByID(source)[toolID]
			if len(sites) > 0 && !exactMockResponderAdmitsEveryEntry(plan, toolID, entries) {
				return SourceBootEffectReachability{}, fmt.Errorf("runtime.execution_posture=mock_only requires an exact mock response for provider activity tool %q at %s", toolID, strings.Join(sites, ", "))
			}
		}
		fact.unreachableOutboundTools = map[string]struct{}{}
		for toolID, entries := range sourceToolEntriesByID(source) {
			if exactMockResponderAdmitsEveryEntry(plan, toolID, entries) {
				fact.unreachableOutboundTools[toolID] = struct{}{}
			}
		}
		return fact, nil
	}
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
