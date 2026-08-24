package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ConfiguredToolFinding struct {
	AgentID  string
	ToolName string
	Reason   string
}

// ValidateConfiguredToolFulfillability consumes the same actor candidate and
// grant owners used to plan provider turns. Inventory or handler presence is
// deliberately insufficient evidence.
func ValidateConfiguredToolFulfillability(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) []ConfiguredToolFinding {
	var findings []ConfiguredToolFinding
	for _, err := range ValidateHITLIdentityLifecycleReferences(source) {
		findings = append(findings, ConfiguredToolFinding{Reason: err.Error()})
	}
	discoveredNames := make([]string, 0, len(discovered))
	for name := range discovered {
		discoveredNames = append(discoveredNames, name)
	}
	sort.Strings(discoveredNames)
	for _, name := range discoveredNames {
		if err := hitlIdentityDefinitionError(name, "discovered MCP tool "+strings.TrimSpace(name)); err != nil {
			findings = append(findings, ConfiguredToolFinding{ToolName: strings.TrimSpace(name), Reason: err.Error()})
		}
	}
	if len(findings) > 0 {
		return findings
	}
	for _, declaration := range semanticview.AgentDeclarations(source) {
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			findings = append(findings, ConfiguredToolFinding{AgentID: declaration.LocalID, Reason: err.Error()})
			continue
		}
		permissions, err := ResolveAgentPermissions(source, plan.OwnerFlowID, declaration.Entry)
		if err != nil {
			findings = append(findings, ConfiguredToolFinding{AgentID: plan.AgentID, Reason: err.Error()})
			continue
		}
		actor := nativeToolAgentConfig(plan.AgentID, plan.EffectiveRole(declaration.Entry), declaration.Entry)
		actor.FlowID = plan.OwnerFlowID
		actor.Permissions = permissions
		candidates, err := executionToolsForActor(source, actor, discovered)
		if err != nil {
			findings = append(findings, ConfiguredToolFinding{AgentID: plan.AgentID, Reason: err.Error()})
			continue
		}
		provider := runtimeauthority.NewSourceProvider(source)
		emits := NewEmitRegistry(source, provider)
		for _, rawName := range declaration.Entry.ConfiguredTools() {
			name := strings.TrimSpace(rawName)
			if name == "" || IsAgentRequiredMCPToolReference(source, declaration, name) {
				continue
			}
			if err := hitlIdentityReferenceError(name, "configured tool"); err != nil {
				findings = append(findings, ConfiguredToolFinding{AgentID: plan.AgentID, ToolName: name, Reason: err.Error()})
				continue
			}
			if _, ok := candidates[name]; !ok {
				findings = append(findings, ConfiguredToolFinding{
					AgentID:  plan.AgentID,
					ToolName: name,
					Reason:   fmt.Sprintf("not available in the effective candidate surface for agent %s", plan.AgentID),
				})
				continue
			}
			decision := classifyToolAuthorization(actor, name, provider, emits)
			if decision.allowed {
				continue
			}
			reason := "not granted to the effective agent capability surface"
			if permission, ok := requiredPermissionForTool(name); ok {
				reason = fmt.Sprintf("requires permission %q", permission)
			}
			findings = append(findings, ConfiguredToolFinding{AgentID: plan.AgentID, ToolName: name, Reason: reason})
		}
	}
	return findings
}
