package tools

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type RequiredMCPToolAvailabilityFinding struct {
	AgentID  string
	ToolName string
	Reason   string
}

func RequiredMCPToolAvailabilityFindings(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) []RequiredMCPToolAvailabilityFinding {
	if source == nil {
		return nil
	}
	findings := make([]RequiredMCPToolAvailabilityFinding, 0)
	for _, declaration := range semanticview.AgentDeclarations(source) {
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			continue
		}
		projection, ok := semanticview.ScopedAgentContractProjection(source, declaration)
		if !ok {
			continue
		}
		agent := declaration.Entry
		for _, toolName := range agent.ConfiguredTools() {
			toolName = strings.TrimSpace(toolName)
			if toolName == "" || !isAgentRequiredMCPToolReference(source, projection, toolName) {
				continue
			}
			if MCPToolDiscovered(toolName, discovered) {
				continue
			}
			findings = append(findings, RequiredMCPToolAvailabilityFinding{
				AgentID:  plan.AgentID,
				ToolName: toolName,
				Reason:   "no exact discovered MCP catalog entry exists for required agent tool",
			})
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].AgentID == findings[j].AgentID {
			return findings[i].ToolName < findings[j].ToolName
		}
		return findings[i].AgentID < findings[j].AgentID
	})
	return findings
}

func IsAgentRequiredMCPToolReference(source semanticview.Source, declaration semanticview.AgentDeclaration, toolName string) bool {
	projection, ok := semanticview.ScopedAgentContractProjection(source, declaration)
	if !ok {
		return false
	}
	return isAgentRequiredMCPToolReference(source, projection, toolName)
}

func isAgentRequiredMCPToolReference(source semanticview.Source, projection semanticview.AgentContractProjection, toolName string) bool {
	if source == nil {
		return false
	}
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	if entry, ok := projection.ToolEntry(toolName); ok {
		return toolEntryRequiresMCPDiscovery(entry)
	}
	prefix, _, ok := strings.Cut(toolName, ".")
	if !ok || strings.TrimSpace(prefix) == "" {
		return false
	}
	_, exists := declaredMCPServerPrefixes(source)[strings.TrimSpace(prefix)]
	return exists
}

func MCPToolDiscovered(toolName string, discovered map[string]runtimemcp.DiscoveredTool) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || len(discovered) == 0 {
		return false
	}
	_, ok := discovered[toolName]
	return ok
}

func toolEntryRequiresMCPDiscovery(entry runtimecontracts.ToolSchemaEntry) bool {
	return entry.Handler() == runtimecontracts.ToolHandlerMCP
}

func declaredMCPServerPrefixes(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "mcp_servers")
	if !ok {
		return nil
	}
	root, ok := mcpPolicyMap(value.Value)
	if !ok || len(root) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(root))
	for _, raw := range root {
		server, ok := mcpPolicyMap(raw)
		if !ok {
			continue
		}
		prefix := strings.TrimSpace(mcpPolicyString(server["prefix"]))
		if prefix != "" {
			out[prefix] = struct{}{}
		}
	}
	return out
}

func mcpPolicyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	default:
		return nil, false
	}
}

func mcpPolicyString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}
