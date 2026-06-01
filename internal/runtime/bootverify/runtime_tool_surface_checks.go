package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func checkToolResolution(c *checkerContext) []Finding { return c.toolResolution() }
func checkPlatformToolUsageHints(c *checkerContext) []Finding {
	return c.platformToolUsageHints()
}
func checkGeneratedToolSchemaClosure(c *checkerContext) []Finding {
	return c.generatedToolSchemaClosure()
}

func (c *checkerContext) toolResolution() []Finding {
	if c.toolLoaded {
		return c.toolFindings
	}
	c.toolLoaded = true
	mcpPrefixes := declaredMCPPrefixes(c.source)
	discoveredTools := c.mcpDiscovered()
	// Boot tool warnings must consume the same runtime inventory truth that the
	// generic runtime ships, then layer MCP discovery on top of it.
	runtimeToolNames := c.runtimeAvailableToolNames()
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		for _, toolID := range agent.ConfiguredTools() {
			toolID = strings.TrimSpace(toolID)
			if toolID == "" {
				continue
			}
			if entry, ok := c.source.ToolEntryForAgent(agentID, toolID); ok {
				if mcpToolEntryRequiresDiscovery(entry) && !toolReferenceAllowedByMCPCatalog(toolID, discoveredTools, mcpPrefixes) {
					c.toolFindings = append(c.toolFindings, Finding{
						CheckID:  "tool_resolution",
						Severity: "warning",
						Message:  fmt.Sprintf("agent %s references missing tool %s", agentID, toolID),
						Location: agentID,
					})
				}
				continue
			}
			if toolReferenceAllowedByMCPCatalog(toolID, discoveredTools, mcpPrefixes) {
				continue
			}
			if _, ok := runtimeToolNames[toolID]; ok {
				continue
			}
			c.toolFindings = append(c.toolFindings, Finding{
				CheckID:  "tool_resolution",
				Severity: "warning",
				Message:  fmt.Sprintf("agent %s references missing tool %s", agentID, toolID),
				Location: agentID,
			})
		}
	}
	return c.toolFindings
}

func (c *checkerContext) platformToolUsageHints() []Finding {
	if c.toolUsageLoaded {
		return c.toolUsageFindings
	}
	c.toolUsageLoaded = true
	for _, item := range runtimetools.ValidateUsageHintCoverage(c.source, c.mcpDiscovered()) {
		severity := SeverityLintEvidence
		if item.Severity == "error" {
			severity = SeverityHardInvalidity
		}
		location := strings.TrimSpace(item.ToolName)
		if location == "" {
			location = "runtime-tools"
		}
		c.toolUsageFindings = append(c.toolUsageFindings, Finding{
			CheckID:  "platform_tool_usage_hints",
			Severity: severity,
			Message:  item.Message,
			Location: location,
		})
	}
	return c.toolUsageFindings
}

func (c *checkerContext) generatedToolSchemaClosure() []Finding {
	if c.generatedToolSchemaClosureLoaded {
		return c.generatedToolSchemaClosureFindings
	}
	c.generatedToolSchemaClosureLoaded = true
	for _, err := range runtimetools.ValidateGeneratedToolSchemaClosureForSource(c.source) {
		c.generatedToolSchemaClosureFindings = append(c.generatedToolSchemaClosureFindings, Finding{
			CheckID:  "generated_tool_schema_closure",
			Severity: SeverityHardInvalidity,
			Message:  err.Error(),
			Location: "generated-tools",
		})
	}
	return c.generatedToolSchemaClosureFindings
}

func (c *checkerContext) runtimeAvailableToolNames() map[string]struct{} {
	if c.runtimeToolNamesLoaded {
		return c.runtimeToolNames
	}
	c.runtimeToolNamesLoaded = true
	c.runtimeToolNames = make(map[string]struct{})
	for _, name := range runtimetools.RuntimeAvailableToolNamesForSource(c.source) {
		c.runtimeToolNames[strings.TrimSpace(name)] = struct{}{}
	}
	return c.runtimeToolNames
}

func declaredMCPPrefixes(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "mcp_servers")
	if !ok {
		return nil
	}
	root, ok := anyMap(value.Value)
	if !ok || len(root) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(root))
	for _, raw := range root {
		server, ok := anyMap(raw)
		if !ok {
			continue
		}
		prefix := strings.TrimSpace(anyString(server["prefix"]))
		if prefix != "" {
			out[prefix] = struct{}{}
		}
	}
	return out
}

func toolReferenceAllowedByMCPPrefix(toolID string, prefixes map[string]struct{}) bool {
	if len(prefixes) == 0 {
		return false
	}
	prefix, _, ok := strings.Cut(strings.TrimSpace(toolID), ".")
	if !ok || strings.TrimSpace(prefix) == "" {
		return false
	}
	_, exists := prefixes[strings.TrimSpace(prefix)]
	return exists
}

func toolReferenceAllowedByMCPCatalog(toolID string, discovered map[string]runtimemcp.DiscoveredTool, prefixes map[string]struct{}) bool {
	toolID = strings.TrimSpace(toolID)
	if toolID == "" {
		return false
	}
	if len(discovered) > 0 {
		_, ok := discovered[toolID]
		return ok
	}
	return toolReferenceAllowedByMCPPrefix(toolID, prefixes)
}

func mcpToolEntryRequiresDiscovery(entry runtimecontracts.ToolSchemaEntry) bool {
	return strings.EqualFold(strings.TrimSpace(entry.HandlerType), "mcp")
}
