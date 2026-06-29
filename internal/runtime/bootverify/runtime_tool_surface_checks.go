package bootverify

import (
	"fmt"
	"strings"

	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func checkToolResolution(c *checkerContext) []Finding { return c.toolResolution() }
func checkRequiredMCPToolAvailability(c *checkerContext) []Finding {
	return c.requiredMCPToolAvailability()
}
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
			if runtimetools.IsAgentRequiredMCPToolReference(c.source, agentID, toolID) {
				continue
			}
			if _, ok := c.source.ToolEntryForAgent(agentID, toolID); ok {
				continue
			}
			if runtimetools.MCPToolDiscovered(toolID, discoveredTools) {
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

func (c *checkerContext) requiredMCPToolAvailability() []Finding {
	if c.requiredMCPLoaded {
		return c.requiredMCPFindings
	}
	c.requiredMCPLoaded = true
	for _, item := range runtimetools.RequiredMCPToolAvailabilityFindings(c.source, c.mcpDiscovered()) {
		c.requiredMCPFindings = append(c.requiredMCPFindings, Finding{
			CheckID:  "required_mcp_tool_availability",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("agent %s requires MCP tool %s but %s", item.AgentID, item.ToolName, item.Reason),
			Location: item.AgentID,
		})
	}
	return c.requiredMCPFindings
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
