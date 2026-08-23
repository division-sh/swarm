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
	for _, finding := range runtimetools.ValidateConfiguredToolFulfillability(c.source, c.mcpDiscovered()) {
		toolName := strings.TrimSpace(finding.ToolName)
		message := finding.Reason
		if toolName != "" {
			message = fmt.Sprintf("agent %s references unfulfillable tool %s: %s", finding.AgentID, toolName, finding.Reason)
		}
		c.toolFindings = append(c.toolFindings, Finding{
			CheckID:     "tool_resolution",
			Severity:    SeverityHardInvalidity,
			Message:     message,
			Location:    finding.AgentID,
			Remediation: "Declare an executable tool candidate and its required permission for this exact agent, or remove the tool reference.",
		})
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
