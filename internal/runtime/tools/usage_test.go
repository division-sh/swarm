package tools

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestValidateUsageHintCoverage_RejectsMissingPlatformOwnedHint(t *testing.T) {
	original := builtinToolUsageHints["query_entities"]
	builtinToolUsageHints["query_entities"] = ""
	t.Cleanup(func() { builtinToolUsageHints["query_entities"] = original })

	findings := ValidateUsageHintCoverage(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), nil)
	for _, finding := range findings {
		if finding.ToolName == "query_entities" && finding.Severity == "error" && strings.Contains(finding.Message, "missing a usage hint") {
			return
		}
	}
	t.Fatalf("findings = %#v, want hard missing usage finding for query_entities", findings)
}

func TestValidateUsageHintCoverage_ExternalMCPMissingHintIsLintOnly(t *testing.T) {
	findings := ValidateUsageHintCoverage(
		semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		map[string]runtimemcp.DiscoveredTool{
			"infra.ping": {
				ServerName: "infra",
				RemoteName: "ping",
				Contract: runtimecontracts.MustToolSchemaEntry(
					runtimecontracts.WithToolDescription("Ping infra"),
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
					runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}),
					runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("infra", "ping")),
				),
			},
		},
	)
	for _, finding := range findings {
		if finding.ToolName == "infra.ping" {
			if finding.Severity != "lint" {
				t.Fatalf("external MCP severity = %q, want lint", finding.Severity)
			}
			return
		}
	}
	t.Fatalf("findings = %#v, want external MCP lint finding", findings)
}

func TestValidateUsageHintCoverage_GeneratedEmitUsesModelDescription(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent": {
				ID:         "agent",
				EmitEvents: []string{"item.done"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.done": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	})

	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	for _, definition := range exec.ToolDefinitionsForActor(models.AgentConfig{ID: "agent", EmitEvents: []string{"item.done"}}) {
		if definition.Name == "emit_item_done" {
			if got := llm.DeliveredToolDescription(definition); got != "Send a item.done event" {
				t.Fatalf("model emit description=%q", got)
			}
			return
		}
	}
	t.Fatal("declared emit tool was not generated")
}

func TestValidateUsageHintCoverageDoesNotInventRoleDerivedEmitTools(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-instance-1": {
				ID:   "agent-instance-1",
				Role: "reviewer",
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"review.requested": {
						Emit: runtimecontracts.EmitSpec{Event: "review.completed"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"review.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	})

	findings := ValidateUsageHintCoverage(source, nil)
	for _, finding := range findings {
		if finding.ToolName == "emit_review_completed" {
			t.Fatalf("node role invented an agent emit tool: %#v", findings)
		}
	}
}
