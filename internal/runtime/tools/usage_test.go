package tools

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
				ServerName:  "infra",
				RemoteName:  "ping",
				Description: "Ping infra",
				InputSchema: map[string]any{"type": "object"},
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

func TestValidateUsageHintCoverage_RejectsGeneratedEmitHintMentioningCEL(t *testing.T) {
	original := emitToolUsageHint
	emitToolUsageHint = "Use CEL"
	t.Cleanup(func() { emitToolUsageHint = original })

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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

	findings := ValidateUsageHintCoverage(source, nil)
	for _, finding := range findings {
		if finding.ToolName == "emit_item_done" && finding.Severity == "error" && strings.Contains(finding.Message, "must not instruct") {
			return
		}
	}
	t.Fatalf("findings = %#v, want generated emit CEL usage rejection", findings)
}

func TestValidateUsageHintCoverage_CoversRoleDerivedEmitTools(t *testing.T) {
	original := emitToolUsageHint
	emitToolUsageHint = "Use CEL"
	t.Cleanup(func() { emitToolUsageHint = original })

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-instance-1": {
				ID:   "agent-instance-1",
				Role: "reviewer",
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				Produces: []string{"review.completed"},
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
		if finding.ToolName == "emit_review_completed" && finding.Severity == "error" && strings.Contains(finding.Message, "must not instruct") {
			return
		}
	}
	t.Fatalf("findings = %#v, want role-derived generated emit usage rejection", findings)
}
