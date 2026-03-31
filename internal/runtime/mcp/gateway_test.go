package mcp

import (
	"net/http/httptest"
	"testing"

	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
)

func TestGatewayHydrateActorMergesResolvedConfig(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "market-research-agent" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ID:          "market-research-agent",
				Role:        "market_research",
				Mode:        "discovery",
				EntityID:    "entity-1",
				Permissions: []string{"schedule"},
				Config:      []byte(`{"emit_events":["category.assessed","market_research.scan_complete"]}`),
			}, true
		},
	})

	hydrated := g.hydrateActor(models.AgentConfig{
		ID:   "market-research-agent",
		Role: "market_research",
	})

	if string(hydrated.Config) == "" {
		t.Fatal("expected hydrated actor config")
	}
	if hydrated.Mode != "discovery" {
		t.Fatalf("mode = %q, want discovery", hydrated.Mode)
	}
	if len(hydrated.Permissions) != 1 || hydrated.Permissions[0] != "schedule" {
		t.Fatalf("permissions = %#v", hydrated.Permissions)
	}
}

func TestGatewayMCPToolsForRequest_UsesHydratedActorRoleForEmitTools(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "campaign-coordinator" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ID:   "campaign-coordinator",
				Role: "campaign_coordinator",
			}, true
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			if role != "campaign_coordinator" {
				return nil
			}
			return []llm.ToolDefinition{{
				Name:        "emit_scan_requested",
				Description: "Emit scan.requested",
				Schema:      map[string]any{"type": "object"},
			}}
		},
	})

	req := httptest.NewRequest("POST", "/mcp?agent_id=campaign-coordinator", nil)
	tools := g.mcpToolsForRequest(req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested not found in MCP tools: %#v", tools)
}
