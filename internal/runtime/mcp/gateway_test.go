package mcp

import (
	"testing"

	models "swarm/internal/runtime/core/actors"
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

