package runtime

import (
	"testing"

	"empireai/internal/models"
)

func TestContractPromptAgentIDForConfig_ShardsAndOpCo(t *testing.T) {
	if got := contractPromptAgentIDForConfig(models.AgentConfig{
		ID:          "market-research-agent-shard-0-abc123",
		Role:        "market-research-agent",
		ParentAgent: "market-research-agent",
		Mode:        "factory",
	}); got != "market-research-agent" {
		t.Fatalf("expected shard prompt id market-research-agent, got %q", got)
	}

	if got := contractPromptAgentIDForConfig(models.AgentConfig{
		ID:         "vp-product-v123",
		Role:       "vp-product",
		Mode:       "operating",
		VerticalID: "v123",
	}); got != "opco-head-of-product" {
		t.Fatalf("expected operating prompt id opco-head-of-product, got %q", got)
	}
}
