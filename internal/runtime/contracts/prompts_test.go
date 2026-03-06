package contracts

import (
	"strings"
	"testing"

	"empireai/internal/models"
)

func TestContractPromptAgentIDForConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  models.AgentConfig
		want string
	}{
		{
			name: "factory shard uses parent prompt id",
			cfg: models.AgentConfig{
				ID:          "market-research-agent-shard-0-abc123",
				Role:        "market-research-agent",
				ParentAgent: "market-research-agent",
				Mode:        "factory",
			},
			want: "market-research-agent",
		},
		{
			name: "operating vp-product maps to opco-head-of-product",
			cfg: models.AgentConfig{
				ID:         "vp-product-v123",
				Role:       "vp-product",
				Mode:       "operating",
				VerticalID: "v123",
			},
			want: "opco-head-of-product",
		},
		{
			name: "operating chief of staff maps to opco-chief-of-staff",
			cfg: models.AgentConfig{
				ID:         "chief-of-staff-v123",
				Role:       "chief-of-staff",
				Mode:       "operating",
				VerticalID: "v123",
			},
			want: "opco-chief-of-staff",
		},
		{
			name: "operating backend maps to opco-backend",
			cfg: models.AgentConfig{
				ID:         "backend-agent-v123",
				Role:       "backend-agent",
				Mode:       "operating",
				VerticalID: "v123",
			},
			want: "opco-backend",
		},
		{
			name: "operating unknown role falls back to ID",
			cfg: models.AgentConfig{
				ID:         "custom-agent-v123",
				Role:       "custom-agent",
				Mode:       "operating",
				VerticalID: "v123",
			},
			want: "custom-agent-v123",
		},
		{
			name: "factory non-shard falls back to ID",
			cfg: models.AgentConfig{
				ID:   "analysis-agent",
				Role: "analysis-agent",
				Mode: "factory",
			},
			want: "analysis-agent",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PromptAgentIDForConfig(tc.cfg); got != tc.want {
				t.Fatalf("prompt id mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestLoadContractPromptForAgent(t *testing.T) {
	cases := []struct {
		name     string
		cfg      models.AgentConfig
		mode     string
		contains string
	}{
		{
			name: "scanner prompt loads for shard clone via parent id",
			cfg: models.AgentConfig{
				ID:          "scanner-agent-shard-0-abc123",
				Role:        "scanner-agent",
				ParentAgent: "scanner-agent",
				Mode:        "factory",
			},
			mode:     "",
			contains: "Scanner Agent",
		},
		{
			name: "market research corpus mode variant loads",
			cfg: models.AgentConfig{
				ID:   "market-research-agent",
				Role: "market-research-agent",
				Mode: "factory",
			},
			mode:     "corpus",
			contains: "CORPUS MODE",
		},
		{
			name: "analysis prompt loads in default mode",
			cfg: models.AgentConfig{
				ID:   "analysis-agent",
				Role: "analysis-agent",
				Mode: "factory",
			},
			mode:     "",
			contains: "Analysis Agent",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prompt, found, err := LoadPromptForAgent(tc.cfg, tc.mode)
			if err != nil {
				t.Fatalf("loadContractPromptForAgent error: %v", err)
			}
			if !found {
				t.Fatalf("expected prompt to be found for cfg=%+v mode=%q", tc.cfg, tc.mode)
			}
			if strings.TrimSpace(prompt) == "" {
				t.Fatalf("expected non-empty prompt for cfg=%+v mode=%q", tc.cfg, tc.mode)
			}
			if !strings.Contains(prompt, tc.contains) {
				t.Fatalf("expected prompt to contain %q, got %q", tc.contains, prompt)
			}
		})
	}
}
