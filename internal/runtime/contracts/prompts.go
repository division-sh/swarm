package contracts

import (
	"strings"

	"empireai/internal/models"
	"empireai/internal/promptcontracts"
)

func LoadPromptForAgent(cfg models.AgentConfig, mode string) (string, bool, error) {
	agentID := PromptAgentIDForConfig(cfg)
	if agentID == "" {
		return "", false, nil
	}
	return promptcontracts.Load(agentID, mode)
}

func PromptAgentIDForConfig(cfg models.AgentConfig) string {
	agentID := strings.TrimSpace(cfg.ID)
	parent := strings.TrimSpace(cfg.ParentAgent)

	if parent != "" && strings.Contains(agentID, "-shard-") {
		agentID = parent
	}

	role := canonicalRuntimeRole(cfg.Role)
	if strings.EqualFold(strings.TrimSpace(cfg.Mode), "operating") {
		if mapped, ok := map[string]string{
			"opco-ceo":        "opco-ceo",
			"chief-of-staff":  "opco-chief-of-staff",
			"vp-product":      "opco-head-of-product",
			"vp-growth":       "opco-head-of-growth",
			"cto-agent":       "opco-cto",
			"pm-agent":        "opco-pm",
			"support-agent":   "opco-support",
			"marketing-agent": "opco-marketing",
			"tech-writer":     "opco-tech-writer",
			"backend-agent":   "opco-backend",
			"frontend-agent":  "opco-frontend",
			"qa-agent":        "opco-qa",
			"devops-agent":    "opco-devops",
		}[role]; ok {
			return mapped
		}
	}

	return strings.TrimSpace(agentID)
}

func canonicalRuntimeRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}
