package actors

import (
	"encoding/json"
	"strings"
)

// AgentConfig is the runtime-owned actor descriptor used by manager, tools,
// LLM, and semantic/runtime contract resolution. It is intentionally distinct
// from persistence-row ownership even when stored verbatim.
type AgentConfig struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Role           string          `json:"role"`
	Mode           string          `json:"mode"`
	Subscriptions  []string        `json:"subscriptions,omitempty"`
	EntityID       string          `json:"entity_id,omitempty"`
	VerticalID     string          `json:"vertical_id,omitempty"`
	ParentAgent    string          `json:"parent_agent_id,omitempty"`
	Config         json.RawMessage `json:"config,omitempty"`
	BudgetEnvelope float64         `json:"budget_envelope,omitempty"`
}

func (cfg AgentConfig) EffectiveEntityID() string {
	entityID := strings.TrimSpace(cfg.EntityID)
	if entityID != "" {
		return entityID
	}
	return strings.TrimSpace(cfg.VerticalID)
}

func (cfg *AgentConfig) NormalizeEntityID() {
	if cfg == nil {
		return
	}
	entityID := cfg.EffectiveEntityID()
	if strings.TrimSpace(cfg.EntityID) == "" {
		cfg.EntityID = entityID
	}
	if strings.TrimSpace(cfg.VerticalID) == "" {
		cfg.VerticalID = entityID
	}
}
