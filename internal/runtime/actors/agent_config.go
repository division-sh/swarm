package actors

import "encoding/json"

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
