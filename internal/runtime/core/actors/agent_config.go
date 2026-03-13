package actors

import (
	"context"
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
	ParentAgent    string          `json:"parent_agent_id,omitempty"`
	Config         json.RawMessage `json:"config,omitempty"`
	BudgetEnvelope float64         `json:"budget_envelope,omitempty"`
}

func (cfg AgentConfig) EffectiveEntityID() string { return strings.TrimSpace(cfg.EntityID) }

func (cfg *AgentConfig) NormalizeEntityID() {
	if cfg == nil {
		return
	}
	entityID := cfg.EffectiveEntityID()
	if strings.TrimSpace(cfg.EntityID) == "" {
		cfg.EntityID = entityID
	}
}

type actorContextKey struct{}

func WithActor(ctx context.Context, actor AgentConfig) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

func ActorFromContext(ctx context.Context) (AgentConfig, bool) {
	v := ctx.Value(actorContextKey{})
	if v == nil {
		return AgentConfig{}, false
	}
	cfg, ok := v.(AgentConfig)
	return cfg, ok
}
