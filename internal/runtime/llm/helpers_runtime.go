package llm

import (
	"context"
	"strings"
	"time"

	models "swarm/internal/runtime/core/actors"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
)

func budgetExecutionScopeKey(actor models.AgentConfig) string {
	entityID := actor.EffectiveEntityID()
	if entityID != "" {
		return entityID
	}
	if agentID := strings.TrimSpace(actor.ID); agentID != "" {
		return "__agent__:" + agentID
	}
	return ""
}

func BudgetExecutionScopeKey(actor models.AgentConfig) string {
	return budgetExecutionScopeKey(actor)
}

func coalesce(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func asString(v any) string {
	return runtimesharedjson.AsString(v)
}

type MCPTurnContextStore interface {
	RegisterTurnContextWithTTL(context.Context, time.Duration) string
	RegisterTurnContextWithAllowedTools(context.Context, time.Duration, []string) string
	UnregisterTurnContext(string)
}
