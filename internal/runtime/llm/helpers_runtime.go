package llm

import (
	"context"
	"strings"
	"time"

	models "empireai/internal/runtime/actors"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

func budgetExecutionScopeKey(actor models.AgentConfig) string {
	verticalID := strings.TrimSpace(actor.VerticalID)
	if verticalID != "" {
		return verticalID
	}
	mode := strings.ToLower(strings.TrimSpace(actor.Mode))
	if mode == "factory" {
		if agentID := strings.TrimSpace(actor.ID); agentID != "" {
			return "__factory_agent__:" + agentID
		}
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

var mcpTurnContextRegister = func(context.Context, time.Duration) string { return "" }
var mcpTurnContextUnregister = func(string) {}

func SetMCPTurnContextHooks(register func(context.Context, time.Duration) string, unregister func(string)) {
	if register == nil {
		register = func(context.Context, time.Duration) string { return "" }
	}
	if unregister == nil {
		unregister = func(string) {}
	}
	mcpTurnContextRegister = register
	mcpTurnContextUnregister = unregister
}
