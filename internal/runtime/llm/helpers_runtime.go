package llm

import (
	"context"
	"strings"
	"time"

	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

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
