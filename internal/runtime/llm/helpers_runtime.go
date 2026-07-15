package llm

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
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
	RegisterTurnContextWithCapabilitySurface(context.Context, time.Duration, managedcapabilities.Surface) string
	RegisterConversationForkSandboxTurnContext(context.Context, time.Duration, []string) string
	ResolveManagedCapabilitySurface(string) (managedcapabilities.Surface, bool)
	UnregisterTurnContext(string)
}
