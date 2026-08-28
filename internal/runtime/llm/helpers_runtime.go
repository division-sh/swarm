package llm

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

func asString(v any) string {
	return runtimesharedjson.AsString(v)
}

type MCPTurnContextStore interface {
	RegisterTurnContextWithCapabilitySurface(context.Context, time.Duration, managedcapabilities.Surface) string
	RegisterConversationForkSandboxTurnContext(context.Context, time.Duration, []string) string
	ResolveManagedCapabilitySurface(string) (managedcapabilities.Surface, bool)
	UnregisterTurnContext(string)
}
