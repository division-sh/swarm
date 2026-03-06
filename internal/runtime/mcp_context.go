package runtime

import (
	"context"
	"sync"
	"time"

	runtimemcp "empireai/internal/runtime/mcp"
)

type mcpTurnContext = runtimemcp.TurnContext

type mcpTurnRegistry struct {
	mu sync.Mutex
}

var globalMCPTurnRegistry = newMCPTurnRegistry()
var defaultMCPTurnContextTTL = 2 * time.Hour

func init() {
	runtimemcp.SetActorResolver(ActorFromContext)
}

func newMCPTurnRegistry() *mcpTurnRegistry {
	return &mcpTurnRegistry{}
}

func registerMCPTurnContext(ctx context.Context) string {
	return runtimemcp.RegisterTurnContext(ctx)
}

func registerMCPTurnContextWithTTL(ctx context.Context, ttl time.Duration) string {
	return runtimemcp.RegisterTurnContextWithTTL(ctx, ttl)
}

func resolveMCPTurnContext(token string) (mcpTurnContext, bool) {
	return runtimemcp.ResolveTurnContext(token)
}

func unregisterMCPTurnContext(token string) {
	runtimemcp.UnregisterTurnContext(token)
}

func resetMCPTurnContexts() {
	runtimemcp.ResetTurnContexts()
}

func (r *mcpTurnRegistry) put(token string, data mcpTurnContext) {
	runtimemcp.PutTurnContextForTest(token, data)
}

func (r *mcpTurnRegistry) get(token string) (mcpTurnContext, bool) {
	return runtimemcp.ResolveTurnContext(token)
}

func (r *mcpTurnRegistry) delete(token string) {
	runtimemcp.UnregisterTurnContext(token)
}

func (r *mcpTurnRegistry) reset() {
	runtimemcp.ResetTurnContexts()
}

func (r *mcpTurnRegistry) pruneLocked(now time.Time) {
	runtimemcp.PruneTurnContextsBefore(now)
}
