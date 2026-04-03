package runtime

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimetools "swarm/internal/runtime/tools"
)

type mcpTurnContext = runtimemcp.TurnContext

type mcpTurnRegistry struct {
	mu sync.Mutex
}

var globalMCPTurnRegistry = newMCPTurnRegistry()
var defaultMCPTurnContextTTL = 2 * time.Hour

const (
	ErrCodeMCPAuthMissingBearer = runtimemcp.ErrCodeAuthMissingBearer
	ErrCodeMCPAuthInvalidBearer = runtimemcp.ErrCodeAuthInvalidBearer
	ErrCodeMCPContextMissing    = runtimemcp.ErrCodeContextMissing
	ErrCodeMCPContextNotFound   = runtimemcp.ErrCodeContextNotFound
	ErrCodeMCPContextStale      = runtimemcp.ErrCodeContextStale
	ErrCodeMCPActorMissing      = runtimemcp.ErrCodeActorMissing
	ErrCodeMCPToolNotAllowed    = runtimemcp.ErrCodeToolNotAllowed
	ErrCodeMCPToolExecFailed    = runtimemcp.ErrCodeToolExecFailed
	ErrCodeMCPInvalidRequest    = runtimemcp.ErrCodeInvalidRequest
)

func init() {
	runtimemcp.SetActorResolver(runtimeactors.ActorFromContext)
	llm.SetMCPTurnContextHooks(runtimemcp.RegisterTurnContextWithTTL, runtimemcp.UnregisterTurnContext)
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

func newMCPRuntimeError(code, operation string, retryable bool, cause error, format string, args ...any) error {
	return WrapRuntimeError(code, "mcp-gateway", operation, retryable, cause, format, args...)
}

func runtimeErrorCodeFromText(raw string) string {
	return runtimemcp.RuntimeErrorCodeFromText(raw)
}

func runtimeErrorEnvelope(raw string) string {
	return runtimemcp.RuntimeErrorEnvelope(raw)
}

func RuntimeMCPGatewayHooks(logger *RuntimeLogger, resolveActorConfig func(string) (runtimeactors.AgentConfig, bool)) runtimemcp.GatewayHooks {
	return runtimemcp.GatewayHooks{
		RuntimeIngressPaused:      runtimebus.RuntimeIngressPaused,
		FormatError:               FormatRuntimeError,
		NewRuntimeError:           newMCPRuntimeError,
		RetryableFromError:        retryableFromGatewayError,
		WithActor:                 runtimeactors.WithActor,
		ActorFromContext:          runtimeactors.ActorFromContext,
		ResolveActorConfig:        resolveActorConfig,
		WithRuntimeEpoch:          runtimebus.WithRuntimeEpoch,
		WithCurrentRuntimeEpoch:   runtimebus.WithCurrentRuntimeEpoch,
		IsCurrentRuntimeEpoch:     runtimebus.IsCurrentRuntimeEpoch,
		WithInboundEvent:          runtimebus.WithInboundEvent,
		WithEmittedEventsRecorder: runtimebus.WithEmittedEventsRecorder,
		ResolveTurnContext:        resolveMCPTurnContext,
		EmitToolsForActor: func(actor runtimeactors.AgentConfig) []llm.ToolDefinition {
			return runtimetools.GenerateEmitToolsForConfig(actor.Config, processWarnOnce)
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			return runtimetools.GenerateEmitToolsForRole(role, processWarnOnce)
		},
		EmitSchemaForTool: runtimeGatewayEmitSchemaForTool,
		Log: func(ctx context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
			runtimeMCPLog(logger, ctx, level, action, agentID, entityID, detail, errText)
		},
		AfterToolSuccess: func(ctx context.Context, r *http.Request, toolName string) {
			runtimeMCPAfterToolSuccess(logger, ctx, r, toolName)
		},
	}
}

func retryableFromGatewayError(err error) (bool, bool) {
	if runtimeErr, ok := AsRuntimeError(err); ok {
		return runtimeErr.Retryable, true
	}
	return false, false
}

func runtimeGatewayEmitSchemaForTool(name string) (string, any, bool) {
	if !strings.HasPrefix(name, "emit_") {
		return "", nil, false
	}
	snapshot := runtimetools.EventSchemaSnapshot()
	toolToEvent := make(map[string]string, len(snapshot))
	for eventType := range snapshot {
		toolToEvent[runtimetools.EmitToolName(eventType)] = eventType
	}
	evtType, mapped := runtimetools.EventTypeFromEmitToolName(name, toolToEvent)
	if !mapped {
		return "", nil, false
	}
	evtSchema, ok := snapshot[evtType]
	if !ok {
		return "", nil, false
	}
	desc := strings.TrimSpace(evtSchema.Description)
	if desc == "" {
		desc = "Emit event tool"
	}
	return desc, evtSchema.Schema, true
}

func runtimeMCPLog(logger *RuntimeLogger, ctx context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
	if logger == nil {
		return
	}
	logger.Log(ctx, RuntimeLogEntry{
		Level:     strings.ToLower(strings.TrimSpace(level)),
		Component: "mcp-gateway",
		Action:    strings.TrimSpace(action),
		AgentID:   strings.TrimSpace(agentID),
		EntityID:  strings.TrimSpace(entityID),
		Detail:    detail,
		Error:     strings.TrimSpace(errText),
	})
}

func runtimeMCPAfterToolSuccess(logger *RuntimeLogger, ctx context.Context, r *http.Request, toolName string) {
	_, _, _, _ = logger, ctx, r, toolName
}
