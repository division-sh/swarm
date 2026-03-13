package runtime

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"sync"
	"time"

	runtimeactor "empireai/internal/runtime/actorctx"
	runtimebus "empireai/internal/runtime/bus"
	runtimecorpus "empireai/internal/runtime/corpusobs"
	llm "empireai/internal/runtime/llm"
	runtimemcp "empireai/internal/runtime/mcp"
	runtimetools "empireai/internal/runtime/tools"
)

type mcpTurnContext = runtimemcp.TurnContext

type mcpTurnRegistry struct {
	mu sync.Mutex
}

var globalMCPTurnRegistry = newMCPTurnRegistry()
var defaultMCPTurnContextTTL = 2 * time.Hour
var mcpStallDiagSeen sync.Map

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
	ErrCodeMCPStallDetected     = runtimemcp.ErrCodeStallDetected
)

func init() {
	runtimemcp.SetActorResolver(runtimeactor.ActorFromContext)
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

func RuntimeMCPGatewayHooks(logger *RuntimeLogger) runtimemcp.GatewayHooks {
	return runtimemcp.GatewayHooks{
		RuntimeIngressPaused:      runtimebus.RuntimeIngressPaused,
		FormatError:               FormatRuntimeError,
		NewRuntimeError:           newMCPRuntimeError,
		RetryableFromError:        retryableFromGatewayError,
		WithActor:                 runtimeactor.WithActor,
		ActorFromContext:          runtimeactor.ActorFromContext,
		WithRuntimeEpoch:          runtimebus.WithRuntimeEpoch,
		WithCurrentRuntimeEpoch:   runtimebus.WithCurrentRuntimeEpoch,
		IsCurrentRuntimeEpoch:     runtimebus.IsCurrentRuntimeEpoch,
		WithInboundEvent:          runtimebus.WithInboundEvent,
		WithEmittedEventsRecorder: runtimebus.WithEmittedEventsRecorder,
		ResolveTurnContext:        resolveMCPTurnContext,
		EmitTools: func(role string) []llm.ToolDefinition {
			return runtimetools.GenerateEmitToolsForRole(role, runtimeWarnOnce)
		},
		EmitSchemaForTool: runtimeGatewayEmitSchemaForTool,
		Log: func(ctx context.Context, level, action, agentID, verticalID string, detail map[string]any, errText string) {
			runtimeMCPLog(logger, ctx, level, action, agentID, verticalID, detail, errText)
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

func runtimeMCPLog(logger *RuntimeLogger, ctx context.Context, level, action, agentID, verticalID string, detail map[string]any, errText string) {
	if logger == nil {
		return
	}
	logger.Log(ctx, RuntimeLogEntry{
		Level:      strings.ToLower(strings.TrimSpace(level)),
		Component:  "mcp-gateway",
		Action:     strings.TrimSpace(action),
		AgentID:    strings.TrimSpace(agentID),
		EntityID:   strings.TrimSpace(verticalID),
		VerticalID: strings.TrimSpace(verticalID),
		Detail:     detail,
		Error:      strings.TrimSpace(errText),
	})
}

func runtimeMCPAfterToolSuccess(logger *RuntimeLogger, ctx context.Context, r *http.Request, toolName string) {
	if logger == nil {
		return
	}
	if meta, snapshot, ok := runtimecorpus.RecordEmitFromContext(ctx, toolName, time.Now().UTC()); ok && snapshot.EmitCount == 1 {
		runtimeMCPLogCorpusFirstEmit(logger, ctx, r, meta, snapshot, toolName)
	}
}

func runtimeMCPLogCorpusFirstEmit(logger *RuntimeLogger, ctx context.Context, r *http.Request, meta runtimecorpus.TurnMeta, snapshot runtimecorpus.EmitSnapshot, toolName string) {
	if logger == nil {
		return
	}
	agentID := strings.TrimSpace(meta.AgentID)
	if actor, ok := runtimeactor.ActorFromContext(ctx); ok && strings.TrimSpace(actor.ID) != "" {
		agentID = strings.TrimSpace(actor.ID)
	}
	verticalID := strings.TrimSpace(meta.VerticalID)
	if actor, ok := runtimeactor.ActorFromContext(ctx); ok && strings.TrimSpace(actor.VerticalID) != "" {
		verticalID = strings.TrimSpace(actor.VerticalID)
	}
	msToFirstEmit := int64(0)
	if !meta.AssignedAt.IsZero() && !snapshot.FirstEmitAt.IsZero() {
		msToFirstEmit = snapshot.FirstEmitAt.Sub(meta.AssignedAt).Milliseconds()
		if msToFirstEmit < 0 {
			msToFirstEmit = 0
		}
	}
	logger.Log(ctx, RuntimeLogEntry{
		Level:      "debug",
		Component:  "mcp-gateway",
		Action:     "corpus.first_emit",
		EventID:    strings.TrimSpace(meta.EventID),
		EventType:  strings.TrimSpace(meta.EventType),
		AgentID:    agentID,
		EntityID:   verticalID,
		VerticalID: verticalID,
		CampaignID: strings.TrimSpace(meta.CampaignID),
		ScanID:     strings.TrimSpace(meta.ScanID),
		Detail: map[string]any{
			"tool_name":        strings.TrimSpace(toolName),
			"trace_id":         runtimemcp.TraceIDFromRequest(r),
			"batch_size":       meta.BatchSize,
			"payload_bytes":    meta.PayloadBytes,
			"ms_to_first_emit": msToFirstEmit,
		},
	})
}

func DefaultMCPStallDiagnosticConfig() runtimemcp.StallDiagnosticConfig {
	return runtimemcp.DefaultStallDiagnosticConfig()
}

func StartMCPStallDiagnosticLoop(ctx context.Context, db *sql.DB, logger *RuntimeLogger, cfg runtimemcp.StallDiagnosticConfig) {
	if logger == nil {
		return
	}
	runtimemcp.StartStallDiagnosticLoop(ctx, db, runtimeMCPDiagnosticLogger(logger), cfg)
}

func runMCPStallDiagnosticsPass(ctx context.Context, db *sql.DB, logger *RuntimeLogger, cfg runtimemcp.StallDiagnosticConfig) {
	runtimemcp.ResetStallDiagnosticsForTest()
	mcpStallDiagSeen = sync.Map{}
	if logger == nil {
		return
	}
	runtimemcp.RunStallDiagnosticsPass(ctx, db, runtimeMCPDiagnosticLogger(logger), cfg)
}

func classifyMCPStallCause(code string) string {
	return runtimemcp.ClassifyStallCause(code)
}

func runtimeMCPDiagnosticLogger(logger *RuntimeLogger) runtimemcp.StallDiagnosticLogger {
	return func(ctx context.Context, level, component, action, agentID, verticalID string, detail map[string]any, errText string) {
		logger.Log(ctx, RuntimeLogEntry{
			Level:      level,
			Component:  component,
			Action:     action,
			AgentID:    agentID,
			EntityID:   verticalID,
			VerticalID: verticalID,
			Detail:     detail,
			Error:      errText,
		})
	}
}
