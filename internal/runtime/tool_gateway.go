package runtime

import (
	"context"
	"net/http"
	"strings"
	"time"

	llm "empireai/internal/runtime/llm"
	runtimemcp "empireai/internal/runtime/mcp"
)

type mcpRPCResponse = runtimemcp.RPCResponse

type ToolGateway struct {
	inner  *runtimemcp.Gateway
	logger *RuntimeLogger
}

func NewToolGateway(executor llm.ToolExecutor, authToken string) *ToolGateway {
	gw := &ToolGateway{}
	gw.inner = runtimemcp.NewGateway(executor, authToken, gw.hooks())
	return gw
}

func (g *ToolGateway) SetRuntimeLogger(logger *RuntimeLogger) {
	if g == nil {
		return
	}
	g.logger = logger
	if g.inner != nil {
		g.inner.SetHooks(g.hooks())
	}
}

func (g *ToolGateway) Handler() http.Handler {
	if g == nil || g.inner == nil {
		return http.NewServeMux()
	}
	return g.inner.Handler()
}

func (g *ToolGateway) authorize(r *http.Request) error {
	if g == nil || g.inner == nil {
		return nil
	}
	return g.inner.AuthorizeForTest(r)
}

func (g *ToolGateway) writeMCPError(w http.ResponseWriter, id any, code int, message string) {
	runtimemcp.WriteRPCError(w, id, code, message)
}

func (g *ToolGateway) hooks() runtimemcp.GatewayHooks {
	return runtimemcp.GatewayHooks{
		RuntimeIngressPaused:      RuntimeIngressPaused,
		FormatError:               FormatRuntimeError,
		NewRuntimeError:           newMCPRuntimeError,
		RetryableFromError:        retryableFromGatewayError,
		WithActor:                 WithActor,
		ActorFromContext:          ActorFromContext,
		WithRuntimeEpoch:          WithRuntimeEpoch,
		WithCurrentRuntimeEpoch:   WithCurrentRuntimeEpoch,
		IsCurrentRuntimeEpoch:     IsCurrentRuntimeEpoch,
		WithInboundEvent:          WithInboundEvent,
		WithEmittedEventsRecorder: WithEmittedEventsRecorder,
		ResolveTurnContext:        resolveMCPTurnContext,
		EmitTools:                 GenerateEmitTools,
		EmitSchemaForTool:         gatewayEmitSchemaForTool,
		Log:                       g.logMCP,
		AfterToolSuccess:          g.afterToolSuccess,
	}
}

func retryableFromGatewayError(err error) (bool, bool) {
	if runtimeErr, ok := AsRuntimeError(err); ok {
		return runtimeErr.Retryable, true
	}
	return false, false
}

func gatewayEmitSchemaForTool(name string) (string, any, bool) {
	if !strings.HasPrefix(name, "emit_") {
		return "", nil, false
	}
	evtType, mapped := eventTypeFromEmitToolName(name)
	if !mapped {
		return "", nil, false
	}
	evtSchema := schemaForEventType(evtType)
	desc := strings.TrimSpace(evtSchema.Description)
	if desc == "" {
		desc = "Emit event tool"
	}
	return desc, evtSchema.Schema, true
}

func toolResultText(v any) string {
	return runtimemcp.ToolResultText(v)
}

func (g *ToolGateway) logMCP(ctx context.Context, level, action, agentID, verticalID string, detail map[string]any, errText string) {
	if g == nil || g.logger == nil {
		return
	}
	g.logger.Log(ctx, RuntimeLogEntry{
		Level:      strings.ToLower(strings.TrimSpace(level)),
		Component:  "mcp-gateway",
		Action:     strings.TrimSpace(action),
		AgentID:    strings.TrimSpace(agentID),
		VerticalID: strings.TrimSpace(verticalID),
		Detail:     detail,
		Error:      strings.TrimSpace(errText),
	})
}

func (g *ToolGateway) afterToolSuccess(ctx context.Context, r *http.Request, toolName string) {
	if g == nil || g.logger == nil {
		return
	}
	if meta, snapshot, ok := recordCorpusEmitFromContext(ctx, toolName, time.Now().UTC()); ok && snapshot.EmitCount == 1 {
		g.logCorpusFirstEmit(ctx, r, meta, snapshot, toolName)
	}
}

func (g *ToolGateway) logCorpusFirstEmit(
	ctx context.Context,
	r *http.Request,
	meta corpusTurnMeta,
	snapshot corpusEmitSnapshot,
	toolName string,
) {
	if g == nil || g.logger == nil {
		return
	}
	agentID := strings.TrimSpace(meta.AgentID)
	if actor, ok := ActorFromContext(ctx); ok && strings.TrimSpace(actor.ID) != "" {
		agentID = strings.TrimSpace(actor.ID)
	}
	verticalID := strings.TrimSpace(meta.VerticalID)
	if actor, ok := ActorFromContext(ctx); ok && strings.TrimSpace(actor.VerticalID) != "" {
		verticalID = strings.TrimSpace(actor.VerticalID)
	}
	msToFirstEmit := int64(0)
	if !meta.AssignedAt.IsZero() && !snapshot.FirstEmitAt.IsZero() {
		msToFirstEmit = snapshot.FirstEmitAt.Sub(meta.AssignedAt).Milliseconds()
		if msToFirstEmit < 0 {
			msToFirstEmit = 0
		}
	}
	g.logger.Log(ctx, RuntimeLogEntry{
		Level:      "debug",
		Component:  "mcp-gateway",
		Action:     "corpus.first_emit",
		EventID:    strings.TrimSpace(meta.EventID),
		EventType:  strings.TrimSpace(meta.EventType),
		AgentID:    agentID,
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
