package runtime

import (
	"context"
	"net/http"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

const (
	ErrCodeMCPAuthUnconfigured  = runtimemcp.ErrCodeAuthUnconfigured
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

func RuntimeMCPGatewayHooks(logger *RuntimeLogger, runtimeIngress *runtimeingress.Controller, resolveActorConfig func(string) (runtimeactors.AgentConfig, bool), runtimeShutdownAdmissionClosed func() bool, emitRegistry *runtimetools.EmitRegistry, turnContexts *runtimemcp.TurnContextRegistry) runtimemcp.GatewayHooks {
	if emitRegistry == nil {
		emitRegistry = runtimetools.NewEmitRegistry(nil, nil)
	}
	runtimeIngressRequestPaused := func(ctx context.Context) (bool, error) {
		if runtimeIngress == nil {
			return false, nil
		}
		return runtimeIngress.RequestResponseIngressPaused(ctx)
	}
	return runtimemcp.GatewayHooks{
		RuntimeIngressRequestPaused:    runtimeIngressRequestPaused,
		RuntimeShutdownAdmissionClosed: runtimeShutdownAdmissionClosed,
		WithActor:                      runtimeactors.WithActor,
		ActorFromContext:               runtimeactors.ActorFromContext,
		ResolveActorConfig:             resolveActorConfig,
		WithCurrentRuntimeEpoch:        runtimebus.WithCurrentRuntimeEpoch,
		WithInboundEvent:               runtimebus.WithInboundEvent,
		WithEmittedEventsRecorder:      runtimebus.WithEmittedEventsRecorder,
		ResolveTurnContext: func(token string) (runtimemcp.TurnContext, bool) {
			if turnContexts == nil {
				return runtimemcp.TurnContext{}, false
			}
			return turnContexts.ResolveTurnContext(token)
		},
		MarkEmitKeyUsed: func(token, key string) bool {
			if turnContexts == nil {
				return false
			}
			return turnContexts.MarkEmitKeyUsed(token, key)
		},
		EmitToolsForActor: func(actor runtimeactors.AgentConfig) []llm.ToolDefinition {
			return emitRegistry.GenerateEmitToolsForActor(actor, processWarnOnce)
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			return emitRegistry.GenerateEmitToolsForRole(role, processWarnOnce)
		},
		EmitSchemaForTool: runtimeGatewayEmitSchemaForTool(emitRegistry),
		Log: func(ctx context.Context, level, action, agentID, entityID string, detail map[string]any, failure *runtimefailures.Envelope) {
			runtimeMCPLog(logger, ctx, level, action, agentID, entityID, detail, failure)
		},
		AfterToolSuccess: func(ctx context.Context, r *http.Request, toolName string) {
			runtimeMCPAfterToolSuccess(logger, ctx, r, toolName)
		},
	}
}

func runtimeGatewayEmitSchemaForTool(emitRegistry *runtimetools.EmitRegistry) func(string) (string, any, bool) {
	return func(name string) (string, any, bool) {
		if !strings.HasPrefix(name, "emit_") || emitRegistry == nil {
			return "", nil, false
		}
		evtType, mapped := emitRegistry.EventTypeFromToolName(name)
		if !mapped {
			return "", nil, false
		}
		evtSchema, ok := emitRegistry.EventSchemaSnapshot()[evtType]
		if !ok {
			return "", nil, false
		}
		desc := strings.TrimSpace(evtSchema.Description)
		if desc == "" {
			desc = "Emit event tool"
		}
		return desc, evtSchema.Schema, true
	}
}

func runtimeMCPLog(logger *RuntimeLogger, ctx context.Context, level, action, agentID, entityID string, detail map[string]any, failure *runtimefailures.Envelope) {
	if logger == nil {
		return
	}
	handleRuntimeLogPersistenceError("mcp-gateway", action, logger.Log(ctx, RuntimeLogEntry{
		Level:     diaglog.NormalizeLevel(level),
		Message:   runtimeMCPMessage(action, detail, failure),
		Component: "mcp-gateway",
		Action:    strings.TrimSpace(action),
		AgentID:   strings.TrimSpace(agentID),
		EntityID:  strings.TrimSpace(entityID),
		Detail:    detail,
		Failure:   runtimefailures.CloneEnvelope(failure),
	}))
}

func runtimeMCPAfterToolSuccess(logger *RuntimeLogger, ctx context.Context, r *http.Request, toolName string) {
	_, _, _, _ = logger, ctx, r, toolName
}

func runtimeMCPMessage(action string, detail map[string]any, failure *runtimefailures.Envelope) string {
	action = strings.TrimSpace(action)
	toolName := strings.TrimSpace(runtimeMCPDetailString(detail, "tool_name"))
	switch action {
	case "tool.authorize_failed":
		return "Tool gateway authorization failed"
	case "mcp.authorize_failed":
		return "MCP authorization failed"
	case "mcp.tools.call.invalid":
		return "MCP tools/call request was invalid"
	case "tool.context_error":
		return "Tool gateway context resolution failed"
	case "mcp.tools.call.context_error":
		return "MCP tool context resolution failed"
	case "tool.execute.denied":
		if toolName != "" {
			return "Tool gateway denied execution for " + toolName
		}
		return "Tool gateway denied tool execution"
	case "mcp.tools.call.denied":
		if toolName != "" {
			return "MCP tool call was denied for " + toolName
		}
		return "MCP tool call was denied"
	case "mcp.tools.call.exec_error":
		if toolName != "" {
			return "MCP tool execution failed for " + toolName
		}
		return "MCP tool execution failed"
	case "mcp.tools.call.success":
		if toolName != "" {
			return "MCP tool execution succeeded for " + toolName
		}
		return "MCP tool execution succeeded"
	case "mcp.context.fallback_used":
		return "MCP context fallback was used"
	case "mcp.context.fallback_blocked":
		return "MCP context fallback was blocked"
	case "tool.context.fallback_used":
		return "Tool context fallback was used"
	case "tool.context.fallback_blocked":
		return "Tool context fallback was blocked"
	default:
		if failure != nil || detail["protocol_error"] != nil {
			return "MCP gateway runtime log recorded an error"
		}
		return "MCP gateway runtime log recorded an event"
	}
}

func runtimeMCPDetailString(detail map[string]any, key string) string {
	if len(detail) == 0 {
		return ""
	}
	value, _ := detail[strings.TrimSpace(key)].(string)
	return strings.TrimSpace(value)
}
