package runtime

import (
	"context"
	"net/http"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimetools "swarm/internal/runtime/tools"
)

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

func newMCPRuntimeError(code, operation string, retryable bool, cause error, format string, args ...any) error {
	return WrapRuntimeError(code, "mcp-gateway", operation, retryable, cause, format, args...)
}

func runtimeErrorCodeFromText(raw string) string {
	return runtimemcp.RuntimeErrorCodeFromText(raw)
}

func runtimeErrorEnvelope(raw string) string {
	return runtimemcp.RuntimeErrorEnvelope(raw)
}

func RuntimeMCPGatewayHooks(logger *RuntimeLogger, resolveActorConfig func(string) (runtimeactors.AgentConfig, bool), emitRegistry *runtimetools.EmitRegistry, turnContexts *runtimemcp.TurnContextRegistry) runtimemcp.GatewayHooks {
	if emitRegistry == nil {
		emitRegistry = runtimetools.NewEmitRegistry(nil, nil)
	}
	return runtimemcp.GatewayHooks{
		RuntimeIngressPaused:      runtimebus.RuntimeIngressPaused,
		FormatError:               FormatRuntimeError,
		NewRuntimeError:           newMCPRuntimeError,
		RetryableFromError:        retryableFromGatewayError,
		WithActor:                 runtimeactors.WithActor,
		ActorFromContext:          runtimeactors.ActorFromContext,
		ResolveActorConfig:        resolveActorConfig,
		WithCurrentRuntimeEpoch:   runtimebus.WithCurrentRuntimeEpoch,
		WithInboundEvent:          runtimebus.WithInboundEvent,
		WithEmittedEventsRecorder: runtimebus.WithEmittedEventsRecorder,
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

func runtimeMCPLog(logger *RuntimeLogger, ctx context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
	if logger == nil {
		return
	}
	handleRuntimeLogPersistenceError("mcp-gateway", action, logger.Log(ctx, RuntimeLogEntry{
		Level:     strings.ToLower(strings.TrimSpace(level)),
		Message:   runtimeMCPMessage(action, detail, errText),
		Component: "mcp-gateway",
		Action:    strings.TrimSpace(action),
		AgentID:   strings.TrimSpace(agentID),
		EntityID:  strings.TrimSpace(entityID),
		Detail:    detail,
		Error:     strings.TrimSpace(errText),
	}))
}

func runtimeMCPAfterToolSuccess(logger *RuntimeLogger, ctx context.Context, r *http.Request, toolName string) {
	_, _, _, _ = logger, ctx, r, toolName
}

func runtimeMCPMessage(action string, detail map[string]any, errText string) string {
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
		if strings.TrimSpace(errText) != "" {
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
