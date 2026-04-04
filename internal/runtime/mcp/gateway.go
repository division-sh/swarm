package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	"swarm/internal/runtime/core/toolidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	llm "swarm/internal/runtime/llm"
)

type GatewayHooks struct {
	RuntimeIngressPaused      func() bool
	FormatError               func(error) string
	NewRuntimeError           func(code, operation string, retryable bool, cause error, format string, args ...any) error
	RetryableFromError        func(error) (bool, bool)
	WithActor                 func(context.Context, models.AgentConfig) context.Context
	ActorFromContext          func(context.Context) (models.AgentConfig, bool)
	ResolveActorConfig        func(string) (models.AgentConfig, bool)
	WithRuntimeEpoch          func(context.Context, int64) context.Context
	WithCurrentRuntimeEpoch   func(context.Context) context.Context
	IsCurrentRuntimeEpoch     func(int64) bool
	WithInboundEvent          func(context.Context, events.Event) context.Context
	WithEmittedEventsRecorder func(context.Context, *runtimebus.EmittedEventsRecorder) context.Context
	ResolveTurnContext        func(string) (TurnContext, bool)
	EmitToolsForActor         func(models.AgentConfig) []llm.ToolDefinition
	EmitTools                 func(string) []llm.ToolDefinition
	EmitSchemaForTool         func(string) (description string, schema any, ok bool)
	Log                       func(context.Context, string, string, string, string, map[string]any, string)
	AfterToolSuccess          func(context.Context, *http.Request, string)
}

type Gateway struct {
	executor  runtimeGatewayExecutor
	authToken string
	hooks     GatewayHooks
}

type runtimeGatewayExecutor interface {
	llm.CapabilityAwareToolExecutor
	ToolDefinitions() []llm.ToolDefinition
	ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition
}

func NewGateway(executor runtimeGatewayExecutor, authToken string, hooks GatewayHooks) *Gateway {
	return &Gateway{
		executor:  executor,
		authToken: strings.TrimSpace(authToken),
		hooks:     hooks,
	}
}

func (g *Gateway) hydrateActor(actor models.AgentConfig) models.AgentConfig {
	actor.ID = strings.TrimSpace(actor.ID)
	if actor.ID == "" || g.hooks.ResolveActorConfig == nil {
		actor.NormalizeEntityID()
		return actor
	}
	if resolved, ok := g.hooks.ResolveActorConfig(actor.ID); ok {
		if strings.TrimSpace(actor.Role) != "" {
			resolved.Role = strings.TrimSpace(actor.Role)
		}
		if strings.TrimSpace(actor.Mode) != "" {
			resolved.Mode = strings.TrimSpace(actor.Mode)
		}
		if strings.TrimSpace(actor.ParentAgent) != "" {
			resolved.ParentAgent = strings.TrimSpace(actor.ParentAgent)
		}
		if strings.TrimSpace(actor.EntityID) != "" {
			resolved.EntityID = strings.TrimSpace(actor.EntityID)
		}
		resolved.NormalizeEntityID()
		return resolved
	}
	actor.NormalizeEntityID()
	return actor
}

func (g *Gateway) SetHooks(hooks GatewayHooks) {
	if g == nil {
		return
	}
	g.hooks = hooks
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/tools/", g.handleTool)
	mux.HandleFunc("/mcp", g.handleMCP)
	return mux
}

func (g *Gateway) handleTool(w http.ResponseWriter, r *http.Request) {
	if g.runtimeIngressPaused() {
		WriteJSON(w, http.StatusServiceUnavailable, ToolGatewayResponse{OK: false, Error: "runtime reset in progress"})
		return
	}
	if r.Method != http.MethodPost {
		WriteJSON(w, http.StatusMethodNotAllowed, ToolGatewayResponse{OK: false, Error: "method not allowed"})
		return
	}
	if err := g.authorize(r); err != nil {
		g.logMCP(r, "warn", "tool.authorize_failed", err, map[string]any{
			"path":         strings.TrimSpace(r.URL.Path),
			"denial_layer": "gateway",
		})
		WriteJSON(w, http.StatusUnauthorized, ToolGatewayResponse{OK: false, Error: g.formatError(err)})
		return
	}
	toolName := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/tools/"))
	if toolName == "" {
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: "tool name is required"})
		return
	}
	if g.executor == nil {
		WriteJSON(w, http.StatusServiceUnavailable, ToolGatewayResponse{OK: false, Error: "tool executor unavailable"})
		return
	}

	var req ToolGatewayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: "invalid json body"})
		return
	}
	req.NormalizeEntityID()
	actor := req.Actor
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = strings.TrimSpace(req.AgentID)
		actor.Role = strings.TrimSpace(req.AgentRole)
		actor.EntityID = strings.TrimSpace(req.EffectiveEntityID())
		actor.Mode = strings.TrimSpace(req.Mode)
	}
	actor.NormalizeEntityID()
	if strings.TrimSpace(actor.ID) == "" {
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: "actor id is required"})
		return
	}
	actor = g.hydrateActor(actor)

	ctx, err := g.toolExecutionContext(r, actor, toolName)
	if err != nil {
		g.logMCP(r, "warn", "tool.context_error", err, map[string]any{
			"tool_name": toolName,
		})
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: g.formatError(err)})
		return
	}
	if !toolAllowedInContext(ctx, toolName) {
		err := g.newRuntimeError(ErrCodeToolNotAllowed, "tool.execute.authorize_tool", false, nil, "tool is not allowed for this agent: %s", toolName)
		g.logMCP(r, "warn", "tool.execute.denied", err, map[string]any{
			"tool_name":    toolName,
			"denial_layer": "gateway",
		})
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: g.formatError(err)})
		return
	}
	out, err := g.executor.Execute(ctx, toolName, req.Input)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, ToolGatewayResponse{OK: true, Result: out})
}

func (g *Gateway) handleMCP(w http.ResponseWriter, r *http.Request) {
	if g.runtimeIngressPaused() {
		WriteRPCError(w, nil, -32002, "runtime reset in progress")
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("content-type", "text/plain")
		w.Header().Set("mcp-protocol-version", "2025-03-26")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	case http.MethodPost:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err := g.authorize(r); err != nil {
		g.logMCP(r, "warn", "mcp.authorize_failed", err, map[string]any{
			"method":       strings.TrimSpace(reqMethodForLog(r)),
			"denial_layer": "gateway",
		})
		WriteRPCError(w, nil, -32001, g.formatError(err))
		return
	}

	var req RPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteRPCError(w, nil, -32700, "invalid json body")
		return
	}

	switch strings.TrimSpace(req.Method) {
	case "initialize":
		WriteRPCResult(w, req.ID, map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo": map[string]any{
				"name":    "tool-gateway",
				"version": "1.0.0",
			},
		})
		return
	case "notifications/initialized":
		w.Header().Set("mcp-protocol-version", "2025-03-26")
		w.WriteHeader(http.StatusAccepted)
		return
	case "tools/list":
		WriteRPCResult(w, req.ID, map[string]any{"tools": g.mcpToolsForRequest(r)})
		return
	case "tools/call":
		if g.executor == nil {
			WriteRPCResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "tool executor unavailable"}},
				"isError": true,
			})
			return
		}
		toolName := strings.TrimSpace(asString(req.Params["name"]))
		if toolName == "" {
			err := g.newRuntimeError(ErrCodeInvalidRequest, "mcp.tools.call", false, nil, "tool name is required")
			g.logMCP(r, "warn", "mcp.tools.call.invalid", err, map[string]any{
				"method": "tools/call",
			})
			WriteRPCResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
				"isError": true,
			})
			return
		}
		ctx, err := g.mcpExecutionContext(r, toolName)
		if err != nil {
			g.logMCP(r, "warn", "mcp.tools.call.context_error", err, map[string]any{
				"method":    "tools/call",
				"tool_name": toolName,
			})
			WriteRPCResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
				"isError": true,
			})
			return
		}
		if !toolAllowedInContext(ctx, toolName) {
			err := g.newRuntimeError(ErrCodeToolNotAllowed, "mcp.tools.call.authorize_tool", false, nil, "tool is not allowed for this agent: %s", toolName)
			g.logMCP(r, "warn", "mcp.tools.call.denied", err, map[string]any{
				"method":       "tools/call",
				"tool_name":    toolName,
				"denial_layer": "gateway",
			})
			WriteRPCResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
				"isError": true,
			})
			return
		}
		if toolIsKindInContext(ctx, toolName, toolcapabilities.KindEmit) {
			if token := ContextTokenFromRequest(r); token != "" && MarkEmitKeyUsed(token, emitTurnDedupeKey(toolName, req.Params["arguments"])) {
				WriteRPCResult(w, req.ID, map[string]any{
					"content": []map[string]any{{
						"type": "text",
						"text": ToolResultText(map[string]any{
							"ok":     false,
							"reason": "duplicate emit already executed this turn",
						}),
					}},
					"isError": false,
				})
				return
			}
		}
		out, execErr := g.executor.Execute(ctx, toolName, req.Params["arguments"])
		if execErr != nil {
			retryable := true
			if g.hooks.RetryableFromError != nil {
				if rv, ok := g.hooks.RetryableFromError(execErr); ok {
					retryable = rv
				}
			}
			err = g.newRuntimeError(ErrCodeToolExecFailed, "mcp.tools.call.execute", retryable, execErr, "tool execution failed: %s", toolName)
			g.logMCP(r, "warn", "mcp.tools.call.exec_error", err, map[string]any{
				"method":    "tools/call",
				"tool_name": toolName,
			})
			WriteRPCResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
				"isError": true,
			})
			return
		}
		g.logMCP(r, "debug", "mcp.tools.call.success", nil, map[string]any{
			"method":    "tools/call",
			"tool_name": toolName,
		})
		if g.hooks.AfterToolSuccess != nil {
			g.hooks.AfterToolSuccess(ctx, r, toolName)
		}
		WriteRPCResult(w, req.ID, map[string]any{
			"content": []map[string]any{{"type": "text", "text": ToolResultText(out)}},
			"isError": false,
		})
		return
	case "ping":
		WriteRPCResult(w, req.ID, map[string]any{})
		return
	default:
		WriteRPCError(w, req.ID, -32601, "method not found")
		return
	}
}

func (g *Gateway) mcpExecutionContext(r *http.Request, toolName string) (context.Context, error) {
	toolName = normalizeGatewayToolName(toolName)
	allowed := ParseAllowedToolsFromRequest(r)
	if token := ContextTokenFromRequest(r); token != "" {
		if g.hooks.ResolveTurnContext != nil {
			if turn, ok := g.hooks.ResolveTurnContext(token); ok {
				if g.hooks.IsCurrentRuntimeEpoch != nil && !g.hooks.IsCurrentRuntimeEpoch(turn.Epoch) {
					return nil, g.newRuntimeError(ErrCodeContextStale, "mcp.context.resolve", false, nil, "stale mcp context token")
				}
				ctx := context.Background()
				if g.hooks.WithActor != nil {
					ctx = g.hooks.WithActor(ctx, g.hydrateActor(turn.Actor))
				}
				if g.hooks.WithRuntimeEpoch != nil {
					ctx = g.hooks.WithRuntimeEpoch(ctx, turn.Epoch)
				}
				ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(turn.Inbound.RunID))
				if turn.HasInbound && g.hooks.WithInboundEvent != nil {
					ctx = g.hooks.WithInboundEvent(ctx, turn.Inbound)
				}
				if turn.Recorder != nil && g.hooks.WithEmittedEventsRecorder != nil {
					ctx = g.hooks.WithEmittedEventsRecorder(ctx, turn.Recorder)
				}
				actor := g.hydrateActor(turn.Actor)
				ctx = g.withToolCapabilities(ctx, actor, g.catalogNames(g.toolCatalog(actor, true, strings.TrimSpace(actor.Role))), allowed)
				return ctx, nil
			}
		}
		if actor, ok := ActorFromRequest(r); ok {
			actor = g.hydrateActor(actor)
			fallbackAllowed := g.contextFallbackAllowed(actor, true, strings.TrimSpace(actor.Role), toolName, allowed)
			if AllowContextFallbackOnMiss() && fallbackAllowed {
				g.logContextFallback(r, "mcp.context.fallback_used", toolName, "token_not_found", token, true, fallbackAllowed)
				ctx := context.Background()
				if g.hooks.WithActor != nil {
					ctx = g.hooks.WithActor(ctx, actor)
				}
				if g.hooks.WithCurrentRuntimeEpoch != nil {
					ctx = g.hooks.WithCurrentRuntimeEpoch(ctx)
				}
				ctx = g.withToolCapabilities(ctx, actor, g.catalogNames(g.toolCatalog(actor, true, strings.TrimSpace(actor.Role))), allowed)
				return ctx, nil
			}
			g.logContextFallback(r, "mcp.context.fallback_blocked", toolName, "token_not_found", token, false, fallbackAllowed)
			return nil, g.newRuntimeError(ErrCodeContextNotFound, "mcp.context.resolve", false, nil, "missing or invalid mcp context token")
		}
		g.logContextFallback(r, "mcp.context.fallback_blocked", toolName, "token_not_found", token, false, false)
		return nil, g.newRuntimeError(ErrCodeContextNotFound, "mcp.context.resolve", false, nil, "missing or invalid mcp context token")
	}
	actor, ok := ActorFromRequest(r)
	if !ok {
		return nil, g.newRuntimeError(ErrCodeActorMissing, "mcp.context.resolve", false, nil, "missing actor id for mcp tool execution")
	}
	actor = g.hydrateActor(actor)
	fallbackAllowed := g.contextFallbackAllowed(actor, true, strings.TrimSpace(actor.Role), toolName, allowed)
	if !fallbackAllowed {
		g.logContextFallback(r, "mcp.context.fallback_blocked", toolName, "token_missing", "", false, fallbackAllowed)
		return nil, g.newRuntimeError(ErrCodeContextMissing, "mcp.context.resolve", false, nil, "missing or invalid mcp context token")
	}
	if RequireContextToken() {
		g.logContextFallback(r, "mcp.context.fallback_blocked", toolName, "token_missing", "", false, fallbackAllowed)
		return nil, g.newRuntimeError(ErrCodeContextMissing, "mcp.context.resolve", false, nil, "missing or invalid mcp context token")
	}
	g.logContextFallback(r, "mcp.context.fallback_used", toolName, "token_missing", "", true, fallbackAllowed)
	ctx := context.Background()
	if g.hooks.WithActor != nil {
		ctx = g.hooks.WithActor(ctx, actor)
	}
	if g.hooks.WithCurrentRuntimeEpoch != nil {
		ctx = g.hooks.WithCurrentRuntimeEpoch(ctx)
	}
	ctx = g.withToolCapabilities(ctx, actor, g.catalogNames(g.toolCatalog(actor, true, strings.TrimSpace(actor.Role))), allowed)
	return ctx, nil
}

func (g *Gateway) toolExecutionContext(r *http.Request, actor models.AgentConfig, toolName string) (context.Context, error) {
	toolName = normalizeGatewayToolName(toolName)
	allowed := ParseAllowedToolsFromRequest(r)
	if token := ContextTokenFromRequest(r); token != "" {
		if g.hooks.ResolveTurnContext != nil {
			if turn, ok := g.hooks.ResolveTurnContext(token); ok {
				if g.hooks.IsCurrentRuntimeEpoch != nil && !g.hooks.IsCurrentRuntimeEpoch(turn.Epoch) {
					return nil, g.newRuntimeError(ErrCodeContextStale, "tool.context.resolve", false, nil, "stale mcp context token")
				}
				ctx := r.Context()
				if g.hooks.WithActor != nil {
					ctx = g.hooks.WithActor(ctx, g.hydrateActor(turn.Actor))
				}
				if g.hooks.WithRuntimeEpoch != nil {
					ctx = g.hooks.WithRuntimeEpoch(ctx, turn.Epoch)
				}
				ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(turn.Inbound.RunID))
				if turn.HasInbound && g.hooks.WithInboundEvent != nil {
					ctx = g.hooks.WithInboundEvent(ctx, turn.Inbound)
				}
				if turn.Recorder != nil && g.hooks.WithEmittedEventsRecorder != nil {
					ctx = g.hooks.WithEmittedEventsRecorder(ctx, turn.Recorder)
				}
				resolvedActor := g.hydrateActor(turn.Actor)
				ctx = g.withToolCapabilities(ctx, resolvedActor, g.catalogNames(g.toolCatalog(resolvedActor, true, strings.TrimSpace(resolvedActor.Role))), allowed)
				return ctx, nil
			}
		}
		fallbackAllowed := g.contextFallbackAllowed(actor, true, strings.TrimSpace(actor.Role), toolName, allowed)
		if AllowContextFallbackOnMiss() && fallbackAllowed {
			g.logContextFallback(r, "tool.context.fallback_used", toolName, "token_not_found", token, true, fallbackAllowed)
			ctx := r.Context()
			if g.hooks.WithActor != nil {
				ctx = g.hooks.WithActor(ctx, actor)
			}
			if g.hooks.WithCurrentRuntimeEpoch != nil {
				ctx = g.hooks.WithCurrentRuntimeEpoch(ctx)
			}
			ctx = g.withToolCapabilities(ctx, actor, g.catalogNames(g.toolCatalog(actor, true, strings.TrimSpace(actor.Role))), allowed)
			return ctx, nil
		}
		g.logContextFallback(r, "tool.context.fallback_blocked", toolName, "token_not_found", token, false, fallbackAllowed)
		return nil, g.newRuntimeError(ErrCodeContextNotFound, "tool.context.resolve", false, nil, "missing or invalid mcp context token")
	}
	fallbackAllowed := g.contextFallbackAllowed(actor, true, strings.TrimSpace(actor.Role), toolName, allowed)
	if !fallbackAllowed {
		g.logContextFallback(r, "tool.context.fallback_blocked", toolName, "token_missing", "", false, fallbackAllowed)
		return nil, g.newRuntimeError(ErrCodeContextMissing, "tool.context.resolve", false, nil, "missing or invalid mcp context token")
	}
	g.logContextFallback(r, "tool.context.fallback_used", toolName, "token_missing", "", true, fallbackAllowed)
	ctx := r.Context()
	if g.hooks.WithActor != nil {
		ctx = g.hooks.WithActor(ctx, actor)
	}
	if g.hooks.WithCurrentRuntimeEpoch != nil {
		ctx = g.hooks.WithCurrentRuntimeEpoch(ctx)
	}
	ctx = g.withToolCapabilities(ctx, actor, g.catalogNames(g.toolCatalog(actor, true, strings.TrimSpace(actor.Role))), allowed)
	return ctx, nil
}

func (g *Gateway) withToolCapabilities(ctx context.Context, actor models.AgentConfig, names []string, requestAllowed map[string]struct{}) context.Context {
	if g == nil || ctx == nil {
		return ctx
	}
	if g.executor == nil {
		return ctx
	}
	set := g.executor.ToolCapabilitiesForActor(actor, names, requestAllowed)
	return toolcapabilities.WithContext(ctx, set)
}

func toolAllowedInContext(ctx context.Context, toolName string) bool {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return true
	}
	cap, ok := set.Capability(toolName)
	if !ok {
		return false
	}
	return cap.Callable
}

func toolIsKindInContext(ctx context.Context, toolName string, kind toolcapabilities.ToolKind) bool {
	cap, ok := toolCapabilityInContext(ctx, toolName)
	if !ok {
		return false
	}
	return cap.Kind == kind
}

func toolCapabilityInContext(ctx context.Context, toolName string) (toolcapabilities.Capability, bool) {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return toolcapabilities.Capability{}, false
	}
	return set.Capability(toolName)
}

func normalizeGatewayToolName(name string) string {
	return toolidentity.CanonicalName(name)
}

func (g *Gateway) logContextFallback(r *http.Request, action, toolName, reason, token string, used bool, fallbackAllowed bool) {
	if g == nil || r == nil {
		return
	}
	g.logMCP(r, "warn", action, nil, map[string]any{
		"tool_name":         strings.TrimSpace(toolName),
		"reason":            strings.TrimSpace(reason),
		"context_token":     strings.TrimSpace(token),
		"fallback_used":     used,
		"fallback_allowed":  fallbackAllowed,
		"require_ctx_token": RequireContextToken(),
		"fallback_on_miss":  AllowContextFallbackOnMiss(),
		"request_path":      strings.TrimSpace(r.URL.Path),
		"request_method":    strings.TrimSpace(r.Method),
	})
}

func emitTurnDedupeKey(toolName string, arguments any) string {
	normalized := strings.TrimSpace(toolName)
	encoded, err := json.Marshal(arguments)
	if err != nil {
		return normalized + "\n" + fmt.Sprintf("%#v", arguments)
	}
	return normalized + "\n" + string(encoded)
}

func (g *Gateway) mcpToolsForRequest(r *http.Request) []ToolDef {
	allowed := ParseAllowedToolsFromRequest(r)
	role := ""
	actor, actorOK := ActorFromRequest(r)
	if actorOK {
		actor = g.hydrateActor(actor)
		role = strings.TrimSpace(actor.Role)
	}
	if role == "" {
		role = headerValue(r, actorRoleHeader)
	}
	catalog := g.toolCatalog(actor, actorOK, role)
	set, hasSet := g.requestToolCapabilities(actor, actorOK, role, catalog, allowed)

	names := make([]string, 0, len(catalog))
	if hasSet {
		for name, cap := range set.ByName {
			if !cap.Visible {
				continue
			}
			if _, ok := catalog[name]; !ok {
				continue
			}
			names = append(names, name)
		}
	} else {
		for name := range catalog {
			if len(allowed) > 0 {
				if _, ok := allowed[name]; !ok {
					continue
				}
			}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]ToolDef, 0, len(names))
	for _, name := range names {
		def, ok := catalog[name]
		desc := "Runtime tool"
		schema := any(map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": true,
		})
		if ok {
			if strings.TrimSpace(def.Description) != "" {
				desc = def.Description
			}
			if def.Schema != nil {
				schema = def.Schema
			}
		} else if g.hooks.EmitSchemaForTool != nil {
			if hookDesc, hookSchema, ok := g.hooks.EmitSchemaForTool(name); ok {
				if strings.TrimSpace(hookDesc) != "" {
					desc = hookDesc
				} else {
					desc = "Emit event tool"
				}
				if hookSchema != nil {
					schema = hookSchema
				}
			}
		}
		out = append(out, ToolDef{Name: name, Description: desc, InputSchema: schema})
	}
	return out
}

func (g *Gateway) toolCatalog(actor models.AgentConfig, actorOK bool, role string) map[string]llm.ToolDefinition {
	catalog := map[string]llm.ToolDefinition{}
	if g.executor != nil && actorOK {
		for _, def := range g.executor.ToolDefinitionsForActor(actor) {
			name := normalizeGatewayToolName(def.Name)
			if name != "" {
				def.Name = name
				catalog[name] = def
			}
		}
	}
	if g.executor != nil && len(catalog) == 0 {
		for _, def := range g.executor.ToolDefinitions() {
			name := normalizeGatewayToolName(def.Name)
			if name != "" {
				def.Name = name
				catalog[name] = def
			}
		}
	}
	if actorOK && g.hooks.EmitToolsForActor != nil {
		for _, def := range g.hooks.EmitToolsForActor(actor) {
			name := normalizeGatewayToolName(def.Name)
			if name != "" {
				def.Name = name
				catalog[name] = def
			}
		}
	} else if role != "" && g.hooks.EmitTools != nil {
		for _, def := range g.hooks.EmitTools(role) {
			name := normalizeGatewayToolName(def.Name)
			if name != "" {
				def.Name = name
				catalog[name] = def
			}
		}
	}
	return catalog
}

func (g *Gateway) requestToolCapabilities(actor models.AgentConfig, actorOK bool, _ string, catalog map[string]llm.ToolDefinition, requestAllowed map[string]struct{}) (toolcapabilities.Set, bool) {
	if g.executor == nil || !actorOK {
		return toolcapabilities.Set{}, false
	}
	return g.executor.ToolCapabilitiesForActor(actor, g.catalogNames(catalog), requestAllowed), true
}

func (g *Gateway) requestToolCapability(actor models.AgentConfig, actorOK bool, role string, toolName string, requestAllowed map[string]struct{}) (toolcapabilities.Capability, bool) {
	catalog := g.toolCatalog(actor, actorOK, role)
	set, ok := g.requestToolCapabilities(actor, actorOK, role, catalog, requestAllowed)
	if !ok {
		return toolcapabilities.Capability{}, false
	}
	return set.Capability(toolName)
}

func (g *Gateway) contextFallbackAllowed(actor models.AgentConfig, actorOK bool, role string, toolName string, requestAllowed map[string]struct{}) bool {
	cap, ok := g.requestToolCapability(actor, actorOK, role, toolName, requestAllowed)
	if !ok {
		return false
	}
	return cap.ContextRequirement == toolcapabilities.ContextRequirementActorContext
}

func (g *Gateway) catalogNames(catalog map[string]llm.ToolDefinition) []string {
	names := make([]string, 0, len(catalog))
	for name := range catalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (g *Gateway) authorize(r *http.Request) error {
	if strings.TrimSpace(g.authToken) == "" {
		return nil
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return g.newRuntimeError(ErrCodeAuthMissingBearer, "mcp.authorize", false, nil, "missing authorization bearer token")
	}
	const prefix = "bearer "
	if !strings.HasPrefix(strings.ToLower(authz), prefix) {
		return g.newRuntimeError(ErrCodeAuthInvalidBearer, "mcp.authorize", false, nil, "invalid authorization header")
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token != g.authToken {
		return g.newRuntimeError(ErrCodeAuthInvalidBearer, "mcp.authorize", false, nil, "invalid token")
	}
	return nil
}

func (g *Gateway) AuthorizeForTest(r *http.Request) error {
	return g.authorize(r)
}

func reqMethodForLog(r *http.Request) string {
	if r == nil {
		return ""
	}
	if strings.TrimSpace(r.Method) != http.MethodPost {
		return strings.TrimSpace(r.Method)
	}
	var req RPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return strings.TrimSpace(r.Method)
	}
	return strings.TrimSpace(req.Method)
}

func (g *Gateway) logMCP(r *http.Request, level, action string, err error, detail map[string]any) {
	if g == nil || g.hooks.Log == nil || r == nil {
		return
	}
	agentID := FirstNonEmpty(
		headerValue(r, actorIDHeader),
		strings.TrimSpace(r.URL.Query().Get(actorIDQuery)),
	)
	entityID := FirstNonEmpty(
		headerValue(r, entityIDHeader),
		strings.TrimSpace(r.URL.Query().Get(entityIDQuery)),
	)
	errText := ""
	if err != nil {
		errText = g.formatError(err)
	}
	g.hooks.Log(r.Context(), strings.ToLower(strings.TrimSpace(level)), strings.TrimSpace(action), agentID, entityID, detail, errText)
}

func (g *Gateway) runtimeIngressPaused() bool {
	if g == nil || g.hooks.RuntimeIngressPaused == nil {
		return false
	}
	return g.hooks.RuntimeIngressPaused()
}

func (g *Gateway) formatError(err error) string {
	if err == nil {
		return ""
	}
	if g != nil && g.hooks.FormatError != nil {
		return g.hooks.FormatError(err)
	}
	return err.Error()
}

func (g *Gateway) newRuntimeError(code, operation string, retryable bool, cause error, format string, args ...any) error {
	if g != nil && g.hooks.NewRuntimeError != nil {
		return g.hooks.NewRuntimeError(code, operation, retryable, cause, format, args...)
	}
	message := strings.TrimSpace(fmt.Sprintf(format, args...))
	if message == "" && cause != nil {
		message = strings.TrimSpace(cause.Error())
	}
	if message == "" {
		message = strings.TrimSpace(code)
	}
	if cause != nil {
		return fmt.Errorf("%s: %w", message, cause)
	}
	return fmt.Errorf("%s", message)
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return ""
	}
}

func _unusedTimeRef() time.Time { return time.Time{} }
