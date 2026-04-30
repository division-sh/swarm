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
	"swarm/internal/runtime/core/toolresultpolicy"
	runtimecorrelation "swarm/internal/runtime/correlation"
	llm "swarm/internal/runtime/llm"
	runtimerterr "swarm/internal/runtime/rterrors"
)

type GatewayHooks struct {
	RuntimeIngressPaused           func() bool
	RuntimeShutdownAdmissionClosed func() bool
	FormatError                    func(error) string
	NewRuntimeError                func(code, operation string, retryable bool, cause error, format string, args ...any) error
	RetryableFromError             func(error) (bool, bool)
	WithActor                      func(context.Context, models.AgentConfig) context.Context
	ActorFromContext               func(context.Context) (models.AgentConfig, bool)
	ResolveActorConfig             func(string) (models.AgentConfig, bool)
	WithCurrentRuntimeEpoch        func(context.Context) context.Context
	WithInboundEvent               func(context.Context, events.Event) context.Context
	WithEmittedEventsRecorder      func(context.Context, *runtimebus.EmittedEventsRecorder) context.Context
	ResolveTurnContext             func(string) (TurnContext, bool)
	MarkEmitKeyUsed                func(string, string) bool
	EmitToolsForActor              func(models.AgentConfig) []llm.ToolDefinition
	EmitTools                      func(string) []llm.ToolDefinition
	EmitSchemaForTool              func(string) (description string, schema any, ok bool)
	Log                            func(context.Context, string, string, string, string, map[string]any, string)
	AfterToolSuccess               func(context.Context, *http.Request, string)
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
	actor.NormalizeRuntimeDescriptor()
	actor.NormalizeEntityID()
	actor.ID = strings.TrimSpace(actor.ID)
	if actor.ID == "" || g.hooks.ResolveActorConfig == nil {
		return actor
	}
	if resolved, ok := g.hooks.ResolveActorConfig(actor.ID); ok {
		resolved.NormalizeRuntimeDescriptor()
		resolved.NormalizeEntityID()
		return resolved
	}
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
	if r.Method != http.MethodPost {
		WriteJSON(w, http.StatusMethodNotAllowed, ToolGatewayResponse{OK: false, Error: "method not allowed"})
		return
	}
	if g.runtimeShutdownAdmissionClosed() {
		WriteJSON(w, http.StatusServiceUnavailable, ToolGatewayResponse{OK: false, Error: "runtime shutting down"})
		return
	}
	if g.runtimeIngressPaused() {
		WriteJSON(w, http.StatusServiceUnavailable, ToolGatewayResponse{OK: false, Error: "runtime reset in progress"})
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
	ctx, err := g.toolExecutionContext(r, toolName)
	if err != nil {
		g.logMCP(r, "warn", "tool.context_error", err, map[string]any{
			"tool_name": toolName,
		})
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: g.formatError(err)})
		return
	}
	r = r.WithContext(ctx)
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
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("content-type", "text/plain")
		w.Header().Set("mcp-protocol-version", "2025-03-26")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	case http.MethodPost:
		if g.runtimeShutdownAdmissionClosed() {
			WriteRPCError(w, nil, -32002, "runtime shutting down")
			return
		}
		if g.runtimeIngressPaused() {
			WriteRPCError(w, nil, -32002, "runtime reset in progress")
			return
		}
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
		tools, err := g.mcpToolsForRequest(r)
		if err != nil {
			g.logMCP(r, "warn", "mcp.tools.list.context_error", err, map[string]any{
				"method": "tools/list",
			})
			WriteRPCError(w, req.ID, -32003, g.formatError(err))
			return
		}
		WriteRPCResult(w, req.ID, map[string]any{"tools": tools})
		return
	case "tools/call":
		if g.executor == nil {
			err := g.newRuntimeError(ErrCodeToolExecFailed, "mcp.tools.call.execute", false, nil, "tool executor unavailable")
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		toolName := strings.TrimSpace(asString(req.Params["name"]))
		if toolName == "" {
			err := g.newRuntimeError(ErrCodeInvalidRequest, "mcp.tools.call", false, nil, "tool name is required")
			g.logMCP(r, "warn", "mcp.tools.call.invalid", err, map[string]any{
				"method": "tools/call",
			})
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		startupProbe, err := DecodeStartupProbeRequest(req.Params[startupProbeParamKey])
		if err != nil {
			err = g.newRuntimeError(ErrCodeInvalidRequest, "mcp.tools.call.startup_probe", false, err, "invalid startup probe request")
			g.logMCP(r, "warn", "mcp.tools.call.invalid_probe", err, map[string]any{
				"method":    "tools/call",
				"tool_name": toolName,
			})
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		ctx, err := g.mcpExecutionContext(r, toolName)
		if err != nil {
			g.logMCP(r, "warn", "mcp.tools.call.context_error", err, map[string]any{
				"method":    "tools/call",
				"tool_name": toolName,
			})
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		r = r.WithContext(ctx)
		if !toolAllowedInContext(ctx, toolName) {
			err := g.newRuntimeError(ErrCodeToolNotAllowed, "mcp.tools.call.authorize_tool", false, nil, "tool is not allowed for this agent: %s", toolName)
			g.logMCP(r, "warn", "mcp.tools.call.denied", err, map[string]any{
				"method":       "tools/call",
				"tool_name":    toolName,
				"denial_layer": "gateway",
			})
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		if toolIsKindInContext(ctx, toolName, toolcapabilities.KindEmit) {
			if token := ContextTokenFromRequest(r); token != "" && g.hooks.MarkEmitKeyUsed != nil && g.hooks.MarkEmitKeyUsed(token, emitTurnDedupeKey(toolName, req.Params["arguments"])) {
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
			payload := map[string]any{
				"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
				"isError": true,
			}
			if runtimeErr := RuntimeErrorPayloadFromError(err); runtimeErr != nil {
				payload["runtimeError"] = runtimeErr
				if startupProbe != nil {
					outcome, outcomeErr := StartupProbeResultForRuntimeError(startupProbe.Contract, toolName, runtimeErr)
					if outcomeErr != nil {
						err = g.newRuntimeError(ErrCodeInvalidRequest, "mcp.tools.call.startup_probe", false, outcomeErr, "invalid startup probe request")
						g.writeToolCallErrorResult(w, req.ID, err)
						return
					}
					payload[startupProbeResultKey] = outcome
				}
			}
			WriteRPCResult(w, req.ID, payload)
			return
		}
		g.logMCP(r, "debug", "mcp.tools.call.success", nil, map[string]any{
			"method":    "tools/call",
			"tool_name": toolName,
		})
		if g.hooks.AfterToolSuccess != nil {
			g.hooks.AfterToolSuccess(ctx, r, toolName)
		}
		resultText, err := projectToolCallSuccessText(ctx, g.executor, toolName, req.Params["arguments"], out)
		if err != nil {
			err = g.newRuntimeError(ErrCodeToolExecFailed, "mcp.tools.call.result_project", false, err, "tool result continuation failed: %s", toolName)
			g.logMCP(r, "warn", "mcp.tools.call.exec_error", err, map[string]any{
				"method":    "tools/call",
				"tool_name": toolName,
			})
			payload := map[string]any{
				"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
				"isError": true,
			}
			if runtimeErr := RuntimeErrorPayloadFromError(err); runtimeErr != nil {
				payload["runtimeError"] = runtimeErr
			}
			WriteRPCResult(w, req.ID, payload)
			return
		}
		result := map[string]any{
			"content": []map[string]any{{"type": "text", "text": resultText}},
			"isError": false,
		}
		if startupProbe != nil {
			result[startupProbeResultKey] = StartupProbeSuccessResult(startupProbe.Contract, toolName)
		}
		WriteRPCResult(w, req.ID, result)
		return
	case "ping":
		WriteRPCResult(w, req.ID, map[string]any{})
		return
	default:
		WriteRPCError(w, req.ID, -32601, "method not found")
		return
	}
}

func projectToolCallSuccessText(ctx context.Context, executor runtimeGatewayExecutor, toolName string, input any, out any) (string, error) {
	if out == nil {
		return ToolResultText(nil), nil
	}
	raw, err := json.Marshal(out)
	if err != nil {
		if toolIsRoleScopedTypedReadInContext(ctx, toolName) {
			return "", toolresultpolicy.NewTypedReadResultMarshalError("mcp-gateway", "mcp.tools.call.result_project", toolName, err)
		}
		return ToolResultText(out), nil
	}
	if toolIsRoleScopedTypedReadInContext(ctx, toolName) {
		if len(raw) > toolresultpolicy.MaxCompleteTypedReadResultBytes {
			return "", toolresultpolicy.NewTypedReadResultTooLargeError("mcp-gateway", "mcp.tools.call.result_project", toolName, len(raw))
		}
		return ToolResultText(out), nil
	}
	if len(raw) <= toolCallRelayResultLimit(toolName, input) {
		return ToolResultText(out), nil
	}
	if !runtimeReadFileFollowUpAllowedInContext(ctx) {
		return ToolResultText(map[string]any{
			"truncated": true,
			"bytes":     len(raw),
			"preview":   clampRunes(string(raw), maxToolResultPreviewRunes),
		}), nil
	}
	writer, ok := executor.(OversizedToolResultRelayWriter)
	if !ok {
		return ToolResultText(out), nil
	}
	relay, err := writer.PersistOversizedToolResultRelay(ctx, toolName, raw)
	if err != nil {
		return "", err
	}
	followUp := map[string]any{
		"kind":        "runtime_read_file",
		"tool":        relay.ReadTool,
		"format":      relay.Format,
		"visibility":  relay.Visibility,
		"description": "full tool result stored in a runtime-accessible workspace file",
	}
	if len(relay.Chunks) > 0 {
		followUp["kind"] = "runtime_read_file_chunks"
		followUp["chunks"] = relay.Chunks
		followUp["description"] = "full tool result stored across runtime-accessible workspace chunk files; read chunks in order"
	} else {
		followUp["path"] = relay.Path
	}
	return ToolResultText(map[string]any{
		"truncated": true,
		"bytes":     len(raw),
		"preview":   clampRunes(string(raw), maxToolResultPreviewRunes),
		"follow_up": followUp,
	}), nil
}

func toolIsRoleScopedTypedReadInContext(ctx context.Context, name string) bool {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return false
	}
	return toolresultpolicy.IsRoleScopedTypedReadInContext(set, name)
}

func toolCallRelayResultLimit(toolName string, input any) int {
	if toolIsLargeRelayFileRead(toolName) && !toolInputTargetsRuntimeRelayPath(input) {
		return maxToolResultBytes
	}
	if toolIsLargeRelayFileRead(toolName) {
		return maxReadFileResultBytes
	}
	return maxToolResultBytes
}

func toolIsLargeRelayFileRead(name string) bool {
	return toolidentity.CanonicalName(name) == "read_file"
}

func toolInputTargetsRuntimeRelayPath(input any) bool {
	rawPath := strings.TrimSpace(toolInputPath(input))
	if rawPath == "" {
		return false
	}
	cleaned := strings.TrimSpace(rawPath)
	if strings.HasPrefix(cleaned, "/.swarm/tool-results/") || strings.HasPrefix(cleaned, "/workspace/.swarm/tool-results/") {
		return true
	}
	return strings.Contains(cleaned, "/.swarm/tool-results/")
}

func toolInputPath(input any) string {
	switch v := input.(type) {
	case map[string]any:
		if path, ok := v["path"].(string); ok {
			return strings.TrimSpace(path)
		}
	case map[string]string:
		return strings.TrimSpace(v["path"])
	}
	var pathCarrier struct {
		Path string `json:"path"`
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return ""
	}
	if err := json.Unmarshal(raw, &pathCarrier); err != nil {
		return ""
	}
	return strings.TrimSpace(pathCarrier.Path)
}

func clampRunes(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "...(truncated)"
}

func (g *Gateway) mcpExecutionContext(r *http.Request, _ string) (context.Context, error) {
	return g.transportExecutionContext(r, "mcp.context.resolve")
}

func (g *Gateway) toolExecutionContext(r *http.Request, _ string) (context.Context, error) {
	return g.transportExecutionContext(r, "tool.context.resolve")
}

func (g *Gateway) transportExecutionContext(r *http.Request, operation string) (context.Context, error) {
	turn, err := g.runtimeTurnContextForRequest(r, operation)
	if err != nil {
		return nil, err
	}
	return g.contextForResolvedTurn(r.Context(), turn), nil
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

func runtimeReadFileFollowUpAllowedInContext(ctx context.Context) bool {
	cap, ok := toolCapabilityInContext(ctx, "read_file")
	if !ok {
		return false
	}
	return cap.Visible && cap.Callable
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

func emitTurnDedupeKey(toolName string, arguments any) string {
	normalized := strings.TrimSpace(toolName)
	encoded, err := json.Marshal(arguments)
	if err != nil {
		return normalized + "\n" + fmt.Sprintf("%#v", arguments)
	}
	return normalized + "\n" + string(encoded)
}

func (g *Gateway) mcpToolsForRequest(r *http.Request) ([]ToolDef, error) {
	turn, err := g.runtimeTurnContextForRequest(r, "mcp.tools.list.context.resolve")
	if err != nil {
		return nil, err
	}
	return g.mcpToolsForActor(turn.Actor, turn.Allowed, true), nil
}

func (g *Gateway) MCPToolsForActor(actor models.AgentConfig) []ToolDef {
	actor = g.hydrateActor(actor)
	if strings.TrimSpace(actor.ID) == "" {
		return nil
	}
	return g.mcpToolsForActor(actor, nil, true)
}

func (g *Gateway) mcpToolsForActor(actor models.AgentConfig, allowed map[string]struct{}, actorOK bool) []ToolDef {
	catalog := g.toolCatalog(actor, actorOK)
	set, hasSet := g.requestToolCapabilities(actor, actorOK, catalog, allowed)

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
			if delivered := strings.TrimSpace(llm.DeliveredToolDescription(def)); delivered != "" {
				desc = delivered
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

func (g *Gateway) toolCatalog(actor models.AgentConfig, actorOK bool) map[string]llm.ToolDefinition {
	catalog := map[string]llm.ToolDefinition{}
	if !actorOK {
		return catalog
	}
	if g.executor != nil {
		for _, def := range g.executor.ToolDefinitionsForActor(actor) {
			name := normalizeGatewayToolName(def.Name)
			if name != "" {
				def.Name = name
				catalog[name] = def
			}
		}
	}
	if g.hooks.EmitToolsForActor != nil {
		for _, def := range g.hooks.EmitToolsForActor(actor) {
			name := normalizeGatewayToolName(def.Name)
			if name != "" {
				def.Name = name
				catalog[name] = def
			}
		}
	}
	if g.hooks.EmitTools != nil {
		role := strings.TrimSpace(actor.Role)
		if role != "" {
			for _, def := range g.hooks.EmitTools(role) {
				name := normalizeGatewayToolName(def.Name)
				if name != "" {
					def.Name = name
					catalog[name] = def
				}
			}
		}
	}
	return catalog
}

func (g *Gateway) requestToolCapabilities(actor models.AgentConfig, actorOK bool, catalog map[string]llm.ToolDefinition, requestAllowed map[string]struct{}) (toolcapabilities.Set, bool) {
	if g.executor == nil || !actorOK {
		return toolcapabilities.Set{}, false
	}
	return g.executor.ToolCapabilitiesForActor(actor, g.catalogNames(catalog), requestAllowed), true
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
		return g.newRuntimeError(ErrCodeAuthUnconfigured, "mcp.authorize", false, nil, "gateway authorization token is not configured")
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

func (g *Gateway) resolveRuntimeTurnContext(token string) (TurnContext, bool) {
	if g == nil || g.hooks.ResolveTurnContext == nil {
		return TurnContext{}, false
	}
	turn, ok := g.hooks.ResolveTurnContext(strings.TrimSpace(token))
	if !ok {
		return TurnContext{}, false
	}
	turn.Actor = g.hydrateActor(turn.Actor)
	turn.Allowed = copyAllowedTools(turn.Allowed)
	return turn, strings.TrimSpace(turn.Actor.ID) != ""
}

func (g *Gateway) runtimeTurnContextForRequest(r *http.Request, operation string) (TurnContext, error) {
	token := ContextTokenFromRequest(r)
	if token == "" {
		return TurnContext{}, g.newRuntimeError(ErrCodeContextMissing, operation, false, nil, "missing or invalid mcp context token")
	}
	turn, ok := g.resolveRuntimeTurnContext(token)
	if !ok {
		return TurnContext{}, g.newRuntimeError(ErrCodeContextNotFound, operation, false, nil, "missing or invalid mcp context token")
	}
	return turn, nil
}

func (g *Gateway) contextForResolvedTurn(ctx context.Context, turn TurnContext) context.Context {
	if g.hooks.WithActor != nil {
		ctx = g.hooks.WithActor(ctx, turn.Actor)
	}
	if g.hooks.WithCurrentRuntimeEpoch != nil {
		ctx = g.hooks.WithCurrentRuntimeEpoch(ctx)
	}
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(turn.Inbound.RunID))
	if turn.HasInbound && g.hooks.WithInboundEvent != nil {
		ctx = g.hooks.WithInboundEvent(ctx, turn.Inbound)
	}
	if turn.Recorder != nil && g.hooks.WithEmittedEventsRecorder != nil {
		ctx = g.hooks.WithEmittedEventsRecorder(ctx, turn.Recorder)
	}
	return g.withToolCapabilities(ctx, turn.Actor, g.catalogNames(g.toolCatalog(turn.Actor, true)), turn.Allowed)
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
	agentID := ""
	entityID := ""
	if g.hooks.ActorFromContext != nil {
		if actor, ok := g.hooks.ActorFromContext(r.Context()); ok {
			agentID = strings.TrimSpace(actor.ID)
			entityID = strings.TrimSpace(actor.EntityID)
		}
	}
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

func (g *Gateway) runtimeShutdownAdmissionClosed() bool {
	if g == nil || g.hooks.RuntimeShutdownAdmissionClosed == nil {
		return false
	}
	return g.hooks.RuntimeShutdownAdmissionClosed()
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

func (g *Gateway) writeToolCallErrorResult(w http.ResponseWriter, id any, err error) {
	payload := map[string]any{
		"content": []map[string]any{{"type": "text", "text": g.formatError(err)}},
		"isError": true,
	}
	if runtimeErr := RuntimeErrorPayloadFromError(err); runtimeErr != nil {
		payload["runtimeError"] = runtimeErr
	}
	WriteRPCResult(w, id, payload)
}

func (g *Gateway) newRuntimeError(code, operation string, retryable bool, cause error, format string, args ...any) error {
	if g != nil && g.hooks.NewRuntimeError != nil {
		return g.hooks.NewRuntimeError(code, operation, retryable, cause, format, args...)
	}
	return runtimerterr.WrapRuntimeError(code, "mcp-gateway", operation, retryable, cause, format, args...)
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
