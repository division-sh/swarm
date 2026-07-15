package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
)

type GatewayHooks struct {
	RuntimeIngressRequestPaused    func(context.Context) (bool, error)
	RuntimeShutdownAdmissionClosed func() bool
	WithActor                      func(context.Context, models.AgentConfig) context.Context
	ActorFromContext               func(context.Context) (models.AgentConfig, bool)
	ResolveActorConfig             func(string) (models.AgentConfig, bool)
	WithCurrentRuntimeEpoch        func(context.Context) context.Context
	WithInboundEvent               func(context.Context, events.Event) context.Context
	WithEmittedEventsRecorder      func(context.Context, *runtimebus.EmittedEventsRecorder) context.Context
	ResolveTurnContext             func(string) (TurnContext, bool)
	ObserveCapabilityEvidence      func(string, ...managedcapabilities.DeliveryEvidence) (managedcapabilities.Surface, bool)
	ObserveCapabilityMismatch      func(string, ...managedcapabilities.DeliveryMismatch) (managedcapabilities.Surface, bool)
	ObserveMCPProviderCall         func(string, string, string) (managedcapabilities.Surface, error)
	MarkEmitKeyUsed                func(string, string) bool
	EmitToolsForActor              func(models.AgentConfig) []llm.ToolDefinition
	EmitTools                      func(string) []llm.ToolDefinition
	EmitSchemaForTool              func(string) (description string, schema any, ok bool)
	Log                            func(context.Context, string, string, string, string, map[string]any, *failures.Envelope)
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

type runtimeGatewayContextAwareExecutor interface {
	ToolDefinitionsForActorInContext(context.Context, models.AgentConfig) []llm.ToolDefinition
	ToolCapabilitiesForActorInContext(context.Context, models.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
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
	if g.runtimeIngressPaused(r.Context()) {
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
		err := g.newGatewayError(ErrCodeToolNotAllowed, "tool.execute.authorize_tool", nil, map[string]any{"tool": toolName})
		g.logMCP(r, "warn", "tool.execute.denied", err, map[string]any{
			"tool_name":    toolName,
			"denial_layer": "gateway",
		})
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, Error: g.formatError(err)})
		return
	}
	out, err := g.executor.Execute(ctx, toolName, req.Input)
	if err != nil {
		execErr := g.newGatewayError(ErrCodeToolExecFailed, "tool.execute", err, map[string]any{"tool": toolName})
		WriteJSON(w, http.StatusBadRequest, ToolGatewayResponse{OK: false, RuntimeError: RuntimeErrorPayloadFromError(execErr)})
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
		if g.runtimeIngressPaused(r.Context()) {
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
		WriteRPCErrorForRuntimeError(w, nil, -32001, err)
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
			WriteRPCErrorForRuntimeError(w, req.ID, -32003, err)
			return
		}
		WriteRPCResult(w, req.ID, map[string]any{"tools": tools})
		return
	case "tools/call":
		if g.executor == nil {
			err := g.newGatewayError(ErrCodeToolExecFailed, "mcp.tools.call.execute", nil, map[string]any{"dependency": "tool_executor"})
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		toolName := strings.TrimSpace(asString(req.Params["name"]))
		if toolName == "" {
			err := g.newGatewayError(ErrCodeInvalidRequest, "mcp.tools.call", nil, map[string]any{"field": "name"})
			g.logMCP(r, "warn", "mcp.tools.call.invalid", err, map[string]any{
				"method": "tools/call",
			})
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		startupProbe, err := DecodeStartupProbeRequest(req.Params[startupProbeParamKey])
		if err != nil {
			err = g.newGatewayError(ErrCodeInvalidRequest, "mcp.tools.call.startup_probe", err, map[string]any{"field": startupProbeParamKey})
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
		occurrence, err := mcpToolCallOccurrenceCoordinate(ctx, req)
		if err != nil {
			if surface, managed := managedcapabilities.FromContext(ctx); managed && surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn && len(surface.BindingNames(managedcapabilities.BindingMCPProvider)) > 0 {
				mismatch := managedcapabilities.DeliveryMismatch{
					BindingKind: managedcapabilities.BindingMCPProvider,
					ExactName:   toolidentity.RuntimeToolsMCPPrefix + toolidentity.CanonicalName(toolName),
					Kind:        "missing_mcp_provider_call_coordinate",
					Detail:      "managed provider tools/call omitted its provider-owned occurrence coordinate",
				}
				if g.hooks.ObserveCapabilityMismatch == nil {
					err = g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.call.identity", err, map[string]any{"reason": "mismatch_owner_missing"})
					g.writeToolCallErrorResult(w, req.ID, err)
					return
				}
				if _, ok := g.hooks.ObserveCapabilityMismatch(ContextTokenFromRequest(r), mismatch); !ok {
					err = g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.call.identity", err, map[string]any{"reason": "mismatch_settlement_failed"})
					g.writeToolCallErrorResult(w, req.ID, err)
					return
				}
			}
			err = g.newGatewayError(ErrCodeInvalidRequest, "mcp.tools.call.identity", err, nil)
			g.writeToolCallErrorResult(w, req.ID, err)
			return
		}
		logicalSegment := mcpToolCallLogicalIdentitySegmentForOccurrence(occurrence)
		if logicalSegment != "" {
			ctx = runtimeeffects.WithLogicalOperationIdentitySegment(ctx, logicalSegment)
		}
		if surface, managed := managedcapabilities.FromContext(ctx); managed && surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn && len(surface.BindingNames(managedcapabilities.BindingMCPProvider)) > 0 {
			if g.hooks.ObserveMCPProviderCall == nil {
				err = g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.call.provider_visibility", nil, map[string]any{"reason": "evidence_owner_missing"})
				g.writeToolCallErrorResult(w, req.ID, err)
				return
			}
			observed, observeErr := g.hooks.ObserveMCPProviderCall(ContextTokenFromRequest(r), toolName, occurrence)
			if observeErr != nil {
				err = g.newGatewayError(ErrCodeToolNotAllowed, "mcp.tools.call.provider_visibility", observeErr, map[string]any{"tool": toolName, "surface_id": surface.ID})
				g.writeToolCallErrorResult(w, req.ID, err)
				return
			}
			ctx = managedcapabilities.WithContext(ctx, observed)
			ctx = toolcapabilities.WithContext(ctx, observed.CapabilitySet())
		}
		r = r.WithContext(ctx)
		if !toolAllowedInContext(ctx, toolName) {
			err := g.newGatewayError(ErrCodeToolNotAllowed, "mcp.tools.call.authorize_tool", nil, map[string]any{"tool": toolName})
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
			err = g.newGatewayError(ErrCodeToolExecFailed, "mcp.tools.call.execute", execErr, map[string]any{"tool": toolName})
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
						err = g.newGatewayError(ErrCodeInvalidRequest, "mcp.tools.call.startup_probe", outcomeErr, map[string]any{"field": startupProbeParamKey})
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
			err = g.newGatewayError(ErrCodeToolExecFailed, "mcp.tools.call.result_project", err, map[string]any{"tool": toolName})
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
	if contextAware, ok := g.executor.(runtimeGatewayContextAwareExecutor); ok {
		return toolcapabilities.WithContext(ctx, contextAware.ToolCapabilitiesForActorInContext(ctx, actor, names, requestAllowed))
	}
	set := g.executor.ToolCapabilitiesForActor(actor, names, requestAllowed)
	return toolcapabilities.WithContext(ctx, set)
}

func toolAllowedInContext(ctx context.Context, toolName string) bool {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return false
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
	ctx := g.baseContextForResolvedTurn(r.Context(), turn)
	if turn.CapabilitySurface == nil {
		if len(turn.ForkSandboxAllowed) == 0 {
			return nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.forkchat_sandbox", nil, map[string]any{"reason": "sandbox_policy_missing"})
		}
		return g.mcpToolsForActorInContext(ctx, turn.Actor, turn.ForkSandboxAllowed, true), nil
	}
	tools, evidence, mismatches, err := g.mcpToolsForCapabilitySurface(ctx, turn)
	if len(mismatches) > 0 {
		if g.hooks.ObserveCapabilityMismatch == nil {
			return nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_mismatch", nil, map[string]any{"reason": "mismatch_owner_missing"})
		}
		if _, ok := g.hooks.ObserveCapabilityMismatch(ContextTokenFromRequest(r), mismatches...); !ok {
			return nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_mismatch", nil, map[string]any{"reason": "mismatch_settlement_failed"})
		}
	}
	if err != nil {
		return nil, err
	}
	if len(evidence) > 0 {
		if g.hooks.ObserveCapabilityEvidence == nil {
			return nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_evidence", nil, map[string]any{"reason": "evidence_owner_missing"})
		}
		if _, ok := g.hooks.ObserveCapabilityEvidence(ContextTokenFromRequest(r), evidence...); !ok {
			return nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_evidence", nil, map[string]any{"reason": "evidence_settlement_failed"})
		}
	}
	return tools, nil
}

func (g *Gateway) mcpToolsForCapabilitySurface(ctx context.Context, turn TurnContext) ([]ToolDef, []managedcapabilities.DeliveryEvidence, []managedcapabilities.DeliveryMismatch, error) {
	if turn.CapabilitySurface == nil {
		return nil, nil, nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_surface", nil, map[string]any{"reason": "surface_missing"})
	}
	if err := turn.CapabilitySurface.Validate(); err != nil || turn.CapabilitySurface.ActorID != strings.TrimSpace(turn.Actor.ID) {
		return nil, nil, nil, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_surface", err, map[string]any{"reason": "surface_invalid_or_mismatched"})
	}
	catalog := g.toolCatalogInContext(ctx, turn.Actor, true)
	var names []string
	var evidence []managedcapabilities.DeliveryEvidence
	delivered := make(map[string]ToolDef, len(turn.CapabilitySurface.Tools))
	for _, tool := range turn.CapabilitySurface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind != managedcapabilities.BindingMCPTool {
				continue
			}
			definition, ok := catalog[tool.Name]
			if !ok {
				mismatch := managedcapabilities.DeliveryMismatch{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: "missing_mcp_definition", Detail: "planned definition is absent from the live MCP catalog"}
				return nil, nil, []managedcapabilities.DeliveryMismatch{mismatch}, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_surface", nil, map[string]any{"reason": "planned_definition_missing", "tool": tool.Name})
			}
			deliveredDefinition := mcpToolDefinition(tool.Name, definition)
			if actual := llm.ToolDefinitionIdentity(llmToolDefinitionForMCP(deliveredDefinition)); actual != tool.DefinitionHash {
				mismatch := managedcapabilities.DeliveryMismatch{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: "mcp_definition_identity_mismatch", Detail: "live MCP description or schema differs from the planned definition"}
				return nil, nil, []managedcapabilities.DeliveryMismatch{mismatch}, g.newGatewayError(ErrCodeContextNotFound, "mcp.tools.list.capability_surface", nil, map[string]any{"reason": "definition_identity_mismatch", "tool": tool.Name})
			}
			delivered[tool.Name] = deliveredDefinition
			names = append(names, tool.Name)
			if !tool.EffectiveVisible || !tool.EffectiveCallable {
				evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: "mcp_listed", Status: managedcapabilities.EvidenceConfirmed, Detail: "exact turn-context tools/list response"})
			}
		}
	}
	sort.Strings(names)
	out := make([]ToolDef, 0, len(names))
	for _, name := range names {
		out = append(out, delivered[name])
	}
	return out, evidence, nil, nil
}

func mcpToolDefinition(name string, definition llm.ToolDefinition) ToolDef {
	description := strings.TrimSpace(llm.DeliveredToolDescription(definition))
	schema := definition.Schema
	if schema == nil {
		schema = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return ToolDef{Name: name, Description: description, InputSchema: schema}
}

func llmToolDefinitionForMCP(definition ToolDef) llm.ToolDefinition {
	return llm.ToolDefinition{
		Name: definition.Name, Description: definition.Description, Schema: definition.InputSchema,
	}
}

func (g *Gateway) MCPToolsForActor(actor models.AgentConfig) []ToolDef {
	actor = g.hydrateActor(actor)
	if strings.TrimSpace(actor.ID) == "" {
		return nil
	}
	return g.mcpToolsForActor(actor, nil, true)
}

func (g *Gateway) mcpToolsForActor(actor models.AgentConfig, allowed map[string]struct{}, actorOK bool) []ToolDef {
	return g.mcpToolsForActorInContext(context.Background(), actor, allowed, actorOK)
}

func (g *Gateway) mcpToolsForActorInContext(ctx context.Context, actor models.AgentConfig, allowed map[string]struct{}, actorOK bool) []ToolDef {
	catalog := g.toolCatalogInContext(ctx, actor, actorOK)
	set, hasSet := g.requestToolCapabilitiesInContext(ctx, actor, actorOK, catalog, allowed)

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
	return g.toolCatalogInContext(context.Background(), actor, actorOK)
}

func (g *Gateway) toolCatalogInContext(ctx context.Context, actor models.AgentConfig, actorOK bool) map[string]llm.ToolDefinition {
	catalog := map[string]llm.ToolDefinition{}
	if !actorOK {
		return catalog
	}
	if g.executor != nil {
		defs := g.executor.ToolDefinitionsForActor(actor)
		if contextAware, ok := g.executor.(runtimeGatewayContextAwareExecutor); ok {
			defs = contextAware.ToolDefinitionsForActorInContext(ctx, actor)
		}
		for _, def := range defs {
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
	return g.requestToolCapabilitiesInContext(context.Background(), actor, actorOK, catalog, requestAllowed)
}

func (g *Gateway) requestToolCapabilitiesInContext(ctx context.Context, actor models.AgentConfig, actorOK bool, catalog map[string]llm.ToolDefinition, requestAllowed map[string]struct{}) (toolcapabilities.Set, bool) {
	if g.executor == nil || !actorOK {
		return toolcapabilities.Set{}, false
	}
	if contextAware, ok := g.executor.(runtimeGatewayContextAwareExecutor); ok {
		return contextAware.ToolCapabilitiesForActorInContext(ctx, actor, g.catalogNames(catalog), requestAllowed), true
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
		return g.newGatewayError(ErrCodeAuthUnconfigured, "mcp.authorize", nil, nil)
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return g.newGatewayError(ErrCodeAuthMissingBearer, "mcp.authorize", nil, nil)
	}
	const prefix = "bearer "
	if !strings.HasPrefix(strings.ToLower(authz), prefix) {
		return g.newGatewayError(ErrCodeAuthInvalidBearer, "mcp.authorize", nil, map[string]any{"reason": "invalid_header"})
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token != g.authToken {
		return g.newGatewayError(ErrCodeAuthInvalidBearer, "mcp.authorize", nil, map[string]any{"reason": "invalid_token"})
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
	if turn.CapabilitySurface != nil {
		copy := turn.CapabilitySurface.Clone()
		turn.CapabilitySurface = &copy
	}
	return turn, strings.TrimSpace(turn.Actor.ID) != ""
}

func (g *Gateway) runtimeTurnContextForRequest(r *http.Request, operation string) (TurnContext, error) {
	token := ContextTokenFromRequest(r)
	if token == "" {
		return TurnContext{}, g.newGatewayError(ErrCodeContextMissing, operation, nil, nil)
	}
	turn, ok := g.resolveRuntimeTurnContext(token)
	if !ok {
		return TurnContext{}, g.newGatewayError(ErrCodeContextNotFound, operation, nil, nil)
	}
	if turn.CapabilitySurface == nil && len(turn.ForkSandboxAllowed) == 0 {
		return TurnContext{}, g.newGatewayError(ErrCodeContextNotFound, operation, nil, map[string]any{"reason": "capability_surface_missing_or_mismatched"})
	}
	if turn.CapabilitySurface != nil && turn.CapabilitySurface.ActorID != strings.TrimSpace(turn.Actor.ID) {
		return TurnContext{}, g.newGatewayError(ErrCodeContextNotFound, operation, nil, map[string]any{"reason": "capability_surface_missing_or_mismatched"})
	}
	return turn, nil
}

func (g *Gateway) baseContextForResolvedTurn(ctx context.Context, turn TurnContext) context.Context {
	if turn.HasAuthorActivityScope {
		ctx = runtimeauthoractivity.WithScope(ctx, turn.AuthorActivityScope)
	}
	if turn.HasBundleSourceFact {
		ctx = runtimecorrelation.WithBundleSourceFact(ctx, turn.BundleSourceFact)
	}
	if turn.HasExecutionMode {
		ctx = runtimeeffects.WithExecutionMode(ctx, turn.ExecutionMode)
	}
	if turn.HasExecutionAdmission {
		ctx = managedexecution.WithAdmission(ctx, turn.ExecutionAdmission)
	}
	if turn.EffectController != nil {
		ctx = runtimeeffects.WithController(ctx, turn.EffectController)
	}
	if turn.HasEffectAuthority {
		ctx = runtimeeffects.WithAuthority(ctx, turn.EffectAuthority)
		ctx = runtimeeffects.WithExecutionMode(ctx, turn.EffectAuthority.ExecutionMode)
	}
	if turn.HasLifecycleToken {
		ctx = runtimeeffects.WithLifecycleToken(ctx, turn.LifecycleToken)
	} else if turn.DifferentOwner != "" {
		ctx = runtimeeffects.WithDifferentOwner(ctx, turn.DifferentOwner)
	}
	if turn.HasLogicalIdentity {
		ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, turn.LogicalIdentity)
	}
	if g.hooks.WithActor != nil {
		ctx = g.hooks.WithActor(ctx, turn.Actor)
	}
	if g.hooks.WithCurrentRuntimeEpoch != nil {
		ctx = g.hooks.WithCurrentRuntimeEpoch(ctx)
	}
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(turn.Inbound.RunID()))
	if turn.HasRuntimeLineage {
		ctx = runtimecorrelation.WithRuntimeLineage(ctx, turn.RuntimeLineage)
	}
	if turn.HasInbound && g.hooks.WithInboundEvent != nil {
		ctx = g.hooks.WithInboundEvent(ctx, turn.Inbound)
	}
	if turn.Recorder != nil && g.hooks.WithEmittedEventsRecorder != nil {
		ctx = g.hooks.WithEmittedEventsRecorder(ctx, turn.Recorder)
	}
	if turn.CapabilitySurface != nil {
		ctx = managedcapabilities.WithContext(ctx, turn.CapabilitySurface.Clone())
	}
	return ctx
}

func mcpToolCallLogicalIdentitySegment(ctx context.Context, req RPCRequest) (string, error) {
	occurrence, err := mcpToolCallOccurrenceCoordinate(ctx, req)
	if err != nil {
		return "", err
	}
	return mcpToolCallLogicalIdentitySegmentForOccurrence(occurrence), nil
}

func mcpToolCallOccurrenceCoordinate(ctx context.Context, req RPCRequest) (string, error) {
	_, lifecycleManaged := runtimeeffects.LifecycleTokenFromContext(ctx)
	authority, authorityManaged := runtimeeffects.AuthorityFromContext(ctx)
	if !lifecycleManaged && (!authorityManaged || (authority.Kind != runtimeeffects.AuthorityNormalAgent && authority.Kind != runtimeeffects.AuthoritySelectedContractFork && authority.Kind != runtimeeffects.AuthorityStartupProbe)) {
		return "", nil
	}
	meta, ok := req.Params["_meta"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("managed MCP tools/call requires params._meta.%s", claudeCodeToolUseIDMetaKey)
	}
	toolUseID, ok := meta[claudeCodeToolUseIDMetaKey].(string)
	toolUseID = strings.TrimSpace(toolUseID)
	if !ok || toolUseID == "" {
		return "", fmt.Errorf("managed MCP tools/call requires non-empty params._meta.%s", claudeCodeToolUseIDMetaKey)
	}
	return toolUseID, nil
}

func mcpToolCallLogicalIdentitySegmentForOccurrence(occurrence string) string {
	occurrence = strings.TrimSpace(occurrence)
	if occurrence == "" {
		return ""
	}
	return "mcp_tool_call:" + runtimeeffects.Fingerprint([]byte(claudeCodeToolUseIDMetaKey+"\x00"+occurrence))
}

func (g *Gateway) contextForResolvedTurn(ctx context.Context, turn TurnContext) context.Context {
	ctx = g.baseContextForResolvedTurn(ctx, turn)
	if turn.CapabilitySurface == nil {
		names := make([]string, 0, len(turn.ForkSandboxAllowed))
		for name := range turn.ForkSandboxAllowed {
			names = append(names, name)
		}
		return g.withToolCapabilities(ctx, turn.Actor, names, turn.ForkSandboxAllowed)
	}
	return toolcapabilities.WithContext(ctx, turn.CapabilitySurface.CapabilitySet())
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
	var failure *failures.Envelope
	if err != nil {
		if payload := RuntimeErrorPayloadFromError(err); payload != nil {
			switch {
			case payload.Failure != nil:
				failure = failures.CloneEnvelope(payload.Failure)
			case payload.Protocol != nil:
				detail = cloneLogDetail(detail)
				detail["protocol_error"] = *payload.Protocol
			}
		} else {
			normalized := failures.Normalize(err, "mcp-gateway", strings.TrimSpace(action))
			failure = &normalized
		}
	}
	g.hooks.Log(r.Context(), strings.ToLower(strings.TrimSpace(level)), strings.TrimSpace(action), agentID, entityID, detail, failure)
}

func cloneLogDetail(detail map[string]any) map[string]any {
	cloned := make(map[string]any, len(detail)+1)
	for key, value := range detail {
		cloned[key] = value
	}
	return cloned
}

func (g *Gateway) runtimeIngressPaused(ctx context.Context) bool {
	if g == nil {
		return false
	}
	if g.hooks.RuntimeIngressRequestPaused != nil {
		paused, err := g.hooks.RuntimeIngressRequestPaused(ctx)
		return err != nil || paused
	}
	return false
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
	if failure, ok := failures.As(err); ok {
		return failure.Failure.Message
	}
	if payload := RuntimeErrorPayloadFromError(err); payload != nil && payload.Protocol != nil {
		return strings.TrimSpace(payload.Protocol.Message)
	}
	return failures.FromError(err, "mcp-gateway", "format_error").Failure.Message
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

func (g *Gateway) newGatewayError(code, operation string, cause error, detail map[string]any) error {
	code = strings.TrimSpace(code)
	switch code {
	case ErrCodeToolNotAllowed:
		attributes := map[string]any{"action": "tool_execute"}
		for key, value := range detail {
			attributes[key] = value
		}
		return failures.Wrap(failures.ClassAuthorizationDenied, "tool_not_allowed", "mcp-gateway", operation, attributes, cause)
	case ErrCodeToolExecFailed:
		if cause != nil {
			return failures.FromError(cause, "mcp-gateway", operation)
		}
		return failures.NewDetail("dependency_unavailable", "mcp-gateway", operation, detail)
	case ErrCodeInvalidRequest,
		ErrCodeAuthUnconfigured,
		ErrCodeAuthMissingBearer,
		ErrCodeAuthInvalidBearer,
		ErrCodeContextMissing,
		ErrCodeContextNotFound,
		ErrCodeContextStale,
		ErrCodeActorMissing,
		ErrCodeStallDetected:
		return NewProtocolError(code, operation, protocolErrorMessage(code), detail, cause)
	default:
		return NewProtocolError(ErrCodeInvalidRequest, operation, protocolErrorMessage(ErrCodeInvalidRequest), map[string]any{"unknown_code": code}, cause)
	}
}

func protocolErrorMessage(code string) string {
	switch strings.TrimSpace(code) {
	case ErrCodeAuthUnconfigured:
		return "Gateway authorization is not configured."
	case ErrCodeAuthMissingBearer:
		return "Authorization bearer token is required."
	case ErrCodeAuthInvalidBearer:
		return "Authorization bearer token is invalid."
	case ErrCodeContextMissing:
		return "MCP context token is required."
	case ErrCodeContextNotFound, ErrCodeContextStale:
		return "MCP context token is not active."
	case ErrCodeActorMissing:
		return "MCP actor context is required."
	case ErrCodeStallDetected:
		return "MCP request stalled."
	default:
		return "MCP request is invalid."
	}
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
