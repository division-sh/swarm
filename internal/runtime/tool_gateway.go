package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"empireai/internal/models"
)

type ToolGateway struct {
	executor  ToolExecutor
	authToken string
}

func NewToolGateway(executor ToolExecutor, authToken string) *ToolGateway {
	return &ToolGateway{
		executor:  executor,
		authToken: strings.TrimSpace(authToken),
	}
}

func (g *ToolGateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/tools/", g.handleTool)
	mux.HandleFunc("/mcp", g.handleMCP)
	return mux
}

type toolGatewayRequest struct {
	Actor      models.AgentConfig `json:"actor"`
	AgentID    string             `json:"agent_id"`
	AgentRole  string             `json:"agent_role"`
	VerticalID string             `json:"vertical_id"`
	Mode       string             `json:"mode"`
	Input      any                `json:"input"`
}

type toolGatewayResponse struct {
	OK     bool `json:"ok"`
	Result any  `json:"result,omitempty"`
	Error  any  `json:"error,omitempty"`
}

type mcpRPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
	ID      any            `json:"id,omitempty"`
}

type mcpRPCResponse struct {
	JSONRPC string       `json:"jsonrpc"`
	ID      any          `json:"id,omitempty"`
	Result  any          `json:"result,omitempty"`
	Error   *mcpRPCError `json:"error,omitempty"`
}

type mcpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpToolDef struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	InputSchema any    `json:"inputSchema"`
}

func (g *ToolGateway) handleTool(w http.ResponseWriter, r *http.Request) {
	if RuntimeIngressPaused() {
		writeToolGatewayJSON(w, http.StatusServiceUnavailable, toolGatewayResponse{OK: false, Error: "runtime reset in progress"})
		return
	}
	if r.Method != http.MethodPost {
		writeToolGatewayJSON(w, http.StatusMethodNotAllowed, toolGatewayResponse{OK: false, Error: "method not allowed"})
		return
	}
	if err := g.authorize(r); err != nil {
		writeToolGatewayJSON(w, http.StatusUnauthorized, toolGatewayResponse{OK: false, Error: err.Error()})
		return
	}
	toolName := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/tools/"))
	if toolName == "" {
		writeToolGatewayJSON(w, http.StatusBadRequest, toolGatewayResponse{OK: false, Error: "tool name is required"})
		return
	}
	if g.executor == nil {
		writeToolGatewayJSON(w, http.StatusServiceUnavailable, toolGatewayResponse{OK: false, Error: "tool executor unavailable"})
		return
	}

	var req toolGatewayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeToolGatewayJSON(w, http.StatusBadRequest, toolGatewayResponse{OK: false, Error: "invalid json body"})
		return
	}
	actor := req.Actor
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = strings.TrimSpace(req.AgentID)
		actor.Role = strings.TrimSpace(req.AgentRole)
		actor.VerticalID = strings.TrimSpace(req.VerticalID)
		actor.Mode = strings.TrimSpace(req.Mode)
	}
	if strings.TrimSpace(actor.ID) == "" {
		writeToolGatewayJSON(w, http.StatusBadRequest, toolGatewayResponse{OK: false, Error: "actor id is required"})
		return
	}
	if strings.TrimSpace(actor.Mode) == "" {
		actor.Mode = "operating"
	}

	ctx := WithActor(r.Context(), actor)
	out, err := g.executor.Execute(ctx, toolName, req.Input)
	if err != nil {
		writeToolGatewayJSON(w, http.StatusBadRequest, toolGatewayResponse{OK: false, Error: err.Error()})
		return
	}
	writeToolGatewayJSON(w, http.StatusOK, toolGatewayResponse{
		OK:     true,
		Result: out,
	})
}

func (g *ToolGateway) handleMCP(w http.ResponseWriter, r *http.Request) {
	if RuntimeIngressPaused() {
		g.writeMCPError(w, nil, -32002, "runtime reset in progress")
		return
	}
	switch r.Method {
	case http.MethodGet:
		// Claude CLI probes liveness with GET between JSON-RPC calls.
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
		g.writeMCPError(w, nil, -32001, err.Error())
		return
	}

	var req mcpRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.writeMCPError(w, nil, -32700, "invalid json body")
		return
	}

	switch strings.TrimSpace(req.Method) {
	case "initialize":
		g.writeMCPResult(w, req.ID, map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    "empire-tool-gateway",
				"version": "1.0.0",
			},
		})
		return
	case "notifications/initialized":
		w.Header().Set("mcp-protocol-version", "2025-03-26")
		w.WriteHeader(http.StatusAccepted)
		return
	case "tools/list":
		tools := g.mcpToolsForRequest(r)
		g.writeMCPResult(w, req.ID, map[string]any{"tools": tools})
		return
	case "tools/call":
		if g.executor == nil {
			g.writeMCPResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "tool executor unavailable"}},
				"isError": true,
			})
			return
		}
		toolName := strings.TrimSpace(asString(req.Params["name"]))
		if toolName == "" {
			g.writeMCPResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "tool name is required"}},
				"isError": true,
			})
			return
		}
		allowed := parseToolListHeader(r.Header.Get("X-Empire-Allowed-Tools"))
		if len(allowed) > 0 {
			if _, ok := allowed[toolName]; !ok {
				g.writeMCPResult(w, req.ID, map[string]any{
					"content": []map[string]any{{"type": "text", "text": "tool is not allowed for this agent"}},
					"isError": true,
				})
				return
			}
		}
		input := req.Params["arguments"]
		ctx, err := g.mcpExecutionContext(r)
		if err != nil {
			g.writeMCPResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": err.Error()}},
				"isError": true,
			})
			return
		}
		out, execErr := g.executor.Execute(ctx, toolName, input)
		if execErr != nil {
			g.writeMCPResult(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": execErr.Error()}},
				"isError": true,
			})
			return
		}
		resultText := toolResultText(out)
		g.writeMCPResult(w, req.ID, map[string]any{
			"content": []map[string]any{{"type": "text", "text": resultText}},
			"isError": false,
		})
		return
	case "ping":
		g.writeMCPResult(w, req.ID, map[string]any{})
		return
	default:
		g.writeMCPError(w, req.ID, -32601, "method not found")
		return
	}
}

func (g *ToolGateway) mcpExecutionContext(r *http.Request) (context.Context, error) {
	if token := strings.TrimSpace(r.Header.Get("X-Empire-Context-Token")); token != "" {
		if turn, ok := resolveMCPTurnContext(token); ok {
			if !IsCurrentRuntimeEpoch(turn.Epoch) {
				return nil, errors.New("stale mcp context token")
			}
			ctx := context.Background()
			ctx = WithActor(ctx, turn.Actor)
			ctx = WithRuntimeEpoch(ctx, turn.Epoch)
			if turn.HasInbound {
				ctx = WithInboundEvent(ctx, turn.Inbound)
			}
			if turn.Recorder != nil {
				ctx = WithEmittedEventsRecorder(ctx, turn.Recorder)
			}
			return ctx, nil
		}
	}
	if requireMCPContextToken() {
		return nil, errors.New("missing or invalid mcp context token")
	}
	actor := models.AgentConfig{
		ID:         strings.TrimSpace(r.Header.Get("X-Empire-Agent-Id")),
		Role:       strings.TrimSpace(r.Header.Get("X-Empire-Agent-Role")),
		VerticalID: strings.TrimSpace(r.Header.Get("X-Empire-Vertical-Id")),
		Mode:       strings.TrimSpace(r.Header.Get("X-Empire-Agent-Mode")),
	}
	if actor.ID == "" {
		return nil, errors.New("missing actor id for mcp tool execution")
	}
	if actor.Mode == "" {
		actor.Mode = "operating"
	}
	ctx := WithActor(context.Background(), actor)
	ctx = WithCurrentRuntimeEpoch(ctx)
	return ctx, nil
}

func requireMCPContextToken() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("EMPIREAI_MCP_REQUIRE_CONTEXT_TOKEN")))
	if v == "" {
		return true
	}
	return v == "1" || v == "true" || v == "yes"
}

func (g *ToolGateway) mcpToolsForRequest(r *http.Request) []mcpToolDef {
	allowed := parseToolListHeader(r.Header.Get("X-Empire-Allowed-Tools"))
	catalog := map[string]ToolDefinition{}
	if provider, ok := g.executor.(interface{ ToolDefinitions() []ToolDefinition }); ok {
		for _, def := range provider.ToolDefinitions() {
			name := strings.TrimSpace(def.Name)
			if name == "" {
				continue
			}
			catalog[name] = def
		}
	}
	// Include role-scoped emit tools with full schemas so MCP constrained decoding
	// can use the same contract as runtime emit validation.
	role := strings.TrimSpace(r.Header.Get("X-Empire-Agent-Role"))
	if role != "" {
		for _, def := range GenerateEmitTools(role) {
			name := strings.TrimSpace(def.Name)
			if name == "" {
				continue
			}
			catalog[name] = def
		}
	}

	names := make([]string, 0, len(allowed))
	if len(allowed) > 0 {
		for name := range allowed {
			names = append(names, name)
		}
	} else {
		for name := range catalog {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]mcpToolDef, 0, len(names))
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
		} else if strings.HasPrefix(name, "emit_") {
			if evtType, mapped := eventTypeFromEmitToolName(name); mapped {
				evtSchema := schemaForEventType(evtType)
				if strings.TrimSpace(evtSchema.Description) != "" {
					desc = strings.TrimSpace(evtSchema.Description)
				} else {
					desc = "Emit event tool"
				}
				if evtSchema.Schema != nil {
					schema = evtSchema.Schema
				}
			} else {
				desc = "Emit event tool"
			}
		}
		out = append(out, mcpToolDef{
			Name:        name,
			Description: desc,
			InputSchema: schema,
		})
	}
	return out
}

func (g *ToolGateway) writeMCPResult(w http.ResponseWriter, id any, result any) {
	resp := mcpRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
	raw, _ := json.Marshal(resp)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("mcp-protocol-version", "2025-03-26")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (g *ToolGateway) writeMCPError(w http.ResponseWriter, id any, code int, message string) {
	resp := mcpRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &mcpRPCError{
			Code:    code,
			Message: strings.TrimSpace(message),
		},
	}
	if strings.TrimSpace(resp.Error.Message) == "" {
		resp.Error.Message = "mcp error"
	}
	raw, _ := json.Marshal(resp)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("mcp-protocol-version", "2025-03-26")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func parseToolListHeader(raw string) map[string]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := map[string]struct{}{}
	for _, p := range strings.Split(raw, ",") {
		name := strings.TrimSpace(p)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func toolResultText(v any) string {
	switch t := v.(type) {
	case nil:
		return "ok"
	case string:
		if strings.TrimSpace(t) == "" {
			return "ok"
		}
		return t
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(raw)
	}
}

func (g *ToolGateway) authorize(r *http.Request) error {
	if g.authToken == "" {
		return nil
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return errors.New("missing authorization bearer token")
	}
	const prefix = "bearer "
	if !strings.HasPrefix(strings.ToLower(authz), prefix) {
		return errors.New("invalid authorization header")
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token != g.authToken {
		return errors.New("invalid token")
	}
	return nil
}

func writeToolGatewayJSON(w http.ResponseWriter, status int, payload toolGatewayResponse) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
