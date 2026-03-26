package mcp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	models "swarm/internal/runtime/core/actors"
)

const (
	actorIDHeader      = "X-MAS-Agent-Id"
	actorRoleHeader    = "X-MAS-Agent-Role"
	actorModeHeader    = "X-MAS-Agent-Mode"
	entityIDHeader     = "X-MAS-Entity-Id"
	allowedToolsHeader = "X-MAS-Allowed-Tools"
	contextTokenHeader = "X-MAS-Context-Token"
	traceIDHeader      = "X-MAS-Trace-Id"

	actorIDQuery      = "agent_id"
	actorRoleQuery    = "agent_role"
	actorModeQuery    = "agent_mode"
	entityIDQuery     = "entity_id"
	allowedToolsQuery = "allowed_tools"
	contextTokenQuery = "ctx_token"
	traceIDQuery      = "trace_id"
)

type ToolGatewayRequest struct {
	Actor     models.AgentConfig `json:"actor"`
	AgentID   string             `json:"agent_id"`
	AgentRole string             `json:"agent_role"`
	EntityID  string             `json:"entity_id"`
	Mode      string             `json:"mode"`
	Input     any                `json:"input"`
}

func (r ToolGatewayRequest) EffectiveEntityID() string { return strings.TrimSpace(r.EntityID) }

func (r *ToolGatewayRequest) NormalizeEntityID() {
	if r == nil {
		return
	}
	entityID := r.EffectiveEntityID()
	r.EntityID = entityID
}

type ToolGatewayResponse struct {
	OK     bool `json:"ok"`
	Result any  `json:"result,omitempty"`
	Error  any  `json:"error,omitempty"`
}

type RPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
	ID      any            `json:"id,omitempty"`
}

type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ToolDef struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	InputSchema any    `json:"inputSchema"`
}

func RequireContextToken() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("SWARM_MCP_REQUIRE_CONTEXT_TOKEN")))
	if v == "" {
		return true
	}
	return v == "1" || v == "true" || v == "yes"
}

func AllowContextFallbackOnMiss() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("SWARM_MCP_CONTEXT_FALLBACK_ON_MISS")))
	if v == "" {
		return true
	}
	return v == "1" || v == "true" || v == "yes"
}

func ActorFromRequest(r *http.Request) (models.AgentConfig, bool) {
	if r == nil {
		return models.AgentConfig{}, false
	}
	actor := models.AgentConfig{
		ID:   FirstNonEmpty(strings.TrimSpace(r.Header.Get(actorIDHeader)), strings.TrimSpace(r.URL.Query().Get(actorIDQuery))),
		Role: FirstNonEmpty(strings.TrimSpace(r.Header.Get(actorRoleHeader)), strings.TrimSpace(r.URL.Query().Get(actorRoleQuery))),
		EntityID: FirstNonEmpty(
			strings.TrimSpace(r.Header.Get(entityIDHeader)),
			strings.TrimSpace(r.URL.Query().Get(entityIDQuery)),
		),
		Mode: FirstNonEmpty(strings.TrimSpace(r.Header.Get(actorModeHeader)), strings.TrimSpace(r.URL.Query().Get(actorModeQuery))),
	}
	actor.NormalizeEntityID()
	if strings.TrimSpace(actor.ID) == "" {
		return models.AgentConfig{}, false
	}
	return actor, true
}

func ParseToolListHeader(raw string) map[string]struct{} {
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

func ParseAllowedToolsFromRequest(r *http.Request) map[string]struct{} {
	allowed := ParseToolListHeader(strings.TrimSpace(r.Header.Get(allowedToolsHeader)))
	if len(allowed) > 0 {
		return allowed
	}
	return ParseToolListHeader(strings.TrimSpace(r.URL.Query().Get(allowedToolsQuery)))
}

func ContextTokenFromRequest(r *http.Request) string {
	if token := strings.TrimSpace(r.Header.Get(contextTokenHeader)); token != "" {
		return token
	}
	return strings.TrimSpace(r.URL.Query().Get(contextTokenQuery))
}

func ToolResultText(v any) string {
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

func TraceIDFromRequest(r *http.Request) string {
	return FirstNonEmpty(
		strings.TrimSpace(r.Header.Get(traceIDHeader)),
		strings.TrimSpace(r.URL.Query().Get(traceIDQuery)),
		strings.TrimSpace(r.URL.Query().Get(contextTokenQuery)),
	)
}

func WriteJSON(w http.ResponseWriter, status int, payload ToolGatewayResponse) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func WriteRPCResult(w http.ResponseWriter, id any, result any) {
	resp := RPCResponse{
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

func WriteRPCError(w http.ResponseWriter, id any, code int, message string) {
	resp := RPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &RPCError{
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

func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
