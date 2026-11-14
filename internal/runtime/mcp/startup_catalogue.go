package mcp

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

// StartupCatalogue is an immutable list-only observation, never a turn context.
// It holds neither an executor nor an execution/capability authority.
type StartupCatalogue struct {
	tools json.RawMessage
	token string
}

// The delivered definitions must come from the tool owner independently of the
// planned contract. The same exact matcher serves runtime and check discovery.
func NewStartupCatalogue(planned []managedcapabilities.PlannedTool, delivered []llm.ToolDefinition) (*StartupCatalogue, error) {
	if err := managedcapabilities.ValidatePlannedTools(planned); err != nil {
		return nil, err
	}
	if err := validateGatewayDefinitions(delivered); err != nil {
		return nil, err
	}
	tools := make([]managedcapabilities.Tool, len(planned))
	for i, tool := range planned {
		tools[i] = managedcapabilities.Tool{
			Name: tool.Name, DefinitionHash: tool.DefinitionHash,
			Capability: tool.Capability, Bindings: tool.Bindings,
		}
	}
	definitions, _, _, err := (&Gateway{}).mcpToolsForDeclaredDefinitions(tools, delivered)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(map[string]any{"tools": definitions})
	if err != nil {
		return nil, err
	}
	return &StartupCatalogue{tools: raw, token: uuid.NewString()}, nil
}

func (c *StartupCatalogue) Observation(url string) toolgateway.HTTPObservation {
	return toolgateway.HTTPObservation{URL: url, Headers: map[string]string{
		"Authorization": "Bearer " + c.token, contextTokenHeader: c.token,
	}}
}

func (c *StartupCatalogue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/mcp" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+c.token)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var req RPCRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, toolgateway.ObservationBytes)).Decode(&req); err != nil {
		WriteRPCError(w, nil, -32700, "invalid json body")
		return
	}
	switch req.Method {
	case "initialize":
		WriteRPCResult(w, req.ID, map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "tool-gateway", "version": "1.0.0"},
		})
	case "notifications/initialized":
		w.Header().Set("mcp-protocol-version", "2025-03-26")
		w.WriteHeader(http.StatusAccepted)
	case "tools/list":
		if subtle.ConstantTimeCompare([]byte(ContextTokenFromRequest(r)), []byte(c.token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		WriteRPCResult(w, req.ID, c.tools)
	default:
		WriteRPCError(w, req.ID, -32003, "activation observation cannot execute tools")
	}
}
