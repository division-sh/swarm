// Package mcpapi supplies the concrete HTTP boundary used by MCP client journeys.
package mcpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type Call struct {
	Method    string
	Tool      string
	Arguments map[string]any
	Auth      string
}

type Server struct {
	*httptest.Server
	mu    sync.Mutex
	calls []Call
}

// New shares the existing client discovery and authenticated-call handlers.
func New(t testing.TB, credential string) *Server {
	t.Helper()
	s := &Server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/" {
			t.Errorf("unexpected MCP endpoint: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if credential != "" && r.Header.Get("Authorization") != "Bearer "+credential {
			t.Error("MCP request has unexpected fixture credential")
			http.Error(w, "fixture credential required", http.StatusUnauthorized)
			return
		}
		var request struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("invalid MCP request JSON: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.calls = append(s.calls, Call{Method: request.Method, Tool: request.Params.Name, Arguments: request.Params.Arguments, Auth: r.Header.Get("Authorization")})
		s.mu.Unlock()
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}}
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
			return
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{
				"name": "ping", "description": "Return the supplied synthetic text",
				"inputSchema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}, "required": []string{"text"}, "additionalProperties": false},
			}}}
		case "tools/call":
			if request.Params.Name != "ping" {
				t.Error("unexpected MCP tool name")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			result = map[string]any{"content": []any{}, "structuredContent": map[string]any{"ok": true, "echo": request.Params.Arguments["text"]}}
		default:
			t.Errorf("unexpected MCP method %s", request.Method)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}
