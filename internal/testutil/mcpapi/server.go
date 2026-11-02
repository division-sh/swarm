// Package mcpapi supplies the concrete service double used by MCP client journeys.
package mcpapi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
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

type request struct {
	ID     any    `json:"id"`
	Method string `json:"method"`
	Params struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	} `json:"params"`
}

// New shares the existing client discovery and authenticated-call handlers.
func New(t testing.TB, credential string, receipt ...string) *Server {
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
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("invalid MCP request JSON: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.calls = append(s.calls, Call{Method: req.Method, Tool: req.Params.Name, Arguments: req.Params.Arguments, Auth: r.Header.Get("Authorization")})
		s.mu.Unlock()
		value := ""
		if len(receipt) != 0 {
			value = receipt[0]
		}
		body, status := response(req, value)
		if status != http.StatusOK {
			if status >= 400 {
				t.Errorf("unexpected MCP method/tool %s/%s", req.Method, req.Params.Name)
			}
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(s.Close)
	return s
}

// ServeStdio exposes only the same four fixed methods, with JSON-only stdout.
func ServeStdio(input io.Reader, output io.Writer, receipt string, record func(Call) error) error {
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return err
		}
		if err := record(Call{Method: req.Method, Tool: req.Params.Name, Arguments: req.Params.Arguments}); err != nil {
			return err
		}
		body, status := response(req, receipt)
		if status == http.StatusAccepted {
			continue
		}
		if status != http.StatusOK {
			return fmt.Errorf("unexpected MCP method/tool %s/%s", req.Method, req.Params.Name)
		}
		if err := json.NewEncoder(output).Encode(body); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func response(req request, receipt string) (any, int) {
	var result any
	switch req.Method {
	case "initialize":
		result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}}
	case "notifications/initialized":
		return nil, http.StatusAccepted
	case "tools/list":
		result = map[string]any{"tools": []any{map[string]any{
			"name": "ping", "description": "Return the supplied synthetic text",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}, "required": []string{"text"}, "additionalProperties": false},
		}}}
	case "tools/call":
		if req.Params.Name != "ping" {
			return nil, http.StatusBadRequest
		}
		value := map[string]any{"ok": true, "echo": req.Params.Arguments["text"]}
		if receipt != "" {
			value["receipt"] = receipt
		}
		result = map[string]any{"content": []any{}, "structuredContent": value}
	default:
		return nil, http.StatusForbidden
	}
	return map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}, http.StatusOK
}

func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}
