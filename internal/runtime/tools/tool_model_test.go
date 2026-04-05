package tools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

func TestExecutor_HTTPToolExecutesTemplateAndResponseMapping(t *testing.T) {
	t.Setenv("TEST_HTTP_API_KEY", "secret-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "Bearer secret-token"; r.Header.Get("Authorization") != want {
			t.Fatalf("Authorization = %q, want %q", r.Header.Get("Authorization"), want)
		}
		if got := r.URL.Query().Get("domain"); got != "example.com" {
			t.Fatalf("domain query = %q, want example.com", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"available": true,
			"provider":  "test",
		})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": {
				Description: "Check domain availability",
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:     "object",
					Required: []string{"domain"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"domain": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    server.URL + "?domain={{input.domain}}",
					Headers: map[string]string{
						"Authorization": "Bearer {{credentials.test_http_api_key}}",
					},
				},
				ResponseMapping: map[string]any{
					"available": "{{response.body.available}}",
					"status":    "{{response.status}}",
				},
				Credentials: []string{"test_http_api_key"},
			},
		},
	})

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:    "agent-1",
		Tools: []string{"check_domain"},
	})
	out, err := exec.Execute(ctx, "check_domain", map[string]any{"domain": "example.com"})
	if err != nil {
		t.Fatalf("Execute(check_domain): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", out)
	}
	if got, ok := result["available"].(bool); !ok || !got {
		t.Fatalf("available = %#v, want true", result["available"])
	}
	if got, ok := result["status"].(int); !ok || got != 200 {
		t.Fatalf("status = %#v, want 200", result["status"])
	}
}

func TestExecutor_MCPToolExecutesDiscoveredServerTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			writeMCPResult(t, w, req["id"], map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
			})
		case "notifications/initialized":
			writeMCPResult(t, w, nil, map[string]any{})
		case "tools/list":
			writeMCPResult(t, w, req["id"], map[string]any{
				"tools": []map[string]any{{
					"name":        "ping",
					"description": "Ping the infra sidecar",
					"inputSchema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"target": map[string]any{"type": "string"},
						},
					},
				}},
			})
		case "tools/call":
			writeMCPResult(t, w, req["id"], map[string]any{
				"structuredContent": map[string]any{"ok": true},
			})
		default:
			t.Fatalf("unexpected mcp method %q", method)
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:    "agent-1",
		Tools: []string{"infra.ping"},
	})
	out, err := exec.Execute(ctx, "infra.ping", map[string]any{"target": "svc"})
	if err != nil {
		t.Fatalf("Execute(infra.ping): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok || result["ok"] != true {
		t.Fatalf("result = %#v, want ok=true", out)
	}
}

func TestExecutor_ToolDefinitionsForActor_UsesSharedActorRegistry(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": {
				Description: "Check domain availability",
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type: "object",
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"domain": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    "https://example.test",
				},
			},
		},
	})

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
	defs := exec.ToolDefinitionsForActor(models.AgentConfig{
		ID:          "agent-1",
		Tools:       []string{"check_domain"},
		NativeTools: models.NativeToolConfig{FileIO: true},
	})

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	if !containsToolName(names, "check_domain") {
		t.Fatalf("expected actor registry to include configured contract tool, got %v", names)
	}
	if !containsToolName(names, "read_file") || !containsToolName(names, "write_file") {
		t.Fatalf("expected actor registry to include enabled native file tools, got %v", names)
	}
}

func containsToolName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func TestValidateToolImplementations_RejectsMalformedHTTPTool(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"bad_http": {
				HandlerType: "http",
				HTTP:        &runtimecontracts.HTTPToolSpec{Method: "GET"},
			},
		},
	})
	_, err := ValidateToolImplementations(source)
	if err == nil {
		t.Fatal("expected malformed http tool to fail validation")
	}
}

func TestValidateToolImplementations_AcceptsDeprecatedHandlerWithoutHTTPAsWarning(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"legacy_call": {
				HandlerType: "api_call",
			},
		},
	})

	warnings, err := ValidateToolImplementations(source)
	if err != nil {
		t.Fatalf("ValidateToolImplementations: %v", err)
	}
	if len(warnings) == 0 {
		t.Fatal("expected deprecated handler warning")
	}
}

func TestContractDefinitionsForSource_DoesNotExposeRemovedInfraBuiltins(t *testing.T) {
	defs, err := ContractDefinitionsForSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	for _, def := range defs {
		switch def.Name {
		case "nginx_reload", "systemd_control", "certbot_execute":
			t.Fatalf("unexpected infra builtin still exposed: %s", def.Name)
		}
	}
}

func writeMCPResult(t *testing.T, w http.ResponseWriter, id any, result any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func mustToolConfigJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return raw
}
