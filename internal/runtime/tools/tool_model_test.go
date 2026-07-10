package tools

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
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

func TestExecutor_HTTPResponseSuccessPolicyParityCases(t *testing.T) {
	t.Setenv("POLICY_SECRET", "provider-secret")
	tests := []struct {
		name        string
		status      int
		body        string
		policy      runtimecontracts.HTTPResponseSuccess
		credential  bool
		wantFailure bool
		forbidError string
	}{
		{name: "status 2xx", status: http.StatusNoContent, policy: runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}},
		{name: "status non-2xx", status: http.StatusMultipleChoices, body: `{}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}, wantFailure: true},
		{name: "boolean equality", status: http.StatusOK, body: `{"ok":true}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}},
		{name: "string equality", status: http.StatusOK, body: `{"state":"accepted"}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}},
		{name: "numeric equality", status: http.StatusOK, body: `{"count":2}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.count", Equals: int64(2)}},
		{name: "provider failure", status: http.StatusOK, body: `{"ok":false}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}, wantFailure: true},
		{name: "unresolved path", status: http.StatusOK, body: `{"ok":true}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.missing", Equals: true}, wantFailure: true},
		{name: "secret redaction", status: http.StatusOK, body: `{"state":"provider-secret"}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}, credential: true, wantFailure: true, forbidError: "provider-secret"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.credential && r.Header.Get("X-Policy-Secret") != "provider-secret" {
					t.Fatalf("X-Policy-Secret = %q", r.Header.Get("X-Policy-Secret"))
				}
				if tc.body != "" {
					w.Header().Set("Content-Type", "application/json")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()

			tool := runtimecontracts.ToolSchemaEntry{
				Description:     "exercise response success semantics",
				HandlerType:     "http",
				ResponseSuccess: &tc.policy,
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: http.MethodPost,
					URL:    server.URL,
				},
			}
			if tc.credential {
				tool.Credentials = []string{"policy_secret"}
				tool.HTTP.Headers = map[string]string{"X-Policy-Secret": "{{credentials.policy_secret}}"}
			}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"policy_probe": tool}})
			exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
			ctx := models.WithActor(context.Background(), models.AgentConfig{ID: "agent-1", Tools: []string{"policy_probe"}})
			_, err := exec.Execute(ctx, "policy_probe", map[string]any{})
			if !tc.wantFailure {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				return
			}
			requireToolFailure(t, err, runtimefailures.ClassConnectorFailure, "provider_response_rejected")
			if tc.forbidError != "" && strings.Contains(err.Error(), tc.forbidError) {
				t.Fatalf("Execute error leaked %q: %v", tc.forbidError, err)
			}
		})
	}
}

func TestExecutor_HTTPToolEncodesURLTemplateComponentsAndPreservesRawHeaderBody(t *testing.T) {
	query := `to:karpathy (agent OR "agentic")`
	var sawEscapedPath string
	var sawRawQuery string
	var sawHeader string
	var sawBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawEscapedPath = r.URL.EscapedPath()
		sawRawQuery = r.URL.RawQuery
		sawHeader = r.Header.Get("X-Search-Query")
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll body: %v", err)
		}
		if err := json.Unmarshal(rawBody, &sawBody); err != nil {
			t.Fatalf("Unmarshal body %s: %v", rawBody, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"x_search_tweets": {
				Description: "Search X tweets",
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:     "object",
					Required: []string{"segment", "query", "cursor"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"segment": {Type: "string"},
						"query":   {Type: "string"},
						"cursor":  {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "POST",
					URL:    server.URL + "/profiles/{{input.segment}}/search?q={{input.query}}&cursor={{input.cursor}}",
					Headers: map[string]string{
						"X-Search-Query": "raw {{input.query}}",
					},
					Body: map[string]any{
						"query": "{{input.query}}",
					},
				},
			},
		},
	})

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:    "agent-1",
		Tools: []string{"x_search_tweets"},
	})
	if _, err := exec.Execute(ctx, "x_search_tweets", map[string]any{
		"segment": "team/a b",
		"query":   query,
		"cursor":  "page 1",
	}); err != nil {
		t.Fatalf("Execute(x_search_tweets): %v", err)
	}
	if want := "/profiles/team%2Fa%20b/search"; sawEscapedPath != want {
		t.Fatalf("escaped path = %q, want %q", sawEscapedPath, want)
	}
	if want := "q=to%3Akarpathy%20%28agent%20OR%20%22agentic%22%29&cursor=page%201"; sawRawQuery != want {
		t.Fatalf("raw query = %q, want %q", sawRawQuery, want)
	}
	if want := "raw " + query; sawHeader != want {
		t.Fatalf("header = %q, want %q", sawHeader, want)
	}
	if got := sawBody["query"]; got != query {
		t.Fatalf("body query = %#v, want %q", got, query)
	}
}

func TestResolveHTTPURLTemplatePreservesCompleteURL(t *testing.T) {
	want := "https://example.test/search?q=to%3Akarpathy%20%28agent%29"
	got, err := resolveHTTPURLTemplate("{{input.url}}", map[string]any{
		"input": map[string]any{
			"url": want,
		},
	})
	if err != nil {
		t.Fatalf("resolveHTTPURLTemplate: %v", err)
	}
	if got != want {
		t.Fatalf("resolved URL = %q, want %q", got, want)
	}
}

func TestResolveHTTPURLTemplatePreservesURLBaseAndAuthorityPlaceholders(t *testing.T) {
	env := map[string]any{
		"credentials": map[string]any{
			"base_url": "https://api.example.com:8443",
		},
		"input": map[string]any{
			"scheme": "https",
			"host":   "api.example.com:8443",
			"query":  "agentic orchestration",
		},
	}
	for _, tt := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "base URL placeholder with fixed path",
			raw:  "{{credentials.base_url}}/v1/search?q={{input.query}}",
			want: "https://api.example.com:8443/v1/search?q=agentic%20orchestration",
		},
		{
			name: "scheme placeholder",
			raw:  "{{input.scheme}}://api.example.com/v1/search?q={{input.query}}",
			want: "https://api.example.com/v1/search?q=agentic%20orchestration",
		},
		{
			name: "authority placeholder",
			raw:  "https://{{input.host}}/v1/search?q={{input.query}}",
			want: "https://api.example.com:8443/v1/search?q=agentic%20orchestration",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveHTTPURLTemplate(tt.raw, env)
			if err != nil {
				t.Fatalf("resolveHTTPURLTemplate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolved URL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExecutor_CustomWebSearchEncodesHTTPURLTemplateComponents(t *testing.T) {
	query := `to:karpathy (agent OR "agentic")`
	var sawEscapedPath string
	var sawRawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawEscapedPath = r.URL.EscapedPath()
		sawRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"title": "hit", "url": "https://example.test/hit", "snippet": "body"},
			},
		})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})
	results, err := exec.executeCustomWebSearch(context.Background(), webSearchProviderConfig{
		HTTP: &runtimecontracts.HTTPToolSpec{
			Method: "GET",
			URL:    server.URL + "/search/{{input.max_results}}?q={{input.query}}",
		},
		ResponsePath: "results",
		FieldMapping: map[string]string{
			"title":   "title",
			"url":     "url",
			"snippet": "snippet",
		},
	}, query, 20, "")
	if err != nil {
		t.Fatalf("executeCustomWebSearch: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %#v, want one result", results)
	}
	if want := "/search/20"; sawEscapedPath != want {
		t.Fatalf("escaped path = %q, want %q", sawEscapedPath, want)
	}
	if want := "q=to%3Akarpathy%20%28agent%20OR%20%22agentic%22%29"; sawRawQuery != want {
		t.Fatalf("raw query = %q, want %q", sawRawQuery, want)
	}
}

func TestExecutor_HTTPToolUsesImportedPackageCredentialBinding(t *testing.T) {
	var sawAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		if want := "Bearer tenant-secret"; sawAuth != want {
			t.Fatalf("Authorization = %q, want %q", sawAuth, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := loadToolImportDependencySource(t, toolImportDependencyOptions{
		serverURL:      server.URL,
		credentialBind: "        provider_key: tenant_provider_key\n",
	})
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "tenant_provider_key", "tenant-secret"); err != nil {
		t.Fatalf("Set tenant_provider_key: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, Credentials: store})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:    "worker-agent",
		Tools: []string{"send_provider"},
	})

	out, err := exec.Execute(ctx, "send_provider", map[string]any{})
	if err != nil {
		t.Fatalf("Execute(send_provider): %v", err)
	}
	if got := out.(map[string]any)["ok"]; got != true {
		t.Fatalf("result ok = %#v, want true", got)
	}
}

func TestExecutor_HTTPToolUsesImportedPackageCredentialBindingForRenderedActorID(t *testing.T) {
	var sawAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		if want := "Bearer tenant-secret"; sawAuth != want {
			t.Fatalf("Authorization = %q, want %q", sawAuth, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := loadToolImportDependencySource(t, toolImportDependencyOptions{
		serverURL:      server.URL,
		credentialBind: "        provider_key: tenant_provider_key\n",
	})
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "tenant_provider_key", "tenant-secret"); err != nil {
		t.Fatalf("Set tenant_provider_key: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, Credentials: store})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:       "worker-agent-rendered",
		FlowPath: "worker/instance-1",
		Tools:    []string{"send_provider"},
	})

	out, err := exec.Execute(ctx, "send_provider", map[string]any{})
	if err != nil {
		t.Fatalf("Execute(send_provider): %v", err)
	}
	if got := out.(map[string]any)["ok"]; got != true {
		t.Fatalf("result ok = %#v, want true", got)
	}
}

func TestExecutor_HTTPToolFailsClosedWhenImportedCredentialBindingMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("HTTP server should not be called when imported credential binding is missing")
	}))
	defer server.Close()

	source := loadToolImportDependencySource(t, toolImportDependencyOptions{serverURL: server.URL})
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "provider_key", "ambient-secret"); err != nil {
		t.Fatalf("Set provider_key: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, Credentials: store})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:    "worker-agent",
		Tools: []string{"send_provider"},
	})

	_, err = exec.Execute(ctx, "send_provider", map[string]any{})
	requireToolFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "tool_credential_required")
}

func TestExecutor_HTTPToolFailsClosedWhenImportedCredentialRequiresMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("HTTP server should not be called when imported credential dependency is undeclared")
	}))
	defer server.Close()

	source := loadToolImportDependencySource(t, toolImportDependencyOptions{
		serverURL:              server.URL,
		omitCredentialRequires: true,
	})
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "provider_key", "ambient-secret"); err != nil {
		t.Fatalf("Set provider_key: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, Credentials: store})
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:    "worker-agent",
		Tools: []string{"send_provider"},
	})

	_, err = exec.Execute(ctx, "send_provider", map[string]any{})
	requireToolFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "tool_credential_required")
}

func TestExecutor_NativeWebSearchUsesImportedPolicyAndCredentialBinding(t *testing.T) {
	source := loadToolImportDependencySource(t, toolImportDependencyOptions{
		credentialBind: "        provider_key: tenant_provider_key\n",
		policyRequires: `  policy:
    web_search_provider:
      default:
        provider: brave
        credentials_key: provider_key
        max_results_default: 3
`,
	})
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "tenant_provider_key", "native-secret"); err != nil {
		t.Fatalf("Set tenant_provider_key: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, Credentials: store})
	actor := models.AgentConfig{ID: "worker-agent-rendered", FlowPath: "worker/instance-1", NativeTools: models.NativeToolConfig{WebSearch: true}}

	cfg, err := exec.resolveWebSearchProviderConfig(actor)
	if err != nil {
		t.Fatalf("resolveWebSearchProviderConfig: %v", err)
	}
	if cfg.Provider != "brave" || cfg.CredentialsKey != "provider_key" || cfg.MaxResultsDefault != 3 {
		t.Fatalf("web search cfg = %+v, want imported policy default provider_key", cfg)
	}
	creds, err := exec.resolveToolCredentialsForActor(context.Background(), actor, []string{cfg.CredentialsKey})
	if err != nil {
		t.Fatalf("resolveToolCredentialsForActor: %v", err)
	}
	if got := creds["provider_key"]; got != "native-secret" {
		t.Fatalf("provider_key credential = %#v, want native-secret from bound deployment key", got)
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
				"content":           []any{},
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

func TestExecutor_ToolDefinitionsForActor_ExcludesContractMCPWithoutDiscovery(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {ID: "agent-1", Tools: []string{"infra.ping"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"infra.ping": {
				Description: "Authored MCP tool should not create runtime availability",
				HandlerType: "mcp",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type: "object",
				},
			},
		},
	})

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})
	defs := exec.ToolDefinitionsForActor(models.AgentConfig{ID: "agent-1", Tools: []string{"infra.ping"}})

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	if containsToolName(names, "infra.ping") {
		t.Fatalf("did not expect authored handler_type mcp entry without discovery proof to be delivered, got %v", names)
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

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: source,
		ModelRuntime:   nativeCapabilityRuntimeStub{},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})
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

type toolImportDependencyOptions struct {
	serverURL              string
	credentialBind         string
	policyRequires         string
	omitCredentialRequires bool
}

func loadToolImportDependencySource(t *testing.T, opts toolImportDependencyOptions) semanticview.Source {
	t.Helper()
	repoRoot := toolsRepoRootForTest(t)
	root := t.TempDir()
	bindCredentials := ""
	if strings.TrimSpace(opts.credentialBind) != "" {
		bindCredentials = "      credentials:\n" + opts.credentialBind
	}
	policyRequires := opts.policyRequires
	if strings.TrimSpace(policyRequires) == "" {
		policyRequires = "  policy: [web_search_provider]\n"
	}
	credentialRequires := "  credentials: [provider_key]\n"
	if opts.omitCredentialRequires {
		credentialRequires = ""
	}
	serverURL := strings.TrimSpace(opts.serverURL)
	if serverURL == "" {
		serverURL = "https://provider.example.test"
	}
	writeToolFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: tool-import-dependencies
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
`+bindCredentials)
	writeToolFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: tool-import-dependencies\n")
	writeToolFixtureFile(t, filepath.Join(root, "policy.yaml"), `
web_search_provider:
  provider: brave
  credentials_key: provider_key
`)
	writeToolFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeToolFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeToolFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeToolFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeToolFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker-package
version: "1.0.0"
requires:
`+policyRequires+credentialRequires+`
`)
	writeToolFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), "name: worker\nmode: static\n")
	writeToolFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writeToolFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), `
worker-agent:
  id: worker-agent
  role: worker
  mode: task
  model: regular
  tools: [send_provider]
`)
	writeToolFixtureFile(t, filepath.Join(root, "flows", "worker", "tools.yaml"), `
send_provider:
  handler_type: http
  credentials: [provider_key]
  input_schema:
    type: object
  http:
    method: GET
    url: `+serverURL+`
    headers:
      Authorization: Bearer {{credentials.provider_key}}
  response_mapping:
    ok: '{{response.body.ok}}'
`)
	writeToolFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "{}\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func toolsRepoRootForTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func writeToolFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
