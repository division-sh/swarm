package tools

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestExecutorHTTPToolNeverRefreshesAndRedispatchesAfterUnauthorized(t *testing.T) {
	ctx := unmanagedToolTestContext()
	var apiCalls atomic.Int32
	var tokenCalls atomic.Int32
	var sawAuth []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			tokenCalls.Add(1)
			if err := r.ParseForm(); err != nil {
				t.Fatalf("ParseForm: %v", err)
			}
			if got := r.Form.Get("grant_type"); got != "refresh_token" {
				t.Fatalf("refresh grant_type = %q, want refresh_token", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token":  "refreshed-token",
				"refresh_token": "refresh-secret",
				"expires_in":    3600,
				"scope":         "repo.read",
			})
		case "/api":
			sawAuth = append(sawAuth, r.Header.Get("Authorization"))
			if apiCalls.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "expired old-token"})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "github",
		Provider:     "github",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		TokenURL:     server.URL + "/token",
		ClientID:     "client-id",
		AccessToken:  "old-token",
		RefreshToken: "refresh-secret",
		Scopes:       []string{"repo.read"},
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(time.Hour),
	})
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource:     managedCredentialSource(server.URL+"/api", "github", []string{"repo.read"}),
		ManagedCredentials: store,
	})
	if _, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{}); err == nil {
		t.Fatal("Execute(send_provider) succeeded after unauthorized target response")
	}
	if apiCalls.Load() != 1 || tokenCalls.Load() != 0 {
		t.Fatalf("api/token calls = (%d, %d), want (1, 0)", apiCalls.Load(), tokenCalls.Load())
	}
	if strings.Join(sawAuth, ",") != "Bearer old-token" {
		t.Fatalf("Authorization sequence = %v, want old token exactly once", sawAuth)
	}
}

func TestValidateToolImplementationsRejectsMalformedManagedCredentialReferences(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"bad_http": {
				HandlerType:       "http",
				HTTP:              &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://provider.example.test"},
				ManagedCredential: &runtimecontracts.ManagedCredentialRef{},
			},
		},
	})
	if _, err := ValidateToolImplementations(source); err == nil || !strings.Contains(err.Error(), "managed_credential.key is required") {
		t.Fatalf("ValidateToolImplementations err = %v, want managed_credential.key failure", err)
	}

	source = semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"bad_mcp": {
				HandlerType:       "mcp",
				ManagedCredential: &runtimecontracts.ManagedCredentialRef{Key: "github"},
			},
		},
	})
	if _, err := ValidateToolImplementations(source); err == nil || !strings.Contains(err.Error(), "managed_credential is only supported") {
		t.Fatalf("ValidateToolImplementations err = %v, want HTTP-only managed credential failure", err)
	}
}

func TestExecutorHTTPToolRejectsInstallationIDInputOutsideActivityPath(t *testing.T) {
	ctx := unmanagedToolTestContext()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("HTTP server should not be called when installation_id_input is used outside activity execution")
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"send_provider": {
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "POST",
					URL:    server.URL,
				},
				ManagedCredential: &runtimecontracts.ManagedCredentialRef{
					Key:                 "github_app",
					GrantType:           runtimemanagedcredentials.GrantGitHubAppInstallation,
					GrantModel:          managedcredentialmodel.GrantModelInstallation,
					InstallationIDInput: "installation_id",
				},
			},
		},
	})
	if _, err := ValidateToolImplementations(source); err != nil {
		t.Fatalf("ValidateToolImplementations: %v", err)
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: source,
		ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:            "github_app",
			Provider:       "github",
			GrantType:      runtimemanagedcredentials.GrantGitHubAppInstallation,
			GrantModel:     managedcredentialmodel.GrantModelInstallation,
			InstallationID: "1001",
			AccessToken:    "github-install-token",
			Status:         runtimemanagedcredentials.StatusConnected,
			ExpiresAt:      time.Now().Add(time.Hour),
		}),
	})
	_, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{"installation_id": "1001"})
	failure := requireToolFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "managed_credential_required")
	if failure.Operation != "resolve_managed_credential" {
		t.Fatalf("failure operation = %q, want resolve_managed_credential", failure.Operation)
	}
}

func TestExecutorHTTPToolRefreshesManagedCredentialBeforeUse(t *testing.T) {
	ctx := unmanagedToolTestContext()
	var sawAuth string
	var tokenCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "preflight-refresh-token",
				"expires_in":   3600,
				"scope":        "repo.read",
			})
		case "/api":
			sawAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "github",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		TokenURL:     server.URL + "/token",
		ClientID:     "client-id",
		AccessToken:  "expired-token",
		RefreshToken: "refresh-secret",
		Scopes:       []string{"repo.read"},
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(-time.Minute),
	})
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource:     managedCredentialSource(server.URL+"/api", "github", []string{"repo.read"}),
		ManagedCredentials: store,
	})
	if _, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{}); err != nil {
		t.Fatalf("Execute(send_provider): %v", err)
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls.Load())
	}
	if sawAuth != "Bearer preflight-refresh-token" {
		t.Fatalf("Authorization = %q, want refreshed token", sawAuth)
	}
}

func TestExecutorHTTPToolManagedCredentialFailuresAreFailClosedAndRedacted(t *testing.T) {
	ctx := unmanagedToolTestContext()
	serverCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalled = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "provider echoed access-secret refresh-secret client-secret",
		})
	}))
	defer server.Close()

	t.Run("missing", func(t *testing.T) {
		serverCalled = false
		exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
			WorkflowSource:     managedCredentialSource(server.URL, "missing", nil),
			ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(),
		})
		_, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{})
		requireToolFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "managed_credential_required")
		if serverCalled {
			t.Fatal("HTTP server should not be called for missing managed credential")
		}
	})

	t.Run("scope insufficient", func(t *testing.T) {
		serverCalled = false
		store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:         "github",
			GrantType:   runtimemanagedcredentials.GrantClientCredentials,
			AccessToken: "access-secret",
			Scopes:      []string{"repo.read"},
			Status:      runtimemanagedcredentials.StatusConnected,
		})
		exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
			WorkflowSource:     managedCredentialSource(server.URL, "github", []string{"repo.write"}),
			ManagedCredentials: store,
		})
		_, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{})
		requireToolFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "managed_credential_required")
		if serverCalled {
			t.Fatal("HTTP server should not be called for scope-insufficient managed credential")
		}
	})

	t.Run("provider error redacted", func(t *testing.T) {
		store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:          "github",
			GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
			AccessToken:  "access-secret",
			RefreshToken: "refresh-secret",
			ClientSecret: "client-secret",
			Status:       runtimemanagedcredentials.StatusConnected,
		})
		exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
			WorkflowSource:     managedCredentialSource(server.URL, "github", nil),
			ManagedCredentials: store,
		})
		_, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{})
		if err == nil {
			t.Fatal("Execute err = nil, want provider error")
		}
		for _, secret := range []string{"access-secret", "refresh-secret", "client-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaked %s: %v", secret, err)
			}
		}
	})
}

func TestExecutorHTTPToolUsesImportedManagedCredentialBinding(t *testing.T) {
	ctx := unmanagedToolTestContext()
	var sawAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := loadManagedCredentialImportSource(t, server.URL, "        provider_oauth: tenant_oauth\n", false)
	store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:         "tenant_oauth",
		GrantType:   runtimemanagedcredentials.GrantClientCredentials,
		AccessToken: "tenant-token",
		Scopes:      []string{"repo.read"},
		Status:      runtimemanagedcredentials.StatusConnected,
	})
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, ManagedCredentials: store})
	if _, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{}); err != nil {
		t.Fatalf("Execute(send_provider): %v", err)
	}
	if sawAuth != "Bearer tenant-token" {
		t.Fatalf("Authorization = %q, want bound tenant token", sawAuth)
	}
}

func TestExecutorHTTPToolRejectsAmbientManagedCredentialFallback(t *testing.T) {
	ctx := unmanagedToolTestContext()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("HTTP server should not be called when managed credential binding is missing")
	}))
	defer server.Close()

	source := loadManagedCredentialImportSource(t, server.URL, "", false)
	store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:         "provider_oauth",
		GrantType:   runtimemanagedcredentials.GrantClientCredentials,
		AccessToken: "ambient-token",
		Status:      runtimemanagedcredentials.StatusConnected,
	})
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source, ManagedCredentials: store})
	_, err := exec.Execute(models.WithActor(ctx, managedCredentialActor()), "send_provider", map[string]any{})
	requireToolFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "managed_credential_required")
}

func requireToolFailure(t *testing.T, err error, class runtimefailures.Class, detailCode string) runtimefailures.Envelope {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want canonical runtime failure")
	}
	failure, ok := runtimefailures.As(err)
	if !ok {
		t.Fatalf("error = %T %v, want canonical runtime failure", err, err)
	}
	if failure.Failure.Class != class || failure.Failure.Detail.Code != detailCode {
		t.Fatalf("failure = %s/%s, want %s/%s", failure.Failure.Class, failure.Failure.Detail.Code, class, detailCode)
	}
	return failure.Failure
}

func TestExecutorHTTPToolManagedCredentialServedAndMCPTransportsUseSameOwner(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer served-token" {
			t.Fatalf("Authorization = %q, want served-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		WorkflowSource: managedCredentialSource(server.URL, "github", nil),
		ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:         "github",
			GrantType:   runtimemanagedcredentials.GrantClientCredentials,
			AccessToken: "served-token",
			Status:      runtimemanagedcredentials.StatusConnected,
		}),
	})
	actor := managedCredentialActor()
	gateway := runtimemcp.NewGateway(exec, "gateway-token", runtimemcp.GatewayHooks{
		WithActor:        models.WithActor,
		ActorFromContext: models.ActorFromContext,
		ResolveTurnContext: func(token string) (runtimemcp.TurnContext, bool) {
			if token != "ctx-managed" {
				return runtimemcp.TurnContext{}, false
			}
			return runtimemcp.TurnContext{
				Actor:          actor,
				Allowed:        map[string]struct{}{"send_provider": {}},
				DifferentOwner: runtimeeffects.OwnerBuildTestInfrastructure,
			}, true
		},
	})

	toolBody, _ := json.Marshal(map[string]any{"input": map[string]any{}})
	toolReq := httptest.NewRequest(http.MethodPost, "/tools/send_provider", bytes.NewReader(toolBody))
	toolReq.Header.Set("Authorization", "Bearer gateway-token")
	toolReq.Header.Set("X-SWARM-Context-Token", "ctx-managed")
	toolRec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(toolRec, toolReq)
	if toolRec.Code != http.StatusOK {
		t.Fatalf("/tools status = %d body=%s", toolRec.Code, toolRec.Body.String())
	}

	mcpBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "send_provider",
			"arguments": map[string]any{},
		},
	})
	mcpReq := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(mcpBody))
	mcpReq.Header.Set("Authorization", "Bearer gateway-token")
	mcpReq.Header.Set("X-SWARM-Context-Token", "ctx-managed")
	mcpRec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(mcpRec, mcpReq)
	if mcpRec.Code != http.StatusOK {
		t.Fatalf("/mcp status = %d body=%s", mcpRec.Code, mcpRec.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("HTTP provider calls = %d, want 2", calls.Load())
	}
}

func managedCredentialActor() models.AgentConfig {
	return models.AgentConfig{
		ID:       "worker-agent",
		FlowPath: "worker/instance-1",
		Tools:    []string{"send_provider"},
	}
}

func managedCredentialSource(serverURL, key string, scopes []string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"send_provider": {
				HandlerType: "http",
				InputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    serverURL,
				},
				ManagedCredential: &runtimecontracts.ManagedCredentialRef{
					Key:    key,
					Scopes: scopes,
				},
				ResponseMapping: map[string]any{"ok": "{{response.body.ok}}"},
			},
		},
	})
}

func loadManagedCredentialImportSource(t *testing.T, serverURL, credentialBind string, omitRequires bool) semanticview.Source {
	t.Helper()
	repoRoot := toolsRepoRootForTest(t)
	root := t.TempDir()
	bindCredentials := ""
	if strings.TrimSpace(credentialBind) != "" {
		bindCredentials = "      credentials:\n" + credentialBind
	}
	writeToolFixtureFile(t, root+"/package.yaml", `
name: managed-credential-import
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
`+bindCredentials)
	writeToolFixtureFile(t, root+"/schema.yaml", "name: managed-credential-import\n")
	writeToolFixtureFile(t, root+"/tools.yaml", "{}\n")
	writeToolFixtureFile(t, root+"/agents.yaml", "{}\n")
	writeToolFixtureFile(t, root+"/events.yaml", "{}\n")
	writeToolFixtureFile(t, root+"/nodes.yaml", "{}\n")
	requires := "  credentials: [provider_oauth]\n"
	if omitRequires {
		requires = ""
	}
	writeToolFixtureFile(t, root+"/flows/worker/package.yaml", `
name: worker-package
version: "1.0.0"
requires:
`+requires)
	writeToolFixtureFile(t, root+"/flows/worker/schema.yaml", "name: worker\nmode: static\n")
	writeToolFixtureFile(t, root+"/flows/worker/policy.yaml", "{}\n")
	writeToolFixtureFile(t, root+"/flows/worker/agents.yaml", `
worker-agent:
  id: worker-agent
  role: worker
  mode: task
  model: regular
  tools: [send_provider]
`)
	writeToolFixtureFile(t, root+"/flows/worker/tools.yaml", `
send_provider:
  handler_type: http
  managed_credential:
    key: provider_oauth
    scopes: [repo.read]
  input_schema:
    type: object
  http:
    method: GET
    url: `+serverURL+`
  response_mapping:
    ok: '{{response.body.ok}}'
`)
	writeToolFixtureFile(t, root+"/flows/worker/events.yaml", "{}\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
