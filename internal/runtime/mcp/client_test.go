package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestClientCallWithCredentialKeyResolverUsesBoundDeploymentCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer mcp-secret"; got != want {
			t.Fatalf("Authorization = %q, want %q", got, want)
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"structuredContent": map[string]any{"ok": true},
			},
		})
	}))
	defer server.Close()

	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "tenant_mcp_key", "mcp-secret"); err != nil {
		t.Fatalf("Set tenant_mcp_key: %v", err)
	}
	client := NewClient(store)
	client.httpClient = server.Client()
	client.servers["infra"] = &registeredServer{cfg: ServerConfig{
		Name:           "infra",
		Transport:      "http",
		URL:            server.URL,
		CredentialsKey: "provider_key",
	}}
	client.tools["infra.ping"] = DiscoveredTool{
		Name:       "infra.ping",
		RemoteName: "ping",
		ServerName: "infra",
	}

	out, err := client.CallWithCredentialKeyResolver(context.Background(), "infra.ping", map[string]any{}, func(key string) (string, error) {
		if key != "provider_key" {
			t.Fatalf("resolver key = %q, want package handle provider_key", key)
		}
		return "tenant_mcp_key", nil
	})
	if err != nil {
		t.Fatalf("CallWithCredentialKeyResolver: %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok || result["ok"] != true {
		t.Fatalf("result = %#v, want ok=true", out)
	}
}
