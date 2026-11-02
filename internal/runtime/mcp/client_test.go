package mcp

import (
	"path/filepath"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/testutil/mcpapi"
)

func TestClientCallWithCredentialKeyResolverUsesBoundDeploymentCredential(t *testing.T) {
	server := mcpapi.New(t, "mcp-secret")
	defer server.Close()

	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(unmanagedMCPTestContext(), "tenant_mcp_key", "mcp-secret"); err != nil {
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

	out, err := client.CallWithCredentialKeyResolver(unmanagedMCPTestContext(), "infra.ping", map[string]any{}, func(key string) (string, error) {
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
