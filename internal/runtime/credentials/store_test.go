package credentials

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestFileStore_SetGetDeleteInspect(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(ctx, "sendgrid_api_key", "secret-1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	value, ok, err := store.Get(ctx, "sendgrid_api_key")
	if err != nil || !ok || value != "secret-1" {
		t.Fatalf("Get => value=%q ok=%v err=%v", value, ok, err)
	}
	meta, err := store.Inspect(ctx, "sendgrid_api_key")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if !meta.Present || meta.Source != SourceFile || !meta.Writable || meta.UpdatedAt == nil {
		t.Fatalf("Inspect => %+v", meta)
	}
	if err := store.Delete(ctx, "sendgrid_api_key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	meta, err = store.Inspect(ctx, "sendgrid_api_key")
	if err != nil {
		t.Fatalf("Inspect after delete: %v", err)
	}
	if meta.Present {
		t.Fatalf("expected credential to be deleted, got %+v", meta)
	}
}

func TestOverlayStore_EnvOverridesFile(t *testing.T) {
	ctx := context.Background()
	t.Setenv("SENDGRID_API_KEY", "env-secret")
	fileStore, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := fileStore.Set(ctx, "sendgrid_api_key", "file-secret"); err != nil {
		t.Fatalf("file Set: %v", err)
	}
	store := NewOverlayStore(NewEnvStore(), fileStore)
	value, ok, err := store.Get(ctx, "sendgrid_api_key")
	if err != nil || !ok || value != "env-secret" {
		t.Fatalf("Get => value=%q ok=%v err=%v", value, ok, err)
	}
	meta, err := store.Inspect(ctx, "sendgrid_api_key")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if !meta.Present || meta.Source != SourceEnv || meta.Writable {
		t.Fatalf("Inspect => %+v", meta)
	}
}

func TestListDescriptors_IndexesToolsMCPServersAndWebSearchProvider(t *testing.T) {
	ctx := context.Background()
	fileStore, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := fileStore.Set(ctx, "sendgrid_api_key", "file-secret"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	store := NewOverlayStore(NewEnvStore(), fileStore)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"email_api": {Credentials: []string{"sendgrid_api_key"}},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"prefix":          "infra",
						"credentials_key": "infra_mcp_token",
					},
				},
			},
			"web_search_provider": {
				Value: map[string]any{
					"provider":        "brave",
					"credentials_key": "brave_search_api_key",
				},
			},
		}},
	})
	items, err := ListDescriptors(ctx, store, source)
	if err != nil {
		t.Fatalf("ListDescriptors: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 credential descriptors, got %d", len(items))
	}
	byKey := map[string]Descriptor{}
	for _, item := range items {
		byKey[item.Key] = item
	}
	if got := byKey["sendgrid_api_key"]; !got.Present || got.Source != SourceFile || len(got.RequiredBy) != 1 || got.RequiredBy[0].Kind != "tool" || got.RequiredBy[0].Name != "email_api" {
		t.Fatalf("sendgrid descriptor = %+v", got)
	}
	if got := byKey["infra_mcp_token"]; got.Present || len(got.RequiredBy) != 1 || got.RequiredBy[0].Kind != "mcp_server" || got.RequiredBy[0].Name != "infra" {
		t.Fatalf("infra descriptor = %+v", got)
	}
	if got := byKey["brave_search_api_key"]; got.Present || len(got.RequiredBy) != 1 || got.RequiredBy[0].Kind != "web_search_provider" || got.RequiredBy[0].Name != "brave" {
		t.Fatalf("web search descriptor = %+v", got)
	}
}

func TestBuildRequirementIndex_IndexesImportedPackageCredentialBindings(t *testing.T) {
	repoRoot := credentialsRepoRootForTest(t)
	root := t.TempDir()
	writeCredentialsFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: credential-binding
version: "1.0.0"
flows:
  - id: alpha
    flow: alpha
    mode: static
    bind:
      credentials:
        provider_key: tenant_alpha_key
  - id: beta
    flow: beta
    mode: static
    bind:
      credentials:
        provider_key: tenant_beta_key
`)
	writeCredentialsFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: credential-binding\n")
	writeCredentialsFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeCredentialsFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeCredentialsFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeCredentialsFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	for _, flowID := range []string{"alpha", "beta"} {
		writeCredentialsFixtureFile(t, filepath.Join(root, "flows", flowID, "package.yaml"), `
name: worker-package
version: "1.0.0"
requires:
  credentials: [provider_key]
`)
		writeCredentialsFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), "name: "+flowID+"\nmode: static\n")
		writeCredentialsFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeCredentialsFixtureFile(t, filepath.Join(root, "flows", flowID, "tools.yaml"), `
call_provider:
  handler_type: http
  credentials: [provider_key]
  http:
    method: GET
    url: https://provider.example.test
`)
		writeCredentialsFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
		writeCredentialsFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), "{}\n")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	index := BuildRequirementIndex(semanticview.Wrap(bundle))

	if refs := index["provider_key"]; len(refs) != 0 {
		t.Fatalf("raw package credential handle indexed as deployment key: %#v", refs)
	}
	if refs := index["tenant_alpha_key"]; len(refs) != 1 || refs[0].Kind != "tool" || refs[0].Name != "call_provider" {
		t.Fatalf("tenant_alpha_key refs = %#v, want call_provider tool", refs)
	}
	if refs := index["tenant_beta_key"]; len(refs) != 1 || refs[0].Kind != "tool" || refs[0].Name != "call_provider" {
		t.Fatalf("tenant_beta_key refs = %#v, want call_provider tool", refs)
	}
}

func TestDefaultFilePath_UsesSwarmConfigDir(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, ".config"))
	configRoot, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("UserConfigDir: %v", err)
	}
	path, err := DefaultFilePath()
	if err != nil {
		t.Fatalf("DefaultFilePath: %v", err)
	}
	if filepath.Base(path) != "credentials.json" {
		t.Fatalf("unexpected credential file path %q", path)
	}
	if filepath.Base(filepath.Dir(path)) != "swarm" {
		t.Fatalf("unexpected credential dir %q", filepath.Dir(path))
	}
	if !strings.HasPrefix(path, configRoot) {
		t.Fatalf("expected credential path under temp config dir %q, got %q", configRoot, path)
	}
}

func credentialsRepoRootForTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func writeCredentialsFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
