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
	if !meta.Present || meta.Source != SourceEnv || meta.Writable || !meta.Shadowed {
		t.Fatalf("Inspect => %+v", meta)
	}
}

func TestEnvStoreRejectsSwarmCredentialBackdoor(t *testing.T) {
	ctx := context.Background()
	t.Setenv("SWARM_SECRET_TOKEN", "secret")

	value, ok, err := EnvStore{}.Get(ctx, "SWARM_SECRET_TOKEN")
	if err == nil {
		t.Fatalf("Get returned value=%q ok=%v err=nil, want SWARM_* rejection", value, ok)
	}
	for _, want := range []string{"SWARM_SECRET_TOKEN", "not accepted through dynamic credential lookup", "swarm secrets"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
	if ok || value != "" {
		t.Fatalf("Get returned value=%q ok=%v, want no dynamic SWARM_* credential", value, ok)
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
			"email_api": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolCredentials([]string{"sendgrid_api_key"}...)),
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

func TestBuildRequirementIndex_IndexesDirectFilesystemFlowCredentialBindings(t *testing.T) {
	repoRoot := credentialsRepoRootForTest(t)
	root := t.TempDir()

	writeCredentialsFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: credential-binding\n")
	for _, flowID := range []string{"alpha", "beta"} {

		writeCredentialsFixtureFile(t, filepath.Join(root, flowID, "schema.yaml"), "name: "+flowID+"\nmode: static\n")
		writeCredentialsFixtureFile(t, filepath.Join(root, flowID, "tools.yaml"), `
call_provider:
  handler_type: http
  credentials: [provider_key]
  http:
    method: GET
    url: https://provider.example.test
`)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	index := BuildRequirementIndex(semanticview.Wrap(bundle))

	if refs := index["provider_key"]; len(refs) != 1 || refs[0].Kind != "tool" || refs[0].Name != "call_provider" {
		t.Fatalf("provider_key refs = %#v, want one deduplicated call_provider tool requirement", refs)
	}
}

func TestMissingRequired_IndexesDirectFilesystemNativeWebSearchCredentialBinding(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(ctx, "provider_key", "raw-secret"); err != nil {
		t.Fatalf("Set provider_key: %v", err)
	}

	repoRoot := credentialsRepoRootForTest(t)
	root := t.TempDir()

	writeCredentialsFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: native-web-search-credential-binding\n")

	writeCredentialsFixtureFile(t, filepath.Join(root, "worker", "schema.yaml"), "name: worker\nmode: static\n")
	writeCredentialsFixtureFile(t, filepath.Join(root, "worker", "policy.yaml"), `
web_search_provider:
  provider: brave
  credentials_key: provider_key
`)
	writeCredentialsFixtureFile(t, filepath.Join(root, "worker", "agents.yaml"), `
worker-agent:
  id: worker-agent
  model: regular
  intent:
    inline: Research the requested subject using native web search.
  native_tools:
    web_search: true
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	index := BuildRequirementIndex(source)
	if refs := index["provider_key"]; len(refs) != 1 || refs[0].Kind != "web_search_provider" || refs[0].Name != "brave" {
		t.Fatalf("provider_key refs = %#v, want brave web_search_provider", refs)
	}

	missing, err := MissingRequired(ctx, store, source)
	if err != nil {
		t.Fatalf("MissingRequired: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("MissingRequired = %#v, want provider_key to satisfy the direct flow requirement", missing)
	}

	descriptors, err := ListDescriptors(ctx, store, source)
	if err != nil {
		t.Fatalf("ListDescriptors: %v", err)
	}
	byKey := map[string]Descriptor{}
	for _, desc := range descriptors {
		byKey[desc.Key] = desc
	}
	if got := byKey["provider_key"]; !got.Present || len(got.RequiredBy) != 1 || got.RequiredBy[0].Kind != "web_search_provider" || got.RequiredBy[0].Name != "brave" {
		t.Fatalf("provider_key descriptor = %+v, want present brave web_search requirement", got)
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

func TestFileStoreDeleteWithReceiptCannotDeleteSuccessorOccurrence(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.AdmitWithReceipt(ctx, "channel.telegram.provider", "first", "operation-a/provider")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AdmitWithReceipt(ctx, "channel.telegram.provider", "second", "operation-b/provider")
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.DeleteWithReceipt(ctx, first.Key, first.Receipt, first.Epoch); err != nil || deleted {
		t.Fatalf("stale delete = %v, %v; want false, nil", deleted, err)
	}
	if value, found, err := store.Get(ctx, second.Key); err != nil || !found || value != "second" {
		t.Fatalf("successor after stale delete = %q, %v, %v", value, found, err)
	}
	if deleted, err := store.DeleteWithReceipt(ctx, second.Key, second.Receipt, second.Epoch); err != nil || !deleted {
		t.Fatalf("current delete = %v, %v; want true, nil", deleted, err)
	}
	if _, found, err := store.Get(ctx, second.Key); err != nil || found {
		t.Fatalf("credential after current delete found=%v err=%v", found, err)
	}
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
