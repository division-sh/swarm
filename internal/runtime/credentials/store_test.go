package credentials

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
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

func TestListDescriptors_IndexesToolsAndMCPServers(t *testing.T) {
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
		}},
	})
	items, err := ListDescriptors(ctx, store, source)
	if err != nil {
		t.Fatalf("ListDescriptors: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 credential descriptors, got %d", len(items))
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
