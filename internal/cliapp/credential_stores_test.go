package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestProviderIngressCredentialTierDoesNotAdoptToolEnvironment(t *testing.T) {
	ctx := context.Background()
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	t.Setenv("WEBHOOK_SIGNING_TELEGRAM", "tool-environment-value")
	provider, err := BuildProviderCredentialStore()
	if err != nil {
		t.Fatal(err)
	}
	tool, err := BuildCredentialStore()
	if err != nil {
		t.Fatal(err)
	}
	observe := func(store runtimecredentials.Store) runtimecredentials.AdmittedSnapshot {
		t.Helper()
		owner, err := runtimecredentials.NewSnapshotOwner(store)
		if err != nil {
			t.Fatal(err)
		}
		projection := owner.BeginSecretBindingProjection()
		observed, err := projection.ObserveActivationCredential(ctx, "webhook_signing.telegram")
		if err != nil {
			t.Fatal(err)
		}
		return observed
	}
	if observe(provider).Present || !observe(tool).Present {
		t.Fatal("provider ingress and tool credential tiers lost their distinct precedence")
	}
	if err := provider.Set(ctx, "webhook_signing.telegram", "provider-file-value"); err != nil {
		t.Fatal(err)
	}
	if !observe(provider).Present {
		t.Fatal("provider file tier did not admit its own exact key")
	}
	if value, present, err := tool.Get(ctx, "webhook_signing.telegram"); err != nil || !present || value != "tool-environment-value" {
		t.Fatalf("generic tool overlay lost environment precedence: present=%t err=%v", present, err)
	}
	providerFact := observe(provider).SourceFact()
	toolFact := observe(tool).SourceFact()
	wantFile := "file:" + os.Getenv("SWARM_CREDENTIALS_FILE")
	want := "credential webhook_signing.telegram · source env:WEBHOOK_SIGNING_TELEGRAM\ncredential webhook_signing.telegram · source " + wantFile
	lines := CredentialSourceLines([]runtimecredentials.SourceFact{providerFact, toolFact, providerFact})
	if strings.Join(lines, "\n") != want {
		t.Fatalf("same-key source golden: got %q, want %q", strings.Join(lines, "\n"), want)
	}
	report := LocalPreflightReport{CredentialSources: []runtimecredentials.SourceFact{providerFact, toolFact}}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"provider-file-value", "tool-environment-value"} {
		if strings.Contains(string(encoded)+strings.Join(lines, "\n"), secret) {
			t.Fatal("credential presentation leaked a value")
		}
	}
	metadata, err := json.Marshal(observe(provider).Metadata())
	if err != nil || strings.Contains(string(metadata), os.Getenv("SWARM_CREDENTIALS_FILE")) {
		t.Fatalf("locator escaped into unrelated metadata JSON: %v", err)
	}
	for _, fact := range []runtimecredentials.SourceFact{
		{Key: "key\nforged", Reference: "file:/tmp/credentials\r\x1b[31m"},
		{Key: "key\u2028forged", Reference: "env:KEY\tOTHER"},
	} {
		line := CredentialSourceLines([]runtimecredentials.SourceFact{fact})[0]
		if strings.ContainsAny(line, "\n\r\t\x1b\u2028") || !strings.Contains(line, "\\") {
			t.Fatalf("credential locator injected terminal or line control: %q", line)
		}
	}
	t.Run("preflight_uses_required_observations_only", func(t *testing.T) {
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_probe": runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
				runtimecontracts.WithToolCredentials("webhook_signing.telegram")),
		}})
		report := LocalPreflightReport{OK: true, Mode: "serve"}
		report.checkContractSecrets(ctx, source, LocalPreflightSeverityBlocker, runtimebootverify.SourceBootEffectReachability{})
		var text bytes.Buffer
		WriteLocalPreflightText(&text, report)
		if report.HasBlockers() || len(report.CredentialSources) != 1 || !strings.Contains(text.String(), "credential webhook_signing.telegram · source env:WEBHOOK_SIGNING_TELEGRAM") {
			t.Fatalf("preflight lost exact successful source: %s", text.String())
		}
		encoded, err := json.Marshal(report)
		if err != nil || strings.Contains(string(encoded)+text.String(), "tool-environment-value") || strings.Contains(string(encoded)+text.String(), "provider-file-value") {
			t.Fatalf("preflight serialization disclosed a value: %v", err)
		}
	})
	t.Run("default_and_explicit_file_tier", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		t.Setenv("XDG_CONFIG_HOME", t.TempDir())
		t.Setenv("SWARM_CREDENTIALS_FILE", "")
		defaultPath, err := runtimecredentials.DefaultFilePath()
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{defaultPath, filepath.Join(t.TempDir(), "explicit.json")} {
			if path != defaultPath {
				t.Setenv("SWARM_CREDENTIALS_FILE", path)
			}
			store, err := BuildCredentialStore()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Set(ctx, "file_only_provenance", "file-only-value"); err != nil {
				t.Fatal(err)
			}
			if fact := observeKeyForSourceTest(t, store, "file_only_provenance"); fact.Reference != "file:"+path {
				t.Fatalf("file tier source = %q, want file:%s", fact.Reference, path)
			}
		}
	})
}

func observeKeyForSourceTest(t *testing.T, store runtimecredentials.Store, key string) runtimecredentials.SourceFact {
	t.Helper()
	owner, err := runtimecredentials.NewSnapshotOwner(store)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := owner.Observe(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	return observed.SourceFact()
}
