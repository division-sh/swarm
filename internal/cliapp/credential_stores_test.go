package cliapp

import (
	"context"
	"path/filepath"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
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
}
