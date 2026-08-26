package channelonboarding

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestOnboardingCredentialWriterCrashConvergence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "super-secret-token", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "different-retry-bytes", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	if replayed != first || first.Epoch == "" || first.Receipt != "operation-a/provider" {
		t.Fatalf("receipt replay = first:%#v replay:%#v", first, replayed)
	}
	value, found, err := store.Get(context.Background(), "channel.telegram.provider")
	if err != nil || !found || value != "super-secret-token" {
		t.Fatalf("replay rewrote credential: value=%q found=%v err=%v", value, found, err)
	}
	rotated, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "rotated-token", Receipt: "operation-b/provider"})
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Epoch == first.Epoch || rotated.Receipt == first.Receipt {
		t.Fatalf("explicit new receipt did not rotate occurrence: old=%#v new=%#v", first, rotated)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), first.Epoch) && strings.Contains(first.Epoch, "super-secret-token") {
		t.Fatal("credential epoch was derived from secret bytes")
	}
}

func TestCredentialAdmissionJSONNeverContainsValue(t *testing.T) {
	admission := CredentialAdmission{Role: "provider", StoreKey: "channel.telegram.provider", Kind: CredentialAdmissionWritten, Receipt: "receipt-a", Epoch: "epoch-a"}
	if strings.Contains(strings.ToLower(strings.Join([]string{admission.Role, admission.StoreKey, admission.Receipt, admission.Epoch}, " ")), "secret") {
		t.Fatal("test admission unexpectedly contains secret material")
	}
}

func TestOnboardingCredentialOccurrenceSurvivesSnapshotOwnerRestart(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	written, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token", Receipt: "operation/provider"})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := restarted.Observe(context.Background(), written.StoreKey)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Epoch != written.Epoch {
		t.Fatalf("restarted epoch = %q, want %q", observed.Epoch, written.Epoch)
	}
}
