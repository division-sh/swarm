package credentials

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCredentialSnapshotOwnerUsesAtomicValueMetadataAndPrivateEpoch(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(ctx, "bot", "token-a"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	owner, err := NewSnapshotOwner(store)
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	first, err := owner.Observe(ctx, "bot")
	if err != nil {
		t.Fatalf("Observe first: %v", err)
	}
	second, err := owner.Observe(ctx, "bot")
	if err != nil {
		t.Fatalf("Observe second: %v", err)
	}
	if first.CredentialValue() != "token-a" || first.Epoch() == "" || second.Epoch() != first.Epoch() {
		t.Fatalf("stable snapshots = %#v/%#v", first.Metadata(), second.Metadata())
	}
	if err := store.Set(ctx, "bot", "token-b"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	rotated, err := owner.Observe(ctx, "bot")
	if err != nil {
		t.Fatalf("Observe rotated: %v", err)
	}
	if rotated.CredentialValue() != "token-b" || rotated.Epoch() == first.Epoch() {
		t.Fatal("credential rotation did not mint a private snapshot epoch")
	}
	if err := store.Delete(ctx, "bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	missing, err := owner.Observe(ctx, "bot")
	if err != nil {
		t.Fatalf("Observe missing: %v", err)
	}
	if missing.Present || missing.Epoch() == rotated.Epoch() {
		t.Fatal("credential disappearance did not revoke the snapshot epoch")
	}
}

func TestCredentialSnapshotOverlayPreservesSourcePrecedenceAtomically(t *testing.T) {
	ctx := context.Background()
	t.Setenv("BOT", "environment-token")
	writable, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := writable.Set(ctx, "bot", "file-token"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	overlay := NewOverlayStore(EnvStore{}, writable)
	snapshot, err := overlay.Snapshot(ctx, "bot")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshot.CredentialValue() != "environment-token" || snapshot.Source != SourceEnv || snapshot.Writable || !snapshot.Shadowed {
		t.Fatalf("overlay snapshot = %#v", snapshot.Metadata())
	}
}
