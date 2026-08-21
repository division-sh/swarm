package credentials

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type failingSnapshotStore struct {
	Store
	err error
}

func (s failingSnapshotStore) Snapshot(context.Context, string) (AtomicSnapshot, error) {
	return AtomicSnapshot{}, s.err
}

func TestCredentialSnapshotOwnerOwnsSecretBindingUsability(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		value      string
		set        bool
		wantStatus SecretBindingStatus
		wantValue  string
	}{
		{name: "absent", wantStatus: SecretBindingUnbound},
		{name: "empty", set: true, wantStatus: SecretBindingUnbound},
		{name: "whitespace", value: " \t\n", set: true, wantStatus: SecretBindingUnbound},
		{name: "non-empty", value: "  signing-secret  ", set: true, wantStatus: SecretBindingBound, wantValue: "  signing-secret  "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			if tc.set {
				if err := store.Set(ctx, "webhook_signing.acme", tc.value); err != nil {
					t.Fatalf("Set: %v", err)
				}
			}
			owner, err := NewSnapshotOwner(store)
			if err != nil {
				t.Fatalf("NewSnapshotOwner: %v", err)
			}
			binding, err := owner.ObserveSecretBinding(ctx, "webhook_signing.acme")
			if err != nil {
				t.Fatalf("ObserveSecretBinding: %v", err)
			}
			if binding.Status() != tc.wantStatus || binding.Bound() != (tc.wantStatus == SecretBindingBound) {
				t.Fatalf("binding status = %q/%t, want %q", binding.Status(), binding.Bound(), tc.wantStatus)
			}
			if got := binding.CredentialValue(); got != tc.wantValue {
				t.Fatalf("credential value = %q, want %q", got, tc.wantValue)
			}
			if binding.Epoch() == "" {
				t.Fatal("binding observation has no private epoch")
			}
		})
	}
}

func TestCredentialSnapshotOwnerTypesSecretBindingObservationFailure(t *testing.T) {
	sentinel := errors.New("credential backend unavailable")
	owner, err := NewSnapshotOwner(failingSnapshotStore{Store: EnvStore{}, err: sentinel})
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	_, err = owner.ObserveSecretBinding(context.Background(), "webhook_signing.acme")
	var observationErr *SecretBindingObservationError
	if !errors.As(err, &observationErr) || !errors.Is(err, sentinel) || observationErr.Key != "webhook_signing.acme" {
		t.Fatalf("ObserveSecretBinding error = %#v, want typed observation failure wrapping sentinel", err)
	}
}

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
