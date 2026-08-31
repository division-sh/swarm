package credentials

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type failingSnapshotStore struct {
	Store
	err error
}

type sequenceSnapshotStore struct {
	Store
	mu        sync.Mutex
	snapshots []AtomicSnapshot
	calls     int
}

func (s *sequenceSnapshotStore) Snapshot(context.Context, string) (AtomicSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := s.calls
	s.calls++
	if index >= len(s.snapshots) {
		index = len(s.snapshots) - 1
	}
	return s.snapshots[index], nil
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
			if binding.ObservationToken() == "" {
				t.Fatal("binding observation has no private token")
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

func TestSecretBindingProjectionReusesExactKeyAndRejectsRotation(t *testing.T) {
	bound := NewAtomicSnapshot(Metadata{Key: "webhook_signing.acme", Present: true}, "secret-a")
	unbound := NewAtomicSnapshot(Metadata{Key: "webhook_signing.acme"}, "")
	for _, tc := range []struct {
		name      string
		snapshots []AtomicSnapshot
		wantStale bool
	}{
		{name: "stable", snapshots: []AtomicSnapshot{bound, bound}},
		{name: "rotated", snapshots: []AtomicSnapshot{bound, unbound}, wantStale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &sequenceSnapshotStore{Store: EnvStore{}, snapshots: tc.snapshots}
			owner, err := NewSnapshotOwner(store)
			if err != nil {
				t.Fatal(err)
			}
			projection := owner.BeginSecretBindingProjection()
			first, err := projection.ObserveSecretBinding(context.Background(), "webhook_signing.acme")
			if err != nil {
				t.Fatal(err)
			}
			second, err := projection.ObserveSecretBinding(context.Background(), " webhook_signing.acme ")
			if err != nil {
				t.Fatal(err)
			}
			if first.ObservationToken() != second.ObservationToken() || store.calls != 1 {
				t.Fatalf("shared-key observation tokens=%q/%q calls=%d, want one capture", first.ObservationToken(), second.ObservationToken(), store.calls)
			}
			err = projection.ValidateCurrent(context.Background())
			var staleErr *SecretBindingProjectionStaleError
			if errors.As(err, &staleErr) != tc.wantStale {
				t.Fatalf("ValidateCurrent error = %v, want stale=%t", err, tc.wantStale)
			}
			if store.calls != 2 {
				t.Fatalf("snapshot calls = %d, want one capture plus one validation", store.calls)
			}
		})
	}
}

func TestCredentialSnapshotOwnerUsesAtomicValueMetadataAndPrivateObservationToken(t *testing.T) {
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
	if first.CredentialValue() != "token-a" || first.ObservationToken() == "" || second.ObservationToken() != first.ObservationToken() {
		t.Fatalf("stable snapshots = %#v/%#v", first.Metadata(), second.Metadata())
	}
	if err := store.Set(ctx, "bot", "token-b"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	rotated, err := owner.Observe(ctx, "bot")
	if err != nil {
		t.Fatalf("Observe rotated: %v", err)
	}
	if rotated.CredentialValue() != "token-b" || rotated.ObservationToken() == first.ObservationToken() {
		t.Fatal("credential rotation did not mint a private observation token")
	}
	if err := store.Delete(ctx, "bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	missing, err := owner.Observe(ctx, "bot")
	if err != nil {
		t.Fatalf("Observe missing: %v", err)
	}
	if missing.Present || missing.ObservationToken() == rotated.ObservationToken() {
		t.Fatal("credential disappearance did not revoke the observation token")
	}
}

func TestCredentialSnapshotOwnerAcceptsPlainCredentialFileWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	original := `{"version":1,"entries":{"bot":{"value":"unsupported-token","updated_at":"2026-08-28T00:00:00Z"}}}`
	writeCredentialsFixtureFile(t, path, original)
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	owner, err := NewSnapshotOwner(store)
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	snapshot, err := owner.Observe(context.Background(), "bot")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if snapshot.CredentialValue() != "unsupported-token" || snapshot.ObservationToken() == "" {
		t.Fatalf("snapshot = %#v", snapshot.Metadata())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(raw) != original {
		t.Fatalf("unsupported credential file was mutated:\n%s", raw)
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
