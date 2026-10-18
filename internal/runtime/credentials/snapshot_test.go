package credentials

import (
	"context"
	"encoding/json"
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

func TestSecretBindingProjectionFrozenReadNeverObservesOrAddsAuthority(t *testing.T) {
	ctx := context.Background()
	const key = "webhook_signing.acme"
	store := &sequenceSnapshotStore{Store: EnvStore{}, snapshots: []AtomicSnapshot{
		NewAtomicSnapshot(Metadata{Key: key, Present: true}, "secret-a"),
		NewAtomicSnapshot(Metadata{Key: key, Present: true}, "secret-b"),
	}}
	owner, err := NewSnapshotOwner(store)
	if err != nil {
		t.Fatal(err)
	}
	projection := owner.BeginSecretBindingProjection()
	if _, err := projection.FrozenSecretBinding(key); err == nil || store.calls != 0 {
		t.Fatalf("unadmitted read err=%v calls=%d, want refusal without observation", err, store.calls)
	}
	admitted, err := projection.ObserveSecretBinding(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := projection.FrozenSecretBinding(key)
	if err != nil || frozen.CredentialValue() != "secret-a" || frozen.ObservationToken() != admitted.ObservationToken() || store.calls != 1 {
		t.Fatalf("frozen read err=%v calls=%d, want unchanged admitted evidence", err, store.calls)
	}
	if _, err := projection.FrozenSecretBinding("sibling"); err == nil || store.calls != 1 {
		t.Fatalf("sibling read err=%v calls=%d, want refusal without observation", err, store.calls)
	}
	_ = projection.SourceFacts()
	_ = owner.SourceFacts()
	if store.calls != 1 {
		t.Fatal("source readback observed credentials or added authority")
	}
	var stale *SecretBindingProjectionStaleError
	if err := projection.ValidateCurrent(ctx); !errors.As(err, &stale) || stale.Key != key || store.calls != 2 {
		t.Fatalf("rotation validation err=%v calls=%d, want stale original authority", err, store.calls)
	}
	frozen, err = projection.FrozenSecretBinding(key)
	if err != nil || frozen.CredentialValue() != "secret-a" || store.calls != 2 {
		t.Fatalf("post-validation frozen read err=%v calls=%d, want no replacement adoption", err, store.calls)
	}
}

func TestSecretBindingProjectionScopedCurrentnessNeverAdoptsSiblingChanges(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"selected", "sibling"} {
		if err := store.Set(ctx, key, "original-"+key); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := NewSnapshotOwner(store)
	if err != nil {
		t.Fatal(err)
	}
	projection := owner.BeginSecretBindingProjection()
	for _, key := range []string{"selected", "sibling"} {
		if _, err := projection.ObserveActivationCredential(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Set(ctx, "selected", "fresh-selected"); err != nil {
		t.Fatal(err)
	}
	if err := projection.ValidateCurrentKeys(ctx, []string{"sibling"}); err != nil {
		t.Fatalf("selected change altered independent sibling evidence: %v", err)
	}
	if err := projection.ValidateCurrentKeys(ctx, []string{"never-observed"}); err == nil {
		t.Fatal("unobserved key was accepted as frozen evidence")
	}
	if err := store.Set(ctx, "sibling", "unadmitted-sibling"); err != nil {
		t.Fatal(err)
	}
	var stale *SecretBindingProjectionStaleError
	if err := projection.ValidateCurrentKeys(ctx, []string{"sibling"}); !errors.As(err, &stale) || stale.Key != "sibling" {
		t.Fatalf("sibling change was adopted: %v", err)
	}
}

func TestAdmittedActivationCredentialRequiresExactValueAndReceipt(t *testing.T) {
	ctx := context.Background()
	for _, change := range []string{"healthy", "absent", "rotated", "restored_value", "new_receipt", "empty", "whitespace"} {
		t.Run(change, func(t *testing.T) {
			store, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.AdmitWithReceipt(ctx, "provider", "original", "owner-receipt"); err != nil {
				t.Fatal(err)
			}
			owner, err := NewSnapshotOwner(store)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := owner.SealCurrentValue(ctx, "provider")
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "absent":
				err = store.Delete(ctx, "provider")
			case "rotated":
				err = store.Set(ctx, "provider", "replacement")
			case "restored_value":
				if err = store.Set(ctx, "provider", "replacement"); err == nil {
					err = store.Set(ctx, "provider", "original")
				}
			case "new_receipt":
				_, err = store.AdmitWithReceipt(ctx, "provider", "original", "foreign-receipt")
			case "empty":
				err = store.Set(ctx, "provider", "")
			case "whitespace":
				err = store.Set(ctx, "provider", " \t\n")
			}
			if err != nil {
				t.Fatal(err)
			}
			projection := owner.BeginSecretBindingProjection()
			_, current, err := projection.ObserveAdmittedActivationCredential(ctx, evidence, "owner-receipt")
			if change == "empty" || change == "whitespace" {
				if !errors.Is(err, ErrCredentialValueUnusable) {
					t.Fatalf("unusable value became recovery success: %v", err)
				}
				return
			}
			if err != nil || current != (change == "healthy") {
				t.Fatalf("admitted currentness = %t, %v", current, err)
			}
			if err := projection.ValidateCurrent(ctx); err != nil {
				t.Fatal(err)
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
	t.Run("source_locator_is_not_authority", func(t *testing.T) {
		const key = "provenance.key-name"
		const value = "same-observed-value"
		candidates := []string{key, "provenance_key_name", "PROVENANCE_KEY_NAME"}
		for _, candidate := range candidates {
			t.Setenv(candidate, value)
		}
		if err := writable.Set(ctx, key, value); err != nil {
			t.Fatal(err)
		}
		owner, err := NewSnapshotOwner(overlay)
		if err != nil {
			t.Fatal(err)
		}
		projection := owner.BeginSecretBindingProjection()
		frozen, err := projection.ObserveActivationCredential(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		reference := func(snapshot AtomicSnapshot) string {
			t.Helper()
			projection, ok := any(snapshot).(interface{ SourceReference() string })
			if !ok {
				t.Fatal("credential observation lacks its winning source reference")
			}
			return projection.SourceReference()
		}
		originalMetadata, err := json.Marshal(frozen.Metadata())
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range candidates {
			observed, err := owner.Observe(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if reference(observed.AtomicSnapshot) != "env:"+candidate {
				t.Fatalf("winning source = %q, want env:%s", reference(observed.AtomicSnapshot), candidate)
			}
			facts := owner.SourceFacts()
			if len(facts) != 1 || facts[0].Key != key || facts[0].Reference != "env:"+candidate {
				t.Fatalf("retained source facts=%v, want env:%s", facts, candidate)
			}
			metadata, err := json.Marshal(observed.Metadata())
			if err != nil {
				t.Fatal(err)
			}
			if string(metadata) != string(originalMetadata) || observed.ObservationToken() != frozen.ObservationToken() {
				t.Fatal("source-name-only change altered existing authority metadata or token")
			}
			if err := projection.ValidateCurrent(ctx); err != nil {
				t.Fatalf("same-value source-name change invalidated frozen binding: %v", err)
			}
			if reference(frozen.AtomicSnapshot) != "env:"+key {
				t.Fatal("frozen binding adopted a later source observation")
			}
			frozenFacts := projection.SourceFacts()
			if len(frozenFacts) != 1 || frozenFacts[0].Key != key || frozenFacts[0].Reference != "env:"+key {
				t.Fatalf("frozen projection adopted later source facts: %v", frozenFacts)
			}
			if err := os.Unsetenv(candidate); err != nil {
				t.Fatal(err)
			}
		}
		file, err := owner.Observe(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if reference(file.AtomicSnapshot) != "file:"+writable.path || file.ObservationToken() == frozen.ObservationToken() {
			t.Fatal("file fallback lost its source or existing tier-change currentness")
		}
		if err := projection.ValidateCurrent(ctx); err == nil {
			t.Fatal("frozen environment binding accepted a tier change")
		}
		if err := writable.Set(ctx, key, "changed-value"); err != nil {
			t.Fatal(err)
		}
		changed, err := owner.Observe(ctx, key)
		if err != nil || changed.ObservationToken() == file.ObservationToken() {
			t.Fatalf("real value change did not change currentness: %v", err)
		}
	})
	t.Run("absent_empty_refused_and_corrupt_sources", func(t *testing.T) {
		file, err := NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
		if err != nil {
			t.Fatal(err)
		}
		absent, err := file.Snapshot(ctx, "not_present")
		if err != nil || absent.Present || absent.SourceReference() != "" {
			t.Fatalf("absence fabricated source provenance: %v", err)
		}
		if err := file.Set(ctx, "empty_provenance", "file-value"); err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"", " \t"} {
			t.Setenv("EMPTY_PROVENANCE", value)
			observed, err := NewOverlayStore(EnvStore{}, file).Snapshot(ctx, "empty_provenance")
			if err != nil || !observed.Present || observed.Source != SourceEnv || observed.SourceReference() != "" || observed.CredentialValue() != value {
				t.Fatalf("unusable environment fabricated success or changed file-shadowing behavior: %v", err)
			}
		}
		t.Setenv("SWARM_PROVENANCE", "private-refused-value")
		refused, err := (EnvStore{}).Snapshot(ctx, "SWARM_PROVENANCE")
		if err == nil || refused.SourceReference() != "" || refused.Present {
			t.Fatal("refused source acquired successful provenance")
		}
		if err := os.WriteFile(file.path, []byte("invalid private-corrupt-value"), 0o600); err != nil {
			t.Fatal(err)
		}
		corrupt, err := file.Snapshot(ctx, "empty_provenance")
		if err == nil || corrupt.SourceReference() != "" || corrupt.Present {
			t.Fatal("corrupt file acquired successful provenance")
		}
	})
}
