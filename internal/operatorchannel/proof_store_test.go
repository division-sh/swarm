package operatorchannel

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func TestFileProofStoreExactRevisionRevocationAndAdmission(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	proof := testVerifiedProof(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	if err := store.Put(ctx, proof); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("proof mode = %o, want 600", info.Mode().Perm())
	}
	if err := store.Put(ctx, proof); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	changed := proof
	changed.AccountPresentation = "@changed"
	if err := store.Put(ctx, changed); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("changed same-revision Put error = %v", err)
	}

	revokedAt := proof.VerifiedAt.Add(time.Hour)
	revoked, err := store.Revoke(ctx, proof.Interface, 1, revokedAt)
	if err != nil {
		t.Fatal(err)
	}
	if revoked.Status != ProofRevoked || revoked.Revision != 2 {
		t.Fatalf("revoked proof = %#v", revoked)
	}
	replayed, err := store.Revoke(ctx, proof.Interface, 1, revokedAt.Add(time.Minute))
	if err != nil || replayed.Revision != revoked.Revision || replayed.Status != revoked.Status {
		t.Fatalf("revoke replay = %#v, %v", replayed, err)
	}
	if _, err := store.Revoke(ctx, proof.Interface, 2, revokedAt); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("changed revoke error = %v", err)
	}
}

func TestFileProofStoreRejectsCorruptUnknownAndConflictingDocuments(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "unknown version", raw: `{"version":3,"entries":{}}`},
		{name: "unknown field", raw: `{"version":2,"entries":{},"extra":true}`},
		{name: "malformed", raw: `{`},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := NewFileProofStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(store.Path(), []byte(test.raw), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := store.List(context.Background()); err == nil {
				t.Fatal("corrupt proof document was admitted")
			}
		})
	}

	dir := t.TempDir()
	store, err := NewFileProofStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	proof := testVerifiedProof(time.Now().UTC())
	raw := `{"version":2,"entries":{"wrong-key":` + mustProofRecordJSON(t, proof) + `}}`
	if err := os.WriteFile(store.Path(), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(context.Background()); err == nil || !strings.Contains(err.Error(), "conflicting entry") {
		t.Fatalf("conflicting key error = %v", err)
	}
}

func TestFileProofStoreClassifiesV1WithoutDecodingAuthorityAndQuarantinesOnFreshProof(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileProofStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `{"version":1,"entries":{"untrusted":{"format":"swarm-verified-account-proof-v1","epoch":"legacy-authority","external_account_reference":"must-not-load"}}}`
	if err := os.WriteFile(store.Path(), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	proofs, err := store.List(context.Background())
	if err != nil || len(proofs) != 0 {
		t.Fatalf("v1 classification = %#v, %v", proofs, err)
	}
	proof := testVerifiedProof(time.Now().UTC())
	if err := store.Put(context.Background(), proof); err != nil {
		t.Fatal(err)
	}
	quarantined, err := os.ReadFile(store.Path() + ".unsupported-v1")
	if err != nil || string(quarantined) != legacy {
		t.Fatalf("v1 quarantine = %q, %v", quarantined, err)
	}
	current, found, err := store.Get(context.Background(), proof.Interface)
	if err != nil || !found || current.ProofID != proof.ProofID || current.ProviderCredential != proof.ProviderCredential {
		t.Fatalf("fresh v2 proof = %#v, found=%v err=%v", current, found, err)
	}
}

func testVerifiedProof(at time.Time) VerifiedProof {
	identity := testInterfaceIdentity().Normalized()
	return VerifiedProof{
		Format: ProofFormat, ProofID: ProofIDForInterface(identity), Revision: 1, Status: ProofActive,
		Interface: identity, ExternalAccountRef: "account-1", ConversationRef: "conversation-1",
		ConversationScope: ConversationScopeDirect, AccountPresentation: "@operator", Method: "connect",
		Challenge: "SWARM-AAAAAAAAAAAAAAAA", OriginalOperationID: uuid.NewString(), MintingStoreID: uuid.NewString(),
		MintingDeploymentID: uuid.NewString(), VerifiedAt: at, OperatorConfirmed: true,
		ConsentScopes:      []ConsentScope{ConsentNotify, ConsentDecide},
		ProviderCredential: runtimecredentials.ValueEvidence{Key: "channel.telegram.provider", Seal: runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("a", 64))},
	}
}

func mustProofRecordJSON(t *testing.T, proof VerifiedProof) string {
	t.Helper()
	raw, err := json.Marshal(recordFromProof(proof))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
