package operatorchannel

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func TestActiveProofRemainsRevocableAfterCredentialRetirement(t *testing.T) {
	proofs, err := NewFileProofStore(filepath.Join(t.TempDir(), ".swarm"))
	if err != nil {
		t.Fatal(err)
	}
	proof := testVerifiedProof(time.Date(2026, 8, 31, 2, 0, 0, 0, time.UTC))
	if err := proofs.Put(context.Background(), proof); err != nil {
		t.Fatal(err)
	}
	service := &Service{proofs: proofs}
	active, err := service.loadActiveProof(context.Background(), proof.Interface)
	if err != nil {
		t.Fatalf("load active proof after credential retirement: %v", err)
	}
	if active.ProofID != proof.ProofID || active.Revision != proof.Revision || active.Status != ProofActive {
		t.Fatalf("active proof = %#v, want exact revocable record %#v", active, proof)
	}
}

func TestProofResponsibilityRejectsProviderEvidenceContradictions(t *testing.T) {
	responsibility := testProofResponsibility(t)
	if _, err := validatedResponsibilityProof(responsibility); err != nil {
		t.Fatalf("valid proof responsibility: %v", err)
	}
	other := runtimecredentials.ValueEvidence{
		Key:  responsibility.Operation.ProviderCredential.Key,
		Seal: runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("b", 64)),
	}
	for _, test := range []struct {
		name   string
		mutate func(*ProofResponsibility)
	}{
		{name: "binding", mutate: func(value *ProofResponsibility) { value.Binding.ProviderCredential = other }},
		{name: "proof projection", mutate: func(value *ProofResponsibility) { value.Proof.ProviderCredential = other }},
	} {
		t.Run(test.name, func(t *testing.T) {
			contradictory := responsibility
			test.mutate(&contradictory)
			if _, err := validatedResponsibilityProof(contradictory); !errors.Is(err, ErrRevisionConflict) {
				t.Fatalf("contradictory evidence error = %v, want revision conflict", err)
			}
		})
	}
}

func TestProviderCredentialEvidenceIsPrivateInPublicLifecycleJSON(t *testing.T) {
	responsibility := testProofResponsibility(t)
	for name, value := range map[string]any{
		"operation": responsibility.Operation,
		"binding":   responsibility.Binding,
		"proof":     responsibility.Proof,
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			serialized := string(raw)
			if strings.Contains(serialized, responsibility.Operation.ProviderCredential.Key) ||
				strings.Contains(serialized, string(responsibility.Operation.ProviderCredential.Seal)) ||
				strings.Contains(serialized, "provider_credential") {
				t.Fatalf("public JSON exposed provider credential evidence: %s", serialized)
			}
		})
	}
}

func testProofResponsibility(t *testing.T) ProofResponsibility {
	t.Helper()
	proof := testVerifiedProof(time.Date(2026, 8, 30, 21, 0, 0, 0, time.UTC))
	operation := Operation{
		OperationID: proof.OriginalOperationID, Kind: OperationConnect, PrincipalID: proof.MintingStoreID,
		Interface: proof.Interface, Challenge: proof.Challenge, State: StateBound, Revision: 3, BindingRevision: 1,
		ExternalAccountRef: proof.ExternalAccountRef, ConversationRef: proof.ConversationRef,
		ConversationScope: proof.ConversationScope, AccountPresentation: proof.AccountPresentation,
		SaveProof: true, ProofID: proof.ProofID, ProofRevision: proof.Revision, ProofStatus: ProofPending,
		CompletedAt: proof.VerifiedAt, ProviderCredential: proof.ProviderCredential,
	}
	binding := Binding{
		PrincipalID: operation.PrincipalID, Interface: operation.Interface,
		ExternalAccountRef: operation.ExternalAccountRef, ConversationRef: operation.ConversationRef,
		ConversationScope: operation.ConversationScope, AccountPresentation: operation.AccountPresentation,
		Revision: operation.BindingRevision, Status: BindingCurrent, Source: BindingSourceLiveVerification,
		ProofID: operation.ProofID, ProofRevision: operation.ProofRevision, OperationID: operation.OperationID,
		UpdatedAt: operation.CompletedAt, ProviderCredential: operation.ProviderCredential,
	}
	projected := proofFromOperation(operation, binding)
	if _, err := uuid.Parse(projected.ProofID); err != nil {
		t.Fatal(err)
	}
	return ProofResponsibility{Operation: operation, Binding: binding, Proof: projected}
}
