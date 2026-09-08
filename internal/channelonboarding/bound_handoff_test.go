package channelonboarding

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
	"path/filepath"
	"testing"
	"time"
)

func TestRestoredOriginalValueCanResumeBoundReset(t *testing.T) {
	now := time.Date(2026, 8, 31, 20, 30, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	parentID := uuid.NewString()
	reservations := credentialReservations(candidate)
	written, err := credentials.Admit(context.Background(), CredentialWriteRequest{
		StoreKey: operationCredentialStoreKey(reservations[0].StoreKey, parentID, reservations[0].Role),
		Value:    "old-provider-token", Receipt: credentialReceipt(parentID, reservations[0].Role),
	})
	if err != nil {
		t.Fatal(err)
	}
	admission := CredentialAdmission{
		Role: reservations[0].Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten,
		Receipt: written.Receipt, ValueSeal: written.ValueSeal,
	}
	identityID := uuid.NewString()
	op := Operation{
		OperationID: parentID, RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(), PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseAwaitingOperatorConfirmation, Revision: 5, CredentialReservations: reservations,
		CredentialAdmissions: []CredentialAdmission{admission}, IdentityOperationID: identityID, BindingRevision: 1,
		RequestedAt: now, UpdatedAt: now,
	}
	op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
	identities := &cancellationTestIdentities{
		operation: operatorchannel.Operation{
			OperationID: identityID, OnboardingOperationID: parentID, State: operatorchannel.StateBound,
			Revision: 3, BindingRevision: 1, ProviderCredential: runtimecredentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal},
		},
		binding: operatorchannel.Binding{
			PrincipalID: "principal-a", Interface: candidate.Interface, Revision: 1, Status: operatorchannel.BindingCurrent,
			ExternalAccountRef: "account-a", ConversationRef: "conversation-a", ConversationScope: operatorchannel.ConversationScopeDirect,
			ProviderCredential: runtimecredentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal},
		},
		bindingErr: operatorchannel.ErrCredentialStale,
	}
	store := &cancellationTestStore{op: op}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: fileBackedHandoffIdentities{cancellationTestIdentities: identities, store: credentialStore}, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
		Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := credentialStore.Set(context.Background(), admission.StoreKey, "rotated-token"); err != nil {
		t.Fatal(err)
	}
	result, err := service.Retry(context.Background(), RetryInput{OperationID: parentID})
	var required *CredentialRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("stale bound retry = %#v err=%v", result, err)
	}
	if store.op.Phase != PhasePreparing || store.op.IdentityOperationID != "" || store.op.BindingRevision != 1 || len(store.op.CredentialAdmissions) != 0 {
		t.Fatalf("reset parent = %#v", store.op)
	}
	if _, found, err := credentialStore.Get(context.Background(), admission.StoreKey); err != nil || !found {
		t.Fatalf("rotated raw value found=%v err=%v", found, err)
	}

	identities.operation = operatorchannel.Operation{}
	identities.bindingErr = nil
	result, err = service.Retry(context.Background(), RetryInput{OperationID: parentID, ProviderCredential: "old-provider-token"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operation.Phase != PhaseAwaitingExternalIdentity || identities.beginKind != operatorchannel.OperationReconnect || identities.beginExpectedRevision != 1 {
		t.Fatalf("replacement ceremony = result:%#v kind:%s revision:%d", result.Operation, identities.beginKind, identities.beginExpectedRevision)
	}
	if identities.operation.OperationID == identityID || identities.operation.OnboardingOperationID != parentID {
		t.Fatalf("replacement child = %#v", identities.operation)
	}
}

type fileBackedHandoffIdentities struct {
	*cancellationTestIdentities
	store runtimecredentials.Store
}

func (i fileBackedHandoffIdentities) CurrentBinding(ctx context.Context, _ operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error) {
	current, err := runtimecredentials.CurrentValueMatchesSeal(ctx, i.store, i.binding.ProviderCredential)
	if err != nil {
		return i.binding, err
	}
	if !current {
		return i.binding, operatorchannel.ErrCredentialStale
	}
	return i.binding, nil
}
func (i fileBackedHandoffIdentities) CurrentBindingReadiness(ctx context.Context, id operatorchannel.InterfaceIdentity) (operatorchannel.Binding, bool, error) {
	b, e := i.CurrentBinding(ctx, id)
	return b, e == nil, e
}
