package channelonboarding

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func TestOverdueIdentityTerminalizesOnboardingAndReleasesWrittenCredentials(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
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
	operationID := uuid.NewString()
	reservations := credentialReservations(candidate)
	providerReservation := reservations[0]
	written, err := credentials.Admit(context.Background(), CredentialWriteRequest{
		StoreKey: operationCredentialStoreKey(providerReservation.StoreKey, operationID, providerReservation.Role),
		Value:    "token", Receipt: credentialReceipt(operationID, providerReservation.Role),
	})
	if err != nil {
		t.Fatal(err)
	}
	identityOperationID := uuid.NewString()
	op := Operation{
		OperationID: operationID, RequestKeyHash: "expired-key", RequestHash: "expired-request", PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseAwaitingExternalIdentity, Revision: 3, SaveProof: true, CredentialReservations: reservations,
		CredentialAdmissions: []CredentialAdmission{{Role: candidate.ProviderCredentialRole, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch}},
		IdentityOperationID:  identityOperationID, RequestedAt: now, UpdatedAt: now,
	}
	op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
	store := &cancellationTestStore{op: op}
	identities := &cancellationTestIdentities{operation: operatorchannel.Operation{
		OperationID: identityOperationID, State: operatorchannel.StateAwaitingClaim, Revision: 1, ExpiresAt: now.Add(-time.Second),
	}}
	activations := &cancellationTestActivations{}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Retry(context.Background(), RetryInput{OperationID: op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operation.Phase != PhaseFailed || result.Operation.FailureCode != "identity_expired" {
		t.Fatalf("expired result = %#v", result.Operation)
	}
	if _, found, err := credentialStore.Get(context.Background(), written.StoreKey); err != nil || found {
		t.Fatalf("expired credential found=%v err=%v", found, err)
	}
	if activations.refreshes != 1 {
		t.Fatalf("activation refreshes = %d, want 1", activations.refreshes)
	}
	if identities.expirations != 1 {
		t.Fatalf("identity expirations = %d, want 1", identities.expirations)
	}
}

func TestOverdueAwaitingConfirmationTerminalizesParentOperation(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	identityOperationID := uuid.NewString()
	op := Operation{
		OperationID: uuid.NewString(), RequestKeyHash: "expired-confirm-key", RequestHash: "expired-confirm-request", PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseAwaitingOperatorConfirmation, Revision: 4, SaveProof: true, IdentityOperationID: identityOperationID,
		RequestedAt: now, UpdatedAt: now,
	}
	op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
	store := &cancellationTestStore{op: op}
	identities := &cancellationTestIdentities{operation: operatorchannel.Operation{
		OperationID: identityOperationID, State: operatorchannel.StateAwaitingConfirmation, Revision: 2, ExpiresAt: now.Add(-time.Second),
	}}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Retry(context.Background(), RetryInput{OperationID: op.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operation.Phase != PhaseFailed || result.Operation.FailureCode != "identity_expired" {
		t.Fatalf("expired confirmation result = %#v", result.Operation)
	}
	if identities.expirations != 1 {
		t.Fatalf("identity expirations = %d, want 1", identities.expirations)
	}
}

func TestRecoveryTerminalizesOperationWhoseRuntimeContextIsNoLongerCurrent(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	emptyCatalog, err := NewCandidateCatalog(nil)
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
	operationID := uuid.NewString()
	reservations := credentialReservations(candidate)
	providerReservation := reservations[0]
	written, err := credentials.Admit(context.Background(), CredentialWriteRequest{
		StoreKey: operationCredentialStoreKey(providerReservation.StoreKey, operationID, providerReservation.Role),
		Value:    "token", Receipt: credentialReceipt(operationID, providerReservation.Role),
	})
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{
		OperationID: operationID, RequestKeyHash: "obsolete-key", RequestHash: "obsolete-request", PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseAwaitingExternalIdentity, Revision: 3, SaveProof: true, CredentialReservations: reservations,
		CredentialAdmissions: []CredentialAdmission{{Role: candidate.ProviderCredentialRole, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch}},
		RequestedAt:          now, UpdatedAt: now,
	}
	op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
	store := &cancellationTestStore{op: op}
	activations := &cancellationTestActivations{}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: &cancellationTestIdentities{}, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) { return emptyCatalog, nil }, Activations: activations,
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Recover(context.Background()); err != nil {
		t.Fatalf("Recover obsolete operation: %v", err)
	}
	if store.op.Phase != PhaseFailed || store.op.FailureCode != "runtime_context_retired" {
		t.Fatalf("obsolete operation = %#v, want terminal runtime_context_retired", store.op)
	}
	if _, found, err := credentialStore.Get(context.Background(), written.StoreKey); err != nil || found {
		t.Fatalf("obsolete operation credential found=%v err=%v", found, err)
	}
	if activations.refreshes != 1 {
		t.Fatalf("activation refreshes = %d, want 1", activations.refreshes)
	}
}

func TestRecoveryFailsClosedWhenCurrentCandidateCatalogCannotBeBuilt(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	written, err := credentials.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider.operation", Value: "token", Receipt: "blocked/provider"})
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{
		OperationID: uuid.NewString(), RequestKeyHash: "blocked-key", RequestHash: "blocked-request", PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseAwaitingExternalIdentity, Revision: 3, SaveProof: true, CredentialReservations: credentialReservations(candidate),
		CredentialAdmissions: []CredentialAdmission{{Role: candidate.ProviderCredentialRole, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch}},
		RequestedAt:          now, UpdatedAt: now,
	}
	op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
	store := &cancellationTestStore{op: op}
	activations := &cancellationTestActivations{}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: &cancellationTestIdentities{}, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) {
			return nil, fmt.Errorf("%w: duplicate current coordinates", ErrConflict)
		}, Activations: activations,
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Recover(context.Background()); !errors.Is(err, ErrConflict) {
		t.Fatalf("Recover malformed current catalog error = %v, want conflict", err)
	}
	if store.op.Phase != op.Phase || store.op.Revision != op.Revision {
		t.Fatalf("operation mutated after catalog failure = %#v", store.op)
	}
	if _, found, err := credentialStore.Get(context.Background(), written.StoreKey); err != nil || !found {
		t.Fatalf("credential retained found=%v err=%v", found, err)
	}
	if activations.refreshes != 0 {
		t.Fatalf("activation refreshes = %d, want 0", activations.refreshes)
	}
}

func TestReadbackUsesCurrentActivationOwningOperationInsteadOfLaterFailedAttempt(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	current := Operation{
		OperationID: uuid.NewString(), SlotKey: StartRequest{Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector}.SlotKey(),
		PrincipalID: "principal-a", Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface,
		Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseSucceeded, Revision: 7, BindingRevision: 3, ActivationRevision: 1, RequestedAt: now, UpdatedAt: now,
	}
	failed := current
	failed.OperationID = uuid.NewString()
	failed.Verb = VerbReconnect
	failed.Phase = PhaseFailed
	failed.Revision = 3
	failed.ActivationRevision = 0
	failed.FailureCode = "provider_denied"
	failed.UpdatedAt = now.Add(time.Minute)
	activation := testCurrentActivation(current, nil, now)
	activation.OperationID = current.OperationID
	activation.OperationRevision = current.Revision
	store := &readbackTestStore{
		cancellationTestStore: &cancellationTestStore{op: current, activation: activation},
		operations:            []Operation{current, failed},
	}
	identity := operatorchannel.Readback{
		PrincipalID: "principal-a", Interface: candidate.Interface, Status: operatorchannel.BindingCurrent, BindingRevision: 3,
	}
	identities := &readbackTestIdentities{
		cancellationTestIdentities: &cancellationTestIdentities{binding: operatorchannel.Binding{
			PrincipalID: "principal-a", Interface: candidate.Interface, Revision: 3, Status: operatorchannel.BindingCurrent,
		}},
		rows: []operatorchannel.Readback{identity},
	}
	readiness := &operationReadinessProjector{readyOperationID: current.OperationID}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
		Confirmation: cancellationTestConfirmation{}, Readiness: readiness, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := service.ReadbackConnectedChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Operation == nil || rows[0].Operation.OperationID != current.OperationID {
		t.Fatalf("readback rows = %#v, want activation-owning operation %s", rows, current.OperationID)
	}
	if rows[0].Readiness == nil || !rows[0].Readiness.Ready || readiness.seenOperationID != current.OperationID {
		t.Fatalf("readiness = %#v seen=%q, want current activation owner", rows[0].Readiness, readiness.seenOperationID)
	}
}

func TestReadbackIgnoresTerminalOperationWithoutRetainedIdentity(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	current := Operation{
		OperationID: uuid.NewString(), PrincipalID: "principal-a", Verb: VerbConnect, Provider: candidate.Provider,
		Interface: candidate.Interface, Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector,
		Posture: candidate.Posture, Ceremony: candidate.Ceremony, Phase: PhaseSucceeded, Revision: 9,
		BindingRevision: 3, RequestedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}
	current.SlotKey = StartRequest{Provider: current.Provider, Interface: current.Interface, Coordinate: current.Coordinate, TargetSelector: current.TargetSelector}.SlotKey()
	orphanCandidate := testCandidate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "orphan")
	orphanCandidate.Interface.ChannelPackID = "provider.telegram.orphan_hitl_channel"
	orphanCandidate.Interface.SemanticGeneration = "sha256:orphan-plan"
	orphanCandidate.Interface = orphanCandidate.Interface.Normalized()
	orphan := Operation{
		OperationID: uuid.NewString(), PrincipalID: "principal-a", Verb: VerbConnect, Provider: orphanCandidate.Provider,
		Interface: orphanCandidate.Interface, Coordinate: orphanCandidate.Coordinate, TargetSelector: orphanCandidate.Target.Selector,
		Posture: orphanCandidate.Posture, Ceremony: orphanCandidate.Ceremony, Phase: PhaseFailed, Revision: 4,
		RequestedAt: now, UpdatedAt: now,
	}
	orphan.SlotKey = StartRequest{Provider: orphan.Provider, Interface: orphan.Interface, Coordinate: orphan.Coordinate, TargetSelector: orphan.TargetSelector}.SlotKey()
	activation := testCurrentActivation(current, nil, now)
	store := &readbackTestStore{
		cancellationTestStore: &cancellationTestStore{},
		operations:            []Operation{current, orphan},
		activations:           []ConnectedChannelActivation{activation},
	}
	identities := &readbackTestIdentities{
		cancellationTestIdentities: &cancellationTestIdentities{binding: operatorchannel.Binding{
			PrincipalID: "principal-a", Interface: candidate.Interface, Revision: 3, Status: operatorchannel.BindingCurrent,
		}},
		rows: []operatorchannel.Readback{{PrincipalID: "principal-a", Interface: candidate.Interface, Status: operatorchannel.BindingCurrent, BindingRevision: 3}},
	}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: testCredentialWriter(t),
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
		Confirmation: cancellationTestConfirmation{}, Readiness: &operationReadinessProjector{readyOperationID: current.OperationID}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := service.ReadbackConnectedChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Activation == nil || rows[0].Activation.OperationID != current.OperationID {
		t.Fatalf("readback rows = %#v, want only retained current authority", rows)
	}
}

func TestReadbackRetainsExactRowsForMultipleCurrentContexts(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	firstCandidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support-a")
	secondCandidate := testCandidate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "support-b")
	first := testSucceededOperation(firstCandidate, now.Add(-time.Minute))
	second := testSucceededOperation(secondCandidate, now)
	firstActivation := testCurrentActivation(first, nil, now)
	secondActivation := testCurrentActivation(second, nil, now)
	store := &readbackTestStore{
		cancellationTestStore: &cancellationTestStore{}, operations: []Operation{first, second},
		activations: []ConnectedChannelActivation{firstActivation, secondActivation},
	}
	identity := operatorchannel.Readback{PrincipalID: "principal-a", Interface: firstCandidate.Interface, Status: operatorchannel.BindingCurrent, BindingRevision: 3}
	identities := &readbackTestIdentities{
		cancellationTestIdentities: &cancellationTestIdentities{binding: operatorchannel.Binding{PrincipalID: "principal-a", Interface: firstCandidate.Interface, Revision: 3, Status: operatorchannel.BindingCurrent}},
		rows:                       []operatorchannel.Readback{identity},
	}
	catalog, err := NewCandidateCatalog([]Candidate{firstCandidate, secondCandidate})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: testCredentialWriter(t),
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := service.ReadbackConnectedChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Activation == nil || rows[1].Activation == nil || rows[0].Activation.SlotKey == rows[1].Activation.SlotKey {
		t.Fatalf("readback rows = %#v, want two exact activation coordinates", rows)
	}
}

func TestReplacementCredentialHandoffPreservesPredecessorOnFailureAndRetiresItAfterPublication(t *testing.T) {
	for _, test := range []struct {
		name            string
		activationError error
		wantSucceeded   bool
	}{
		{name: "failed successor preserves predecessor", activationError: NewTerminalActivationError("replacement_denied", errors.New("replacement denied"))},
		{name: "published successor retires predecessor", wantSucceeded: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
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
			predecessor := testSucceededOperation(candidate, now.Add(-time.Hour))
			predecessorAdmissions := make([]CredentialAdmission, 0, len(predecessor.CredentialReservations))
			for _, reservation := range predecessor.CredentialReservations {
				written, writeErr := credentials.Admit(context.Background(), CredentialWriteRequest{StoreKey: operationCredentialStoreKey(reservation.StoreKey, predecessor.OperationID, reservation.Role), Value: "predecessor-" + reservation.Role, Receipt: credentialReceipt(predecessor.OperationID, reservation.Role)})
				if writeErr != nil {
					t.Fatal(writeErr)
				}
				predecessorAdmissions = append(predecessorAdmissions, CredentialAdmission{Role: reservation.Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch})
			}
			predecessor.CredentialAdmissions = predecessorAdmissions
			store := &cancellationTestStore{activation: testCurrentActivation(predecessor, predecessorAdmissions, now), history: []Operation{predecessor}}
			identities := &cancellationTestIdentities{binding: operatorchannel.Binding{PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a", Revision: 3, Status: operatorchannel.BindingCurrent}}
			service, err := NewService(ServiceOptions{
				Store: store, Identities: identities, Credentials: credentials, Catalog: func() (*CandidateCatalog, error) { return catalog, nil },
				Activations: &cancellationTestActivations{err: test.activationError}, Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Start(context.Background(), StartInput{Verb: VerbReconnect, Selection: CandidateSelection{Provider: candidate.Provider}, ProviderCredential: "successor-token"})
			if err != nil {
				t.Fatal(err)
			}
			if test.wantSucceeded != (result.Operation.Phase == PhaseSucceeded) {
				t.Fatalf("operation phase = %s", result.Operation.Phase)
			}
			providerPredecessor := predecessorAdmissions[0]
			_, predecessorFound, readErr := credentialStore.Get(context.Background(), providerPredecessor.StoreKey)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if predecessorFound == test.wantSucceeded {
				t.Fatalf("predecessor credential found=%v, want %v", predecessorFound, !test.wantSucceeded)
			}
			if len(predecessorAdmissions) > 1 {
				if _, signingFound, signingErr := credentialStore.Get(context.Background(), predecessorAdmissions[1].StoreKey); signingErr != nil || !signingFound {
					t.Fatalf("retained signing credential found=%v err=%v", signingFound, signingErr)
				}
			}
			if !test.wantSucceeded && store.activation.OperationID != predecessor.OperationID {
				t.Fatalf("failed replacement changed activation = %#v", store.activation)
			}
			if len(result.Operation.CredentialAdmissions) == 0 || result.Operation.CredentialAdmissions[0].StoreKey == providerPredecessor.StoreKey {
				t.Fatalf("successor admissions = %#v, want operation-owned occurrence", result.Operation.CredentialAdmissions)
			}
		})
	}
}

func TestReplacementCredentialPreflightFailureKeepsOperationRetryableAndPredecessorCurrent(t *testing.T) {
	for _, verb := range []Verb{VerbReconnect, VerbRebind} {
		t.Run(string(verb), func(t *testing.T) {
			now := time.Date(2026, 8, 27, 2, 0, 0, 0, time.UTC)
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
			predecessor := testSucceededOperation(candidate, now.Add(-time.Hour))
			predecessorAdmissions := writeTestOperationCredentials(t, credentials, predecessor, "predecessor")
			predecessor.CredentialAdmissions = predecessorAdmissions
			store := &cancellationTestStore{activation: testCurrentActivation(predecessor, predecessorAdmissions, now), history: []Operation{predecessor}}
			identities := &cancellationTestIdentities{binding: operatorchannel.Binding{
				PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a",
				Revision: 3, Status: operatorchannel.BindingCurrent,
			}}
			activations := &cancellationTestActivations{preflightErr: errors.New("telegram rejected replacement token")}
			service, err := NewService(ServiceOptions{
				Store: store, Identities: identities, Credentials: credentials,
				Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
				Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}

			blocked, err := service.Start(context.Background(), StartInput{
				Verb: verb, Selection: CandidateSelection{Provider: candidate.Provider}, ProviderCredential: "invalid-successor-token",
			})
			if err == nil || !strings.Contains(err.Error(), "rejected replacement token") {
				t.Fatalf("invalid replacement error = %v", err)
			}
			if blocked.Operation.Phase != PhasePreparing || store.op.Phase != PhasePreparing || len(store.op.CredentialAdmissions) != 0 {
				t.Fatalf("blocked replacement operation = %#v, want retryable preparing without admitted credentials", store.op)
			}
			if store.activation.OperationID != predecessor.OperationID || store.activation.Status != ActivationCurrent {
				t.Fatalf("blocked replacement changed predecessor activation = %#v", store.activation)
			}
			for _, reservation := range store.op.CredentialReservations {
				key := operationCredentialStoreKey(reservation.StoreKey, store.op.OperationID, reservation.Role)
				if _, found, getErr := credentialStore.Get(context.Background(), key); getErr != nil || found {
					t.Fatalf("failed successor credential %q found=%v err=%v", key, found, getErr)
				}
			}

			activations.preflightErr = nil
			retried, err := service.Retry(context.Background(), RetryInput{OperationID: store.op.OperationID, ProviderCredential: "valid-successor-token"})
			if err != nil {
				t.Fatalf("retry replacement credential: %v", err)
			}
			if retried.Operation.Phase == PhasePreparing {
				t.Fatalf("retried replacement remained in preparing: %#v", retried.Operation)
			}
			if verb == VerbReconnect && (retried.Operation.Phase != PhaseSucceeded || store.activation.OperationID != store.op.OperationID) {
				t.Fatalf("retried reconnect = operation:%#v activation:%#v", retried.Operation, store.activation)
			}
			if verb == VerbRebind && store.activation.OperationID != predecessor.OperationID {
				t.Fatalf("pending rebind replaced predecessor before identity settlement: %#v", store.activation)
			}
		})
	}
}

func TestStartupLocalReconciliationSettlesOnlyProcessIndependentStates(t *testing.T) {
	now := time.Date(2026, 8, 27, 5, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	currentCatalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	retiredCatalog, err := NewCandidateCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		catalog     *CandidateCatalog
		state       operatorchannel.OperationState
		expiresAt   time.Time
		wantPhase   Phase
		wantFailure string
	}{
		{name: "valid pending remains pending", catalog: currentCatalog, state: operatorchannel.StateAwaitingClaim, expiresAt: now.Add(time.Hour), wantPhase: PhaseAwaitingExternalIdentity},
		{name: "expired settles parent", catalog: currentCatalog, state: operatorchannel.StateExpired, wantPhase: PhaseFailed, wantFailure: "identity_expired"},
		{name: "rejected settles parent", catalog: currentCatalog, state: operatorchannel.StateRejected, wantPhase: PhaseFailed, wantFailure: "identity_rejected"},
		{name: "retired context settles parent", catalog: retiredCatalog, state: operatorchannel.StateAwaitingClaim, expiresAt: now.Add(time.Hour), wantPhase: PhaseFailed, wantFailure: "runtime_context_retired"},
	} {
		t.Run(test.name, func(t *testing.T) {
			identityOperationID := uuid.NewString()
			op := Operation{
				OperationID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
				PrincipalID: "principal-a", Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface,
				Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector, Posture: candidate.Posture,
				Ceremony: candidate.Ceremony, Phase: PhaseAwaitingExternalIdentity, Revision: 3,
				IdentityOperationID: identityOperationID, RequestedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
			}
			op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
			store := &cancellationTestStore{op: op}
			identities := &cancellationTestIdentities{operation: operatorchannel.Operation{
				OperationID: identityOperationID, State: test.state, Revision: 2, ExpiresAt: test.expiresAt,
			}}
			activations := &cancellationTestActivations{}
			service, err := NewService(ServiceOptions{
				Store: store, Identities: identities, Credentials: testCredentialWriter(t),
				Catalog: func() (*CandidateCatalog, error) { return test.catalog, nil }, Activations: activations,
				Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := service.ReconcileLocal(context.Background()); err != nil {
				t.Fatal(err)
			}
			if store.op.Phase != test.wantPhase || store.op.FailureCode != test.wantFailure {
				t.Fatalf("local reconciliation = phase %s failure %q, want %s/%q", store.op.Phase, store.op.FailureCode, test.wantPhase, test.wantFailure)
			}
			if activations.preflights != 0 || activations.refreshes != 0 || activations.publications != 0 || activations.promotions != 0 {
				t.Fatalf("local barrier executed external/runtime effects: %#v", activations)
			}
		})
	}
}

func TestChannelOnboardingRecoversStagedCredentialAfterAdmissionCommitFailure(t *testing.T) {
	for _, verb := range []Verb{VerbConnect, VerbReconnect, VerbRebind} {
		t.Run(string(verb), func(t *testing.T) {
			now := time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC)
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
			store := &cancellationTestStore{advanceErrOnce: errors.New("selected-store admission unavailable")}
			identities := &cancellationTestIdentities{}
			if verb != VerbConnect {
				predecessor := testSucceededOperation(candidate, now.Add(-time.Hour))
				admissions := writeTestOperationCredentials(t, credentials, predecessor, "predecessor")
				predecessor.CredentialAdmissions = admissions
				store.activation = testCurrentActivation(predecessor, admissions, now.Add(-time.Hour))
				store.history = []Operation{predecessor}
				identities.binding = operatorchannel.Binding{
					PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a",
					Revision: 3, Status: operatorchannel.BindingCurrent,
				}
			}
			service, err := NewService(ServiceOptions{
				Store: store, Identities: identities, Credentials: credentials,
				Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
				Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}

			if _, err := service.Start(context.Background(), StartInput{
				Verb: verb, Selection: CandidateSelection{Provider: candidate.Provider}, ProviderCredential: "operation-token",
			}); err == nil || !strings.Contains(err.Error(), "selected-store admission unavailable") {
				t.Fatalf("initial advance error = %v", err)
			}
			if store.op.Phase != PhasePreparing || len(store.op.CredentialAdmissions) != 0 {
				t.Fatalf("operation after lost admission = %#v", store.op)
			}

			if err := service.Recover(context.Background()); err != nil {
				t.Fatalf("recover deterministic credential occurrence: %v", err)
			}
			if store.op.Phase == PhasePreparing || len(store.op.CredentialAdmissions) != len(store.op.CredentialReservations) {
				t.Fatalf("recovered operation = %#v", store.op)
			}
			for _, admission := range store.op.CredentialAdmissions {
				if admission.Receipt != credentialReceipt(store.op.OperationID, admission.Role) && admission.Kind == CredentialAdmissionWritten {
					t.Fatalf("recovered written admission = %#v", admission)
				}
			}
		})
	}
}

func TestRetiredPreparingOperationReleasesUncommittedStagedCredentials(t *testing.T) {
	for _, verb := range []Verb{VerbConnect, VerbReconnect, VerbRebind} {
		for _, recovery := range []string{"local", "startup"} {
			t.Run(string(verb)+"/"+recovery, func(t *testing.T) {
				now := time.Date(2026, 8, 27, 13, 45, 0, 0, time.UTC)
				candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
				currentCatalog, err := NewCandidateCatalog([]Candidate{candidate})
				if err != nil {
					t.Fatal(err)
				}
				retiredCatalog, err := NewCandidateCatalog(nil)
				if err != nil {
					t.Fatal(err)
				}
				catalog := currentCatalog
				credentialPath := filepath.Join(t.TempDir(), "credentials.json")
				credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
				if err != nil {
					t.Fatal(err)
				}
				credentials, err := NewCredentialWriter(credentialStore)
				if err != nil {
					t.Fatal(err)
				}
				store := &cancellationTestStore{advanceErrOnce: errors.New("selected-store admission unavailable")}
				identities := &cancellationTestIdentities{}
				var predecessorAdmissions []CredentialAdmission
				if verb != VerbConnect {
					predecessor := testSucceededOperation(candidate, now.Add(-time.Hour))
					predecessorAdmissions = writeTestOperationCredentials(t, credentials, predecessor, "predecessor")
					predecessor.CredentialAdmissions = predecessorAdmissions
					store.activation = testCurrentActivation(predecessor, predecessorAdmissions, now.Add(-time.Hour))
					store.history = []Operation{predecessor}
					identities.binding = operatorchannel.Binding{
						PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a",
						Revision: 3, Status: operatorchannel.BindingCurrent,
					}
				}
				newService := func(writer *CredentialWriter) *Service {
					service, serviceErr := NewService(ServiceOptions{
						Store: store, Identities: identities, Credentials: writer,
						Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
						Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
					})
					if serviceErr != nil {
						t.Fatal(serviceErr)
					}
					return service
				}

				if _, err := newService(credentials).Start(context.Background(), StartInput{
					Verb: verb, Selection: CandidateSelection{Provider: candidate.Provider}, ProviderCredential: "operation-token",
				}); err == nil || !strings.Contains(err.Error(), "selected-store admission unavailable") {
					t.Fatalf("initial advance error = %v", err)
				}
				if store.op.Phase != PhasePreparing || len(store.op.CredentialAdmissions) != 0 {
					t.Fatalf("operation after lost admission checkpoint = %#v", store.op)
				}
				providerReservation := store.op.CredentialReservations[0]
				stagedKey := operationCredentialStoreKey(providerReservation.StoreKey, store.op.OperationID, providerReservation.Role)
				if _, found, err := credentialStore.Get(context.Background(), stagedKey); err != nil || !found {
					t.Fatalf("staged credential before retirement found=%v err=%v", found, err)
				}

				// Reopen the credential owner to prove crash/startup rediscovery rather than in-memory cleanup.
				reopenedStore, err := runtimecredentials.NewFileStore(credentialPath)
				if err != nil {
					t.Fatal(err)
				}
				reopenedCredentials, err := NewCredentialWriter(reopenedStore)
				if err != nil {
					t.Fatal(err)
				}
				catalog = retiredCatalog
				restarted := newService(reopenedCredentials)
				if recovery == "local" {
					err = restarted.ReconcileLocal(context.Background())
				} else {
					err = restarted.Recover(context.Background())
				}
				if err != nil {
					t.Fatalf("%s retirement recovery: %v", recovery, err)
				}
				if store.op.Phase != PhaseFailed || store.op.FailureCode != "runtime_context_retired" {
					t.Fatalf("retired operation = %#v", store.op)
				}
				if _, found, err := reopenedStore.Get(context.Background(), stagedKey); err != nil || found {
					t.Fatalf("retired staged credential found=%v err=%v", found, err)
				}
				for _, admission := range predecessorAdmissions {
					if _, found, err := reopenedStore.Get(context.Background(), admission.StoreKey); err != nil || !found {
						t.Fatalf("predecessor credential %q found=%v err=%v", admission.StoreKey, found, err)
					}
				}
			})
		}
	}
}

func TestChannelOnboardingIdentityExpiryRevisionRaceConvergesParent(t *testing.T) {
	for _, successorState := range []operatorchannel.OperationState{
		operatorchannel.StateExpired,
		operatorchannel.StateAwaitingConfirmation,
		operatorchannel.StateBound,
	} {
		t.Run(string(successorState), func(t *testing.T) {
			now := time.Date(2026, 8, 27, 3, 30, 0, 0, time.UTC)
			candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
			catalog, err := NewCandidateCatalog([]Candidate{candidate})
			if err != nil {
				t.Fatal(err)
			}
			identityOperationID := uuid.NewString()
			op := Operation{
				OperationID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(), SlotKey: "slot-a",
				PrincipalID: "principal-a", Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface,
				Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector, Posture: candidate.Posture,
				Ceremony: candidate.Ceremony, Phase: PhaseAwaitingExternalIdentity, Revision: 3,
				IdentityOperationID: identityOperationID, RequestedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
			}
			store := &cancellationTestStore{op: op}
			identities := &cancellationTestIdentities{
				operation:       operatorchannel.Operation{OperationID: identityOperationID, State: operatorchannel.StateAwaitingClaim, Revision: 1, ExpiresAt: now.Add(-time.Second)},
				expiryRaceState: successorState,
				binding:         operatorchannel.Binding{PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a", Revision: 2, Status: operatorchannel.BindingCurrent},
			}
			service, err := NewService(ServiceOptions{
				Store: store, Identities: identities, Credentials: testCredentialWriter(t),
				Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: &cancellationTestActivations{},
				Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := service.Recover(context.Background()); err != nil {
				t.Fatalf("recover raced identity state: %v", err)
			}
			if successorState == operatorchannel.StateExpired && store.op.Phase != PhaseFailed {
				t.Fatalf("expired parent phase = %s", store.op.Phase)
			}
			if successorState != operatorchannel.StateExpired && store.op.Phase == PhaseAwaitingExternalIdentity {
				t.Fatalf("parent did not consume raced identity state: %#v", store.op)
			}
		})
	}
}

func TestChannelOnboardingRecoveryRetriesProcessPublicationBeforePromotion(t *testing.T) {
	now := time.Date(2026, 8, 27, 4, 0, 0, 0, time.UTC)
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	op := testSucceededOperation(candidate, now)
	op.Phase = PhasePublishingActivation
	op.CompletedAt = time.Time{}
	op.Revision = 5
	op.BindingRevision = 3
	store := &cancellationTestStore{op: op}
	activations := &cancellationTestActivations{publishErrOnce: errors.New("process publication unavailable")}
	service, err := NewService(ServiceOptions{
		Store: store,
		Identities: &cancellationTestIdentities{binding: operatorchannel.Binding{
			PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a", Revision: 3, Status: operatorchannel.BindingCurrent,
		}},
		Credentials: testCredentialWriter(t), Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
		Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Retry(context.Background(), RetryInput{OperationID: op.OperationID}); err == nil || !strings.Contains(err.Error(), "process publication unavailable") {
		t.Fatalf("first process publication error = %v", err)
	}
	if store.op.Phase != PhasePublishingProcessActivation || activations.promotions != 0 {
		t.Fatalf("operation crossed failed process handoff: phase=%s promotions=%d", store.op.Phase, activations.promotions)
	}
	activations.publishErrOnce = errors.New("process publication still unavailable after restart")
	if err := service.Recover(context.Background()); err == nil || !strings.Contains(err.Error(), "still unavailable") {
		t.Fatalf("repeated process publication error = %v", err)
	}
	if store.op.Phase != PhasePublishingProcessActivation || activations.promotions != 0 {
		t.Fatalf("operation crossed repeated failed process handoff: phase=%s promotions=%d", store.op.Phase, activations.promotions)
	}
	if err := service.Recover(context.Background()); err != nil {
		t.Fatalf("recover process publication: %v", err)
	}
	if store.op.Phase != PhaseSucceeded || activations.publications != 3 || activations.promotions != 1 {
		t.Fatalf("recovered handoff phase=%s publications=%d promotions=%d", store.op.Phase, activations.publications, activations.promotions)
	}
}

func TestReplacementRecoveryRetiresPredecessorCredentialAfterDurableHandoff(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
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
	predecessor := testSucceededOperation(candidate, now.Add(-time.Hour))
	predecessorAdmissions := writeTestOperationCredentials(t, credentials, predecessor, "predecessor")
	predecessor.CredentialAdmissions = predecessorAdmissions
	successor := testSucceededOperation(candidate, now)
	successor.Verb = VerbReconnect
	successor.Phase = PhaseRetiringPredecessor
	successor.ActivationRevision = 1
	successor.CompletedAt = time.Time{}
	successor.ConfirmationOperationID = uuid.NewString()
	successorAdmissions := writeTestOperationCredentials(t, credentials, successor, "successor")
	// The signing occurrence is intentionally retained across the handoff.
	if _, err := credentials.Release(context.Background(), successorAdmissions[1]); err != nil {
		t.Fatal(err)
	}
	successorAdmissions[1] = observedCredentialAdmissionForKey(successor.OperationID, predecessorAdmissions[1].Role, predecessorAdmissions[1].StoreKey, CredentialWriteResult{StoreKey: predecessorAdmissions[1].StoreKey, Epoch: predecessorAdmissions[1].Epoch})
	successor.CredentialAdmissions = successorAdmissions
	activation := testCurrentActivation(successor, successorAdmissions, now)
	store := &cancellationTestStore{op: successor, activation: activation, history: []Operation{predecessor}}
	identities := &cancellationTestIdentities{binding: operatorchannel.Binding{PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a", Revision: 3, Status: operatorchannel.BindingCurrent}}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: credentials, Catalog: func() (*CandidateCatalog, error) { return catalog, nil },
		Activations: &cancellationTestActivations{}, Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.op.Phase != PhaseSucceeded {
		t.Fatalf("recovered phase = %s", store.op.Phase)
	}
	if _, found, err := credentialStore.Get(context.Background(), predecessorAdmissions[0].StoreKey); err != nil || found {
		t.Fatalf("predecessor provider credential found=%v err=%v", found, err)
	}
	for _, admission := range successorAdmissions {
		if _, found, err := credentialStore.Get(context.Background(), admission.StoreKey); err != nil || !found {
			t.Fatalf("successor credential %q found=%v err=%v", admission.StoreKey, found, err)
		}
	}
}

func writeTestOperationCredentials(t *testing.T, credentials *CredentialWriter, op Operation, prefix string) []CredentialAdmission {
	t.Helper()
	admissions := make([]CredentialAdmission, 0, len(op.CredentialReservations))
	for _, reservation := range op.CredentialReservations {
		written, err := credentials.Admit(context.Background(), CredentialWriteRequest{
			StoreKey: operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role),
			Value:    prefix + "-" + reservation.Role, Receipt: credentialReceipt(op.OperationID, reservation.Role),
		})
		if err != nil {
			t.Fatal(err)
		}
		admissions = append(admissions, CredentialAdmission{Role: reservation.Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch})
	}
	return admissions
}

func TestPermanentActivationFailureTerminalizesAndReleasesWrittenCredentials(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
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
	store := &cancellationTestStore{}
	activations := &cancellationTestActivations{err: NewTerminalActivationError("public_ingress_unavailable", errors.New("public ingress is disabled"))}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: &cancellationTestIdentities{}, Credentials: credentials,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Start(context.Background(), StartInput{Verb: VerbConnect, Selection: CandidateSelection{Provider: candidate.Provider}, ProviderCredential: "token", SaveProof: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operation.Phase != PhaseFailed || result.Operation.FailureCode != "public_ingress_unavailable" {
		t.Fatalf("activation result = %#v", result.Operation)
	}
	for _, admission := range result.Operation.CredentialAdmissions {
		if admission.Kind != CredentialAdmissionWritten {
			continue
		}
		if _, found, err := credentialStore.Get(context.Background(), admission.StoreKey); err != nil || found {
			t.Fatalf("failed credential %q found=%v err=%v", admission.StoreKey, found, err)
		}
	}
}

func TestChannelOnboardingClientCancellationStopsNarrationNotOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancellationTestStore{cancelAfterReserve: cancel}
	identities := &cancellationTestIdentities{}
	activations := &cancellationTestActivations{}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Store: store, Identities: identities, Credentials: credentials,
		Catalog:     func() (*CandidateCatalog, error) { return catalog, nil },
		Activations: activations, Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{},
		Now: func() time.Time { return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.Start(ctx, StartInput{
		Verb: VerbConnect, Selection: CandidateSelection{Provider: candidate.Provider},
		IdempotencyKey: "client-cancel", ProviderCredential: "provider-secret", SaveProof: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operation.Phase != PhaseAwaitingExternalIdentity {
		t.Fatalf("phase = %s, want %s", result.Operation.Phase, PhaseAwaitingExternalIdentity)
	}
	if !store.cancelled || ctx.Err() != context.Canceled {
		t.Fatalf("reservation did not cancel the client context: cancelled=%v err=%v", store.cancelled, ctx.Err())
	}
	if store.sawCanceledContext || identities.sawCanceledContext || activations.sawCanceledContext {
		t.Fatalf("admitted responsibility observed client cancellation: store=%v identities=%v activations=%v",
			store.sawCanceledContext, identities.sawCanceledContext, activations.sawCanceledContext)
	}
}

func TestChannelOnboardingRecoveryEveryNonterminalPhase(t *testing.T) {
	for _, phase := range []Phase{
		PhasePreparing,
		PhaseCredentialsAdmitted,
		PhaseActivatingProvider,
		PhaseAwaitingExternalIdentity,
		PhaseAwaitingOperatorConfirmation,
		PhasePublishingActivation,
		PhasePublishingProcessActivation,
		PhasePromotingRegistration,
		PhaseRetiringPredecessor,
		PhaseDeliveringConfirmation,
	} {
		t.Run(string(phase), func(t *testing.T) {
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
			reservations := credentialReservations(candidate)
			admissions := make([]CredentialAdmission, 0, len(reservations))
			for index, reservation := range reservations {
				if err := credentialStore.Set(context.Background(), reservation.StoreKey, "test-secret"); err != nil {
					t.Fatal(err)
				}
				observed, err := credentials.Observe(context.Background(), reservation.StoreKey)
				if err != nil {
					t.Fatal(err)
				}
				admissions = append(admissions, CredentialAdmission{
					Role: reservation.Role, StoreKey: reservation.StoreKey, Kind: CredentialAdmissionObserved,
					Receipt: "recovery-receipt-" + string(rune('a'+index)), Epoch: observed.Epoch,
				})
			}
			now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
			identityOperationID := uuid.NewString()
			op := Operation{
				OperationID: uuid.NewString(), RequestKeyHash: "recovery-key", RequestHash: "recovery-request",
				PrincipalID: "principal-a", Verb: VerbConnect, Provider: candidate.Provider,
				Interface: candidate.Interface, Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector,
				Posture: candidate.Posture, Ceremony: candidate.Ceremony, Phase: phase, Revision: 1, SaveProof: true,
				CredentialReservations: reservations, RequestedAt: now, UpdatedAt: now,
			}
			op.SlotKey = StartRequest{
				Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
			}.SlotKey()
			if phase != PhasePreparing {
				op.CredentialAdmissions = admissions
			}
			if phase == PhaseAwaitingExternalIdentity || phase == PhaseAwaitingOperatorConfirmation || phase == PhasePublishingActivation ||
				phase == PhasePublishingProcessActivation || phase == PhasePromotingRegistration || phase == PhaseRetiringPredecessor || phase == PhaseDeliveringConfirmation {
				op.IdentityOperationID = identityOperationID
			}
			if phase == PhaseAwaitingOperatorConfirmation || phase == PhasePublishingActivation ||
				phase == PhasePublishingProcessActivation || phase == PhasePromotingRegistration || phase == PhaseRetiringPredecessor || phase == PhaseDeliveringConfirmation {
				op.BindingRevision = 4
			}
			store := &cancellationTestStore{op: op}
			if phase == PhasePublishingProcessActivation || phase == PhasePromotingRegistration || phase == PhaseRetiringPredecessor || phase == PhaseDeliveringConfirmation {
				store.activation = testCurrentActivation(op, admissions, now)
				store.op.ActivationRevision = store.activation.Revision
			}
			identities := &cancellationTestIdentities{
				operation: operatorchannel.Operation{OperationID: identityOperationID, State: operatorchannel.StateBound, BindingRevision: 4},
				binding: operatorchannel.Binding{
					PrincipalID: "principal-a", Interface: candidate.Interface, ConversationRef: "conversation-a",
					Revision: 4, Status: operatorchannel.BindingCurrent,
				},
			}
			service, err := NewService(ServiceOptions{
				Store: store, Identities: identities, Credentials: credentials,
				Catalog:     func() (*CandidateCatalog, error) { return catalog, nil },
				Activations: &cancellationTestActivations{}, Confirmation: successfulTestConfirmation{}, Readiness: cancellationTestReadiness{},
				Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 3 && !store.op.Phase.Terminal(); attempt++ {
				if err := service.Recover(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if store.op.Phase != PhaseSucceeded {
				t.Fatalf("recovered phase = %s, want %s", store.op.Phase, PhaseSucceeded)
			}
			if store.activation.Status != ActivationCurrent || store.activation.OperationID != store.op.OperationID {
				t.Fatalf("recovered activation = %#v", store.activation)
			}
		})
	}
}

func testCurrentActivation(op Operation, admissions []CredentialAdmission, now time.Time) ConnectedChannelActivation {
	return ConnectedChannelActivation{
		ActivationID: uuid.NewString(), SlotKey: op.SlotKey, OperationID: op.OperationID, OperationRevision: op.Revision,
		PrincipalID: op.PrincipalID, Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate,
		TargetSelector: op.TargetSelector, Posture: op.Posture, BindingRevision: op.BindingRevision, ConversationRef: "conversation-a",
		CredentialAdmissions: append([]CredentialAdmission(nil), admissions...), Revision: 1, Status: ActivationCurrent,
		CreatedAt: now, UpdatedAt: now,
	}
}

func testSucceededOperation(candidate Candidate, now time.Time) Operation {
	op := Operation{
		OperationID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(), PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseSucceeded, Revision: 9, BindingRevision: 3, CredentialReservations: credentialReservations(candidate),
		RequestedAt: now, UpdatedAt: now, CompletedAt: now,
	}
	op.SlotKey = StartRequest{Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector}.SlotKey()
	return op
}

func testCredentialWriter(t *testing.T) *CredentialWriter {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

type cancellationTestStore struct {
	op                 Operation
	activation         ConnectedChannelActivation
	history            []Operation
	cancelAfterReserve context.CancelFunc
	cancelled          bool
	sawCanceledContext bool
	advanceErrOnce     error
}

func (s *cancellationTestStore) observe(ctx context.Context) {
	if ctx.Err() != nil {
		s.sawCanceledContext = true
	}
}

func (s *cancellationTestStore) ReserveChannelOnboarding(ctx context.Context, req StartRequest) (Operation, error) {
	s.observe(ctx)
	if err := req.Validate(); err != nil {
		return Operation{}, err
	}
	s.op = Operation{
		OperationID: req.OperationID, RequestKeyHash: req.RequestKeyHash, RequestHash: req.RequestHash,
		SlotKey: req.SlotKey(), PrincipalID: req.PrincipalID, Verb: req.Verb, Provider: req.Provider,
		Interface: req.Interface, Coordinate: req.Coordinate, TargetSelector: req.TargetSelector,
		Posture: req.Posture, Ceremony: req.Ceremony, Phase: PhasePreparing, Revision: 1,
		SaveProof: req.SaveProof, CredentialReservations: append([]CredentialReservation(nil), req.CredentialReservations...),
		RequestedAt: req.RequestedAt, UpdatedAt: req.RequestedAt,
	}
	if s.cancelAfterReserve != nil {
		s.cancelAfterReserve()
		s.cancelled = true
	}
	return s.op, nil
}

func (s *cancellationTestStore) GetChannelOnboarding(ctx context.Context, operationID string) (Operation, error) {
	s.observe(ctx)
	if s.op.OperationID != operationID {
		return Operation{}, ErrNotFound
	}
	return s.op, nil
}

func (s *cancellationTestStore) ListChannelOnboardingOperations(ctx context.Context) ([]Operation, error) {
	s.observe(ctx)
	out := append([]Operation(nil), s.history...)
	if s.op.OperationID == "" {
		return out, nil
	}
	for index := range out {
		if out[index].OperationID == s.op.OperationID {
			out[index] = s.op
			return out, nil
		}
	}
	return append(out, s.op), nil
}

func (s *cancellationTestStore) AdvanceChannelOnboarding(ctx context.Context, req AdvanceRequest) (Operation, error) {
	s.observe(ctx)
	if s.advanceErrOnce != nil {
		err := s.advanceErrOnce
		s.advanceErrOnce = nil
		return Operation{}, err
	}
	if req.OperationID != s.op.OperationID {
		return Operation{}, ErrNotFound
	}
	if req.ExpectedRevision != s.op.Revision {
		return Operation{}, ErrRevisionConflict
	}
	s.op.Phase = req.Phase
	s.op.Revision++
	s.op.UpdatedAt = req.Now
	if req.ReplaceCredentialAdmissions {
		s.op.CredentialAdmissions = append([]CredentialAdmission(nil), req.CredentialAdmissions...)
	}
	if req.IdentityOperationID != "" {
		s.op.IdentityOperationID = req.IdentityOperationID
	}
	if req.BindingRevision > 0 {
		s.op.BindingRevision = req.BindingRevision
	}
	if req.ConfirmationOperationID != "" {
		s.op.ConfirmationOperationID = req.ConfirmationOperationID
	}
	if req.FailureCode != "" {
		s.op.FailureCode = req.FailureCode
		s.op.FailureMessage = req.FailureMessage
	}
	return s.op, nil
}

func (s *cancellationTestStore) PublishConnectedChannelActivation(ctx context.Context, req PublishActivationRequest) (Operation, ConnectedChannelActivation, error) {
	s.observe(ctx)
	if req.OperationID != s.op.OperationID {
		return Operation{}, ConnectedChannelActivation{}, ErrNotFound
	}
	if req.ExpectedRevision != s.op.Revision {
		return Operation{}, ConnectedChannelActivation{}, ErrRevisionConflict
	}
	s.activation = testCurrentActivation(s.op, s.op.CredentialAdmissions, req.Now)
	s.activation.ActivationID = req.ActivationID
	s.activation.BindingRevision = req.BindingRevision
	s.activation.ConversationRef = req.ConversationRef
	s.activation.ProofID = req.ProofID
	s.activation.ProofRevision = req.ProofRevision
	s.op.Revision++
	s.op.Phase = PhasePublishingProcessActivation
	s.op.ActivationRevision = s.activation.Revision
	s.op.BindingRevision = req.BindingRevision
	s.op.UpdatedAt = req.Now
	return s.op, s.activation, nil
}

func (s *cancellationTestStore) GetConnectedChannelActivation(ctx context.Context, slotKey string) (ConnectedChannelActivation, error) {
	s.observe(ctx)
	if s.activation.SlotKey != slotKey || s.activation.Status != ActivationCurrent {
		return ConnectedChannelActivation{}, ErrNotFound
	}
	return s.activation, nil
}

func (s *cancellationTestStore) ListCurrentConnectedChannelActivations(ctx context.Context) ([]ConnectedChannelActivation, error) {
	s.observe(ctx)
	if s.activation.Status != ActivationCurrent {
		return nil, nil
	}
	return []ConnectedChannelActivation{s.activation}, nil
}

func (s *cancellationTestStore) RetireConnectedChannelActivation(ctx context.Context, _ RetireActivationRequest) (ConnectedChannelActivation, error) {
	s.observe(ctx)
	return ConnectedChannelActivation{}, ErrNotFound
}

func (s *cancellationTestStore) ReserveChannelTeardown(ctx context.Context, _ ReserveTeardownRequest) (TeardownOperation, error) {
	s.observe(ctx)
	return TeardownOperation{}, ErrNotFound
}

func (s *cancellationTestStore) GetChannelTeardown(ctx context.Context, _ string) (TeardownOperation, error) {
	s.observe(ctx)
	return TeardownOperation{}, ErrNotFound
}

func (s *cancellationTestStore) ListChannelTeardowns(ctx context.Context) ([]TeardownOperation, error) {
	s.observe(ctx)
	return nil, nil
}

func (s *cancellationTestStore) RetireChannelTeardownAuthority(ctx context.Context, _ RetireTeardownAuthorityRequest) (TeardownOperation, error) {
	s.observe(ctx)
	return TeardownOperation{}, ErrNotFound
}

func (s *cancellationTestStore) CompleteChannelTeardown(ctx context.Context, _ CompleteTeardownRequest) (TeardownOperation, error) {
	s.observe(ctx)
	return TeardownOperation{}, ErrNotFound
}

type cancellationTestIdentities struct {
	operation          operatorchannel.Operation
	binding            operatorchannel.Binding
	sawCanceledContext bool
	expirations        int
	expiryRaceState    operatorchannel.OperationState
}

func (i *cancellationTestIdentities) observe(ctx context.Context) {
	if ctx.Err() != nil {
		i.sawCanceledContext = true
	}
}

func (i *cancellationTestIdentities) Principal() (operatorchannel.Principal, error) {
	return operatorchannel.Principal{ID: "principal-a"}, nil
}

func (i *cancellationTestIdentities) Begin(ctx context.Context, _ string, _ operatorchannel.OperationKind, _ int64, _, _ string, _ bool, _ time.Time) (operatorchannel.Operation, error) {
	i.observe(ctx)
	if i.operation.OperationID == "" {
		i.operation = operatorchannel.Operation{OperationID: uuid.NewString(), State: operatorchannel.StateAwaitingClaim}
	}
	return i.operation, nil
}

func (i *cancellationTestIdentities) GetOperation(ctx context.Context, operationID string) (operatorchannel.Operation, error) {
	i.observe(ctx)
	if operationID != i.operation.OperationID {
		return operatorchannel.Operation{}, operatorchannel.ErrNotFound
	}
	return i.operation, nil
}

func (i *cancellationTestIdentities) ExpireOperation(ctx context.Context, operationID string, expectedRevision int64, now time.Time) (operatorchannel.Operation, error) {
	i.observe(ctx)
	if operationID != i.operation.OperationID {
		return operatorchannel.Operation{}, operatorchannel.ErrNotFound
	}
	if expectedRevision != i.operation.Revision {
		return operatorchannel.Operation{}, operatorchannel.ErrRevisionConflict
	}
	if i.expiryRaceState != "" {
		i.operation.State = i.expiryRaceState
		i.operation.Revision++
		if i.expiryRaceState == operatorchannel.StateAwaitingConfirmation || i.expiryRaceState == operatorchannel.StateBound {
			i.operation.BindingRevision = i.binding.Revision
		}
		if i.expiryRaceState.Terminal() {
			i.operation.CompletedAt = now
		}
		i.expiryRaceState = ""
		return operatorchannel.Operation{}, operatorchannel.ErrRevisionConflict
	}
	if i.operation.ExpiresAt.After(now) {
		return operatorchannel.Operation{}, operatorchannel.ErrConflict
	}
	i.expirations++
	i.operation.State = operatorchannel.StateExpired
	i.operation.Revision++
	i.operation.CompletedAt = now
	return i.operation, nil
}

func (i *cancellationTestIdentities) CurrentBinding(ctx context.Context, _ operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error) {
	i.observe(ctx)
	if i.binding.Status == operatorchannel.BindingCurrent {
		return i.binding, nil
	}
	return operatorchannel.Binding{}, operatorchannel.ErrNotFound
}

func (i *cancellationTestIdentities) Readback(ctx context.Context) ([]operatorchannel.Readback, error) {
	i.observe(ctx)
	return nil, nil
}

type cancellationTestActivations struct {
	sawCanceledContext bool
	preflightErr       error
	preflights         int
	refreshes          int
	err                error
	publishErrOnce     error
	publications       int
	promotions         int
}

func (a *cancellationTestActivations) PreflightChannelActivation(ctx context.Context, _ Operation, _ Candidate) error {
	if ctx.Err() != nil {
		a.sawCanceledContext = true
	}
	a.preflights++
	return a.preflightErr
}

func (a *cancellationTestActivations) RefreshChannelActivations(ctx context.Context) error {
	if ctx.Err() != nil {
		a.sawCanceledContext = true
	}
	a.refreshes++
	err := a.err
	if _, terminal := AsTerminalActivationError(err); terminal {
		a.err = nil
	}
	return err
}

func (a *cancellationTestActivations) RefreshChannelActivationCandidates(ctx context.Context) error {
	return a.RefreshChannelActivations(ctx)
}

func (a *cancellationTestActivations) PublishChannelActivation(ctx context.Context, _ Operation, _ ConnectedChannelActivation) error {
	if ctx.Err() != nil {
		a.sawCanceledContext = true
	}
	a.publications++
	if a.publishErrOnce != nil {
		err := a.publishErrOnce
		a.publishErrOnce = nil
		return err
	}
	return nil
}

func (a *cancellationTestActivations) PromoteChannelRegistration(ctx context.Context, _ Operation, _ ConnectedChannelActivation) error {
	if ctx.Err() != nil {
		a.sawCanceledContext = true
	}
	a.promotions++
	return nil
}

type cancellationTestConfirmation struct{}

func (cancellationTestConfirmation) DispatchChannelConfirmation(context.Context, ConfirmationRequest) (ConfirmationResult, error) {
	return ConfirmationResult{}, nil
}

type successfulTestConfirmation struct{}

func (successfulTestConfirmation) DispatchChannelConfirmation(_ context.Context, request ConfirmationRequest) (ConfirmationResult, error) {
	return ConfirmationResult{OperationID: request.Operation.ConfirmationOperationID, TerminalSuccess: true}, nil
}

type cancellationTestReadiness struct{}

func (cancellationTestReadiness) ProjectConnectedChannelReadiness(context.Context, Operation, Candidate) (ConnectedChannelReadiness, bool, error) {
	return ConnectedChannelReadiness{}, false, nil
}

type readbackTestStore struct {
	*cancellationTestStore
	operations  []Operation
	activations []ConnectedChannelActivation
}

func (s *readbackTestStore) GetChannelOnboarding(ctx context.Context, operationID string) (Operation, error) {
	s.observe(ctx)
	for _, operation := range s.operations {
		if operation.OperationID == operationID {
			return operation, nil
		}
	}
	return Operation{}, ErrNotFound
}

func (s *readbackTestStore) ListChannelOnboardingOperations(ctx context.Context) ([]Operation, error) {
	s.observe(ctx)
	return append([]Operation(nil), s.operations...), nil
}

func (s *readbackTestStore) GetConnectedChannelActivation(ctx context.Context, slotKey string) (ConnectedChannelActivation, error) {
	s.observe(ctx)
	for _, activation := range s.activations {
		if activation.SlotKey == slotKey && activation.Status == ActivationCurrent {
			return activation, nil
		}
	}
	if s.cancellationTestStore != nil {
		return s.cancellationTestStore.GetConnectedChannelActivation(ctx, slotKey)
	}
	return ConnectedChannelActivation{}, ErrNotFound
}

func (s *readbackTestStore) ListCurrentConnectedChannelActivations(ctx context.Context) ([]ConnectedChannelActivation, error) {
	s.observe(ctx)
	if len(s.activations) > 0 {
		return append([]ConnectedChannelActivation(nil), s.activations...), nil
	}
	if s.cancellationTestStore != nil {
		return s.cancellationTestStore.ListCurrentConnectedChannelActivations(ctx)
	}
	return nil, nil
}

type readbackTestIdentities struct {
	*cancellationTestIdentities
	rows []operatorchannel.Readback
}

func (i *readbackTestIdentities) Readback(ctx context.Context) ([]operatorchannel.Readback, error) {
	i.observe(ctx)
	return append([]operatorchannel.Readback(nil), i.rows...), nil
}

type operationReadinessProjector struct {
	readyOperationID string
	seenOperationID  string
}

func (p *operationReadinessProjector) ProjectConnectedChannelReadiness(_ context.Context, op Operation, _ Candidate) (ConnectedChannelReadiness, bool, error) {
	p.seenOperationID = op.OperationID
	return ConnectedChannelReadiness{Ready: op.OperationID == p.readyOperationID, Reason: ReadinessReady}, true, nil
}
