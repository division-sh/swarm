package channelonboarding

import (
	"context"
	"errors"
	"path/filepath"
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
	written, err := credentials.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token", Receipt: "operation/provider"})
	if err != nil {
		t.Fatal(err)
	}
	identityOperationID := uuid.NewString()
	op := Operation{
		OperationID: uuid.NewString(), RequestKeyHash: "expired-key", RequestHash: "expired-request", PrincipalID: "principal-a",
		Verb: VerbConnect, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		Phase: PhaseAwaitingExternalIdentity, Revision: 3, SaveProof: true, CredentialReservations: credentialReservations(candidate),
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
		PhaseActivatingProvider,
		PhaseAwaitingExternalIdentity,
		PhaseAwaitingOperatorConfirmation,
		PhasePublishingActivation,
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
			if phase == PhaseAwaitingExternalIdentity || phase == PhaseAwaitingOperatorConfirmation || phase == PhasePublishingActivation || phase == PhaseDeliveringConfirmation {
				op.IdentityOperationID = identityOperationID
			}
			if phase == PhaseAwaitingOperatorConfirmation || phase == PhasePublishingActivation || phase == PhaseDeliveringConfirmation {
				op.BindingRevision = 4
			}
			store := &cancellationTestStore{op: op}
			if phase == PhaseDeliveringConfirmation {
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
		TargetSelector: op.TargetSelector, Posture: op.Posture, BindingRevision: op.BindingRevision,
		CredentialAdmissions: append([]CredentialAdmission(nil), admissions...), Revision: 1, Status: ActivationCurrent,
		CreatedAt: now, UpdatedAt: now,
	}
}

type cancellationTestStore struct {
	op                 Operation
	activation         ConnectedChannelActivation
	cancelAfterReserve context.CancelFunc
	cancelled          bool
	sawCanceledContext bool
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
	if s.op.OperationID == "" {
		return nil, nil
	}
	return []Operation{s.op}, nil
}

func (s *cancellationTestStore) AdvanceChannelOnboarding(ctx context.Context, req AdvanceRequest) (Operation, error) {
	s.observe(ctx)
	if req.OperationID != s.op.OperationID {
		return Operation{}, ErrNotFound
	}
	if req.ExpectedRevision != s.op.Revision {
		return Operation{}, ErrRevisionConflict
	}
	s.op.Phase = req.Phase
	s.op.Revision++
	s.op.UpdatedAt = req.Now
	if len(req.CredentialAdmissions) > 0 {
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
	s.activation.ProofID = req.ProofID
	s.activation.ProofRevision = req.ProofRevision
	s.op.Revision++
	s.op.Phase = PhaseDeliveringConfirmation
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
	refreshes          int
	err                error
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
	operations []Operation
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
