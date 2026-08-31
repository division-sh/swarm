package runtimepersistence

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestChannelOnboardingSelectedStoreContractParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var store channelonboarding.Store
			switch backend {
			case "sqlite":
				store = newBootstrappedSQLiteRuntimeStoreForTest(t)
			case "postgres":
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store = admitTestPostgresStore(t, db)
			}
			runChannelOnboardingStoreContract(t, store)
		})
	}
}

func TestRetiredPreparingOnboardingReleasesUncheckpointedCredentialSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected channelonboarding.Store
			switch backend {
			case "sqlite":
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			case "postgres":
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = admitTestPostgresStore(t, db)
			}
			ctx := context.Background()
			now := time.Date(2026, 8, 27, 14, 15, 0, 0, time.UTC)
			principal, err := selected.(operatorchannel.Store).EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			request := channelOnboardingStartRequest(principal.ID, now)
			op, err := selected.ReserveChannelOnboarding(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			reservation := op.CredentialReservations[0]
			storeKey := strings.TrimSpace(reservation.StoreKey) + ".operation." + operatorchannel.Hash(
				"channel-onboarding-credential-occurrence-v1", strings.TrimSpace(op.OperationID), strings.TrimSpace(reservation.Role),
			)
			receipt := operatorchannel.Hash("channel-onboarding-credential-receipt-v1", op.OperationID, reservation.Role)
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := channelonboarding.NewCredentialWriter(credentialStore)
			if err != nil {
				t.Fatal(err)
			}
			written, err := writer.Admit(ctx, channelonboarding.CredentialWriteRequest{StoreKey: storeKey, Value: "operation-secret", Receipt: receipt})
			if err != nil {
				t.Fatal(err)
			}

			// Reconstruct the credential and onboarding owners before startup reconciliation.
			reopenedStore, err := runtimecredentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			reopenedWriter, err := channelonboarding.NewCredentialWriter(reopenedStore)
			if err != nil {
				t.Fatal(err)
			}
			emptyCatalog, err := channelonboarding.NewCandidateCatalog(nil)
			if err != nil {
				t.Fatal(err)
			}
			service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{
				Store: selected, Identities: retiredOnboardingIdentities{principal: principal}, Credentials: reopenedWriter,
				Catalog:     func() (*channelonboarding.CandidateCatalog, error) { return emptyCatalog, nil },
				Activations: retiredOnboardingActivations{}, Confirmation: retiredOnboardingConfirmation{},
				Readiness: retiredOnboardingReadiness{}, Now: func() time.Time { return now.Add(time.Minute) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := service.ReconcileLocal(ctx); err != nil {
				t.Fatal(err)
			}
			retired, err := selected.GetChannelOnboarding(ctx, op.OperationID)
			if err != nil || retired.Phase != channelonboarding.PhaseFailed || retired.FailureCode != "runtime_context_retired" {
				t.Fatalf("retired operation = %#v err=%v", retired, err)
			}
			if _, found, err := reopenedStore.Get(ctx, written.StoreKey); err != nil || found {
				t.Fatalf("restarted operation credential found=%v err=%v", found, err)
			}
		})
	}
}

func TestFailedOnboardingFenceRejectsCredentialReadmissionSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected channelonboarding.Store
			switch backend {
			case "sqlite":
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			case "postgres":
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = admitTestPostgresStore(t, db)
			}
			ctx := context.Background()
			now := time.Date(2026, 8, 30, 23, 0, 0, 0, time.UTC)
			principal, err := selected.(operatorchannel.Store).EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			request := channelOnboardingStartRequest(principal.ID, now)
			op, err := selected.ReserveChannelOnboarding(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			admissions := []channelonboarding.CredentialAdmission{
				{Role: "telegram_bot_token", StoreKey: "channel.telegram.provider." + op.OperationID, Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "provider-receipt", ValueSeal: operatorChannelProviderEvidence().Seal},
				{Role: "webhook_signing_secret", StoreKey: "channel.telegram.signing." + op.OperationID, Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "signing-receipt", ValueSeal: operatorChannelProviderEvidence().Seal},
			}
			op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseCredentialsAdmitted,
				CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			failed, err := selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseFailed,
				FailureCode: "provider_rejected", FailureMessage: "provider rejected credential", Now: now.Add(2 * time.Second),
			})
			if err != nil || failed.Phase != channelonboarding.PhaseFailed {
				t.Fatalf("terminal fence = %#v err=%v", failed, err)
			}
			if _, err := selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
				OperationID: failed.OperationID, ExpectedRevision: failed.Revision, Phase: channelonboarding.PhasePreparing,
				ReplaceCredentialAdmissions: true, Now: now.Add(3 * time.Second),
			}); !errors.Is(err, channelonboarding.ErrConflict) {
				t.Fatalf("terminal credential readmission error = %v, want conflict", err)
			}
			replay := request
			replay.OperationID = uuid.NewString()
			replayed, err := selected.ReserveChannelOnboarding(ctx, replay)
			if err != nil || replayed.OperationID != failed.OperationID || replayed.Phase != channelonboarding.PhaseFailed || replayed.Revision != failed.Revision {
				t.Fatalf("terminal request replay = %#v err=%v", replayed, err)
			}
		})
	}
}

type retiredOnboardingIdentities struct {
	principal operatorchannel.Principal
}

func (i retiredOnboardingIdentities) Principal() (operatorchannel.Principal, error) {
	return i.principal, nil
}
func (retiredOnboardingIdentities) Begin(context.Context, string, operatorchannel.OperationKind, int64, string, string, runtimecredentials.ValueEvidence, bool, time.Time) (operatorchannel.Operation, error) {
	return operatorchannel.Operation{}, operatorchannel.ErrNotFound
}
func (retiredOnboardingIdentities) GetOperation(context.Context, string) (operatorchannel.Operation, error) {
	return operatorchannel.Operation{}, operatorchannel.ErrNotFound
}
func (retiredOnboardingIdentities) ExpireOperation(context.Context, string, int64, time.Time) (operatorchannel.Operation, error) {
	return operatorchannel.Operation{}, operatorchannel.ErrNotFound
}
func (retiredOnboardingIdentities) CurrentBinding(context.Context, operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error) {
	return operatorchannel.Binding{}, operatorchannel.ErrNotFound
}
func (retiredOnboardingIdentities) CurrentBindingReadiness(context.Context, operatorchannel.InterfaceIdentity) (operatorchannel.Binding, bool, error) {
	return operatorchannel.Binding{}, false, operatorchannel.ErrNotFound
}
func (retiredOnboardingIdentities) Readback(context.Context) ([]operatorchannel.Readback, error) {
	return nil, nil
}

type retiredOnboardingActivations struct{}

func (retiredOnboardingActivations) RefreshChannelActivations(context.Context) error { return nil }
func (retiredOnboardingActivations) PreflightChannelActivation(context.Context, channelonboarding.Operation, channelonboarding.Candidate) error {
	return nil
}
func (retiredOnboardingActivations) RefreshChannelActivationCandidates(context.Context) error {
	return nil
}
func (retiredOnboardingActivations) PublishChannelActivation(context.Context, channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) error {
	return nil
}
func (retiredOnboardingActivations) PromoteChannelRegistration(context.Context, channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) error {
	return nil
}

type retiredOnboardingConfirmation struct{}

func (retiredOnboardingConfirmation) ReconcileChannelEffectsBeforeRebind(context.Context, channelonboarding.Operation) (channelonboarding.EffectRebindDisposition, error) {
	return channelonboarding.EffectRebindDisposition{RetryAllowed: true}, nil
}

func (retiredOnboardingConfirmation) DispatchChannelConfirmation(context.Context, channelonboarding.ConfirmationRequest) (channelonboarding.ConfirmationResult, error) {
	return channelonboarding.ConfirmationResult{}, nil
}

type retiredOnboardingReadiness struct{}

func (retiredOnboardingReadiness) ProjectConnectedChannelReadiness(context.Context, channelonboarding.Operation, channelonboarding.Candidate) (channelonboarding.ConnectedChannelReadiness, bool, error) {
	return channelonboarding.ConnectedChannelReadiness{}, false, nil
}

func TestChannelRebindPublicationRetiresEverySiblingActivationForInterface(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected channelonboarding.Store
			switch backend {
			case "sqlite":
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			case "postgres":
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = admitTestPostgresStore(t, db)
			}
			ctx := context.Background()
			now := time.Date(2026, 8, 27, 2, 0, 0, 0, time.UTC)
			principal, err := selected.(operatorchannel.Store).EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			first := channelOnboardingStartRequest(principal.ID, now)
			first.RequestKeyHash, first.RequestHash = "first-key", "first-request"
			_, firstActivation := publishChannelOnboardingTestActivation(t, selected, first, 1, now)

			sibling := first
			sibling.OperationID = uuid.NewString()
			sibling.RequestKeyHash, sibling.RequestHash = "sibling-key", "sibling-request"
			sibling.Coordinate.BundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			sibling.TargetSelector = "ingress:support:sibling:telegram"
			_, siblingActivation := publishChannelOnboardingTestActivation(t, selected, sibling, 1, now.Add(time.Minute))

			rebind := first
			rebind.OperationID = uuid.NewString()
			rebind.RequestKeyHash, rebind.RequestHash = "rebind-key", "rebind-request"
			rebind.Verb = channelonboarding.VerbRebind
			_, successor := publishChannelOnboardingTestActivation(t, selected, rebind, 2, now.Add(2*time.Minute))

			current, err := selected.ListCurrentConnectedChannelActivations(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(current) != 1 || current[0].ActivationID != successor.ActivationID || current[0].BindingRevision != 2 {
				t.Fatalf("current activations after rebind = %#v, want only successor %s", current, successor.ActivationID)
			}
			for _, retired := range []channelonboarding.ConnectedChannelActivation{firstActivation, siblingActivation} {
				if _, err := selected.GetConnectedChannelActivation(ctx, retired.SlotKey); !errors.Is(err, channelonboarding.ErrNotFound) && retired.SlotKey != successor.SlotKey {
					t.Fatalf("retired sibling slot %s remained current: %v", retired.SlotKey, err)
				}
			}
		})
	}
}

func publishChannelOnboardingTestActivation(t *testing.T, selected channelonboarding.Store, request channelonboarding.StartRequest, bindingRevision int64, now time.Time) (channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) {
	t.Helper()
	ctx := context.Background()
	op, err := selected.ReserveChannelOnboarding(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	admissions := make([]channelonboarding.CredentialAdmission, 0, len(request.CredentialReservations))
	for _, reservation := range request.CredentialReservations {
		admissions = append(admissions, channelonboarding.CredentialAdmission{
			Role: reservation.Role, StoreKey: reservation.StoreKey + "." + request.OperationID,
			Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "receipt-" + request.OperationID, ValueSeal: operatorChannelProviderEvidence().Seal,
		})
	}
	op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseCredentialsAdmitted,
		CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
		op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase,
			IdentityOperationID: uuid.NewString(), BindingRevision: bindingRevision, Now: now.Add(time.Duration(op.Revision) * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	op, activation, err := selected.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(), BindingRevision: bindingRevision,
		ConversationRef: "conversation-" + request.OperationID, ProofID: uuid.NewString(), ProofRevision: bindingRevision, Now: now.Add(8 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration, channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation, channelonboarding.PhaseSucceeded} {
		op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now.Add(time.Duration(op.Revision) * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return op, activation
}

func runChannelOnboardingStoreContract(t *testing.T, store channelonboarding.Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	principal, err := store.(operatorchannel.Store).EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	request := channelOnboardingStartRequest(principal.ID, now)
	op, err := store.ReserveChannelOnboarding(ctx, request)
	if err != nil || op.Phase != channelonboarding.PhasePreparing || op.Revision != 1 {
		t.Fatalf("reserve = %#v, %v", op, err)
	}
	operations, err := store.ListChannelOnboardingOperations(ctx)
	if err != nil || len(operations) != 1 || operations[0].OperationID != op.OperationID {
		t.Fatalf("list operations = %#v, %v", operations, err)
	}
	replay := request
	replay.OperationID = uuid.NewString()
	replayed, err := store.ReserveChannelOnboarding(ctx, replay)
	if err != nil || replayed.OperationID != op.OperationID || replayed.Revision != op.Revision {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
	changed := replay
	changed.RequestHash = "changed-semantic-input"
	if _, err := store.ReserveChannelOnboarding(ctx, changed); !errors.Is(err, channelonboarding.ErrConflict) {
		t.Fatalf("changed same-key replay = %v", err)
	}
	concurrent := request
	concurrent.OperationID = uuid.NewString()
	concurrent.RequestKeyHash = "different-key"
	if _, err := store.ReserveChannelOnboarding(ctx, concurrent); !errors.Is(err, channelonboarding.ErrConflict) {
		t.Fatalf("concurrent slot operation = %v", err)
	}
	admissions := []channelonboarding.CredentialAdmission{
		{Role: "telegram_bot_token", StoreKey: "channel.telegram.provider", Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "receipt-provider", ValueSeal: operatorChannelProviderEvidence().Seal},
		{Role: "webhook_signing_secret", StoreKey: "channel.telegram.signing", Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "receipt-signing", ValueSeal: operatorChannelProviderEvidence().Seal},
	}
	op, err = store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: 1, Phase: channelonboarding.PhaseCredentialsAdmitted,
		CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: now.Add(time.Second),
	})
	if err != nil || op.Revision != 2 || len(op.CredentialAdmissions) != 2 {
		t.Fatalf("advance credentials = %#v, %v", op, err)
	}
	if _, err := store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: 1, Phase: channelonboarding.PhaseAwaitingExternalIdentity, Now: now.Add(2 * time.Second)}); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
		t.Fatalf("stale phase advance = %v", err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
		op, err = store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase,
			IdentityOperationID: uuid.NewString(), BindingRevision: 4, Now: now.Add(time.Duration(op.Revision) * time.Second),
		})
		if err != nil {
			t.Fatalf("advance %s: %v", phase, err)
		}
	}
	op, activation, err := store.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(), BindingRevision: 4,
		ConversationRef: "conversation-a", ProofID: uuid.NewString(), ProofRevision: 2, Now: now.Add(8 * time.Second),
	})
	if err != nil || activation.Status != channelonboarding.ActivationCurrent || activation.Revision != 1 || activation.ConversationRef != "conversation-a" || op.Phase != channelonboarding.PhasePublishingProcessActivation {
		t.Fatalf("publish = op:%#v activation:%#v err:%v", op, activation, err)
	}
	current, err := store.GetConnectedChannelActivation(ctx, activation.SlotKey)
	if err != nil || current.ActivationID != activation.ActivationID {
		t.Fatalf("get activation = %#v, %v", current, err)
	}
	listed, err := store.ListCurrentConnectedChannelActivations(ctx)
	if err != nil || len(listed) != 1 || listed[0].ActivationID != activation.ActivationID {
		t.Fatalf("list activations = %#v, %v", listed, err)
	}
	semanticMismatch := op.Coordinate
	semanticMismatch.BundleSource = "ephemeral"
	if _, err := store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: op.Phase,
		RebindCoordinate: &semanticMismatch, Now: now.Add(9 * time.Second),
	}); !errors.Is(err, channelonboarding.ErrConflict) {
		t.Fatalf("semantic coordinate rebind = %v, want conflict", err)
	}
	successorCoordinate := op.Coordinate
	successorCoordinate.RuntimeInstanceID = uuid.NewString()
	successorCoordinate.ContextPublicationGeneration += 17
	successorCoordinate.TargetGeneration += 19
	predecessorOperationRevision := op.Revision
	predecessorActivationRevision := activation.Revision
	op, err = store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: op.Phase,
		RebindCoordinate: &successorCoordinate, Now: now.Add(9 * time.Second),
	})
	if err != nil || !op.Coordinate.Matches(successorCoordinate) || op.Revision != predecessorOperationRevision+1 || op.ActivationRevision != predecessorActivationRevision+1 {
		t.Fatalf("successor occurrence rebind = %#v, %v", op, err)
	}
	reboundActivation, err := store.GetConnectedChannelActivation(ctx, activation.SlotKey)
	if err != nil || !reboundActivation.Coordinate.Matches(successorCoordinate) || reboundActivation.Revision != predecessorActivationRevision+1 || reboundActivation.OperationRevision != op.Revision {
		t.Fatalf("rebound activation = %#v, %v", reboundActivation, err)
	}
	if _, err := store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: predecessorOperationRevision, Phase: op.Phase,
		RebindCoordinate: &successorCoordinate, Now: now.Add(10 * time.Second),
	}); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
		t.Fatalf("stale successor occurrence rebind = %v, want revision conflict", err)
	}
	teardownRequest := channelonboarding.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: "teardown-key", RequestHash: "teardown-input",
		Kind: channelonboarding.TeardownUnbind, PrincipalID: principal.ID,
		Scope: channelonboarding.TeardownScope{Interface: activation.Interface}, ExpectedBindingRevision: activation.BindingRevision,
		RequestedAt: now.Add(11 * time.Second),
	}
	teardown, err := store.ReserveChannelTeardown(ctx, teardownRequest)
	if err != nil || teardown.Phase != channelonboarding.TeardownReserved || teardown.Revision != 1 {
		t.Fatalf("reserve teardown = %#v, %v", teardown, err)
	}
	teardownReplay := teardownRequest
	teardownReplay.TeardownID = uuid.NewString()
	replayedTeardown, err := store.ReserveChannelTeardown(ctx, teardownReplay)
	if err != nil || replayedTeardown.TeardownID != teardown.TeardownID {
		t.Fatalf("replay teardown = %#v, %v", replayedTeardown, err)
	}
	teardownChanged := teardownReplay
	teardownChanged.RequestHash = "changed-teardown-input"
	if _, err := store.ReserveChannelTeardown(ctx, teardownChanged); !errors.Is(err, channelonboarding.ErrConflict) {
		t.Fatalf("changed teardown replay = %v", err)
	}
	teardown, err = store.RetireChannelTeardownAuthority(ctx, channelonboarding.RetireTeardownAuthorityRequest{
		TeardownID: teardown.TeardownID, ExpectedRevision: teardown.Revision, Reason: "unbind", Now: now.Add(10 * time.Second),
	})
	if err != nil || teardown.Phase != channelonboarding.TeardownAuthorityRetired || teardown.Revision != 2 || teardown.RetiredOperations != 1 || teardown.RetiredActivations != 1 {
		t.Fatalf("retire teardown authority = %#v, %v", teardown, err)
	}
	retiredOperation, err := store.GetChannelOnboarding(ctx, op.OperationID)
	if err != nil || retiredOperation.Phase != channelonboarding.PhaseRetired {
		t.Fatalf("retired onboarding operation = %#v, %v", retiredOperation, err)
	}
	teardown, err = store.CompleteChannelTeardown(ctx, channelonboarding.CompleteTeardownRequest{
		TeardownID: teardown.TeardownID, ExpectedRevision: teardown.Revision, Succeeded: true, Now: now.Add(11 * time.Second),
	})
	if err != nil || teardown.Phase != channelonboarding.TeardownSucceeded || teardown.Revision != 3 || teardown.CompletedAt.IsZero() {
		t.Fatalf("complete teardown = %#v, %v", teardown, err)
	}
	listedTeardowns, err := store.ListChannelTeardowns(ctx)
	if err != nil || len(listedTeardowns) != 1 || listedTeardowns[0].TeardownID != teardown.TeardownID {
		t.Fatalf("list teardowns = %#v, %v", listedTeardowns, err)
	}
	listed, err = store.ListCurrentConnectedChannelActivations(ctx)
	if err != nil || len(listed) != 0 {
		t.Fatalf("retired list = %#v, %v", listed, err)
	}
}

func channelOnboardingStartRequest(principalID string, now time.Time) channelonboarding.StartRequest {
	planGeneration, err := plangeneration.FromCanonicalValue(map[string]string{"test": "channel-onboarding-store"})
	if err != nil {
		panic(err)
	}
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan",
	}.Normalized()
	return channelonboarding.StartRequest{
		OperationID: uuid.NewString(), RequestKeyHash: "request-key", RequestHash: "semantic-input", PrincipalID: principalID,
		Verb: channelonboarding.VerbConnect, Provider: "telegram", Interface: identity,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BundleSource: "persisted",
			BundleIdentity: "support-bundle", PackInventoryGeneration: "sha256:inventory", RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 2,
			PlanGeneration: planGeneration, TargetGeneration: 3,
		},
		TargetSelector: "ingress:support:flow:telegram", Posture: channelonboarding.ActivationWebhookRegistration,
		Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge, SaveProof: true,
		CredentialReservations: []channelonboarding.CredentialReservation{{Role: "bot_token", StoreKey: "telegram_bot_token"}}, RequestedAt: now,
	}
}
