package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
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
			Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "receipt-" + request.OperationID, Epoch: "epoch-" + request.OperationID,
		})
	}
	op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseActivatingProvider,
		CredentialAdmissions: admissions, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
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
		ProofID: uuid.NewString(), ProofRevision: bindingRevision, Now: now.Add(8 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseSucceeded, Now: now.Add(9 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
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
		{Role: "telegram_bot_token", StoreKey: "channel.telegram.provider", Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "receipt-provider", Epoch: "epoch-provider"},
		{Role: "webhook_signing_secret", StoreKey: "channel.telegram.signing", Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "receipt-signing", Epoch: "epoch-signing"},
	}
	op, err = store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: 1, Phase: channelonboarding.PhaseActivatingProvider,
		CredentialAdmissions: admissions, Now: now.Add(time.Second),
	})
	if err != nil || op.Revision != 2 || len(op.CredentialAdmissions) != 2 {
		t.Fatalf("advance credentials = %#v, %v", op, err)
	}
	if _, err := store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: 1, Phase: channelonboarding.PhaseAwaitingExternalIdentity, Now: now.Add(2 * time.Second)}); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
		t.Fatalf("stale phase advance = %v", err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
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
		ProofID: uuid.NewString(), ProofRevision: 2, Now: now.Add(8 * time.Second),
	})
	if err != nil || activation.Status != channelonboarding.ActivationCurrent || activation.Revision != 1 || op.Phase != channelonboarding.PhaseDeliveringConfirmation {
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
	teardownRequest := channelonboarding.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: "teardown-key", RequestHash: "teardown-input",
		Kind: channelonboarding.TeardownUnbind, PrincipalID: principal.ID,
		Scope: channelonboarding.TeardownScope{Interface: activation.Interface}, ExpectedBindingRevision: activation.BindingRevision,
		RequestedAt: now.Add(9 * time.Second),
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
			BundleIdentity: "support-bundle", PackInventoryGeneration: "sha256:inventory", ContextPublicationGeneration: 2,
			PlanGeneration: planGeneration, TargetGeneration: 3,
		},
		TargetSelector: "ingress:support:flow:telegram", Posture: channelonboarding.ActivationWebhookRegistration,
		Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge, SaveProof: true,
		CredentialReservations: []channelonboarding.CredentialReservation{{Role: "bot_token", StoreKey: "telegram_bot_token"}}, RequestedAt: now,
	}
}
