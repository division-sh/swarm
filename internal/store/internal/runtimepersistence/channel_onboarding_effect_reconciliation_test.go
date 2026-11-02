package runtimepersistence

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type channelOnboardingEffectSelectedStore interface {
	channelonboarding.Store
	operatorchannel.Store
	startupAuthorityParityStore
	runtimeeffects.ChannelOnboardingOutcomeStore
}

func TestChannelOnboardingEffectReconciliationSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected channelOnboardingEffectSelectedStore
			switch backend {
			case "sqlite":
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			case "postgres":
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = admitTestPostgresStore(t, db)
			}
			ctx := testAuthorActivityContext()
			capability, _, startup := admitRegistrationTestGeneration(t, ctx, selected, "channel-effect-reconciliation")
			t.Cleanup(func() { _ = capability.Release(context.Background()) })
			ctx, err := runtimestartupownership.WithProcessMutationAuthority(ctx, capability)
			if err != nil {
				t.Fatal(err)
			}

			for _, authorityKind := range []runtimeeffects.AuthorityKind{
				runtimeeffects.AuthorityServeRegistration,
				runtimeeffects.AuthorityChannelConfirmation,
			} {
				for _, state := range []runtimeeffects.State{
					runtimeeffects.StateAuthorized,
					runtimeeffects.StateLaunched,
					runtimeeffects.StateResponseObserved,
					runtimeeffects.StateOutcomeUncertain,
					runtimeeffects.StateSettled,
				} {
					t.Run(string(authorityKind)+"/"+string(state), func(t *testing.T) {
						handle, onboardingID, coordinate := beginSelectedChannelEffect(t, ctx, selected, startup, authorityKind, state)
						outcomes, err := selected.ReconcileChannelOnboardingEffectOutcomes(
							runtimestartupownership.CarryMutationAuthority(testAuthorActivityContextForBundle(coordinate.BundleHash), ctx), onboardingID, time.Now().UTC(),
						)
						if err != nil {
							t.Fatal(err)
						}
						if len(outcomes) != 1 {
							t.Fatalf("outcomes=%#v, want one", outcomes)
						}
						outcome := outcomes[0]
						want := state
						switch state {
						case runtimeeffects.StateAuthorized:
							want = runtimeeffects.StateTerminalFailure
						case runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved:
							want = runtimeeffects.StateOutcomeUncertain
						}
						if outcome.OperationID != handle.Attempt().OperationID || outcome.OnboardingOperationID != onboardingID ||
							outcome.AuthorityKind != authorityKind || outcome.State != want || outcome.AttemptState != want {
							t.Fatalf("reconciled outcome=%#v, want operation=%s authority=%s state=%s", outcome, handle.Attempt().OperationID, authorityKind, want)
						}
						if state == runtimeeffects.StateAuthorized && (!outcome.LaunchRejected || outcome.Launched) {
							t.Fatalf("prelaunch disposition=%#v, want launch-rejected without launch", outcome)
						}
						if outcome.BundleHash != coordinate.BundleHash ||
							outcome.BundleIdentity != coordinate.BundleIdentity || outcome.PackInventoryGeneration != coordinate.PackInventoryGeneration ||
							outcome.RuntimeInstanceID != coordinate.RuntimeInstanceID ||
							outcome.ContextPublicationGeneration != coordinate.ContextPublicationGeneration ||
							!outcome.PlanGeneration.Equal(coordinate.PlanGeneration) {
							t.Fatalf("effect lost exact onboarding coordinate: outcome=%#v coordinate=%#v", outcome, coordinate)
						}
						replayed, err := selected.ReconcileChannelOnboardingEffectOutcomes(
							runtimestartupownership.CarryMutationAuthority(testAuthorActivityContextForBundle(coordinate.BundleHash), ctx), onboardingID, time.Now().UTC().Add(time.Second),
						)
						if err != nil || len(replayed) != 1 || replayed[0].State != want || replayed[0].AttemptState != want {
							t.Fatalf("idempotent reconciliation=%#v err=%v", replayed, err)
						}
					})
				}
			}
		})
	}
}

func beginSelectedChannelEffect(
	t *testing.T,
	original context.Context,
	selected channelOnboardingEffectSelectedStore,
	startup runtimestartupownership.GrantEvidence,
	authorityKind runtimeeffects.AuthorityKind,
	state runtimeeffects.State,
) (*runtimeeffects.Handle, string, channelonboarding.ChannelRuntimeContextCoordinate) {
	t.Helper()
	now := time.Now().UTC()
	var authority runtimeeffects.Authority
	var onboardingID string
	var coordinate channelonboarding.ChannelRuntimeContextCoordinate
	var ctx context.Context
	controller := runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live)
	if authorityKind == runtimeeffects.AuthorityServeRegistration {
		onboardingID = uuid.NewString()
		request := channelOnboardingStartRequest(uuid.NewString(), now)
		coordinate = request.Coordinate
		intentID := uuid.NewString()
		authority = runtimeeffects.Authority{
			Kind: authorityKind, ID: intentID, ExecutionOwner: startup.ProcessOwnerID,
			LeaseExpiresAt: now.Add(time.Minute), FenceGeneration: startup.RuntimeGeneration,
			ExecutionMode: runtimeeffects.ExecutionModeLive,
			ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
				IntentID: intentID, StartupAuthorityID: startup.GrantID, StartupStateVersion: startup.StateVersion,
				OnboardingOperationID: onboardingID, OnboardingRevision: 1,
				BundleHash: coordinate.BundleHash, BundleIdentity: coordinate.BundleIdentity,
				PackInventoryGeneration: coordinate.PackInventoryGeneration, RuntimeInstanceID: coordinate.RuntimeInstanceID,
				ContextPublicationGeneration: coordinate.ContextPublicationGeneration, PlanGeneration: coordinate.PlanGeneration,
			},
		}
		ctx = runtimeeffects.WithLogicalOperationIdentity(testAuthorActivityContextForBundle(coordinate.BundleHash), "registration:"+string(state)+":"+onboardingID)
		ctx = runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), controller)
	} else {
		op, activation := selectedChannelConfirmationAuthorityFixture(t, original, selected, now, state)
		onboardingID, coordinate = op.OperationID, op.Coordinate
		authority = runtimeeffects.Authority{
			Kind: authorityKind, ID: op.ConfirmationOperationID, ExecutionOwner: "channel-onboarding:" + op.OperationID,
			LeaseExpiresAt: now.Add(time.Minute), FenceGeneration: op.Coordinate.ContextPublicationGeneration,
			ExecutionMode: runtimeeffects.ExecutionModeLive,
			ChannelConfirmation: runtimeeffects.ChannelConfirmationAuthority{
				EffectOperationID: op.ConfirmationOperationID, OnboardingOperationID: op.OperationID, OnboardingRevision: op.Revision,
				ActivationID: activation.ActivationID, ActivationRevision: activation.Revision, BindingRevision: activation.BindingRevision,
				PrincipalID: activation.PrincipalID, BundleHash: coordinate.BundleHash,
				BundleIdentity: coordinate.BundleIdentity, PackInventoryGeneration: coordinate.PackInventoryGeneration,
				RuntimeInstanceID: coordinate.RuntimeInstanceID, ContextPublicationGeneration: coordinate.ContextPublicationGeneration,
				PlanGeneration: coordinate.PlanGeneration,
			},
		}
		ctx = runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContextForBundle(coordinate.BundleHash), authority), controller)
	}
	if !authority.Valid() {
		t.Fatalf("invalid %s authority: %#v", authorityKind, authority)
	}
	ctx = runtimestartupownership.CarryMutationAuthority(ctx, original)
	var handle *runtimeeffects.Handle
	var err error
	if authorityKind == runtimeeffects.AuthorityServeRegistration {
		handle, err = runtimeeffects.BeginServeRegistration(ctx, []byte("registration:"+string(state)), map[string]string{"state": string(state)})
	} else {
		handle, err = runtimeeffects.BeginChannelConfirmation(ctx, []byte("confirmation:"+string(state)), map[string]string{"state": string(state)})
	}
	if err != nil {
		t.Fatalf("authorize %s/%s: %v", authorityKind, state, err)
	}
	advanceSelectedChannelEffect(t, ctx, handle, state)
	return handle, onboardingID, coordinate
}

func advanceSelectedChannelEffect(t *testing.T, ctx context.Context, handle *runtimeeffects.Handle, state runtimeeffects.State) {
	t.Helper()
	switch state {
	case runtimeeffects.StateAuthorized:
		return
	case runtimeeffects.StateLaunched:
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
	case runtimeeffects.StateResponseObserved:
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkResponseObserved(ctx, map[string]any{"state": state}); err != nil {
			t.Fatal(err)
		}
	case runtimeeffects.StateOutcomeUncertain:
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		failureErr := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "test_channel_effect_uncertain", "test", "settle", nil)
		failure, _ := runtimefailures.EnvelopeFromError(failureErr)
		if err := handle.Settle(ctx, runtimeeffects.StateOutcomeUncertain, &failure, nil); err != nil {
			t.Fatal(err)
		}
	case runtimeeffects.StateSettled:
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkResponseObserved(ctx, map[string]any{"state": state}); err != nil {
			t.Fatal(err)
		}
		if err := handle.Succeed(ctx, map[string]any{"state": state}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported test state %s", state)
	}
}

func selectedChannelConfirmationAuthorityFixture(
	t *testing.T,
	ctx context.Context,
	selected channelOnboardingEffectSelectedStore,
	now time.Time,
	state runtimeeffects.State,
) (channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) {
	t.Helper()
	principal, err := selected.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	request := channelOnboardingStartRequest(principal.ID, now)
	request.RequestKeyHash = "confirmation-key-" + string(state)
	request.RequestHash = "confirmation-request-" + string(state)
	request.TargetSelector = "ingress:support/flow/" + strings.ReplaceAll(string(state), "_", "-") + ":telegram"
	request.Coordinate.BundleIdentity = fmt.Sprintf("confirmation-%s", state)
	op, err := selected.ReserveChannelOnboarding(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	admissions := []channelonboarding.CredentialAdmission{{
		Role: "bot_token", StoreKey: "telegram_bot_token." + string(state), Kind: channelonboarding.CredentialAdmissionWritten,
		Receipt: "receipt-" + string(state), ValueSeal: operatorChannelProviderAuthority().Credential.Seal,
	}}
	op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseCredentialsAdmitted,
		CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	identityID := uuid.NewString()
	for _, phase := range []channelonboarding.Phase{
		channelonboarding.PhaseActivatingProvider,
		channelonboarding.PhaseAwaitingExternalIdentity,
		channelonboarding.PhaseAwaitingOperatorConfirmation,
		channelonboarding.PhasePublishingActivation,
	} {
		req := channelonboarding.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase,
			IdentityOperationID: identityID, Now: now.Add(time.Duration(op.Revision) * time.Second),
		}
		if phase == channelonboarding.PhaseAwaitingOperatorConfirmation || phase == channelonboarding.PhasePublishingActivation {
			req.BindingRevision = 1
		}
		op, err = selected.AdvanceChannelOnboarding(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
	}
	op, activation, err := selected.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(), BindingRevision: 1,
		ConversationRef: "conversation-" + string(state), Now: now.Add(8 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{
		channelonboarding.PhasePromotingRegistration,
		channelonboarding.PhaseRetiringPredecessor,
		channelonboarding.PhaseDeliveringConfirmation,
	} {
		req := channelonboarding.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now.Add(time.Duration(op.Revision) * time.Second),
		}
		if phase == channelonboarding.PhaseDeliveringConfirmation {
			req.ConfirmationOperationID = uuid.NewString()
		}
		op, err = selected.AdvanceChannelOnboarding(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
	}
	return op, activation
}
