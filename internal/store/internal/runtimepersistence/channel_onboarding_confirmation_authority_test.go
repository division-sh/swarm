package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type channelOnboardingConfirmationStore interface {
	channelonboarding.Store
	operatorchannel.Store
}

type channelOnboardingConfirmationFixture struct {
	store  channelOnboardingConfirmationStore
	settle func(context.Context, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
}

func TestOperatorChannelConfirmationRequiresActiveOwningOnboardingParentSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openChannelOnboardingConfirmationFixture(t, backend)
			ctx := context.Background()
			now := time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)

			t.Run("retired parent rejects nonterminal child without mutation", func(t *testing.T) {
				parent, child := prepareChannelOnboardingConfirmation(t, fixture, "retired-parent", now)
				retireChannelOnboardingParent(t, fixture.store, parent, now.Add(5*time.Second))

				confirmed, binding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
					OperationID: child.OperationID, PrincipalID: child.PrincipalID, ExpectedRevision: child.Revision,
					Approve: true, ProviderCredentialCurrent: true, ConfirmedAt: now.Add(6 * time.Second),
				})
				if !errors.Is(err, operatorchannel.ErrConflict) || confirmed != (operatorchannel.Operation{}) || binding != (operatorchannel.Binding{}) {
					t.Fatalf("retired-parent confirmation = op:%#v binding:%#v err:%v", confirmed, binding, err)
				}
				persisted, err := fixture.store.ListOperatorChannelOperations(ctx, child.PrincipalID)
				if err != nil {
					t.Fatal(err)
				}
				assertOperatorChannelOperationState(t, persisted, child.OperationID, operatorchannel.StateAwaitingConfirmation, child.Revision)
			})

			t.Run("active parent rejects a child it no longer owns", func(t *testing.T) {
				parent, child := prepareChannelOnboardingConfirmation(t, fixture, "wrong-child", now.Add(time.Minute))
				parent, err := fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
					OperationID: parent.OperationID, ExpectedRevision: parent.Revision,
					Phase: channelonboarding.PhaseAwaitingOperatorConfirmation, IdentityOperationID: uuid.NewString(), Now: now.Add(65 * time.Second),
				})
				if err != nil {
					t.Fatal(err)
				}
				if parent.IdentityOperationID == child.OperationID {
					t.Fatal("test did not replace parent child ownership")
				}

				_, _, err = fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
					OperationID: child.OperationID, PrincipalID: child.PrincipalID, ExpectedRevision: child.Revision,
					Approve: true, ProviderCredentialCurrent: true, ConfirmedAt: now.Add(66 * time.Second),
				})
				if !errors.Is(err, operatorchannel.ErrRevisionConflict) {
					t.Fatalf("unowned-child confirmation error = %v, want revision conflict", err)
				}
			})

			t.Run("retirement preserves exact terminal replay", func(t *testing.T) {
				parent, child := prepareChannelOnboardingConfirmation(t, fixture, "terminal-replay", now.Add(2*time.Minute))
				confirmed, binding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
					OperationID: child.OperationID, PrincipalID: child.PrincipalID, ExpectedRevision: child.Revision,
					Approve: true, ProviderCredentialCurrent: true, ConfirmedAt: now.Add(125 * time.Second),
				})
				if err != nil || confirmed.State != operatorchannel.StateBound || binding.Status != operatorchannel.BindingCurrent {
					t.Fatalf("initial confirmation = op:%#v binding:%#v err:%v", confirmed, binding, err)
				}
				retireChannelOnboardingParent(t, fixture.store, parent, now.Add(126*time.Second))

				replayed, replayedBinding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
					OperationID: child.OperationID, PrincipalID: child.PrincipalID, ExpectedRevision: child.Revision,
					Approve: true, ProviderCredentialCurrent: false, ConfirmedAt: now.Add(127 * time.Second),
				})
				if err != nil || replayed != confirmed || replayedBinding != binding {
					t.Fatalf("post-retirement replay = op:%#v binding:%#v err:%v; want op:%#v binding:%#v", replayed, replayedBinding, err, confirmed, binding)
				}
			})
		})
	}
}

func openChannelOnboardingConfirmationFixture(t *testing.T, backend string) channelOnboardingConfirmationFixture {
	t.Helper()
	switch backend {
	case "sqlite":
		selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return channelOnboardingConfirmationFixture{
			store: selected,
			settle: func(ctx context.Context, claim operatorchannel.InboundClaim, now time.Time) (operatorchannel.ClaimSettlement, error) {
				var result operatorchannel.ClaimSettlement
				err := selected.backend.RunTransaction(ctx, "onboarding confirmation authority claim", func(txctx context.Context, tx *sql.Tx) error {
					var err error
					result, err = selected.operatorChannelSQLiteOwner.SettleInboundClaimTx(txctx, tx, claim, now)
					return err
				})
				return result, err
			},
		}
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := admitTestPostgresStore(t, db)
		return channelOnboardingConfirmationFixture{
			store: selected,
			settle: func(ctx context.Context, claim operatorchannel.InboundClaim, now time.Time) (operatorchannel.ClaimSettlement, error) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return operatorchannel.ClaimSettlement{}, err
				}
				result, err := selected.operatorChannelPostgresOwner.SettleInboundClaimTx(ctx, tx, claim, now)
				if err != nil {
					_ = tx.Rollback()
					return result, err
				}
				return result, tx.Commit()
			},
		}
	default:
		t.Fatalf("unknown backend %q", backend)
		return channelOnboardingConfirmationFixture{}
	}
}

func prepareChannelOnboardingConfirmation(t *testing.T, fixture channelOnboardingConfirmationFixture, suffix string, now time.Time) (channelonboarding.Operation, operatorchannel.Operation) {
	t.Helper()
	ctx := context.Background()
	principal, err := fixture.store.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	request := channelOnboardingStartRequest(principal.ID, now)
	request.OperationID = uuid.NewString()
	request.RequestKeyHash = "confirmation-parent-key-" + suffix
	request.RequestHash = "confirmation-parent-input-" + suffix
	request.Interface.SemanticGeneration = "confirmation-authority-" + suffix
	request.Interface.Selector = ""
	request.Interface = request.Interface.Normalized()
	request.TargetSelector = "ingress:support:flow:telegram-" + suffix
	parent, err := fixture.store.ReserveChannelOnboarding(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	evidence := operatorChannelProviderEvidence()
	evidence.Key = request.CredentialReservations[0].StoreKey
	parent, err = fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: parent.OperationID, ExpectedRevision: parent.Revision, Phase: channelonboarding.PhaseCredentialsAdmitted,
		CredentialAdmissions: []channelonboarding.CredentialAdmission{{
			Role: request.CredentialReservations[0].Role, StoreKey: evidence.Key,
			Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: evidence.Seal,
		}},
		ReplaceCredentialAdmissions: true, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err = fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: parent.OperationID, ExpectedRevision: parent.Revision,
		Phase: channelonboarding.PhaseActivatingProvider, Now: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: request.Interface, ExpectedRevision: 0,
		RequestKeyHash: "confirmation-child-key-" + suffix, RequestHash: "confirmation-child-input-" + suffix,
		OnboardingOperationID: parent.OperationID, ProviderCredential: evidence,
		RequestedAt: now.Add(2 * time.Second), ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err = fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: parent.OperationID, ExpectedRevision: parent.Revision, Phase: channelonboarding.PhaseAwaitingExternalIdentity,
		IdentityOperationID: child.OperationID, Now: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := fixture.settle(ctx, operatorChannelContractClaim(child, operatorchannel.ConversationScopeDirect, "account-"+suffix, "conversation-"+suffix, uuid.NewString()), now.Add(4*time.Second))
	if err != nil || settlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("claim settlement = %#v, %v", settlement, err)
	}
	parent, err = fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: parent.OperationID, ExpectedRevision: parent.Revision, Phase: channelonboarding.PhaseAwaitingOperatorConfirmation,
		IdentityOperationID: child.OperationID, Now: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	return parent, settlement.Operation
}

func retireChannelOnboardingParent(t *testing.T, store channelOnboardingConfirmationStore, parent channelonboarding.Operation, now time.Time) {
	t.Helper()
	ctx := context.Background()
	teardown, err := store.ReserveChannelTeardown(ctx, channelonboarding.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: "confirmation-retire-key-" + parent.OperationID,
		RequestHash: "confirmation-retire-input-" + parent.OperationID, Kind: channelonboarding.TeardownContextRetirement,
		PrincipalID: parent.PrincipalID,
		Scope: channelonboarding.TeardownScope{
			BundleHash: parent.Coordinate.BundleHash, BundleSource: parent.Coordinate.BundleSource,
			ContextPublicationGeneration: parent.Coordinate.ContextPublicationGeneration,
		},
		RequestedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	teardown, err = store.RetireChannelTeardownAuthority(ctx, channelonboarding.RetireTeardownAuthorityRequest{
		TeardownID: teardown.TeardownID, ExpectedRevision: teardown.Revision, Reason: "confirmation authority test", Now: now,
	})
	if err != nil || teardown.Phase != channelonboarding.TeardownAuthorityRetired {
		t.Fatalf("retire parent authority = %#v, %v", teardown, err)
	}
}

func assertOperatorChannelOperationState(t *testing.T, operations []operatorchannel.Operation, operationID string, state operatorchannel.OperationState, revision int64) {
	t.Helper()
	for _, operation := range operations {
		if operation.OperationID != operationID {
			continue
		}
		if operation.State != state || operation.Revision != revision {
			t.Fatalf("operation %s = %s/%d, want %s/%d", operationID, operation.State, operation.Revision, state, revision)
		}
		return
	}
	t.Fatalf("operation %s not found", operationID)
}
