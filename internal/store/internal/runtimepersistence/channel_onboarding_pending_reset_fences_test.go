package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	"github.com/google/uuid"
)

func TestChannelOnboardingPendingResetInterruptionSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		fixture := openChannelOnboardingConfirmationFixture(t, backend)
		for _, inherited := range []bool{false, true} {
			for _, boundary := range []channelonboarding.TestLifecycleBoundary{channelonboarding.TestAfterStaleIdentitySettlement, channelonboarding.TestAfterPendingResetCleanup, channelonboarding.TestAfterPendingResetCommit} {
				for _, recovery := range []string{"retry", "local"} {
					t.Run(fmt.Sprintf("%s/inherited=%t/%s/%s", backend, inherited, boundary, recovery), func(t *testing.T) {
						rig := newBoundHandoffRig(t, fixture)
						ctx := context.Background()
						begun, account, retained := beginPendingResetJourney(t, rig, channelonboarding.VerbConnect, true, inherited)
						claimed := claimPendingResetJourney(t, rig, begun, account, true)
						before := rig.parent(t, begun.Operation.OperationID)
						if err := rig.file.Set(ctx, claimed.ProviderCredential.Key, "corrected-value"); err != nil {
							t.Fatal(err)
						}
						interrupted := errors.New("pending reset interrupted")
						rig.testBarrier = func(at channelonboarding.TestLifecycleBoundary) error {
							if at == boundary {
								return interrupted
							}
							return nil
						}
						if _, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now); !errors.Is(err, interrupted) {
							t.Fatalf("interrupt: %v", err)
						}
						checkpoint := rig.parent(t, before.OperationID)
						if checkpoint.BindingRevision != retained {
							t.Fatalf("lost inherited obligation: %#v", checkpoint)
						}
						if boundary != channelonboarding.TestAfterPendingResetCommit && !reflect.DeepEqual(checkpoint, before) {
							t.Fatalf("cleanup responsibility changed before commit: %#v", checkpoint)
						}
						if value, found, err := rig.file.Get(ctx, claimed.ProviderCredential.Key); err != nil || !found || value != "corrected-value" {
							t.Fatalf("cleanup removed corrected value: %q %t %v", value, found, err)
						}
						rig.testBarrier = nil
						if recovery == "local" {
							if err := rig.service.ReconcileLocal(ctx); err != nil {
								t.Fatal(err)
							}
							reset := rig.parent(t, before.OperationID)
							if reset.Phase != channelonboarding.PhasePreparing || reset.BindingRevision != retained || reset.IdentityOperationID != "" {
								t.Fatalf("local reset: %#v", reset)
							}
						}
						// Supplying replacement input to live retry must complete reset and admission in one call.
						fresh, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID, ProviderCredential: "replacement-token"})
						if err != nil || fresh.IdentityOperation == nil || fresh.IdentityOperation.OperationID == claimed.OperationID {
							t.Fatalf("replacement retry: %#v %v", fresh, err)
						}
						rig.confirm(t, fresh, account)
						rig.complete(t, before.OperationID)
					})
				}
			}
		}
	}
}

func TestChannelOnboardingPendingResetSelectedStoreFences(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		fixture := openChannelOnboardingConfirmationFixture(t, backend)
		for _, change := range []string{"retired", "revision", "missing_child", "publishing", "bound", "pending", "rejected", "expired", "wrong_parent", "wrong_evidence", "wrong_predecessor", "wrong_retained_revision", "unbound", "wrong_kind", "retire_during_cleanup", "revision_during_cleanup", "reset_during_cleanup"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				rig := newBoundHandoffRig(t, fixture)
				ctx := context.Background()
				begun, account, _ := beginPendingResetJourney(t, rig, channelonboarding.VerbConnect, false, true)
				claimed := claimPendingResetJourney(t, rig, begun, account, false)
				parent := rig.parent(t, begun.Operation.OperationID)
				switch change {
				case "bound", "rejected":
					if _, _, err := rig.identities.Confirm(ctx, claimed.OperationID, claimed.Revision, change == "bound", rig.now); err != nil {
						t.Fatal(err)
					}
				case "pending":
				case "expired":
					if _, err := rig.identities.ExpireOperation(ctx, claimed.OperationID, claimed.Revision, claimed.ExpiresAt); err != nil {
						t.Fatal(err)
					}
				default:
					if err := rig.file.Set(ctx, claimed.ProviderCredential.Key, "corrected-value"); err != nil {
						t.Fatal(err)
					}
					if _, _, err := rig.identities.Confirm(ctx, claimed.OperationID, claimed.Revision, true, rig.now); !errors.Is(err, operatorchannel.ErrCredentialStale) {
						t.Fatal(err)
					}
				}
				advance := func(req channelonboarding.AdvanceRequest) {
					var err error
					parent, err = fixture.store.AdvanceChannelOnboarding(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
				}
				req := channelonboarding.PendingResetRequest{OperationID: parent.OperationID, ExpectedRevision: parent.Revision, Now: rig.now}
				switch change {
				case "retired":
					retireChannelOnboardingParent(t, fixture.store, parent, rig.now)
				case "revision":
					req.ExpectedRevision++
				case "missing_child", "publishing", "wrong_retained_revision":
					a := channelonboarding.AdvanceRequest{OperationID: parent.OperationID, ExpectedRevision: parent.Revision, Phase: parent.Phase, Now: rig.now}
					if change == "missing_child" {
						a.IdentityOperationID = uuid.NewString()
					}
					if change == "publishing" {
						a.Phase = channelonboarding.PhasePublishingActivation
					}
					if change == "wrong_retained_revision" {
						a.BindingRevision = parent.BindingRevision + 10
					}
					advance(a)
					req.ExpectedRevision = parent.Revision
				case "unbound":
					if _, _, err := rig.identities.Unbind(ctx, claimed.Interface.Selector, parent.BindingRevision, uuid.NewString(), uuid.NewString(), rig.now); err != nil {
						t.Fatal(err)
					}
				case "wrong_parent", "wrong_evidence", "wrong_predecessor", "wrong_kind":
					// Corruption is used only for owner-enforced rejection, never to manufacture a successful lifecycle.
					db, _ := pendingResetTestDB(t, fixture.store)
					column, value := "onboarding_operation_id", any(uuid.NewString())
					if change == "wrong_evidence" {
						column, value = "provider_credential_key", "unadmitted-key"
					}
					if change == "wrong_predecessor" {
						column, value = "expected_binding_revision", parent.BindingRevision+1
					}
					if change == "wrong_kind" {
						column, value = "operation_kind", "rebind"
					}
					if _, err := db.ExecContext(ctx, "UPDATE operator_channel_operations SET "+column+"=$1 WHERE operation_id=$2", value, claimed.OperationID); err != nil {
						t.Fatal(err)
					}
				}
				before := rig.parent(t, parent.OperationID)
				if change == "retire_during_cleanup" || change == "revision_during_cleanup" || change == "reset_during_cleanup" {
					rig.credentialFile.afterRelease = func() error {
						rig.credentialFile.afterRelease = nil
						switch change {
						case "retire_during_cleanup":
							retireChannelOnboardingParent(t, fixture.store, before, rig.now)
						case "revision_during_cleanup":
							advance(channelonboarding.AdvanceRequest{OperationID: before.OperationID, ExpectedRevision: before.Revision, Phase: before.Phase, Now: rig.now})
						case "reset_during_cleanup":
							// Competing recovery finishes all exact cleanup before committing its reset.
							if err := rig.service.ReconcileLocal(ctx); err != nil {
								return err
							}
						}
						before = rig.parent(t, parent.OperationID)
						return nil
					}
					if _, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
						t.Fatalf("cleanup race: %v", err)
					}
				} else {
					for _, commit := range []bool{false, true} {
						req.Commit = commit
						if _, err := fixture.store.ResetChannelOnboardingPendingIdentity(ctx, req); err == nil {
							t.Fatalf("contradiction admitted commit=%t", commit)
						}
					}
				}
				if after := rig.parent(t, parent.OperationID); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejection mutated authority: before=%#v after=%#v", before, after)
				}
			})
		}
	}
}

func TestChannelOnboardingPendingResetIdentityJoinSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
			begun, account, retained := beginPendingResetJourney(t, rig, channelonboarding.VerbConnect, false, true)
			child := claimPendingResetJourney(t, rig, begun, account, true)
			ctx := context.Background()
			if err := rig.file.Set(ctx, child.ProviderCredential.Key, "rotated"); err != nil {
				t.Fatal(err)
			}
			if _, _, err := rig.identities.Confirm(ctx, child.OperationID, child.Revision, true, rig.now); !errors.Is(err, operatorchannel.ErrCredentialStale) {
				t.Fatal(err)
			}
			parent := rig.parent(t, begun.Operation.OperationID)
			db, postgres := pendingResetTestDB(t, rig.fixture.store)
			for _, mismatch := range []string{"principal", "interface", "binding_interface"} {
				t.Run(mismatch, func(t *testing.T) {
					req := identityowner.StaleOnboardingChildRequest{ParentID: parent.OperationID, PrincipalID: parent.PrincipalID, Interface: parent.Interface, ChildID: child.OperationID, RetainedBindingRevision: retained, AdmittedCredentials: []credentials.ValueEvidence{child.ProviderCredential}}
					if mismatch == "principal" {
						req.PrincipalID = uuid.NewString()
					}
					if mismatch == "interface" {
						req.Interface.SemanticGeneration = uuid.NewString()
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if mismatch == "binding_interface" {
						if _, err := tx.ExecContext(ctx, "UPDATE operator_channel_bindings SET semantic_generation=$1 WHERE interface_key=$2", uuid.NewString(), parent.Interface.Key()); err != nil {
							t.Fatal(err)
						}
					}
					if err := identityowner.RequireStaleOnboardingChild(ctx, tx, postgres, req); !errors.Is(err, operatorchannel.ErrRevisionConflict) {
						t.Fatalf("identity join: %v", err)
					}
				})
			}
		})
	}
}

func pendingResetTestDB(t *testing.T, store channelOnboardingConfirmationStore) (*sql.DB, bool) {
	t.Helper()
	switch s := store.(type) {
	case *SQLiteRuntimeStore:
		return s.backend.ConstructionHandle(), false
	case *PostgresStore:
		return s.backend.ConstructionHandle(), true
	default:
		t.Fatalf("unexpected selected store %T", store)
		return nil, false
	}
}
