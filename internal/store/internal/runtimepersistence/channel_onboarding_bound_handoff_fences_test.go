package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

func TestChannelOnboardingBoundHandoffObservationAndCleanupReplaySelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
			ctx := context.Background()
			begun := rig.start(t, channelonboarding.VerbConnect, "original-token", false)
			bound := rig.confirm(t, begun, "account-a")
			before := rig.parent(t, begun.Operation.OperationID)
			observationError := errors.New("credential observation unavailable")
			rig.currentness.err = observationError
			if _, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID}); !errors.Is(err, observationError) {
				t.Fatalf("observation error = %v", err)
			}
			unchanged := rig.parent(t, before.OperationID)
			if unchanged.Revision != before.Revision || unchanged.BindingRevision != 0 || unchanged.IdentityOperationID != bound.OperationID {
				t.Fatalf("observation failure mutated parent: %#v", unchanged)
			}
			rig.currentness.err = nil
			if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "intervening-token"); err != nil {
				t.Fatal(err)
			}
			cleanupError := errors.New("interrupted after credential release")
			releases := 0
			rig.credentialFile.afterRelease = func() error {
				releases++
				if releases == len(before.CredentialAdmissions) {
					return cleanupError
				}
				return nil
			}
			if _, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID}); !errors.Is(err, cleanupError) {
				t.Fatalf("cleanup interruption = %v", err)
			}
			checkpoint := rig.parent(t, before.OperationID)
			if checkpoint.BindingRevision != bound.BindingRevision || checkpoint.Phase != before.Phase || len(checkpoint.CredentialAdmissions) != len(before.CredentialAdmissions) {
				t.Fatalf("interrupted cleanup lost checkpointed responsibility: %#v", checkpoint)
			}
			rig.credentialFile.afterRelease = nil
			if err := rig.service.ReconcileLocal(ctx); err != nil {
				t.Fatal(err)
			}
			reset := rig.parent(t, before.OperationID)
			if reset.Phase != channelonboarding.PhasePreparing || reset.BindingRevision != bound.BindingRevision {
				t.Fatalf("cleanup replay = %#v", reset)
			}
			resumed, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID, ProviderCredential: "original-token"})
			if err != nil {
				t.Fatal(err)
			}
			rig.confirm(t, resumed, "account-a")
			rig.complete(t, before.OperationID)
		})
	}
}

func TestChannelOnboardingBoundHandoffSelectedStoreFences(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"retire_before", "retire_during_cleanup", "revision_before", "revision_during_cleanup", "replace_child", "replace_binding", "contradict_parent_binding"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
				ctx := context.Background()
				begun := rig.start(t, channelonboarding.VerbConnect, "original-token", false)
				bound := rig.confirm(t, begun, "account-a")
				parent := rig.parent(t, begun.Operation.OperationID)
				mutate := func() {
					current := rig.parent(t, parent.OperationID)
					if change == "retire_before" || change == "retire_during_cleanup" {
						retireChannelOnboardingParent(t, rig.fixture.store, current, rig.now)
						return
					}
					if change == "replace_binding" {
						if _, _, err := rig.identities.Unbind(ctx, bound.Interface.Selector, bound.BindingRevision, uuid.NewString(), uuid.NewString(), rig.now); err != nil {
							t.Fatal(err)
						}
						return
					}
					req := channelonboarding.AdvanceRequest{OperationID: current.OperationID, ExpectedRevision: current.Revision, Phase: current.Phase, Now: rig.now}
					if change == "replace_child" {
						req.IdentityOperationID = uuid.NewString()
					}
					if change == "contradict_parent_binding" {
						req.BindingRevision = bound.BindingRevision + 10
					}
					if _, err := rig.fixture.store.AdvanceChannelOnboarding(ctx, req); err != nil {
						t.Fatal(err)
					}
				}
				if change == "retire_during_cleanup" || change == "revision_during_cleanup" {
					if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "intervening-token"); err != nil {
						t.Fatal(err)
					}
					rig.credentialFile.afterRelease = func() error { rig.credentialFile.afterRelease = nil; mutate(); return nil }
					_, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: parent.OperationID})
					if !errors.Is(err, channelonboarding.ErrRevisionConflict) {
						t.Fatalf("cleanup race = %v", err)
					}
				} else {
					mutate()
					if change == "replace_child" || change == "contradict_parent_binding" {
						parent = rig.parent(t, parent.OperationID)
					}
					_, err := rig.fixture.store.ReconcileChannelOnboardingBinding(ctx, channelonboarding.ReconcileBindingRequest{
						OperationID: parent.OperationID, ExpectedRevision: parent.Revision, ExpectedBindingRevision: bound.BindingRevision, ResetCredentials: true, Now: rig.now,
					})
					if !errors.Is(err, channelonboarding.ErrRevisionConflict) && !errors.Is(err, channelonboarding.ErrConflict) {
						t.Fatalf("exact selected-store handoff fence = %v", err)
					}
				}
				after := rig.parent(t, parent.OperationID)
				if after.Phase == channelonboarding.PhasePreparing || len(after.CredentialAdmissions) == 0 {
					t.Fatalf("competing owner was reset: %#v", after)
				}
			})
		}
	}
}

func TestChannelOnboardingBoundHandoffProofResponsibilityFencesSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, status := range []operatorchannel.ProofStatus{operatorchannel.ProofPending, operatorchannel.ProofFailed} {
			t.Run(backend+"/"+string(status), func(t *testing.T) {
				rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
				ctx := context.Background()
				begun := rig.start(t, channelonboarding.VerbConnect, "original-token", true)
				claimed := rig.claim(t, begun, "account-a")
				interrupted := errors.New("proof materialization interrupted")
				if status == operatorchannel.ProofPending {
					rig.currentness.err, rig.currentness.failAt = interrupted, rig.currentness.calls+2
				} else {
					rig.proofs.putErr = interrupted
				}
				_, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now)
				if !errors.Is(err, interrupted) {
					t.Fatalf("proof interruption = %v", err)
				}
				rig.currentness.err, rig.proofs.putErr = nil, nil
				bound, err := rig.identities.GetOperation(ctx, claimed.OperationID)
				if err != nil || bound.State != operatorchannel.StateBound || bound.ProofStatus != status {
					t.Fatalf("committed proof responsibility = %#v, %v", bound, err)
				}
				blocked, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID})
				if err != nil || blocked.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation {
					t.Fatalf("unfinished proof published activation: %#v, %v", blocked, err)
				}
				if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "intervening-token"); err != nil {
					t.Fatal(err)
				}
				if err := rig.service.ReconcileLocal(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID, ProviderCredential: "original-token"}); !errors.Is(err, operatorchannel.ErrConflict) {
					t.Fatalf("new ceremony bypassed unresolved proof: %v", err)
				}
				if _, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now); err != nil {
					t.Fatalf("exact terminal confirmation could not finish proof: %v", err)
				}
				resumed, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID})
				if err != nil || resumed.IdentityOperation == nil || resumed.IdentityOperation.OperationID == bound.OperationID {
					t.Fatalf("proof replay did not unblock fresh ceremony: %#v, %v", resumed, err)
				}
				rig.confirm(t, resumed, "account-a")
				rig.complete(t, begun.Operation.OperationID)
			})
		}
	}
}

func TestChannelOnboardingRetainedReconnectFencesSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, changed := range []bool{false, true} {
			name := "observation_error_retry"
			if changed {
				name = "competing_current_binding"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
				ctx := context.Background()
				begun := rig.start(t, channelonboarding.VerbConnect, "original-token", false)
				bound := rig.confirm(t, begun, "account-a")
				if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "intervening-token"); err != nil {
					t.Fatal(err)
				}
				if err := rig.service.ReconcileLocal(ctx); err != nil {
					t.Fatal(err)
				}
				observationError := errors.New("retained binding observation unavailable")
				if changed {
					if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "original-token"); err != nil {
						t.Fatal(err)
					}
					successor, err := rig.identities.Begin(ctx, bound.Interface.Selector, operatorchannel.OperationReconnect, bound.BindingRevision, uuid.NewString(), uuid.NewString(), "", bound.ProviderCredential, false, rig.now)
					if err != nil {
						t.Fatal(err)
					}
					settled, err := rig.fixture.settle(ctx, operatorChannelContractClaim(successor, operatorchannel.ConversationScopeDirect, "account-a", "conversation-account-a", uuid.NewString()), rig.now)
					if err != nil {
						t.Fatal(err)
					}
					_, binding, err := rig.identities.Confirm(ctx, successor.OperationID, settled.Operation.Revision, true, rig.now)
					if err != nil || binding.Revision != bound.BindingRevision+1 {
						t.Fatalf("competing binding = %#v, %v", binding, err)
					}
				} else {
					rig.currentness.err = observationError
				}
				_, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID, ProviderCredential: "original-token"})
				want := observationError
				if changed {
					want = channelonboarding.ErrRevisionConflict
				}
				if !errors.Is(err, want) {
					t.Fatalf("retained reconnect gate = %v, want %v", err, want)
				}
				parent := rig.parent(t, begun.Operation.OperationID)
				if parent.BindingRevision != bound.BindingRevision || parent.IdentityOperationID != "" {
					t.Fatalf("retained reconnect fence changed authority: %#v", parent)
				}
				if !changed {
					rig.currentness.err = nil
					resumed, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID})
					if err != nil {
						t.Fatal(err)
					}
					rig.confirm(t, resumed, "account-a")
					rig.complete(t, begun.Operation.OperationID)
				}
			})
		}
	}
}
