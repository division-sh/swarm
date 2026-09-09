package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

func TestChannelOnboardingPendingResetLifecycleSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openChannelOnboardingConfirmationFixture(t, backend)
			for _, verb := range []channelonboarding.Verb{channelonboarding.VerbConnect, channelonboarding.VerbReconnect, channelonboarding.VerbRebind} {
				for _, proof := range []bool{false, true} {
					for _, inherited := range []bool{false, true} {
						for _, early := range []bool{false, true} {
							for _, entry := range []string{"confirm", "retry", "local"} {
								t.Run(fmt.Sprintf("%s/proof=%t/inherited=%t/early=%t/%s", verb, proof, inherited, early, entry), func(t *testing.T) {
									rig := newBoundHandoffRig(t, fixture)
									begun, account, retained := beginPendingResetJourney(t, rig, verb, proof, inherited)
									ctx := context.Background()
									for cycle := 0; cycle < 2; cycle++ {
										claimed := claimPendingResetJourney(t, rig, begun, account, early)
										before := rig.parent(t, begun.Operation.OperationID)
										bindingsBefore, err := fixture.store.ListOperatorChannelBindings(ctx, before.PrincipalID)
										if err != nil {
											t.Fatal(err)
										}
										observationErr := errors.New("observation interrupted")
										rig.currentness.err = observationErr
										if _, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now); !errors.Is(err, observationErr) {
											t.Fatalf("observation error = %v", err)
										}
										rig.currentness.err = nil
										if rig.parent(t, before.OperationID).Revision != before.Revision {
											t.Fatal("observation error changed parent")
										}
										if err := rig.file.Set(ctx, claimed.ProviderCredential.Key, fmt.Sprintf("rotated-%d", cycle)); err != nil {
											t.Fatal(err)
										}
										if entry == "confirm" {
											_, _, err = rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now)
										} else {
											_, _, err = rig.identities.Confirm(ctx, claimed.OperationID, claimed.Revision, true, rig.now)
											if !errors.Is(err, operatorchannel.ErrCredentialStale) {
												t.Fatalf("settle child before parent recovery: %v", err)
											}
											if entry == "local" {
												err = rig.service.ReconcileLocal(ctx)
											} else {
												_, err = rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID})
											}
										}
										var required *channelonboarding.CredentialRequiredError
										if (entry == "local" && err != nil) || (entry != "local" && !errors.As(err, &required)) {
											t.Fatalf("reset through %s: %v", entry, err)
										}
										reset := rig.parent(t, before.OperationID)
										bindingsAfter, err := fixture.store.ListOperatorChannelBindings(ctx, before.PrincipalID)
										if err != nil || !reflect.DeepEqual(bindingsBefore, bindingsAfter) {
											t.Fatalf("pending reset erased or changed preexisting bindings: %v", err)
										}
										if reset.Phase != channelonboarding.PhasePreparing || reset.IdentityOperationID != "" || reset.BindingRevision != retained || len(reset.CredentialAdmissions) != 0 {
											t.Fatalf("cycle %d lost reset obligation: %#v; retained=%d", cycle, reset, retained)
										}
										stale, err := rig.identities.GetOperation(ctx, claimed.OperationID)
										if err != nil || stale.State != operatorchannel.StateCredentialStale || stale.BindingRevision != 0 || stale.ProofID != "" {
											t.Fatalf("stale child minted authority: %#v, %v", stale, err)
										}
										if _, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now); !errors.As(err, &required) {
											t.Fatalf("reset response replay: %v", err)
										}
										begun, err = rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: reset.OperationID, ProviderCredential: "replacement-token"})
										if err != nil || begun.IdentityOperation == nil || begun.IdentityOperation.OperationID == claimed.OperationID {
											t.Fatalf("replacement ceremony: %#v, %v", begun, err)
										}
										if inherited && (begun.IdentityOperation.Kind != operatorchannel.OperationReconnect || begun.IdentityOperation.ExpectedBindingRevision != retained) {
											t.Fatalf("cycle %d lost inherited reconnect: %#v", cycle, begun.IdentityOperation)
										}
										newParent := rig.parent(t, reset.OperationID)
										if _, _, err := rig.service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, rig.now); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
											t.Fatalf("old stale replay could reset new child: %v", err)
										}
										if rig.parent(t, reset.OperationID).Revision != newParent.Revision {
											t.Fatal("old replay changed new parent")
										}
									}
									rig.confirm(t, begun, account)
									rig.complete(t, begun.Operation.OperationID)
								})
							}
						}
					}
				}
			}
		})
	}
}

func beginPendingResetJourney(t *testing.T, rig *boundHandoffRig, verb channelonboarding.Verb, proof, inherited bool) (channelonboarding.Result, string, int64) {
	t.Helper()
	account := "account-a"
	if verb != channelonboarding.VerbConnect {
		prior := rig.start(t, channelonboarding.VerbConnect, "prior-token", proof)
		rig.confirm(t, prior, account)
		rig.complete(t, prior.Operation.OperationID)
	}
	if verb == channelonboarding.VerbRebind {
		account = "account-b"
	}
	begun := rig.start(t, verb, "original-token", proof)
	if !inherited {
		return begun, account, 0
	}
	bound := rig.confirm(t, begun, account)
	ctx := context.Background()
	if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "bound-rotation"); err != nil {
		t.Fatal(err)
	}
	if err := rig.service.ReconcileLocal(ctx); err != nil {
		t.Fatal(err)
	}
	begun, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID, ProviderCredential: "replacement-token"})
	if err != nil {
		t.Fatal(err)
	}
	return begun, account, bound.BindingRevision
}

func claimPendingResetJourney(t *testing.T, rig *boundHandoffRig, begun channelonboarding.Result, account string, early bool) operatorchannel.Operation {
	t.Helper()
	if !early {
		return rig.claim(t, begun, account)
	}
	settled, err := rig.fixture.settle(context.Background(), operatorChannelContractClaim(*begun.IdentityOperation, operatorchannel.ConversationScopeDirect, account, "conversation-"+account, uuid.NewString()), rig.now)
	if err != nil {
		t.Fatal(err)
	}
	return settled.Operation
}
