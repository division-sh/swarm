package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

func TestChannelOnboardingConfirmedReconnectBeforeParentPhaseSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
			ctx := context.Background()
			prior := rig.start(t, channelonboarding.VerbConnect, "predecessor-token", false)
			rig.confirm(t, prior, "account-a")
			rig.complete(t, prior.Operation.OperationID)
			begun := rig.start(t, channelonboarding.VerbReconnect, "original-token", false)
			claimed, err := rig.fixture.settle(ctx, operatorChannelContractClaim(*begun.IdentityOperation, operatorchannel.ConversationScopeDirect, "account-a", "conversation-account-a", uuid.NewString()), rig.now)
			if err != nil {
				t.Fatal(err)
			}
			bound, _, err := rig.service.ConfirmIdentity(ctx, claimed.Operation.OperationID, claimed.Operation.Revision, true, rig.now)
			if err != nil {
				t.Fatal(err)
			}
			before := rig.parent(t, begun.Operation.OperationID)
			if before.Phase != channelonboarding.PhaseAwaitingExternalIdentity || before.BindingRevision != 0 {
				t.Fatalf("API confirmation changed parent phase: %#v", before)
			}
			if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "intervening-token"); err != nil {
				t.Fatal(err)
			}
			if err := rig.service.ReconcileLocal(ctx); err != nil {
				t.Fatal(err)
			}
			reset := rig.parent(t, before.OperationID)
			if reset.Phase != channelonboarding.PhasePreparing || reset.BindingRevision != bound.BindingRevision {
				t.Fatalf("local recovery skipped committed reconnect: %#v", reset)
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

func TestChannelOnboardingStaleConfirmationBeforeParentPhaseSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
			ctx := context.Background()
			begun := rig.start(t, channelonboarding.VerbConnect, "original-token", false)
			claimed, err := rig.fixture.settle(ctx, operatorChannelContractClaim(*begun.IdentityOperation, operatorchannel.ConversationScopeDirect, "account-a", "conversation-account-a", uuid.NewString()), rig.now)
			if err != nil {
				t.Fatal(err)
			}
			if err := rig.file.Set(ctx, claimed.Operation.ProviderCredential.Key, "intervening-token"); err != nil {
				t.Fatal(err)
			}
			_, _, err = rig.service.ConfirmIdentity(ctx, claimed.Operation.OperationID, claimed.Operation.Revision, true, rig.now)
			var required *channelonboarding.CredentialRequiredError
			if !errors.As(err, &required) {
				t.Fatalf("early stale confirmation did not reset parent: %v; parent=%s", err, rig.parent(t, begun.Operation.OperationID).Phase)
			}
			reset := rig.parent(t, begun.Operation.OperationID)
			if reset.Phase != channelonboarding.PhasePreparing || reset.IdentityOperationID != "" {
				t.Fatalf("stale early confirmation reset = %#v", reset)
			}
			resumed, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: reset.OperationID, ProviderCredential: "original-token"})
			if err != nil {
				t.Fatal(err)
			}
			rig.confirm(t, resumed, "account-a")
			rig.complete(t, reset.OperationID)
		})
	}
}
