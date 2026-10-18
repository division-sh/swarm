package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

func TestChannelOnboardingCredentialConnectAfterUnbindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, saveProof := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/proof_%t", backend, saveProof), func(t *testing.T) {
				ctx := context.Background()
				rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
				first := rig.start(t, channelonboarding.VerbConnect, "first-provider-value", saveProof)
				rig.confirm(t, first, "first-account")
				rig.complete(t, first.Operation.OperationID)
				rebound := rig.start(t, channelonboarding.VerbRebind, "rebind-provider-value", saveProof)
				bound := rig.confirm(t, rebound, "different-account")
				rig.complete(t, rebound.Operation.OperationID)
				writer, err := channelonboarding.NewCredentialWriter(rig.credentialFile)
				if err != nil {
					t.Fatal(err)
				}
				teardown, err := channelonboarding.NewDestructiveService(rig.fixture.store, rig.identities, writer,
					retiredOnboardingActivations{}, func() time.Time { return rig.now }, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, unbound, err := teardown.Unbind(ctx, bound.Interface.Selector, bound.BindingRevision, "exact-unbind", "exact-unbind-body")
				if err != nil || unbound.Revision != 3 || unbound.Status != operatorchannel.BindingUnbound {
					t.Fatal("canonical teardown did not retain exact unbound fence", unbound, err)
				}
				fence, err := rig.identities.RetainedBindingFence(ctx, bound.Interface)
				if err != nil || fence.Revision != unbound.Revision || fence.CurrentIdentity != (operatorchannel.Binding{}) {
					t.Fatal("unbound fence observation manufactured current identity", fence, err)
				}
				if _, err := rig.identities.RetainedCurrentBinding(ctx, bound.Interface); !errors.Is(err, operatorchannel.ErrNotFound) {
					t.Fatal("current-only reader widened for explicit connect", err)
				}
				start := channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
					Selection: channelonboarding.CandidateSelection{Provider: "telegram"}, IdempotencyKey: "fresh-unbound-connect", SaveProof: saveProof}
				_, err = rig.service.Start(ctx, start)
				var required *channelonboarding.CredentialRequiredError
				if !errors.As(err, &required) {
					t.Fatal("fresh connect reused old provider credential or lost its responsibility", err)
				}
				parent := rig.parent(t, required.OperationID)
				if parent.ConnectFenceRevision != unbound.Revision || parent.BindingRevision != 0 || parent.IdentityOperationID != "" {
					t.Fatal("stored predecessor became a confirmed binding", parent)
				}
				fresh, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: parent.OperationID, ProviderCredential: "fresh-provider-value"})
				if err != nil || fresh.IdentityOperation == nil || fresh.IdentityOperation.Kind != operatorchannel.OperationConnect ||
					fresh.IdentityOperation.ExpectedBindingRevision != unbound.Revision || fresh.Operation.BindingRevision != 0 {
					t.Fatal("credential retry lost its frozen predecessor or became inherited reconnect", fresh, err)
				}
				confirmed := rig.confirm(t, fresh, "fresh-account")
				if confirmed.BindingRevision != 4 {
					t.Fatal("fresh confirmation failed exact predecessor CAS", confirmed)
				}
				rig.complete(t, parent.OperationID)
				replay, err := rig.service.Start(ctx, start)
				if err != nil || replay.Operation.OperationID != parent.OperationID || replay.Operation.BindingRevision != 4 ||
					replay.Operation.IdentityOperationID != confirmed.OperationID || replay.Operation.ConnectFenceRevision != unbound.Revision {
					t.Fatal("exact parent replay refreshed the fence or created another child", replay, err)
				}
			})
		}
	}
}
