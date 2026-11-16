package serveapp

import (
	"net/http"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelClaimStopRefusesOnlyMatchedCompletionBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, true)
			writeChannelOnboardingLiveToolCredentials(t, h.credentialPath)
			managers := make(chan *runtime.RuntimeContextManager, 2)
			h.opts.TestRuntimeContextsReadyHook = func(m *runtime.RuntimeContextManager) { managers <- m }
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			manager := <-managers
			var begun channelonboarding.Result
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
				"provider": "telegram", "verb": "connect", "provider_credential": "stop-claim-credential",
			}, &begun)
			if begun.IdentityOperation == nil || begun.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
				t.Fatalf("missing pending challenge: %+v", begun)
			}
			callback, signing, _ := h.provider.Registration()
			challenge := begun.IdentityOperation.Challenge
			if err := manager.SealInboundExecution(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := manager.OpenStoredInbound(); err != nil {
				t.Fatal(err)
			}
			refused := submitChannelOnboardingClaimAs(t, callback, signing, challenge, 89101, 8593, 9593, "pending_operator")
			if refused.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("matched completion was not refused during stop: %+v", refused)
			}
			ordinary := submitChannelOnboardingClaimAs(t, callback, signing, "ordinary customer message", 89102, 8594, 9594, "customer")
			if ordinary.StatusCode != http.StatusAccepted || ordinary.Disposition == "consumed_by_binding" {
				t.Fatalf("pending claim blocked or consumed ordinary input: %+v", ordinary)
			}
			pending := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
			if pending.IdentityOperation == nil || pending.IdentityOperation.OperationID != begun.IdentityOperation.OperationID ||
				pending.IdentityOperation.State != operatorchannel.StateAwaitingClaim || pending.IdentityOperation.Challenge != challenge ||
				pending.Operation.BindingRevision != begun.Operation.BindingRevision || pending.Operation.AccountConfirmed {
				t.Fatalf("stop refusal changed pending claim custody: before=%+v after=%+v", begun, pending)
			}
			if err := manager.CloseInboundRequests(t.Context()); err != nil {
				t.Fatal(err)
			}
			h.stop(t)
			h.start(t)
			pending = getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
			if pending.IdentityOperation == nil || pending.IdentityOperation.State != operatorchannel.StateAwaitingClaim || pending.IdentityOperation.Challenge != challenge {
				t.Fatalf("restart did not retain the same pending challenge: %+v", pending)
			}
			callback, signing, _ = h.provider.Registration()
			requireChannelClaimDisposition(t, "resend after restart", submitChannelOnboardingClaimAs(t, callback, signing, challenge, 89101, 8593, 9593, "pending_operator"), "consumed_by_binding")
			claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
			if claimed.IdentityOperation == nil || claimed.IdentityOperation.OperationID != begun.IdentityOperation.OperationID || claimed.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation {
				t.Fatalf("refused delivery was poisoned or settled twice: %+v", claimed)
			}
		})
	}
}
