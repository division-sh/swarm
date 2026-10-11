//go:build linux || darwin

package serveapp

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"go.mau.fi/whatsmeow/types"
)

func TestRunServeWhatsAppConnectAfterUnbindResetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, reset := range []bool{false, true} {
			for _, saveProof := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/reset_%t/proof_%t", backend, reset, saveProof), func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
					peer := newServeNativeProtocolPeer(t)
					opts, _ := servedNativeProcessOptions(t, backend)
					opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session")
					opts.SelfCheck = true
					opts.SwarmDirSet, opts.StoreModeSet = true, true
					opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
					opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
					process := startServeRuntimeTestProcess(t, opts)
					process.waitForReadyLine()
					endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
					original := requireServedNativeProcessPairingRequest(t, endpoint, peer, channelonboarding.VerbConnect,
						"retained-fence-first-connect", "FENCE_FIRST_CLAIM", saveProof)
					rebound := requireServedNativeProcessPairingClaim(t, endpoint, peer, channelonboarding.VerbRebind,
						"retained-fence-rebind", saveProof, func(challenge string) types.JID {
							return peer.claimMessageFrom(challenge, "FENCE_REBIND_CLAIM",
								types.NewJID("15551234569", types.DefaultUserServer), types.NewJID("100000000004", types.HiddenUserServer))
						})
					requireServedJSONRPCResult(t, endpoint, "channel.unbind", map[string]any{
						"interface": rebound.Operation.Interface.Selector, "expected_revision": rebound.Operation.BindingRevision,
						"idempotency_key": "retained-fence-unbind"}, &struct{}{})
					requireServedNativeDisconnect(t, peer)
					if reset {
						requireServedJSONRPCResult(t, endpoint, "runtime.nuke", map[string]any{
							"include_source_artifacts": false, "idempotency_key": "retained-fence-reset"}, &struct{}{})
						requireServedNativeOperationWithdrawn(t, endpoint, original.Operation.OperationID)
						requireServedNativeOperationWithdrawn(t, endpoint, rebound.Operation.OperationID)
					} else {
						for _, id := range []string{original.Operation.OperationID, rebound.Operation.OperationID} {
							var history channelonboarding.Result
							requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &history)
							if history.Operation.OperationID != id || history.Readiness != nil && history.Readiness.Ready {
								t.Fatal("unbind deleted retained history or retained executable readiness", history)
							}
						}
					}
					for _, verb := range []string{"reconnect", "rebind"} {
						refused := requestServedJSONRPC(t, endpoint, "channel.onboarding_start", map[string]any{
							"provider": "whatsapp", "verb": verb, "save_proof": saveProof, "idempotency_key": "unbound-refuse-" + verb})
						if refused.Error == nil || refused.Error.Data["code"] != apiv1.ChannelBindingConflictCode {
							t.Fatal("unbound history became a current reconnect/rebind identity", verb, refused.Error)
						}
					}
					fresh := requireServedUnboundConnectAcrossRestart(t, endpoint, peer, saveProof, func(paired bool) string {
						if code := process.stop(); code != 0 {
							t.Fatal("fresh unconfirmed connect did not join for restart", code, process.outputString())
						}
						requireServedNativeDisconnect(t, peer)
						process = startServeRuntimeTestProcess(t, opts)
						process.waitForReadyLine()
						endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
						if paired {
							peer.awaitPostLogin()
						}
						return endpoint
					})
					if fresh.Operation.Verb != channelonboarding.VerbConnect ||
						fresh.Operation.BindingRevision != rebound.Operation.BindingRevision+2 ||
						fresh.Operation.OperationID == original.Operation.OperationID || fresh.Operation.OperationID == rebound.Operation.OperationID ||
						fresh.IdentityOperation == nil || fresh.IdentityOperation.OperationID == rebound.Operation.IdentityOperationID {
						t.Fatal("explicit connect lost its fresh ceremony or retained revision fence", fresh)
					}
					if code := process.stop(); code != 0 {
						t.Fatal("fresh-fence journey did not completely join", code, process.outputString())
					}
					requireServedNativeDisconnect(t, peer)
					select {
					case logout := <-peer.logouts:
						t.Fatal("unbind/reset inferred destructive provider logout", logout)
					default:
					}
				})
			}
		}
	}
}

func requireServedUnboundConnectAcrossRestart(t *testing.T, endpoint string, peer *serveNativeProtocolPeer,
	saveProof bool, restart func(bool) string,
) channelonboarding.Result {
	t.Helper()
	start := map[string]any{"provider": "whatsapp", "verb": "connect", "save_proof": saveProof, "idempotency_key": "retained-fence-fresh-connect"}
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", start, &result)
	id := result.Operation.OperationID
	waitPairing := func() {
		deadline := time.Now().Add(5 * time.Second)
		for result.Pairing == nil || result.Pairing.Code == "" {
			if time.Now().After(deadline) {
				t.Fatal("explicit unbound connect bypassed or lost fresh pairing", result)
			}
			time.Sleep(10 * time.Millisecond)
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
		}
	}
	waitPairing()
	if !saveProof {
		endpoint = restart(false)
		result = channelonboarding.Result{}
		requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", start, &result)
		if result.Operation.OperationID != id || result.Operation.BindingRevision != 0 || result.IdentityOperation != nil {
			t.Fatal("unpaired restart replaced the parent or promoted its historical fence", result)
		}
		waitPairing()
	}
	peer.pair(result.Pairing.Code)
	peer.awaitPostLogin()
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
	if result.IdentityOperation == nil || result.IdentityOperation.Kind != operatorchannel.OperationConnect ||
		result.IdentityOperation.State != operatorchannel.StateAwaitingClaim || result.Operation.BindingRevision != 0 ||
		result.Operation.ActivationRevision != 0 || result.Operation.Coordinate.TargetGeneration != 0 ||
		result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
		t.Fatal("unbound predecessor became confirmed identity or inherited reconnect", result)
	}
	child := *result.IdentityOperation
	if saveProof {
		endpoint = restart(true)
	}
	result = channelonboarding.Result{}
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", start, &result)
	if result.Operation.OperationID != id || result.IdentityOperation == nil || result.IdentityOperation.OperationID != child.OperationID ||
		result.IdentityOperation.Challenge != child.Challenge || result.Operation.BindingRevision != 0 ||
		result.Operation.ActivationRevision != 0 || result.IdentityOperation.Kind != operatorchannel.OperationConnect {
		t.Fatal("restart replay adopted historical proof or replaced fresh ceremony", result)
	}
	claimant := peer.claimMessage(result.IdentityOperation.Challenge, "FENCE_FRESH_CLAIM")
	deadline := time.Now().Add(5 * time.Second)
	for result.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation {
		if time.Now().After(deadline) {
			t.Fatal("fresh unbound claim did not reach confirmation", result)
		}
		time.Sleep(10 * time.Millisecond)
		requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
	}
	var confirmed struct {
		Binding operatorchannel.Binding `json:"binding"`
	}
	requireServedJSONRPCResult(t, endpoint, "channel.confirm", map[string]any{
		"operation_id": child.OperationID, "expected_revision": result.IdentityOperation.Revision, "approve": true}, &confirmed)
	if confirmed.Binding.Status != operatorchannel.BindingCurrent || confirmed.Binding.ExternalAccountRef != claimant.String() {
		t.Fatal("fresh unbound connect confirmed another claimant", confirmed)
	}
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
	if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready ||
		result.Operation.BindingRevision != confirmed.Binding.Revision || result.Operation.ActivationRevision < 1 || result.Operation.Coordinate.TargetGeneration < 1 {
		t.Fatal("fresh confirmation did not establish exact activation", result)
	}
	awaitServedNativeProcessSend(t, peer, "Swarm channel connected.")
	var replay channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", start, &replay)
	if replay.Operation.OperationID != id || replay.Operation.BindingRevision != result.Operation.BindingRevision ||
		replay.Operation.ConfirmationOperationID != result.Operation.ConfirmationOperationID {
		t.Fatal("completed connect replay repeated binding or confirmation", replay)
	}
	return result
}
