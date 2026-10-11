//go:build linux || darwin

package serveapp

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
)

func TestServedWhatsAppLogoutKeepsFreshSameAccountSuccessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			originalPeer := newServeNativeProtocolPeer(t)
			opts, _ := servedNativeProcessOptions(t, backend)
			process := startServedNativeSDKProcess(t, opts, originalPeer)
			endpoint := process.endpoint(t) + "/v1/rpc"
			original := requireServedNativeProcessPairing(t, endpoint, originalPeer)
			params := map[string]any{"operation_id": original.Operation.OperationID,
				"expected_revision": original.Operation.Revision, "idempotency_key": "logout-before-fresh-pairing"}
			completed := requireServedNativeOriginalLogout(t, endpoint, originalPeer, params)
			if err := process.stop(); err != nil {
				t.Fatal("original logout process did not join", err)
			}

			// A separate genuine peer performs a new, explicit signed pairing
			// for the same account only after original unlink and full join.
			successorPeer := newServeNativeProtocolPeer(t)
			if successorPeer.account != originalPeer.account {
				t.Fatal("successor isolation must exercise the same verified account")
			}
			process = startServedNativeSDKProcess(t, opts, successorPeer)
			endpoint = process.endpoint(t) + "/v1/rpc"
			successor := requireServedNativeProcessPairingRequest(t, endpoint, successorPeer,
				channelonboarding.VerbReconnect, "explicit-successor-pairing", "SERVED_SUCCESSOR_CLAIM")
			if successor.Operation.OperationID == original.Operation.OperationID ||
				successor.Operation.BindingRevision <= original.Operation.BindingRevision {
				t.Fatal("fresh ceremony adopted original operator responsibility", successor.Operation)
			}
			requireServedNativeHistoricalLogoutIsolation(t, endpoint, params, completed, successor)
			successorPeer.text("Inbox", "SERVED_SUCCESSOR_INBOX")
			message := awaitServedNativeProcessSend(t, successorPeer, "Inbox\n")
			successorPeer.mu.Lock()
			from, lid := successorPeer.from.ToNonAD(), successorPeer.fromLID.ToNonAD()
			successorPeer.mu.Unlock()
			if message.To.ToNonAD() != from && message.To.ToNonAD() != lid {
				t.Fatal("current Inbox reply selected a different verified claimant", message.To)
			}
			if err := process.stop(); err != nil {
				t.Fatal("successor SDK owner did not join", err)
			}
			requireServedNativeDisconnect(t, successorPeer)
			for _, peer := range []*serveNativeProtocolPeer{originalPeer, successorPeer} {
				select {
				case frame := <-peer.logouts:
					t.Fatal("old replay or ordinary stop sent another unlink", frame)
				default:
				}
			}
		})
	}
}

func requireServedNativeOriginalLogout(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, params map[string]any) channelonboarding.SessionLogoutReadback {
	t.Helper()
	var completed channelonboarding.SessionLogoutReadback
	requireServedJSONRPCResult(t, endpoint, "channel.logout", params, &completed)
	if completed.Validate() != nil || completed.Teardown.Phase != channelonboarding.TeardownSucceeded {
		t.Fatal("original unlink did not complete through the public journal", completed)
	}
	select {
	case <-peer.logouts:
	case <-time.After(5 * time.Second):
		t.Fatal("completed original logout has no observed SDK unlink")
	}
	requireServedNativeDisconnect(t, peer)
	return completed
}

func requireServedNativeHistoricalLogoutIsolation(t *testing.T, endpoint string, params map[string]any,
	completed channelonboarding.SessionLogoutReadback, successor channelonboarding.Result,
) {
	t.Helper()
	var replay channelonboarding.SessionLogoutReadback
	requireServedJSONRPCResult(t, endpoint, "channel.logout", params, &replay)
	if replay.Teardown.TeardownID != completed.Teardown.TeardownID ||
		replay.Teardown.Revision != completed.Teardown.Revision || replay.EffectOperationID != completed.EffectOperationID ||
		replay.Teardown.Phase != channelonboarding.TeardownSucceeded {
		t.Fatal("old logout replay replaced its historical responsibility", replay)
	}
	params["idempotency_key"] = "old-logout-cannot-adopt-successor"
	refused := requestServedJSONRPC(t, endpoint, "channel.logout", params)
	if refused.Error == nil || refused.Error.Data["code"] != apiv1.ChannelBindingConflictCode {
		t.Fatal("new key on old logout did not refuse exact historical authority", refused)
	}
	var current channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{
		"operation_id": successor.Operation.OperationID}, &current)
	if current.Operation.Revision != successor.Operation.Revision || current.Operation.BindingRevision != successor.Operation.BindingRevision ||
		current.Readiness == nil || !current.Readiness.Ready || current.Logout != nil {
		t.Fatal("historical logout mutated current same-account authority", current)
	}
}
