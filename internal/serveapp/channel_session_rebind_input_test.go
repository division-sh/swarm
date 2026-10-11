//go:build linux || darwin

package serveapp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorread"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/storetest"
	"go.mau.fi/whatsmeow/types"
)

func TestServedWhatsAppIdentityRebindRejectsPendingInputBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts, location := servedNativeProcessOptions(t, backend)
			opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-input")
			process := startServedNativeSDKProcess(t, opts, peer)
			endpoint := process.endpoint(t) + "/v1/rpc"
			peer.onACKTimeout = func(id string) { logServedNativeWait(t, endpoint, peer, "rebind_input_ack:"+id) }
			original := requireServedNativeProcessPairing(t, endpoint, peer)
			originalConnection := readServedNativeConnectionID(t, backend, location, original.Operation.OperationID)
			reader, err := storetest.OpenChannelObservation(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			})
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": original.Operation.Coordinate.BundleHash,
				"payload": map[string]any{"detail": "Rebind pending native input"}, "idempotency_key": "native-rebind-input"})
			cardID := waitServedNativePendingCard(t, endpoint)
			card := awaitServedNativeInputMessage(t, peer, "\nReference: "+cardID+"\n")
			peer.reply("reject", "REBIND_INPUT_BEGIN", card)
			first := awaitServedNativeInputMessage(t, peer, "Input: reason (text) required")
			peer.reply("private-native-reason", "REBIND_INPUT_PARTIAL", first)
			oldPrompt := awaitServedNativeInputMessage(t, peer, "Input: count (integer) required")
			requireServedNativeInputCard(t, endpoint, cardID, false)

			// The existing principal explicitly admits a fresh signed session and
			// a different human claimant. Pairing never substitutes for confirmation.
			from := types.NewJID("15551234569", types.DefaultUserServer)
			lid := types.NewJID("100000000004", types.HiddenUserServer)
			rebound := requireServedNativeProcessPairingClaim(t, endpoint, peer, channelonboarding.VerbRebind,
				"explicit-native-input-rebind", false, func(challenge string) types.JID {
					return peer.claimMessageFrom(challenge, "REBIND_OPERATOR_CLAIM", from, lid)
				})
			if rebound.Operation.OperationID == original.Operation.OperationID ||
				readServedNativeConnectionID(t, backend, location, rebound.Operation.OperationID) == originalConnection ||
				rebound.Operation.BindingRevision <= original.Operation.BindingRevision || rebound.Binding == nil ||
				rebound.Binding.ExternalAccountRef != from.String() {
				t.Fatal("native rebind did not replace exact operator responsibility", rebound)
			}
			peer.reply("27", "REBIND_OBSOLETE_PROMPT", oldPrompt)
			query := render.IntentObservationQuery{Kind: render.IntentText, Provider: "whatsapp",
				ConversationReference: from.String(), InterfaceKey: rebound.Operation.Interface.Key(), MessageReference: "REBIND_OBSOLETE_PROMPT"}
			waitTextReplyIntent(t, reader, query, "teaching")
			requireServedNativeInputCard(t, endpoint, cardID, false)
			peer.reply("reject", "REBIND_OBSOLETE_CARD", card)
			query.MessageReference = "REBIND_OBSOLETE_CARD"
			// Rebind changes the audience epoch. No current receipt can turn
			// the predecessor's physical card into an action for the new claimant.
			waitTextReplyIntent(t, reader, query, "teaching")
			requireServedNativeInputNoAction(t, endpoint, reader, rebound.Operation.Coordinate.BundleHash, query)
			requireServedNativeInputCard(t, endpoint, cardID, false)
			oldClaimant := types.NewJID("15551234568", types.DefaultUserServer)
			oldLID := types.NewJID("100000000002", types.HiddenUserServer)
			peer.replyFrom("27", "REBIND_FORMER_CLAIMANT", oldPrompt, oldClaimant, oldLID)
			formerQuery := query
			formerQuery.MessageReference, formerQuery.ConversationReference = "REBIND_FORMER_CLAIMANT", oldClaimant.String()
			waitTextReplyIntent(t, reader, formerQuery, "rejected")
			requireServedNativeInputNoAction(t, endpoint, reader, rebound.Operation.Coordinate.BundleHash, formerQuery)
			peer.text("/inbox", "REBIND_CURRENT_INBOX")
			query.MessageReference = "REBIND_CURRENT_INBOX"
			waitTextReplyIntent(t, reader, query, "entry")
			// The real SDK sends to the new claimant's observed linked-device
			// address; the selected operator binding still retains the exact PN.
			if inbox := awaitServedNativeInputMessage(t, peer, "Inbox\n"); inbox.To.ToNonAD() != lid {
				t.Fatal("rebound Inbox used a different linked-device destination", inbox.To, lid)
			}
			if err := process.stop(); err != nil {
				t.Fatal("rebound pending input did not join before restart", err)
			}
			requireServedNativeDisconnect(t, peer)
			process = startServedNativeSDKProcess(t, opts, peer)
			endpoint = process.endpoint(t) + "/v1/rpc"
			peer.awaitPostLogin()
			var restored channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": rebound.Operation.OperationID}, &restored)
			if restored.Readiness == nil || !restored.Readiness.Ready || restored.Binding == nil ||
				restored.Binding.ExternalAccountRef != from.String() || restored.Operation.BindingRevision != rebound.Operation.BindingRevision ||
				restored.Operation.Coordinate.RuntimeInstanceID == rebound.Operation.Coordinate.RuntimeInstanceID {
				t.Fatal("restart adopted the former claimant or lost rebound identity", restored)
			}
			peer.reply("27", "REBIND_RESTART_OLD_PROMPT", oldPrompt)
			query.MessageReference = "REBIND_RESTART_OLD_PROMPT"
			waitTextReplyIntent(t, reader, query, "teaching")
			requireServedNativeInputNoAction(t, endpoint, reader, restored.Operation.Coordinate.BundleHash, query)
			peer.reply("reject", "REBIND_RESTART_OLD_CARD", card)
			query.MessageReference = "REBIND_RESTART_OLD_CARD"
			waitTextReplyIntent(t, reader, query, "teaching")
			requireServedNativeInputNoAction(t, endpoint, reader, restored.Operation.Coordinate.BundleHash, query)
			requireServedNativeInputCard(t, endpoint, cardID, false)
			peer.text("/inbox", "REBIND_RESTART_INBOX")
			query.MessageReference = "REBIND_RESTART_INBOX"
			waitTextReplyIntent(t, reader, query, "entry")
			if inbox := awaitServedNativeInputMessage(t, peer, "Inbox\n"); inbox.To.ToNonAD() != lid {
				t.Fatal("restart sent Inbox to a different linked-device destination", inbox.To, lid)
			}
			var decisions operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
				"filter": map[string]any{"run_id": seed.RunID, "event_name": "mailbox.card_decided"}, "limit": 10}, &decisions)
			if len(decisions.Events) != 0 || decisions.NextCursor != "" {
				t.Fatal("stale native input or control completed the pending decision", decisions)
			}
			if err := process.stop(); err != nil {
				t.Fatal("rebound pending input restart did not join", err)
			}
			requireServedNativeDisconnect(t, peer)
		})
	}
}

func requireServedNativeInputNoAction(t *testing.T, endpoint string, reader render.Observer, bundleHash string, text render.IntentObservationQuery) {
	t.Helper()
	query := servedNativeInputActionObservation(t, endpoint, bundleHash, text)
	if action, found, err := reader.ObserveChannelIntent(context.Background(), query); err != nil || found {
		t.Fatal("refused predecessor input still derived an executable action", action, found, err)
	}
}
