//go:build linux || darwin

package serveapp

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedWhatsAppOrderedInputRestartBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/serveapp/testdata/whatsapp-session-input"))
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts, location := servedNativeProcessOptions(t, backend)
			opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-input")
			process := startServedNativeSDKProcess(t, opts, peer)
			endpoint := process.endpoint(t) + "/v1/rpc"
			peer.onACKTimeout = func(id string) { logServedNativeWait(t, endpoint, peer, "ordered_input_ack:"+id) }
			ready := requireServedNativeProcessPairing(t, endpoint, peer)
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
				"event_name": "work.requested", "bundle_hash": ready.Operation.Coordinate.BundleHash,
				"payload": map[string]any{"detail": "Native ordered input"}, "idempotency_key": "native-ordered-input"})
			cardID := waitServedNativePendingCard(t, endpoint)
			card := awaitServedNativeInputMessage(t, peer, "\nReference: "+cardID+"\n")
			if !strings.Contains(card.Body.GetConversation(), "Action: reject") {
				t.Fatal("real stage gate did not expose its input-bearing outcome", card.Body.GetConversation())
			}
			peer.reply("reject", "NATIVE_INPUT_BEGIN", card)
			first := awaitServedNativeInputMessage(t, peer, "Input: reason (text) required")
			peer.reply("private-native-reason", "NATIVE_INPUT_PARTIAL", first)
			second := awaitServedNativeInputMessage(t, peer, "Input: count (integer) required")
			requireServedNativeInputCard(t, endpoint, cardID, false)
			if err := process.stop(); err != nil {
				t.Fatal("pending native input did not join before restart", err, process.output.String())
			}
			requireServedNativeDisconnect(t, peer)
			process = startServedNativeSDKProcess(t, opts, peer)
			endpoint = process.endpoint(t) + "/v1/rpc"
			peer.awaitPostLogin()
			var restored channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID}, &restored)
			if restored.Readiness == nil || !restored.Readiness.Ready || restored.Operation.BindingRevision != ready.Operation.BindingRevision ||
				restored.Operation.Coordinate.RuntimeInstanceID == ready.Operation.Coordinate.RuntimeInstanceID {
				t.Fatal("input restart lost original binding or adopted old execution authority", restored)
			}
			requireServedNativeInputCard(t, endpoint, cardID, false)
			peer.mu.Lock()
			operator := peer.from.ToNonAD().String()
			peer.mu.Unlock()
			query := render.IntentObservationQuery{Kind: render.IntentText, Provider: "whatsapp",
				ConversationReference: operator, InterfaceKey: restored.Operation.Interface.Key()}
			peer.reply("obsolete answer", "NATIVE_INPUT_OLD_PROMPT", first)
			query.MessageReference = "NATIVE_INPUT_OLD_PROMPT"
			waitTextReplyIntent(t, reader, query, "teaching")
			foreign := peer.customerReply("27", "NATIVE_INPUT_FOREIGN", second)
			foreignQuery := query
			foreignQuery.MessageReference, foreignQuery.ConversationReference = "NATIVE_INPUT_FOREIGN", foreign.String()
			waitTextReplyIntent(t, reader, foreignQuery, "rejected")
			peer.reply("not-an-integer", "NATIVE_INPUT_INVALID", second)
			query.MessageReference = "NATIVE_INPUT_INVALID"
			waitTextReplyIntent(t, reader, query, "teaching")
			requireServedNativeInputCard(t, endpoint, cardID, false)
			peer.reply("27", "NATIVE_INPUT_COUNT", second)
			optional := awaitServedNativeInputMessage(t, peer, "Input: note (text)")
			peer.reply("Skip field", "NATIVE_INPUT_FINAL_SKIP", optional)
			decided := waitServedNativeInputDecision(t, endpoint, cardID)
			peer.reply("Skip field", "NATIVE_INPUT_FINAL_REPEAT", optional)
			query.MessageReference = "NATIVE_INPUT_FINAL_REPEAT"
			waitTextReplyIntent(t, reader, query, "teaching")
			peer.reply("reject", "NATIVE_INPUT_OLD_ACTION", card)
			query.MessageReference = "NATIVE_INPUT_OLD_ACTION"
			waitTextReplyIntent(t, reader, query, "control")
			waitTextReplyIntent(t, reader, servedNativeInputActionObservation(t, endpoint, restored.Operation.Coordinate.BundleHash, query), "stale")
			if again := requireServedNativeInputCard(t, endpoint, cardID, true); again != decided {
				t.Fatal("input replay replaced the original decision event", again, decided)
			}
			var decisions operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
				"filter": map[string]any{"run_id": seed.RunID, "event_name": "mailbox.card_decided"}, "limit": 10}, &decisions)
			if len(decisions.Events) != 1 || decisions.Events[0].EventID != decided {
				t.Fatal("native input completion persisted duplicate decisions", decisions)
			}
			if err := process.stop(); err != nil {
				t.Fatal("completed native input did not join", err, process.output.String())
			}
			requireServedNativeDisconnect(t, peer)
		})
	}
}

func servedNativeInputActionObservation(t *testing.T, endpoint, bundleHash string, text render.IntentObservationQuery) render.IntentObservationQuery {
	t.Helper()
	var runs struct {
		Runs       []operatorread.RunHeader `json:"runs"`
		NextCursor string                   `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"bundle_hash": bundleHash, "limit": 10}, &runs)
	if runs.NextCursor != "" {
		t.Fatal("bounded input fixture did not observe every admitted run")
	}
	var result render.IntentObservationQuery
	for _, run := range runs.Runs {
		var published operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
			"filter": map[string]any{"run_id": run.RunID, "event_name": string(events.EventTypePlatformInboundRecord)}, "limit": 200}, &published)
		if published.NextCursor != "" {
			t.Fatal("bounded input fixture did not observe every captured event")
		}
		for _, event := range published.Events {
			providerID, _ := event.Payload["provider_event_id"].(string)
			var dimensions []string
			if event.Payload["provider"] != "whatsapp" || json.Unmarshal([]byte(providerID), &dimensions) != nil || len(dimensions) != 5 {
				continue
			}
			// Observe the published opaque ID, never rebuild publication authority.
			if dimensions[2] != text.ConversationReference || dimensions[3] != text.MessageReference {
				continue
			}
			if result.ProviderEventID != "" {
				t.Fatal("public input evidence has conflicting action identities")
			}
			result = render.IntentObservationQuery{Kind: render.IntentAction, Provider: "whatsapp",
				ProviderEventID: providerID, InterfaceKey: text.InterfaceKey}
		}
	}
	if result.Validate() != nil {
		t.Fatal("public input evidence did not identify its exact derived action", text.MessageReference)
	}
	return result
}

func awaitServedNativeInputMessage(t *testing.T, peer *serveNativeProtocolPeer, fragment string) servedNativeMessage {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case sent := <-peer.sent:
			if strings.Contains(sent.Body.GetConversation(), "private-native-reason") {
				t.Fatal("native presentation disclosed private collected input", sent.Body.GetConversation())
			}
			if strings.Contains(sent.Body.GetConversation(), fragment) {
				return sent
			}
		case <-deadline.C:
			t.Fatal("native input had no genuine provider message", fragment)
		}
	}
}

func requireServedNativeInputCard(t *testing.T, endpoint, cardID string, decided bool) string {
	t.Helper()
	var result struct {
		Card struct {
			ID              string         `json:"card_id"`
			Status          string         `json:"status"`
			Verdict         string         `json:"verdict"`
			DecisionEventID string         `json:"decision_event_id"`
			Fields          map[string]any `json:"fields"`
		} `json:"decision_card"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &result)
	card := result.Card
	if card.ID != cardID || !decided && (card.Status != "pending" || len(card.Fields) != 0 || card.DecisionEventID != "") {
		t.Fatal("partial or refused input completed/replaced the public card", result)
	}
	if decided && (card.Status != "decided" || card.Verdict != "reject" || card.DecisionEventID == "" ||
		len(card.Fields) != 2 || card.Fields["reason"] != "private-native-reason" || card.Fields["count"] != float64(27)) {
		t.Fatal("native input lost exact ordered values or fabricated an optional field", result)
	}
	return card.DecisionEventID
}

func waitServedNativeInputDecision(t *testing.T, endpoint, cardID string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			Card struct {
				Status string `json:"status"`
			} `json:"decision_card"`
		}
		requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &result)
		if result.Card.Status == "decided" {
			return requireServedNativeInputCard(t, endpoint, cardID, true)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("native input did not complete the actual public card")
	return ""
}
