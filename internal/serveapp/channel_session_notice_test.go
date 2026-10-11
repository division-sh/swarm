//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/mailbox"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func configureNativeNoticeProvider(t *testing.T, configPath string) *atomic.Int32 {
	t.Helper()
	secret := uuid.NewString()
	setDoctorProviderSecret(t, "OPENAI_COMPATIBLE_API_KEY", secret)
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
	calls := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer "+secret {
			http.Error(w, "authenticated fixture request required", http.StatusUnauthorized)
			return
		}
		var request struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error("decode native notice provider request", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		found := false
		for _, tool := range request.Tools {
			found = found || tool.Function.Name == "notify_human"
		}
		if !found {
			t.Error("real notice agent had no canonical notify_human tool")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		message := map[string]any{"role": "assistant", "content": "Observed."}
		reason := "stop"
		if count := calls.Add(1); count == 1 {
			message["content"] = ""
			message["tool_calls"] = []any{map[string]any{"id": "native_notice_call", "type": "function",
				"function": map[string]any{"name": "notify_human", "arguments": `{"summary":"Observed native notice","context":{"proof":"whatsapp-notice-ack"}}`}}}
			reason = "tool_calls"
		} else if count != 2 {
			t.Error("native notice provider was invoked beyond its original two-turn completion", count)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"id": "native-notice-completion", "object": "chat.completion",
			"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": reason}},
			"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	llm := config["llm"].(map[string]any)
	llm["backend"] = "openai_compatible"
	llm["openai_compatible"] = map[string]any{"base_url": server.URL + "/v1"}
	raw, err = yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return calls
}

func requireServedNativeNoticeAcknowledgment(t *testing.T, endpoint string, peer *serveNativeProtocolPeer,
	reader storetest.ChannelObservation, op channelonboarding.Operation,
) servedNativeMessage {
	t.Helper()
	requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "notice.requested",
		"bundle_hash": op.Coordinate.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "native-notice"})
	var notice servedNativeMessage
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for notice.ID == "" {
		select {
		case sent := <-peer.sent:
			if strings.HasPrefix(sent.Body.GetConversation(), "Notice: Observed native notice") {
				notice = sent
			}
		case <-deadline.C:
			logServedNativeWait(t, endpoint, peer, "native_notice_delivery")
			t.Fatal("canonical notify_human notice had no genuine native delivery")
		}
	}
	detail := requireNativeNoticeReadback(t, endpoint, 1)
	requireNativeNoticeExactPresentation(t, reader, op, notice, detail)
	foreign := peer.customerReply("Acknowledge", "NOTICE_FOREIGN_PRINCIPAL", notice)
	query := render.IntentObservationQuery{Kind: render.IntentText, Provider: "whatsapp", MessageReference: "NOTICE_FOREIGN_PRINCIPAL",
		ConversationReference: foreign.String(), InterfaceKey: op.Interface.Key()}
	waitTextReplyIntent(t, reader, query, "rejected")
	requireNativeNoticeReadback(t, endpoint, 1)
	unknown := notice
	unknown.ID = "UNKNOWN_NATIVE_NOTICE"
	peer.reply("Acknowledge", "NOTICE_FOREIGN_RECEIPT", unknown)
	peer.mu.Lock()
	operator := peer.from.ToNonAD().String()
	peer.mu.Unlock()
	query.MessageReference, query.ConversationReference = "NOTICE_FOREIGN_RECEIPT", operator
	waitTextReplyIntent(t, reader, query, "teaching")
	requireNativeNoticeReadback(t, endpoint, 1)
	peer.reply("Acknowledge", "NOTICE_ACK", notice)
	query.MessageReference = "NOTICE_ACK"
	waitTextReplyIntent(t, reader, query, "control")
	requireNativeIntentObservationScope(t, reader, query)
	waitNativeNoticeAcknowledged(t, endpoint)
	return notice
}

func waitNativeNoticeAcknowledged(t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var listed struct {
			Unread int `json:"unread_informational_notices"`
		}
		requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"limit": 200}, &listed)
		if listed.Unread == 0 {
			requireNativeNoticeReadback(t, endpoint, 0)
			return
		}
		if listed.Unread != 1 {
			t.Fatal("notice acknowledgment changed unrelated unread ownership", listed.Unread)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("genuine native acknowledgment did not settle its public unread count")
}

func requireNativeNoticeExactPresentation(t *testing.T, reader storetest.ChannelObservation,
	op channelonboarding.Operation, sent servedNativeMessage, detail mailbox.V1ItemDetail,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		plans, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range plans {
			if plan.SourceKind != "notice" || plan.SourceID != detail.Item.MailboxID || plan.State != "sent" {
				continue
			}
			receipt, found, err := reader.GetCurrentChannelSentReceipt(context.Background(), plan.DeliveryID, plan.CurrentReceiptID)
			if err != nil {
				t.Fatal(err)
			}
			if !found || receipt.DeliveryReference != sent.ID {
				continue
			}
			if plan.Audience.PrincipalID != op.PrincipalID || plan.Audience.InterfaceKey != op.Interface.Key() {
				t.Fatal("native notice changed its original operator audience", plan)
			}
			frozen, err := render.FreezeNotice(render.Notice{ID: detail.Item.MailboxID, Type: detail.Item.Type,
				Summary: "Observed native notice", Context: []byte(`{"proof":"whatsapp-notice-ack"}`),
				Priority: detail.Item.Priority, FromAgent: detail.History[0].ActorTokenID,
				FlowInstance: detail.Item.SourceFlow, EntityID: detail.Item.SourceEntityID}, plan.Audience)
			if err != nil {
				t.Fatal(err)
			}
			want := frozen.FullText + "\nReference: " + detail.Item.MailboxID + "\nAction: Acknowledge"
			if sent.Body.GetConversation() != want || sent.ID != strings.ReplaceAll(receipt.OperationID, "-", "") {
				t.Fatal("native notice differs from canonical public content and exact journal", sent.Body.GetConversation(), want, receipt)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("native notice had no exact settled delivery receipt", sent.ID, detail.Item.MailboxID)
}

func requireNativeNoticeReadback(t *testing.T, endpoint string, unread int) mailbox.V1ItemDetail {
	t.Helper()
	var listed struct {
		Unread int `json:"unread_informational_notices"`
		Items  []struct {
			Kind   string         `json:"kind"`
			Notice mailbox.V1Item `json:"notice"`
		} `json:"items"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"type": "operator_notice", "limit": 200}, &listed)
	var notices []mailbox.V1Item
	for _, item := range listed.Items {
		if item.Kind == "notice" {
			notices = append(notices, item.Notice)
		}
	}
	if listed.Unread != unread || len(notices) != 1 || notices[0].Status != "pending" {
		t.Fatal("native acknowledgment changed notice semantics or unread ownership", listed, unread)
	}
	var got struct {
		Kind   string               `json:"kind"`
		Notice mailbox.V1ItemDetail `json:"notice"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": notices[0].MailboxID}, &got)
	if got.Kind != "notice" || got.Notice.Item.Status != "pending" ||
		len(got.Notice.History) != 1 || got.Notice.History[0].Action != "created" {
		t.Fatal("native acknowledgment fabricated decision/history mutation", got)
	}
	return got.Notice
}
