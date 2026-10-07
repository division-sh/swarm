package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func incomingMessageFixture() *events.Message {
	peer := types.NewJID("100000001", types.DefaultUserServer)
	return &events.Message{
		Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: peer, Sender: peer}, ID: "DELIVERY_1",
			Timestamp: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)},
		Message: &waE2E.Message{Conversation: proto.String("exact current text")},
	}
}

func incomingPayloadFixture(t *testing.T, event capturedEvent) incomingPayload {
	t.Helper()
	var payload incomingPayload
	if err := json.Unmarshal(event.Body, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestWhatsAppIncomingPublicSDKMessageEditRevokeProjection(t *testing.T) {
	for _, kind := range []string{"message", "edit", "revoke"} {
		for _, group := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/direct", true: "/group"}[group], func(t *testing.T) {
				scope := captureFixture(t).Scope
				event := incomingMessageFixture()
				self := event.Info.Sender
				bot := types.NewJID("100000002", types.DefaultUserServer)
				if group {
					event.Info.Chat = types.NewJID("100000000003-1", types.GroupServer)
					event.Info.IsGroup = true
				}
				// Use the real SDK builders and public unwrapping. In a direct
				// edit the key RemoteJID is the author's recipient, not our chat.
				client := whatsmeow.NewClient(&store.Device{ID: &self}, nil)
				keyChat := bot
				if group {
					keyChat = event.Info.Chat
				}
				if kind == "edit" {
					event.RawMessage = client.BuildEdit(keyChat, "ORIGINAL", &waE2E.Message{Conversation: proto.String("edited text")})
					event.UnwrapRaw()
					event.Info.Edit = types.EditAttributeMessageEdit
				} else if kind == "revoke" {
					event.Message = client.BuildRevoke(keyChat, self, "ORIGINAL")
					event.Info.Edit = types.EditAttributeSenderRevoke
				}
				captured, err := captureSDKMessage(scope, uuid.NewString(), event)
				if err != nil {
					t.Fatal(err)
				}
				payload := incomingPayloadFixture(t, captured)
				if captured.Kind != kind || captured.EventID != "DELIVERY_1" || captured.Conversation != event.Info.Chat.String() ||
					payload.MessageReference != "DELIVERY_1" || payload.Sender != self.String() {
					t.Fatalf("SDK target key replaced delivery/routing identity: %+v", payload)
				}
				if kind == "message" && (payload.Text == nil || *payload.Text != "exact current text" || payload.TargetReference != nil) {
					t.Fatal("plain text projection changed")
				}
				if kind != "message" && (payload.TargetReference == nil || *payload.TargetReference != "ORIGINAL") {
					t.Fatal("target reference replaced or lost")
				}
				if kind == "edit" && (payload.Text == nil || *payload.Text != "edited text") || kind == "revoke" && payload.Text != nil {
					t.Fatal("edit/revoke content shape changed")
				}
			})
		}
	}
}

func TestWhatsAppIncomingReplyAndForwardUseOnlyCurrentText(t *testing.T) {
	for _, variant := range []string{"reply", "forward", "forwarded_reply", "id_without_quote", "quote_without_id"} {
		t.Run(variant, func(t *testing.T) {
			event := incomingMessageFixture()
			info := &waE2E.ContextInfo{IsForwarded: proto.Bool(true), ForwardingScore: proto.Uint32(1)}
			if variant != "forward" {
				info.StanzaID = proto.String("ORIGINAL_REPLY")
				info.QuotedMessage = &waE2E.Message{Conversation: proto.String("Retire from quoted content")}
			}
			if variant == "reply" {
				info.IsForwarded, info.ForwardingScore = nil, nil
			} else if variant == "id_without_quote" {
				info.QuotedMessage = nil
			} else if variant == "quote_without_id" {
				info.StanzaID = nil
			}
			event.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("current reply text"), ContextInfo: info}}
			captured, err := captureSDKMessage(captureFixture(t).Scope, uuid.NewString(), event)
			if variant == "id_without_quote" || variant == "quote_without_id" {
				if !errors.Is(err, errIncomingMalformed) {
					t.Fatalf("incomplete reply became evidence: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			payload := incomingPayloadFixture(t, captured)
			if payload.Text == nil || *payload.Text != "current reply text" || bytes.Contains(captured.Body, []byte("Retire from quoted content")) {
				t.Fatal("quoted content became current text or retained plaintext")
			}
			if variant == "forward" && payload.ReplyReference != nil || variant != "forward" &&
				(payload.ReplyReference == nil || *payload.ReplyReference != "ORIGINAL_REPLY") {
				t.Fatalf("forward/reply classification changed: %+v", payload)
			}
		})
	}
}

func TestWhatsAppIncomingMalformedUnsupportedAndIdentityConflicts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*events.Message)
		want   error
	}{
		{"missing_id", func(e *events.Message) { e.Info.ID = "" }, errIncomingMalformed},
		{"missing_time", func(e *events.Message) { e.Info.Timestamp = time.Time{} }, errIncomingMalformed},
		{"invalid_epoch", func(e *events.Message) { e.Info.Timestamp = time.Unix(0, 0) }, errIncomingMalformed},
		{"missing_sender", func(e *events.Message) { e.Info.Sender = types.EmptyJID }, errIncomingMalformed},
		{"server_only_sender", func(e *events.Message) { e.Info.Sender = types.NewJID("", types.DefaultUserServer) }, errIncomingMalformed},
		{"server_only_group", func(e *events.Message) { e.Info.Chat = types.NewJID("", types.GroupServer); e.Info.IsGroup = true }, errIncomingMalformed},
		{"group_contradiction", func(e *events.Message) { e.Info.IsGroup = true }, errIncomingMalformed},
		{"edit_contradiction", func(e *events.Message) { e.IsEdit = true }, errIncomingMalformed},
		{"dual_text", func(e *events.Message) {
			e.Message.ExtendedTextMessage = &waE2E.ExtendedTextMessage{Text: proto.String("other")}
		}, errIncomingMalformed},
		{"media", func(e *events.Message) { e.Message.ImageMessage = &waE2E.ImageMessage{} }, errIncomingUnsupported},
		{"outgoing_echo", func(e *events.Message) { e.Info.IsFromMe = true }, errIncomingUnsupported},
		{"view_once", func(e *events.Message) { e.IsViewOnce = true }, errIncomingUnsupported},
		{"re_request", func(e *events.Message) { e.UnavailableRequestID = "old history" }, errIncomingUnsupported},
		{"status_chat", func(e *events.Message) { e.Info.Chat = types.NewJID("status", types.BroadcastServer) }, errIncomingUnsupported},
		{"protocol_no_type", func(e *events.Message) {
			e.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Key: &waCommon.MessageKey{ID: proto.String("ORIGINAL")}}}
		}, errIncomingMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := incomingMessageFixture()
			tc.mutate(event)
			if _, err := captureSDKMessage(captureFixture(t).Scope, uuid.NewString(), event); !errors.Is(err, tc.want) {
				t.Fatalf("wrong refusal: %v", err)
			}
		})
	}
}

func TestWhatsAppIncomingImmutableCaptureDuplicateAndPublicationIdentity(t *testing.T) {
	scope := captureFixture(t).Scope
	event := incomingMessageFixture()
	original, err := captureSDKMessage(scope, uuid.NewString(), event)
	if err != nil {
		t.Fatal(err)
	}
	_, capture := openCaptureFixture(t, filepath.Join(t.TempDir(), "capture.db"), scope.Session.ConnectionID)
	if err := capture.capture(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	event.Info.PushName, event.RetryCount = "changed display name", 3
	repeated, err := captureSDKMessage(scope, uuid.NewString(), event)
	if err != nil || !bytes.Equal(repeated.Body, original.Body) {
		t.Fatal("delivery-attempt metadata changed stable captured content")
	}
	if err := capture.capture(context.Background(), repeated); err != nil {
		t.Fatal(err)
	}
	*event.Message.Conversation = "changed after capture"
	if bytes.Contains(original.Body, []byte("changed after capture")) {
		t.Fatal("SDK mutation changed frozen capture")
	}
	changed, err := captureSDKMessage(scope, uuid.NewString(), event)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.capture(context.Background(), changed); !errors.Is(err, errCaptureConflict) {
		t.Fatalf("identity collision overwrote original: %v", err)
	}
	*event.Message.Conversation = "exact current text"
	event.Info.Sender = types.NewJID("100000004", types.HiddenUserServer)
	changed, err = captureSDKMessage(scope, uuid.NewString(), event)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.capture(context.Background(), changed); !errors.Is(err, errCaptureConflict) {
		t.Fatalf("same message ID from a different sender replaced retained identity: %v", err)
	}
	rows, err := capture.pending(context.Background())
	if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0], original) {
		t.Fatal("duplicate/conflict changed retained original capture")
	}
	key, err := original.publicationProviderEventID()
	if err != nil {
		t.Fatal(err)
	}
	entity := uuid.NewString()
	id, _ := inboundpublication.DeterministicIDs("whatsapp", entity, key)
	for _, mutate := range []func(*capturedEvent){
		func(e *capturedEvent) { e.Conversation = "other conversation" },
		func(e *capturedEvent) { e.Kind = "edit" },
		func(e *capturedEvent) { e.EventID = "other delivery" },
		func(e *capturedEvent) { e.Scope.Session.AccountRef = "other account" },
		func(e *capturedEvent) { e.Scope.Session.ConnectionID = uuid.NewString() },
	} {
		other := original
		mutate(&other)
		key, err := other.publicationProviderEventID()
		if err != nil {
			t.Fatal(err)
		}
		otherID, _ := inboundpublication.DeterministicIDs("whatsapp", entity, key)
		if otherID == id {
			t.Fatal("publication owner conflated a capture-key dimension")
		}
	}
}
