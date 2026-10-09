package whatsapp

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// This admitted BODY fixture is not an installed session pack. Its unrelated
// webhook policy deliberately remains authenticated: projection cannot admit it.
func incomingNormalizationFixture(t *testing.T) providertriggers.Manifest {
	t.Helper()
	source := `provider: whatsapp
payload_object_required: true
secret: {required: true}
signature: {type: token_equality, header: X-Fixture-Signature}
delivery_id: {json_path: $.provider_message_reference, required: true}
event_type: {json_path: $.kind, required: true}
event_name: {literal: inbound.whatsapp}
normalized_events:
`
	for _, kind := range []string{"message", "edit", "revoke"} {
		shape := map[string]string{
			"message": "exists: [text], absent: [target_message_reference]",
			"edit":    "exists: [text, target_message_reference]",
			"revoke":  "exists: [target_message_reference], absent: [text]",
		}[kind]
		source += fmt.Sprintf(`  - event: inbound.whatsapp.%s
    when: {equals: {kind: %s}, %s}
    author_subject: {type: chat, field: conversation_reference}
    fields:
      conversation_reference:
        from: conversation_reference
        schema: {type: string, minLength: 1}
      conversation_scope:
        from: conversation_scope
        schema: {type: string, enum: [direct, shared]}
      external_account_reference:
        from: external_account_reference
        schema: {type: string, minLength: 1}
      provider_message_reference:
        from: provider_message_reference
        schema: {type: string, minLength: 1}
      provider_timestamp_ms:
        from: provider_timestamp_ms
        schema: {type: integer, minimum: 1}
      ephemeral:
        from: ephemeral
        schema: {type: boolean}
      reply_to_message_reference:
        from: reply_to_message_reference
        schema: {type: string, minLength: 1}
        optional: true
`, kind, kind, shape)
		if kind != "revoke" {
			source += `      text:
        from: text
        schema: {type: string, minLength: 1}
`
		}
		if kind != "message" {
			source += `      target_message_reference:
        from: target_message_reference
        schema: {type: string, minLength: 1}
`
		}
	}
	manifest, err := providertriggers.ParseManifest([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestWhatsAppIncomingAcceptedPackProjectionIsNotAdmission(t *testing.T) {
	manifest := incomingNormalizationFixture(t)
	for _, kind := range []string{"message", "edit", "revoke"} {
		for _, group := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/group=%t", kind, group), func(t *testing.T) {
				event := incomingMessageFixture()
				if group {
					event.Info.Chat = types.NewJID("100000000003-1", types.GroupServer)
					event.Info.IsGroup = true
				}
				if kind != "message" {
					protocol := &waE2E.ProtocolMessage{Key: &waCommon.MessageKey{ID: proto.String("ORIGINAL")}}
					if kind == "edit" {
						protocol.Type = waE2E.ProtocolMessage_MESSAGE_EDIT.Enum()
						protocol.EditedMessage = &waE2E.Message{Conversation: proto.String("edited text")}
						event.IsEdit = true
					} else {
						protocol.Type = waE2E.ProtocolMessage_REVOKE.Enum()
					}
					event.Message = &waE2E.Message{ProtocolMessage: protocol}
				}
				captured, err := captureSDKMessage(captureFixture(t).Scope, uuid.NewString(), event, captureFixture(t).ReceivedAt)
				if err != nil {
					t.Fatal(err)
				}
				projected, err := manifest.ProjectNormalizedPayload(captured.Body)
				if err != nil || len(projected) != 1 {
					t.Fatalf("projected = %+v, %v", projected, err)
				}
				got := projected[0]
				if string(got.Name) != "inbound.whatsapp."+kind || got.Kind != providertriggers.OutputKindNormalized ||
					!got.Authorization.Empty() || got.AuthorSubjectType != "chat" || got.AuthorSubjectID != captured.Conversation ||
					got.Payload["provider_message_reference"] != captured.EventID ||
					got.Payload["external_account_reference"] != event.Info.Sender.String() ||
					got.Payload["provider_timestamp_ms"] != json.Number(fmt.Sprint(event.Info.Timestamp.UnixMilli())) {
					t.Fatalf("projection changed identity or granted authority: %+v", got)
				}
				if _, err := manifest.Accept(providertriggers.Request{Body: captured.Body}); err == nil {
					t.Fatal("pure projection bypassed authenticated request admission")
				}
				got.Payload["conversation_reference"] = "mutated caller copy"
				again, err := manifest.ProjectNormalizedPayload(captured.Body)
				if err != nil || again[0].Payload["conversation_reference"] != captured.Conversation {
					t.Fatal("projection mutated the immutable pack or capture")
				}
			})
		}
	}
}

func TestWhatsAppIncomingAcceptedPackReplyAndForwardParity(t *testing.T) {
	manifest := incomingNormalizationFixture(t)
	for _, reply := range []bool{false, true} {
		for _, forwarded := range []bool{false, true} {
			t.Run(fmt.Sprintf("reply=%t/forwarded=%t", reply, forwarded), func(t *testing.T) {
				event := incomingMessageFixture()
				info := &waE2E.ContextInfo{IsForwarded: proto.Bool(forwarded)}
				if reply {
					info.StanzaID = proto.String("ORIGINAL")
					info.QuotedMessage = &waE2E.Message{Conversation: proto.String("private quoted text")}
				}
				event.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: proto.String("current text"), ContextInfo: info}}
				captured, err := captureSDKMessage(captureFixture(t).Scope, uuid.NewString(), event, captureFixture(t).ReceivedAt)
				if err != nil {
					t.Fatal(err)
				}
				projected, err := manifest.ProjectNormalizedPayload(captured.Body)
				if err != nil || len(projected) != 1 {
					t.Fatal(err)
				}
				ref, present := projected[0].Payload["reply_to_message_reference"]
				if present != reply || reply && ref != "ORIGINAL" || projected[0].Payload["text"] != "current text" {
					t.Fatalf("forward/quote became current reply authority: %+v", projected)
				}
			})
		}
	}
}

func TestWhatsAppIncomingAcceptedPackRefusesMalformedPayload(t *testing.T) {
	manifest := incomingNormalizationFixture(t)
	for _, body := range []string{`{"kind":"message","kind":"edit"}`, `null`, `[]`, `{} {}`, `{"text":"\ud800"}`} {
		if _, err := manifest.ProjectNormalizedPayload([]byte(body)); err == nil {
			t.Fatalf("malformed/objectless payload accepted: %s", body)
		}
	}
	captured, err := captureSDKMessage(captureFixture(t).Scope, uuid.NewString(), incomingMessageFixture(), captureFixture(t).ReceivedAt)
	if err != nil {
		t.Fatal(err)
	}
	payload := incomingPayloadFixture(t, captured)
	payload.ConversationScope = "unknown"
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manifest.ProjectNormalizedPayload(body)
	var normalization providertriggers.NormalizationError
	if !errors.As(err, &normalization) || normalization.Path != "conversation_scope" {
		t.Fatalf("lost exact schema error: %v", err)
	}
	if _, err := (providertriggers.Manifest{}).ProjectNormalizedPayload([]byte(`{}`)); err == nil {
		t.Fatal("unadmitted manifest projected a payload")
	}
	// No branch is not an invented normalized event; the existing raw path owns it.
	projected, err := manifest.ProjectNormalizedPayload([]byte(`{"kind":"unsupported"}`))
	if err != nil || len(projected) != 0 {
		t.Fatalf("unmatched payload = %+v, %v", projected, err)
	}
	if !reflect.DeepEqual(manifest.SourceBytes(), incomingNormalizationFixture(t).SourceBytes()) {
		t.Fatal("normalization changed the admitted BODY")
	}
}
