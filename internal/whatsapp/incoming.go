package whatsapp

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var errIncomingMalformed = errors.New("WhatsApp incoming message has contradictory or incomplete SDK evidence")
var errIncomingUnsupported = errors.New("WhatsApp incoming message variant is unsupported")

// This is a provider payload, not a normalized executable event or identity
// ceremony. The accepted trigger pack and publication owners still own those.
type incomingPayload struct {
	Kind                string  `json:"kind"`
	Conversation        string  `json:"conversation_reference"`
	ConversationScope   string  `json:"conversation_scope"`
	Sender              string  `json:"external_account_reference"`
	MessageReference    string  `json:"provider_message_reference"`
	TargetReference     *string `json:"target_message_reference,omitempty"`
	ReplyReference      *string `json:"reply_to_message_reference,omitempty"`
	Text                *string `json:"text,omitempty"`
	ProviderTimestampMS int64   `json:"provider_timestamp_ms"`
	Ephemeral           bool    `json:"ephemeral"`
}

func captureSDKMessage(scope captureScope, occurrenceID string, event *events.Message) (capturedEvent, error) {
	var result capturedEvent
	if err := scope.validate(); err != nil {
		return result, err
	}
	if event == nil || event.Message == nil || !exactMessageID(event.Info.ID) || event.Info.Timestamp.IsZero() || event.Info.Timestamp.UnixMilli() < 1 {
		return result, errIncomingMalformed
	}
	if event.Info.IsFromMe || event.SourceWebMsg != nil || event.UnavailableRequestID != "" ||
		event.IsViewOnce || event.IsViewOnceV2 || event.IsViewOnceV2Extension || event.IsBotInvoke ||
		event.IsDocumentWithCaption || event.IsLottieSticker || event.NewsletterMeta != nil || event.Info.MsgBotInfo.EditType != "" {
		return result, errIncomingUnsupported
	}
	chat, sender := event.Info.Chat.ToNonAD(), event.Info.Sender.ToNonAD()
	if !personJID(sender) || !utf8.ValidString(chat.String()) || chat.IsEmpty() || chat.User == "" {
		return result, errIncomingMalformed
	}
	conversationScope := "direct"
	if chat.Server == types.GroupServer {
		conversationScope = "shared"
	} else if !personJID(chat) {
		return result, errIncomingUnsupported
	}
	if event.Info.IsGroup != (conversationScope == "shared") {
		return result, errIncomingMalformed
	}
	kind, text, target, contextInfo, err := incomingContent(event)
	if err != nil {
		return result, err
	}
	reply, err := incomingReplyReference(contextInfo)
	if err != nil {
		return result, err
	}
	payload := incomingPayload{Kind: kind, Conversation: chat.String(), ConversationScope: conversationScope,
		Sender: sender.String(), MessageReference: event.Info.ID, TargetReference: target, ReplyReference: reply,
		Text: text, ProviderTimestampMS: event.Info.Timestamp.UnixMilli(), Ephemeral: event.IsEphemeral}
	body, err := json.Marshal(payload)
	if err != nil {
		return result, err
	}
	result = capturedEvent{Scope: scope, OccurrenceID: occurrenceID, Conversation: payload.Conversation,
		EventID: event.Info.ID, Kind: kind, Body: body}
	return result, result.validate()
}

func exactMessageID(id string) bool {
	return id != "" && utf8.ValidString(id) && strings.TrimSpace(id) == id
}

func personJID(jid types.JID) bool {
	return !jid.IsEmpty() && jid.User != "" && utf8.ValidString(jid.String()) &&
		(jid.Server == types.DefaultUserServer || jid.Server == types.HiddenUserServer)
}

func incomingContent(event *events.Message) (string, *string, *string, *waE2E.ContextInfo, error) {
	message := event.Message
	if protocol := message.ProtocolMessage; protocol != nil {
		if !onlyMessageFields(message, "protocolMessage", "messageContextInfo") || protocol.Type == nil ||
			protocol.Key == nil || !exactMessageID(protocol.Key.GetID()) {
			return "", nil, nil, nil, errIncomingMalformed
		}
		target := protocol.Key.GetID()
		switch protocol.GetType() {
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			if !event.IsEdit || event.Info.Edit != types.EditAttributeEmpty && event.Info.Edit != types.EditAttributeMessageEdit {
				return "", nil, nil, nil, errIncomingMalformed
			}
			text, contextInfo, err := incomingText(protocol.EditedMessage)
			return "edit", text, &target, contextInfo, err
		case waE2E.ProtocolMessage_REVOKE:
			if event.IsEdit || protocol.EditedMessage != nil || event.Info.Edit != types.EditAttributeEmpty &&
				event.Info.Edit != types.EditAttributeSenderRevoke && event.Info.Edit != types.EditAttributeAdminRevoke {
				return "", nil, nil, nil, errIncomingMalformed
			}
			return "revoke", nil, &target, nil, nil
		default:
			return "", nil, nil, nil, errIncomingUnsupported
		}
	}
	if event.IsEdit || event.Info.Edit != types.EditAttributeEmpty {
		return "", nil, nil, nil, errIncomingMalformed
	}
	text, contextInfo, err := incomingText(message)
	return "message", text, nil, contextInfo, err
}

func incomingText(message *waE2E.Message) (*string, *waE2E.ContextInfo, error) {
	if message == nil || !onlyMessageFields(message, "conversation", "extendedTextMessage", "messageContextInfo") {
		return nil, nil, errIncomingUnsupported
	}
	if message.Conversation != nil && message.ExtendedTextMessage != nil {
		return nil, nil, errIncomingMalformed
	}
	text := message.GetConversation()
	var contextInfo *waE2E.ContextInfo
	if extended := message.ExtendedTextMessage; extended != nil {
		text, contextInfo = extended.GetText(), extended.ContextInfo
	}
	if text == "" || !utf8.ValidString(text) {
		return nil, nil, errIncomingMalformed
	}
	return &text, contextInfo, nil
}

// SDK reply evidence is necessary, never sufficient for a principal action.
// Only current text is projected; forwarded/quoted content is not an action.
func incomingReplyReference(info *waE2E.ContextInfo) (*string, error) {
	if info == nil || info.GetStanzaID() == "" && info.QuotedMessage == nil {
		return nil, nil
	}
	if !exactMessageID(info.GetStanzaID()) || info.QuotedMessage == nil {
		return nil, errIncomingMalformed
	}
	id := info.GetStanzaID()
	return &id, nil
}

func onlyMessageFields(message *waE2E.Message, allowed ...protoreflect.Name) bool {
	ok := len(message.ProtoReflect().GetUnknown()) == 0
	message.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		for _, name := range allowed {
			if field.Name() == name {
				return true
			}
		}
		ok = false
		return false
	})
	return ok
}

// Match the existing capture key dimensions without introducing another hash
// or UUID owner. Runtime's canonical publication identity owner consumes this.
func (event capturedEvent) publicationProviderEventID() (string, error) {
	if err := event.validate(); err != nil {
		return "", err
	}
	key, err := json.Marshal([]string{event.Scope.Session.ConnectionID, event.Scope.Session.AccountRef,
		event.Conversation, event.EventID, event.Kind})
	return string(key), err
}

func (event capturedEvent) publicationIdentity() (runtimeinbound.Identity, error) {
	key, err := event.publicationProviderEventID()
	if err != nil {
		return runtimeinbound.Identity{}, err
	}
	identity := event.Scope.PublicationBinding.Identity("whatsapp", key)
	return identity, identity.Validate()
}
