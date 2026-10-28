package sessionprovider

import (
	"errors"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestWhatsAppOutboundTextAndExactReplyEncoding(t *testing.T) {
	for destination, canonical := range map[string]string{
		"+100000001":               "100000001@s.whatsapp.net",
		"100000001@s.whatsapp.net": "100000001@s.whatsapp.net",
		"100000001@lid":            "100000001@lid",
		"100000000003-1@g.us":      "100000000003-1@g.us",
	} {
		for _, replying := range []bool{false, true} {
			var reference *outboundReply
			if replying {
				reference = &outboundReply{Conversation: canonical, Sender: "100000002@s.whatsapp.net", MessageID: "ORIGINAL", Text: "stored receipt text"}
			}
			to, message, err := encodeOutboundText(destination, "current English control", reference)
			if err != nil || to.String() != canonical {
				t.Fatal("destination changed during encoding", err)
			}
			if reference == nil {
				if message.GetConversation() != "current English control" || message.ExtendedTextMessage != nil {
					t.Fatal("plain text fabricated reply metadata")
				}
			} else {
				info := message.GetExtendedTextMessage().GetContextInfo()
				if message.GetExtendedTextMessage().GetText() != "current English control" || info.GetStanzaID() != "ORIGINAL" ||
					info.GetParticipant() != reference.Sender || info.GetQuotedMessage().GetConversation() != "stored receipt text" || info.GetIsForwarded() {
					t.Fatal("reply reference or current/quoted content changed")
				}
			}
			encoded, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			var decoded waE2E.Message
			if err := proto.Unmarshal(encoded, &decoded); err != nil || !proto.Equal(message, &decoded) {
				t.Fatal("SDK protocol round trip changed encoded text/reply", err)
			}
		}
	}
}

func TestWhatsAppOutboundRefusesGuessedOrForeignReplyTargets(t *testing.T) {
	for _, destination := range []string{"", "+", "+0123456", "+100000001x", "+1 00000001", "+1234567890123456", "100000001", "@s.whatsapp.net", "100000001@s.whatsapp.net ",
		"100000001:3@s.whatsapp.net", "status@broadcast", "100@newsletter"} {
		if _, _, err := encodeOutboundText(destination, "text", nil); !errors.Is(err, errOutboundTarget) ||
			!strings.Contains(err.Error(), "E.164") || !strings.Contains(err.Error(), "JID") ||
			!strings.Contains(err.Error(), "shape=") || len(destination) > 3 && strings.Contains(err.Error(), destination) {
			t.Fatal("invalid destination accepted or privacy-safe teaching omitted", err)
		}
	}
	for _, cell := range []string{"conversation", "sender", "message", "quote", "current_text"} {
		reply := outboundReply{Conversation: "100000001@s.whatsapp.net", Sender: "100000002@s.whatsapp.net", MessageID: "ORIGINAL", Text: "stored text"}
		text := "current text"
		switch cell {
		case "conversation":
			reply.Conversation = "other@g.us"
		case "sender":
			reply.Sender = "not-a-sender"
		case "message":
			reply.MessageID = " guessed "
		case "quote":
			reply.Text = ""
		case "current_text":
			text = ""
		}
		if _, _, err := encodeOutboundText("100000001@s.whatsapp.net", text, &reply); !errors.Is(err, errOutboundTarget) {
			t.Fatal("foreign/incomplete reply was guessed or downgraded", err)
		}
	}
}
