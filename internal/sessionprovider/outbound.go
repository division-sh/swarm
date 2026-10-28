package sessionprovider

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

var errOutboundTarget = errors.New("WhatsApp outbound target requires an E.164 phone number (+digits) or canonical person/group JID, with exact receipt references")

type outboundReply struct {
	Conversation string
	Sender       string
	MessageID    string
	Text         string
}

// Caller-owned compiled schemas set text bounds and existing receipt/principal
// owners authorize replies. This only encodes already selected provider facts;
// it does not choose a destination, send, journal, retry or infer a receipt.
func encodeOutboundText(destination, text string, reply *outboundReply) (types.JID, *waE2E.Message, error) {
	canonical := destination
	if strings.HasPrefix(destination, "+") {
		account := channelonboarding.AccountReference{Provider: "whatsapp", Account: destination}
		if err := account.Validate(); err != nil {
			return types.JID{}, nil, fmt.Errorf("%w: destination shape=phone-like", errOutboundTarget)
		}
		canonical = strings.TrimPrefix(account.Account, "+") + "@" + types.DefaultUserServer
	}
	to, err := types.ParseJID(canonical)
	if err != nil || to.String() != canonical || to.User == "" || to.Device != 0 ||
		!utf8.ValidString(destination) || !personJID(to) && to.Server != types.GroupServer || text == "" || !utf8.ValidString(text) {
		shape := "text"
		if destination == "" {
			shape = "empty"
		} else if strings.Contains(destination, "@") {
			shape = "JID-like"
		}
		return types.JID{}, nil, fmt.Errorf("%w: destination shape=%s", errOutboundTarget, shape)
	}
	if reply == nil {
		return to, &waE2E.Message{Conversation: proto.String(text)}, nil
	}
	sender, err := types.ParseJID(reply.Sender)
	if err != nil || !personJID(sender) || sender.String() != reply.Sender || sender.Device != 0 ||
		reply.Conversation != canonical || !exactMessageID(reply.MessageID) || reply.Text == "" || !utf8.ValidString(reply.Text) {
		return types.JID{}, nil, errOutboundTarget
	}
	return to, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String(text), ContextInfo: &waE2E.ContextInfo{
			StanzaID: proto.String(reply.MessageID), Participant: proto.String(reply.Sender),
			QuotedMessage: &waE2E.Message{Conversation: proto.String(reply.Text)},
		}}}, nil
}
