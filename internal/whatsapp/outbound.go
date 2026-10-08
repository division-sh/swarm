package whatsapp

import (
	"errors"
	"unicode/utf8"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

var errOutboundTarget = errors.New("WhatsApp outbound target requires exact canonical conversation and receipt references")

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
	to, err := types.ParseJID(destination)
	if err != nil || to.String() != destination || to.User == "" || to.Device != 0 ||
		!utf8.ValidString(destination) || !personJID(to) && to.Server != types.GroupServer || text == "" || !utf8.ValidString(text) {
		return types.JID{}, nil, errOutboundTarget
	}
	if reply == nil {
		return to, &waE2E.Message{Conversation: proto.String(text)}, nil
	}
	sender, err := types.ParseJID(reply.Sender)
	if err != nil || !personJID(sender) || sender.String() != reply.Sender || sender.Device != 0 ||
		reply.Conversation != destination || !exactMessageID(reply.MessageID) || reply.Text == "" || !utf8.ValidString(reply.Text) {
		return types.JID{}, nil, errOutboundTarget
	}
	return to, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String(text), ContextInfo: &waE2E.ContextInfo{
			StanzaID: proto.String(reply.MessageID), Participant: proto.String(reply.Sender),
			QuotedMessage: &waE2E.Message{Conversation: proto.String(reply.Text)},
		}}}, nil
}
