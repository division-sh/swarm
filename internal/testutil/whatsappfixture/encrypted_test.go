package whatsappfixture

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/store/sessionstate"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/session"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestSignalSenderRetainsBidirectionalSessionAfterPublicAddressMapping(t *testing.T) {
	ctx := context.Background()
	fixture, owner, err := sessionstate.OpenSDKFixture(ctx, filepath.Join(t.TempDir(), "receiver.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	}()
	receiver := PairedDevice(t, owner)
	var registration, signedID [4]byte
	binary.BigEndian.PutUint32(registration[:], receiver.RegistrationID)
	binary.BigEndian.PutUint32(signedID[:], receiver.SignedPreKey.KeyID)
	account := types.NewJID("15551234567", types.DefaultUserServer)
	from := types.NewJID("15551234568", types.DefaultUserServer)
	fromLID := types.NewJID("100000000002", types.HiddenUserServer)
	account.Device, from.Device, fromLID.Device = 1, 1, 1
	sender := NewPairingSender(t, &waWa6.ClientPayload_DevicePairingRegistrationData{
		ERegid: registration[:], EIdent: receiver.IdentityKey.Pub[:], ESkeyID: signedID[1:],
		ESkeyVal: receiver.SignedPreKey.Pub[:], ESkeySig: receiver.SignedPreKey.Signature[:],
	}, account, from, fromLID)
	defer func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	}()
	decrypt := func(node waBinary.Node, address types.JID) string {
		t.Helper()
		enc := node.GetChildByTag("enc")
		cipher := session.NewCipher(session.NewBuilderFromSignal(receiver, address.SignalAddress(), store.SignalProtobufSerializer), address.SignalAddress())
		var plain []byte
		var err error
		body := enc.Content.([]byte)
		if enc.Attrs["type"] == "pkmsg" {
			message, parseErr := protocol.NewPreKeySignalMessageFromBytes(body, store.SignalProtobufSerializer.PreKeySignalMessage, store.SignalProtobufSerializer.SignalMessage)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			plain, err = cipher.DecryptMessage(ctx, message)
		} else {
			message, parseErr := protocol.NewSignalMessageFromBytes(body, store.SignalProtobufSerializer.SignalMessage)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			plain, err = cipher.Decrypt(ctx, message)
		}
		if err != nil {
			t.Fatal("sender ciphertext failed exact peer decryption", err)
		}
		var decoded waE2E.Message
		if len(plain) == 0 || plain[len(plain)-1] != 1 {
			t.Fatal("unexpected sender padding")
		}
		if err := proto.Unmarshal(plain[:len(plain)-1], &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded.GetConversation()
	}
	claim := sender.EncryptText(t, "claim", "CLAIM")
	if claim.Attrs["sender_lid"] != fromLID || claim.Attrs["from"] != from {
		t.Fatal("initial peer input omitted or guessed its PN/LID evidence", claim.Attrs)
	}
	if got := decrypt(claim, from); got != "claim" {
		t.Fatal(got)
	}
	if err := receiver.Sessions.MigratePNToLID(ctx, from, fromLID); err != nil {
		t.Fatal(err)
	}
	cipher := session.NewCipher(session.NewBuilderFromSignal(receiver, fromLID.SignalAddress(), store.SignalProtobufSerializer), fromLID.SignalAddress())
	plain, err := proto.Marshal(&waE2E.Message{Conversation: proto.String("confirmation")})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := cipher.Encrypt(ctx, append(plain, 1))
	if err != nil {
		t.Fatal(err)
	}
	kind := "msg"
	if encrypted.Type() == protocol.PREKEY_TYPE {
		kind = "pkmsg"
	}
	node := waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{"to": fromLID.ToNonAD(), "peer_recipient_pn": from.ToNonAD()},
		Content: []waBinary.Node{{Tag: "participants", Content: []waBinary.Node{{Tag: "to", Attrs: waBinary.Attrs{"jid": fromLID},
			Content: []waBinary.Node{{Tag: "enc", Attrs: waBinary.Attrs{"type": kind}, Content: encrypted.Serialize()}}}}}}}
	decoded, err := sender.DecryptOutbound(ctx, &node)
	if err != nil || decoded.GetConversation() != "confirmation" {
		t.Fatal("mapped peer response failed sender decryption", err)
	}
	if got := decrypt(sender.EncryptText(t, "/inbox", "INBOX"), fromLID); got != "/inbox" {
		t.Fatal(got)
	}
	for name, bad := range map[string]waBinary.Node{
		"foreign conversation": {Tag: "message", Attrs: waBinary.Attrs{"to": types.NewJID("15550000000", types.DefaultUserServer)}},
		"contradictory LID":    {Tag: "message", Attrs: waBinary.Attrs{"to": fromLID.ToNonAD(), "peer_recipient_pn": account.ToNonAD()}},
		"missing ciphertext":   {Tag: "message", Attrs: waBinary.Attrs{"to": from.ToNonAD()}},
		"malformed ciphertext": {Tag: "message", Attrs: waBinary.Attrs{"to": from.ToNonAD()}, Content: []waBinary.Node{
			{Tag: "participants", Content: []waBinary.Node{{Tag: "to", Attrs: waBinary.Attrs{"jid": from}, Content: []waBinary.Node{
				{Tag: "enc", Attrs: waBinary.Attrs{"type": "msg"}, Content: "not bytes"},
			}}}},
		}},
		"duplicate recipient": {Tag: "message", Attrs: waBinary.Attrs{"to": from.ToNonAD()}, Content: []waBinary.Node{
			{Tag: "participants", Content: []waBinary.Node{
				{Tag: "to", Attrs: waBinary.Attrs{"jid": from}}, {Tag: "to", Attrs: waBinary.Attrs{"jid": fromLID}},
			}},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if decoded, err := sender.DecryptOutbound(ctx, &bad); err == nil || decoded != nil {
				t.Fatal("malformed peer output became a decoded message", decoded, err)
			}
		})
	}
	if got := decrypt(sender.EncryptText(t, "after rejected peer output", "AFTER_REFUSAL"), fromLID); got != "after rejected peer output" {
		t.Fatal("refused peer output changed the original sender session", got)
	}
}
