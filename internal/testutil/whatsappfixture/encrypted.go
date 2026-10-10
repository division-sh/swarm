package whatsappfixture

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/sessionstate"
	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type SignalSender struct {
	mu                      sync.Mutex
	fixture                 *sessionstate.Fixture
	device                  *store.Device
	receiver, from, fromLID types.JID
	builder                 *session.Builder
}

// NewPairingSender uses only public signed keys observed on the SDK wire.
// A signed-prekey-only bundle is the Signal-defined path without a one-time key.
// The peer owns Close after joining its socket handlers, never the receiver DB.
func NewPairingSender(t testing.TB, registration *waWa6.ClientPayload_DevicePairingRegistrationData, receiver, from, fromLID types.JID) *SignalSender {
	t.Helper()
	if registration == nil || len(registration.ERegid) != 4 || len(registration.EIdent) != 32 ||
		len(registration.ESkeyID) != 3 || len(registration.ESkeyVal) != 32 || len(registration.ESkeySig) != 64 ||
		receiver.Device == 0 || from.Device == 0 {
		t.Fatal("incomplete public SDK signed-prekey evidence")
	}
	ctx := context.Background()
	fixture, owner, err := sessionstate.OpenSDKFixture(ctx, filepath.Join(t.TempDir(), "external-sender.db"))
	if err != nil {
		t.Fatal(err)
	}
	sender := PairedDevice(t, owner)
	bundle := prekey.NewBundle(binary.BigEndian.Uint32(registration.ERegid), uint32(receiver.Device),
		optional.NewEmptyUint32(),
		binary.BigEndian.Uint32(append([]byte{0}, registration.ESkeyID...)),
		nil, ecc.NewDjbECPublicKey([32]byte(registration.ESkeyVal)),
		[64]byte(registration.ESkeySig), identity.NewKey(ecc.NewDjbECPublicKey([32]byte(registration.EIdent))))
	builder := session.NewBuilderFromSignal(sender, receiver.SignalAddress(), store.SignalProtobufSerializer)
	if err := builder.ProcessBundle(ctx, bundle); err != nil {
		_ = fixture.Close()
		t.Fatal(err)
	}
	return &SignalSender{fixture: fixture, device: sender, receiver: receiver, from: from, fromLID: fromLID, builder: builder}
}

func (s *SignalSender) Close() error { return s.fixture.Close() }

func (s *SignalSender) EncryptText(t testing.TB, text, messageID string) waBinary.Node {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	plain, err := proto.Marshal(&waE2E.Message{Conversation: proto.String(text)})
	if err != nil {
		t.Fatal(err)
	}
	cipher := session.NewCipher(s.builder, s.receiver.SignalAddress())
	encrypted, err := cipher.Encrypt(context.Background(), append(plain, 1))
	if err != nil {
		t.Fatal(err)
	}
	kind := "msg"
	switch encrypted.Type() {
	case protocol.WHISPER_TYPE:
	case protocol.PREKEY_TYPE:
		kind = "pkmsg"
	default:
		t.Fatal("sender produced an unsupported Signal ciphertext kind", encrypted.Type())
	}
	return waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{
		"from": s.from, "sender_lid": s.fromLID, "id": messageID, "t": time.Now().Unix(), "type": "text",
	}, Content: []waBinary.Node{{Tag: "enc", Attrs: waBinary.Attrs{"type": kind, "v": "2"}, Content: encrypted.Serialize()}}}
}

func (s *SignalSender) DecryptOutbound(ctx context.Context, node *waBinary.Node) (*waE2E.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node == nil || node.Tag != "message" || node.Attrs["to"] != s.from.ToNonAD() && node.Attrs["to"] != s.fromLID.ToNonAD() {
		return nil, fmt.Errorf("outbound message targets another conversation")
	}
	if node.Attrs["to"] == s.fromLID.ToNonAD() && node.Attrs["peer_recipient_pn"] != s.from.ToNonAD() {
		return nil, fmt.Errorf("outbound LID contradicts its original conversation")
	}
	participants := node.GetChildByTag("participants")
	var encrypted *waBinary.Node
	for _, recipient := range participants.GetChildren() {
		if recipient.Tag == "to" && (recipient.Attrs["jid"] == s.from || recipient.Attrs["jid"] == s.fromLID) {
			if encrypted != nil {
				return nil, fmt.Errorf("duplicate encrypted recipient")
			}
			value := recipient.GetChildByTag("enc")
			encrypted = &value
		}
	}
	if encrypted == nil {
		return nil, fmt.Errorf("outbound message has no encrypted Signal recipient")
	}
	body, ok := encrypted.Content.([]byte)
	if !ok {
		return nil, fmt.Errorf("outbound ciphertext is not bytes")
	}
	cipher := session.NewCipher(s.builder, s.receiver.SignalAddress())
	var plain []byte
	var err error
	switch encrypted.Attrs["type"] {
	case "msg":
		message, parseErr := protocol.NewSignalMessageFromBytes(body, store.SignalProtobufSerializer.SignalMessage)
		if parseErr != nil {
			return nil, parseErr
		}
		plain, err = cipher.Decrypt(ctx, message)
	case "pkmsg":
		message, parseErr := protocol.NewPreKeySignalMessageFromBytes(body, store.SignalProtobufSerializer.PreKeySignalMessage, store.SignalProtobufSerializer.SignalMessage)
		if parseErr != nil {
			return nil, parseErr
		}
		plain, err = cipher.DecryptMessage(ctx, message)
	default:
		return nil, fmt.Errorf("unsupported outbound Signal message kind")
	}
	if err != nil {
		return nil, err
	}
	if len(plain) == 0 {
		return nil, fmt.Errorf("outbound plaintext is empty")
	}
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > len(plain) {
		return nil, fmt.Errorf("outbound plaintext has invalid padding")
	}
	for _, value := range plain[len(plain)-padding:] {
		if int(value) != padding {
			return nil, fmt.Errorf("outbound plaintext has inconsistent padding")
		}
	}
	decoded := &waE2E.Message{}
	if err := proto.Unmarshal(plain[:len(plain)-padding], decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func (s *SignalSender) PublicPreKey(ctx context.Context, jid types.JID) (waBinary.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if jid != s.from && jid != s.fromLID {
		return waBinary.Node{}, fmt.Errorf("requested another sender's prekey")
	}
	keys, err := s.device.PreKeys.GetOrGenPreKeys(ctx, 1)
	if err != nil {
		return waBinary.Node{}, fmt.Errorf("sender prekey unavailable: %w", err)
	}
	if len(keys) != 1 {
		return waBinary.Node{}, fmt.Errorf("sender prekey inventory returned %d keys, want one", len(keys))
	}
	var registration, keyID, signedID [4]byte
	binary.BigEndian.PutUint32(registration[:], s.device.RegistrationID)
	binary.BigEndian.PutUint32(keyID[:], keys[0].KeyID)
	binary.BigEndian.PutUint32(signedID[:], s.device.SignedPreKey.KeyID)
	return waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": jid}, Content: []waBinary.Node{
		{Tag: "registration", Content: registration[:]}, {Tag: "identity", Content: s.device.IdentityKey.Pub[:]},
		{Tag: "key", Content: []waBinary.Node{{Tag: "id", Content: keyID[1:]}, {Tag: "value", Content: keys[0].Pub[:]}}},
		{Tag: "skey", Content: []waBinary.Node{{Tag: "id", Content: signedID[1:]}, {Tag: "value", Content: s.device.SignedPreKey.Pub[:]},
			{Tag: "signature", Content: s.device.SignedPreKey.Signature[:]}}},
	}}, nil
}
