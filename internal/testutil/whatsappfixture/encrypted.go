package whatsappfixture

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/sessionstate"
	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// EncryptPairingText uses only public keys observed on the actual SDK wire.
// The separate sender store cannot inspect or mutate the receiving SDK state.
func EncryptPairingText(t testing.TB, registration *waWa6.ClientPayload_DevicePairingRegistrationData,
	key waBinary.Node, receiver, from types.JID, text, messageID string,
) waBinary.Node {
	t.Helper()
	keyID := key.GetChildByTag("id")
	keyValue := key.GetChildByTag("value")
	id, idOK := keyID.Content.([]byte)
	value, valueOK := keyValue.Content.([]byte)
	if registration == nil || len(registration.ERegid) != 4 || len(registration.EIdent) != 32 ||
		len(registration.ESkeyID) != 3 || len(registration.ESkeyVal) != 32 || len(registration.ESkeySig) != 64 ||
		!idOK || len(id) != 3 || !valueOK || len(value) != 32 || receiver.Device == 0 {
		t.Fatal("incomplete public SDK pairing/prekey evidence")
	}
	ctx := context.Background()
	fixture, owner, err := sessionstate.OpenSDKFixture(ctx, filepath.Join(t.TempDir(), "external-sender.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	sender := PairedDevice(t, owner)
	bundle := prekey.NewBundle(binary.BigEndian.Uint32(registration.ERegid), uint32(receiver.Device),
		optional.NewOptionalUint32(binary.BigEndian.Uint32(append([]byte{0}, id...))),
		binary.BigEndian.Uint32(append([]byte{0}, registration.ESkeyID...)),
		ecc.NewDjbECPublicKey([32]byte(value)), ecc.NewDjbECPublicKey([32]byte(registration.ESkeyVal)),
		[64]byte(registration.ESkeySig), identity.NewKey(ecc.NewDjbECPublicKey([32]byte(registration.EIdent))))
	builder := session.NewBuilderFromSignal(sender, receiver.SignalAddress(), store.SignalProtobufSerializer)
	if err := builder.ProcessBundle(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	plain, err := proto.Marshal(&waE2E.Message{Conversation: proto.String(text)})
	if err != nil {
		t.Fatal(err)
	}
	cipher := session.NewCipher(builder, receiver.SignalAddress())
	encrypted, err := cipher.Encrypt(ctx, append(plain, 1))
	if err != nil {
		t.Fatal(err)
	}
	return waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{
		"from": from, "id": messageID, "t": time.Now().Unix(), "type": "text",
	}, Content: []waBinary.Node{{Tag: "enc", Attrs: waBinary.Attrs{"type": "pkmsg", "v": "2"}, Content: encrypted.Serialize()}}}
}
