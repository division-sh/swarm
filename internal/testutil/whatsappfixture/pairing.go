package whatsappfixture

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"
)

// SignedPairingResponse supplies an external phone's protocol evidence from
// the authorized QR. The real SDK verifies the signature/MAC and owns all
// account persistence; this fixture cannot issue Swarm session authority.
func SignedPairingResponse(t testing.TB, qr string, account, lid types.JID) waBinary.Node {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(qr, "https://wa.me/settings/linked_devices#"), ",")
	if len(parts) != 5 {
		t.Fatal("unexpected authenticated pairing QR format")
	}
	identity, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil || len(identity) != 32 {
		t.Fatal("pairing QR has no exact public identity")
	}
	secret, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil || len(secret) != 32 {
		t.Fatal("pairing QR has no exact ADV secret")
	}
	details, err := proto.Marshal(&waAdv.ADVDeviceIdentity{RawID: proto.Uint32(1), KeyIndex: proto.Uint32(1),
		Timestamp: proto.Uint64(uint64(time.Now().Unix()))})
	if err != nil {
		t.Fatal(err)
	}
	phone := keys.NewKeyPair()
	message := append(append(append([]byte(nil), whatsmeow.AdvAccountSignaturePrefix...), details...), identity...)
	signature := ecc.CalculateSignature(ecc.NewDjbECPrivateKey(*phone.Priv), message)
	signed, err := proto.Marshal(&waAdv.ADVSignedDeviceIdentity{Details: details,
		AccountSignatureKey: phone.Pub[:], AccountSignature: signature[:]})
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(signed)
	container, err := proto.Marshal(&waAdv.ADVSignedDeviceIdentityHMAC{Details: signed, HMAC: mac.Sum(nil)})
	if err != nil {
		t.Fatal(err)
	}
	return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": "PAIR_NATIVE", "type": "set", "from": types.ServerJID, "t": time.Now().Unix()},
		Content: []waBinary.Node{{Tag: "pair-success", Content: []waBinary.Node{
			{Tag: "device", Attrs: waBinary.Attrs{"jid": account, "lid": lid}},
			{Tag: "device-identity", Content: container},
			{Tag: "platform", Attrs: waBinary.Attrs{"name": "fixture-phone"}},
			{Tag: "biz", Attrs: waBinary.Attrs{"name": "Fixture Store"}},
		}}}}
}

func PairingRequest(reference string) waBinary.Node {
	return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": fmt.Sprintf("PAIR_REQUEST_%s", reference), "type": "set", "from": types.ServerJID},
		Content: []waBinary.Node{{Tag: "pair-device", Content: []waBinary.Node{{Tag: "ref", Content: []byte(reference)}}}}}
}
