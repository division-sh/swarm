package whatsappfixture

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/store/sessionstate"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// PairedDevice supplies persisted-device fixture data, not native admission or
// an SDK client. Production constructors still verify and own the actual state.
func PairedDevice(t testing.TB, owner *sessionstate.Owner) *store.Device {
	t.Helper()
	device := owner.NewDevice()
	jid := types.NewJID("synthetic_test_account", types.DefaultUserServer)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte{1}, AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := device.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	prekeys, err := device.PreKeys.GetOrGenPreKeys(context.Background(), uint32(whatsmeow.MinPreKeyCount))
	if err != nil {
		t.Fatal(err)
	}
	if err := device.PreKeys.MarkPreKeysAsUploaded(context.Background(), prekeys[len(prekeys)-1].KeyID); err != nil {
		t.Fatal(err)
	}
	return device
}
