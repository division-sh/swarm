package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// Exercise pairing through the current, unmodified SDK, not private dispatch
// hooks. These regressions remain blocking while the SDK callback race is live.
func newPairingOccurrenceFixture(t *testing.T, peer *sdkPeer) *clientOccurrence {
	t.Helper()
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	o, err := newClientOccurrence(peer.ctx, uuid.NewString(), uuid.NewString(), container.NewDevice(), container.LIDMap(), nil)
	if err != nil {
		t.Fatal(err)
	}
	peer.attach(t, o.client)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := o.join(ctx); err != nil {
			t.Error(err)
		}
	})
	return o
}

func pairingSocketFixture(t *testing.T, peer *sdkPeer) *sdkPeerSocket {
	t.Helper()
	select {
	case socket := <-peer.ready:
		return socket
	case <-peer.ctx.Done():
		t.Fatal("SDK pairing handshake did not complete")
		return nil
	}
}

func pairingIQFixture(child waBinary.Node) waBinary.Node {
	return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{
		"from": types.ServerJID, "id": "PAIRING_PROBE", "type": "set", "t": time.Now().Unix(),
	}, Content: []waBinary.Node{child}}
}

func TestWhatsAppSDKPairingRejectionAndRemoteClose(t *testing.T) {
	for iteration := 0; iteration < 16; iteration++ {
		t.Run(fmt.Sprintf("%02d", iteration), func(t *testing.T) {
			peer := newSDKPeerMode(t, false)
			o := newPairingOccurrenceFixture(t, peer)
			errorsSeen := make(chan error, 1)
			o.client.AddEventHandler(func(raw any) {
				if event, ok := raw.(*events.PairError); ok {
					errorsSeen <- event.Error
				}
			})
			if err := o.connect(o.ctx); err != nil {
				t.Fatal(err)
			}
			socket := pairingSocketFixture(t, peer)
			if err := socket.send(peer.ctx, pairingIQFixture(waBinary.Node{Tag: "pair-success", Content: []waBinary.Node{
				{Tag: "device-identity", Content: []byte{}},
				{Tag: "device", Attrs: waBinary.Attrs{"jid": types.NewJID("PAIRING_TEST", types.DefaultUserServer)}},
			}})); err != nil {
				t.Fatal(err)
			}
			// handlePair emits this IQ before its internal Disconnect. Race the
			// remote close with that exact error boundary, not a timed sleep.
			select {
			case <-peer.protocol:
			case <-peer.ctx.Done():
				t.Fatal("pairing rejection was not reached")
			}
			_ = socket.conn.CloseNow()
			select {
			case err := <-errorsSeen:
				if !errors.Is(err, whatsmeow.ErrPairInvalidDeviceIdentityHMAC) {
					t.Fatalf("pairing error = %v", err)
				}
			case <-peer.ctx.Done():
				t.Fatal("SDK pairing rejection did not complete")
			}
			if o.client.Store.ID != nil || o.client.IsLoggedIn() {
				t.Fatal("rejected pairing became an authenticated account")
			}
		})
	}
}
