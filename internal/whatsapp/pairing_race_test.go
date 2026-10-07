package whatsapp

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
	o, err := newClientOccurrence(peer.ctx, uuid.NewString(), uuid.NewString(), container.NewDevice(), container.LIDMap, nil)
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
			if err := o.connect(); err != nil {
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

func TestWhatsAppSDKQRShutdownBoundaries(t *testing.T) {
	for _, boundary := range []string{"empty_codes", "cancellation", "backpressure"} {
		for iteration := 0; iteration < 8; iteration++ {
			t.Run(fmt.Sprintf("%s/%02d", boundary, iteration), func(t *testing.T) {
				peer := newSDKPeerMode(t, false)
				o := newPairingOccurrenceFixture(t, peer)
				qrCtx, cancelQR := context.WithCancel(peer.ctx)
				defer cancelQR()
				qr, err := o.client.GetQRChannel(qrCtx)
				if err != nil {
					t.Fatal(err)
				}
				qrEvents := make(chan struct{}, 10)
				o.client.AddEventHandler(func(raw any) {
					if _, ok := raw.(*events.QR); ok {
						qrEvents <- struct{}{}
					}
				})
				if err := o.connect(); err != nil {
					t.Fatal(err)
				}
				socket := pairingSocketFixture(t, peer)
				count := 1
				if boundary == "backpressure" {
					count = 9 // GetQRChannel's exact output capacity is eight.
				}
				for index := 0; index < count; index++ {
					var refs []waBinary.Node
					if boundary != "empty_codes" {
						refs = []waBinary.Node{{Tag: "ref", Content: []byte(fmt.Sprintf("TEST_QR_%d", index))}}
					}
					if err := socket.send(peer.ctx, pairingIQFixture(waBinary.Node{Tag: "pair-device", Content: refs})); err != nil {
						t.Fatal(err)
					}
					select {
					case <-qrEvents:
					case <-peer.ctx.Done():
						t.Fatal("QR event was not dispatched")
					}
				}
				if boundary == "cancellation" {
					select {
					case item := <-qr:
						if item.Event != whatsmeow.QRChannelEventCode {
							t.Fatalf("missing QR before cancellation: %+v", item)
						}
					case <-peer.ctx.Done():
						t.Fatal("QR code not emitted")
					}
					cancelQR()
				}
				if boundary == "backpressure" {
					// Join the remote endpoint before draining: draining earlier
					// could remove the backpressure this case must exercise.
					select {
					case <-socket.done:
					case <-peer.ctx.Done():
						t.Fatal("QR backpressure did not disconnect its socket")
					}
				}
				codes, timeouts := 0, 0
				for {
					select {
					case item, open := <-qr:
						if !open {
							if o.client.Store.ID != nil || o.client.IsLoggedIn() {
								t.Fatal("QR shutdown manufactured an authenticated account")
							}
							if boundary == "empty_codes" && (codes != 0 || timeouts != 1) {
								t.Fatalf("exhausted QR result: codes=%d timeouts=%d", codes, timeouts)
							}
							if boundary == "backpressure" && codes != 8 {
								t.Fatalf("QR overflow did not preserve its full eight-item buffer: %d", codes)
							}
							return
						}
						if item.Event == whatsmeow.QRChannelEventCode {
							codes++
						} else if item.Event == whatsmeow.QRChannelTimeout.Event {
							timeouts++
						}
						if item.Event == whatsmeow.QRChannelSuccess.Event {
							t.Fatal("QR shutdown became pairing success")
						}
					case <-peer.ctx.Done():
						t.Fatal("QR shutdown did not close its channel")
					}
				}
			})
		}
	}
}
