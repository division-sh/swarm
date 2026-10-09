package sessionprovider

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
)

func TestWhatsAppDirectQRPublicSDKRepeatedBatches(t *testing.T) {
	peer := newSDKPeerMode(t, false)
	o := newPairingOccurrenceFixture(t, peer)
	scope := pairingScopeFixture(t)
	scope.ConnectionID, scope.OccurrenceID = o.connectionID, o.occurrenceID
	q, err := o.bindPairing(scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.connect(); err == nil || o.started {
		t.Fatal("pairing network boot preceded installation of the guarded QR consumer")
	}
	if _, err := o.bindCallbacks(func(context.Context, any) error { return nil },
		func(context.Context, callbackFailure) error { return nil }); err != nil {
		t.Fatal(err)
	}
	observed := make(chan []string, 1)
	o.client.AddEventHandler(func(raw any) {
		if event, ok := raw.(*events.QR); ok {
			observed <- append([]string(nil), event.Codes...)
		}
	})
	if err := o.connect(); err != nil {
		t.Fatal(err)
	}
	socket := pairingSocketFixture(t, peer)
	for index := 0; index < 32; index++ {
		ref := strings.Repeat("q", index+1)
		if err := socket.send(peer.ctx, pairingIQFixture(waBinary.Node{Tag: "pair-device", Content: []waBinary.Node{
			{Tag: "ref", Content: []byte(ref)},
		}})); err != nil {
			t.Fatal(err)
		}
		select {
		case codes := <-observed:
			got, err := q.read(scope)
			if err != nil || len(codes) != 1 || got.Code != codes[0] || got.Paired || got.Connected {
				t.Fatalf("direct guarded SDK QR did not replace the exact current code: %+v, %v", got, err)
			}
		case <-peer.ctx.Done():
			t.Fatal("real public QR event not consumed")
		}
	}
	o.fence()
	if got, err := q.read(scope); got.Code != "" || !errors.Is(err, errPairingStopped) {
		t.Fatal("retired occurrence disclosed SDK QR material")
	}
	if err := o.join(peer.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.done:
	default:
		t.Fatal("client occurrence join omitted its QR expiry worker")
	}
}

func TestWhatsAppDirectQRBindingRequiresExactUnpairedOccurrence(t *testing.T) {
	for _, boundary := range []string{"connection", "occurrence", "after_callbacks", "retired", "repeat"} {
		t.Run(boundary, func(t *testing.T) {
			peer := newSDKPeerMode(t, false)
			o := newPairingOccurrenceFixture(t, peer)
			scope := pairingScopeFixture(t)
			scope.ConnectionID, scope.OccurrenceID = o.connectionID, o.occurrenceID
			switch boundary {
			case "connection":
				scope.ConnectionID = uuid.NewString()
			case "occurrence":
				scope.OccurrenceID = uuid.NewString()
			case "after_callbacks":
				if _, err := o.bindCallbacks(func(context.Context, any) error { return nil },
					func(context.Context, callbackFailure) error { return nil }); err != nil {
					t.Fatal(err)
				}
			case "retired":
				o.fence()
			case "repeat":
				if _, err := o.bindPairing(scope); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := o.bindPairing(scope); err == nil {
				t.Fatal("pairing scope changed after occurrence admission")
			}
		})
	}
}

func TestWhatsAppDirectQRJoinedOccurrenceKeepsAdmittedCallback(t *testing.T) {
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	scope := pairingScopeFixture(t)
	o, err := newClientOccurrence(context.Background(), scope.ConnectionID, scope.OccurrenceID, container.NewDevice(), container.LIDMap(), nil)
	if err != nil {
		t.Fatal(err)
	}
	q, err := o.bindPairing(scope)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	guard, err := o.bindCallbacks(func(context.Context, any) error {
		close(entered)
		<-release
		return nil
	}, func(context.Context, callbackFailure) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan bool, 1)
	go func() { finished <- guard.receive(&events.QR{Codes: []string{"admitted QR"}}) }()
	<-entered
	o.fence()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.join(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("join released an admitted callback early: %v", err)
	}
	if got, err := q.read(scope); got.Code != "" || !errors.Is(err, errPairingStopped) {
		t.Fatal("QR material remained readable while callback settlement waited")
	}
	close(release)
	if <-finished {
		t.Fatal("retired QR callback reported success")
	}
	if err := o.join(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWhatsAppDirectQRRejectsPairedDeviceBeforeConnect(t *testing.T) {
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	scope := pairingScopeFixture(t)
	o, err := newClientOccurrence(context.Background(), scope.ConnectionID, scope.OccurrenceID, device, container.LIDMap(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer o.join(context.Background())
	if q, err := o.bindPairing(scope); !errors.Is(err, errPairingScope) || q != nil || o.started {
		t.Fatal("already-paired private state created a bootstrap QR occurrence")
	}
}
