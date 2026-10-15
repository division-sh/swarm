//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
)

func TestWhatsAppPairingReuseExpiryAndObservationErrorsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"current", "expired", "observation_error", "scope_error", "canceled"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newRuntimeBootstrapFixture(t, backend)
				started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
					Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "pairing-reuse"})
				if err != nil || f.connection == nil {
					t.Fatal("reuse fixture did not reach its unpaired SDK socket")
				}
				c := f.connection
				occurrence := c.state.currentOccurrence()
				pairing := occurrence.pairing
				observed := make(chan struct{}, 1)
				occurrence.client.AddEventHandler(func(event any) {
					if _, ok := event.(*events.QR); ok {
						observed <- struct{}{}
					}
				})
				socket := pairingSocketFixture(t, f.peer)
				if err := socket.send(f.ctx, pairingIQFixture(waBinary.Node{Tag: "pair-device", Content: []waBinary.Node{
					{Tag: "ref", Content: []byte("reuse-control")}}})); err != nil {
					t.Fatal("send controlled pairing request", err)
				}
				select {
				case <-observed:
				case <-time.After(5 * time.Second):
					t.Fatal("reuse fixture did not receive the SDK QR event")
				}
				if view, err := c.PairingReadback(f.ctx, f.principal); err != nil || view.Code == "" {
					t.Fatal("reuse fixture lacks an authorized current SDK QR")
				}
				ctx := f.ctx
				var want error
				switch mode {
				case "expired":
					if err := pairing.handle(&events.QR{}); err != nil {
						t.Fatal("expire the owned batch", err)
					}
				case "observation_error":
					want = errPairingFailed
					if err := pairing.handle(&events.PairError{}); !errors.Is(err, want) {
						t.Fatal("pairing error control did not retain its cause")
					}
				case "scope_error":
					pairing.mu.Lock()
					pairing.scope.Coordinate.BundleIdentity = "different-source"
					pairing.mu.Unlock()
					want = errPairingScope
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(f.ctx)
					cancel()
					want = context.Canceled
				}
				reuse, err := c.CheckSessionReuse(ctx, started.Operation)
				if reuse != (mode == "current") || !errors.Is(err, want) {
					t.Fatal("reuse confused expired progress with health or converted an observation error to replacement", reuse, err)
				}
				if c.CheckBootstrap(f.ctx) != nil {
					t.Fatal("reuse observation retired the live transport instead of leaving explicit resume in charge")
				}
				select {
				case <-c.closed:
					t.Fatal("reuse observation closed the occurrence")
				default:
				}
				if err := c.Close(f.ctx); err != nil {
					t.Fatal("original occurrence did not close and join", err)
				}
				for _, joined := range []<-chan struct{}{c.closed, occurrence.stopDone, pairing.done} {
					select {
					case <-joined:
					default:
						t.Fatal("Close returned before original connection/callback/QR worker join")
					}
				}
				c.state.directory.mu.Lock()
				released := c.state.directory.released
				c.state.directory.mu.Unlock()
				if !released {
					t.Fatal("joined occurrence retained private-directory possession")
				}
			})
		}
	}
}
