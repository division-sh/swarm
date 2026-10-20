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
		for _, mode := range []string{"current", "expired", "observation_error", "scope_error", "canceled", "native_revocation", "display_only"} {
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
				case "display_only":
					occurrence.mu.Lock()
					occurrence.failureReason = "logged out"
					occurrence.mu.Unlock()
				case "native_revocation":
					if err := socket.send(f.ctx, waBinary.Node{Tag: "stream:error", Attrs: waBinary.Attrs{"code": "401"},
						Content: []waBinary.Node{{Tag: "conflict", Attrs: waBinary.Attrs{"type": "device_removed"}}}}); err != nil {
						t.Fatal(err)
					}
					select {
					case <-occurrence.ctx.Done():
					case <-f.ctx.Done():
						t.Fatal("native revocation did not fence its original occurrence")
					}
				}
				fact, factErr := c.RevokedSession(ctx, started.Operation)
				if mode == "native_revocation" {
					if factErr != nil || fact == nil || !fact.MatchesOperation(started.Operation) {
						t.Fatal("native revocation lost its exact unadmitted parent", factErr)
					}
					view, readErr := c.PairingReadback(f.ctx, f.principal)
					if readErr != nil || view != (channelonboarding.PairingReadback{Status: "failed"}) {
						t.Fatal("native revocation readback lost its zero-code, non-executable observation", readErr)
					}
					foreign := f.principal
					foreign.ID = started.Operation.OperationID
					if view, err := c.PairingReadback(f.ctx, foreign); err == nil || view != (channelonboarding.PairingReadback{}) {
						t.Fatal("native revocation disclosed progress to another principal")
					}
					canceled, cancel := context.WithCancel(f.ctx)
					cancel()
					if view, err := c.PairingReadback(canceled, f.principal); err == nil || view != (channelonboarding.PairingReadback{}) {
						t.Fatal("native revocation suppressed caller cancellation")
					}
					pairing.mu.Lock()
					coordinate := pairing.scope.Coordinate
					pairing.scope.Coordinate.BundleIdentity = "another-source"
					pairing.mu.Unlock()
					view, readErr = c.PairingReadback(f.ctx, f.principal)
					pairing.mu.Lock()
					pairing.scope.Coordinate = coordinate
					pairing.mu.Unlock()
					if !errors.Is(readErr, errPairingScope) || view != (channelonboarding.PairingReadback{}) {
						t.Fatal("native revocation suppressed the QR owner's original source scope")
					}
					for _, change := range []func(*channelonboarding.Operation){
						func(op *channelonboarding.Operation) { op.Revision++ },
						func(op *channelonboarding.Operation) { op.PrincipalID = "another-principal" },
						func(op *channelonboarding.Operation) { op.Coordinate.BundleHash = "another-source" },
						func(op *channelonboarding.Operation) { op.SessionAccount.ConnectionID = "already-admitted" },
						func(op *channelonboarding.Operation) { op.IdentityOperationID = "operator-enrollment" },
						func(op *channelonboarding.Operation) { op.BindingRevision = 1 },
						func(op *channelonboarding.Operation) { op.ActivationRevision = 1 },
					} {
						other := started.Operation
						change(&other)
						if fact.MatchesOperation(other) {
							t.Fatal("revocation granted replacement outside its unadmitted occurrence")
						}
					}
					if err := fact.Join(f.ctx); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
						t.Fatal("revocation joined before its exact parent was terminalized", err)
					}
				} else if fact != nil || mode != "canceled" && factErr != nil || mode == "canceled" && factErr == nil {
					t.Fatal("non-revocation observation granted replacement", factErr)
				}
				reuse, err := c.CheckSessionReuse(ctx, started.Operation)
				if reuse != (mode == "current" || mode == "display_only") || !errors.Is(err, want) {
					t.Fatal("reuse confused expired progress with health or converted an observation error to replacement", reuse, err)
				}
				if (c.CheckBootstrap(f.ctx) == nil) != (mode != "native_revocation") {
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
