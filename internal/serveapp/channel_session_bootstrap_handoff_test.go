//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func TestServeSessionOriginalConstructionConnectHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"bootstrap", "resume"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				peer := newServeNativeProtocolPeer(t)
				f := newServeBootstrapTestFixture(t, backend)
				cut, selections, release := installSessionConstructionCut(t, f.adapter)
				caller, cancel := context.WithTimeout(f.ctx, 5*time.Second)
				finished := make(chan error, 1)
				start := f.adapter.BootstrapSession
				if mode == "resume" {
					start = f.adapter.ResumeSession
				}
				go func() { finished <- start(caller, f.op, f.candidate) }()
				// Join the exact construction even if a pre-fix caller returns early.
				t.Cleanup(func() {
					cancel()
					release()
					join, stop := context.WithTimeout(f.ctx, 30*time.Second)
					defer stop()
					f.adapter.mu.Lock()
					attempt := f.adapter.connections[f.op.OperationID]
					f.adapter.mu.Unlock()
					if attempt == nil {
						return
					}
					select {
					case <-attempt.done:
					case <-join.Done():
						t.Error("original construction did not join")
						return
					}
					if attempt.connection != nil {
						if err := attempt.connection.Close(join); err != nil {
							t.Error("original connection did not join", err)
						}
					}
				})
				select {
				case <-cut.entered:
				case <-caller.Done():
					t.Fatal("original constructor did not enter its held cut")
				}
				select {
				case err := <-finished:
					t.Fatal("original live caller abandoned its construction before connect", err)
				default:
				}
				f.adapter.mu.Lock()
				original := f.adapter.connections[f.op.OperationID]
				f.adapter.mu.Unlock()
				if _, err := f.adapter.ReadSessionPairing(caller, f.op, f.principal); !errors.Is(err, channelonboarding.ErrSessionPending) {
					t.Fatal("held readback did not retain its precise pending refusal", err)
				}
				for range 2 {
					if err := start(caller, f.op, f.candidate); !errors.Is(err, channelonboarding.ErrSessionPending) {
						t.Fatal("secondary request waited or changed held responsibility", err)
					}
				}
				peer.mu.Lock()
				before := peer.connections
				peer.mu.Unlock()
				if before != 0 || selections.Load() != 1 {
					t.Fatal("held construction dialed or selected a successor", before, selections.Load())
				}
				release()
				select {
				case err := <-finished:
					if err != nil {
						t.Fatal("original live request failed its construction-to-connect handoff", err)
					}
				case <-caller.Done():
					t.Fatal("original request did not connect within its existing bound")
				}
				requireConstructionHandoffFirstQR(t, caller, f.adapter, f.op, f.principal)
				f.adapter.mu.Lock()
				retained := f.adapter.connections[f.op.OperationID] == original
				f.adapter.mu.Unlock()
				peer.mu.Lock()
				connections := peer.connections
				peer.mu.Unlock()
				if !retained || selections.Load() != 1 || connections != 1 {
					t.Fatal("original handoff substituted an attempt or repeated SDK connect", retained, selections.Load(), connections)
				}
				if _, err := f.adapter.ChannelExecution(caller, f.op); err == nil {
					t.Fatal("unpaired first QR gained channel execution")
				}
			})
		}
	}
}

func TestServeSessionConstructionHandoffRefusalsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"bootstrap", "resume"} {
			for _, cut := range []string{"caller_cancels", "handoff_cancels", "release_fails"} {
				t.Run(backend+"/"+mode+"/"+cut, func(t *testing.T) {
					requireSessionConstructionHandoffRefusal(t, backend, mode, cut)
				})
			}
		}
	}
}

func requireSessionConstructionHandoffRefusal(t *testing.T, backend, mode, cut string) {
	t.Helper()
	peer := newServeNativeProtocolPeer(t)
	f := newServeBootstrapTestFixture(t, backend)
	barrier, selections, release := installSessionConstructionCut(t, f.adapter)
	caller, cancel := context.WithCancelCause(f.ctx)
	defer cancel(context.Canceled)
	cause := errors.New("original caller stopped before connect")
	if cut == "release_fails" {
		cause = errors.New("original construction release failed")
	}
	selectRuntime := f.adapter.selectRuntime
	var releases int
	f.adapter.selectRuntime = func(ctx context.Context, candidate channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, processbinding.Binding, func() error, error) {
		owned, incoming, binding, done, err := selectRuntime(ctx, candidate)
		if err != nil {
			return owned, incoming, binding, done, err
		}
		use, err := f.owner.Begin(ctx)
		if err != nil {
			return nil, nil, processbinding.Binding{}, nil, errors.Join(err, done())
		}
		return owned, incoming, binding, func() error {
			releases++
			err := errors.Join(done(), use.Done())
			if cut == "handoff_cancels" {
				cancel(cause)
			}
			if cut == "release_fails" {
				err = errors.Join(err, cause)
			}
			return err
		}, nil
	}
	start := f.adapter.BootstrapSession
	if mode == "resume" {
		start = f.adapter.ResumeSession
	}
	finished := make(chan error, 1)
	go func() { finished <- start(caller, f.op, f.candidate) }()
	join, stop := context.WithTimeout(f.ctx, 30*time.Second)
	defer stop()
	select {
	case <-barrier.entered:
	case <-join.Done():
		t.Fatal("original constructor did not reach the refusal cut")
	}
	f.adapter.mu.Lock()
	original := f.adapter.connections[f.op.OperationID]
	f.adapter.mu.Unlock()
	t.Cleanup(func() {
		cancel(context.Canceled)
		release()
		wait, done := context.WithTimeout(f.ctx, 30*time.Second)
		defer done()
		select {
		case <-original.done:
			if original.connection != nil {
				if err := original.connection.Close(wait); err != nil {
					t.Error("retained original did not join", err)
				}
			}
		case <-wait.Done():
			t.Error("original refused construction did not join")
		}
	})
	if cut == "caller_cancels" {
		cancel(cause)
	} else {
		release()
	}
	select {
	case err := <-finished:
		if !errors.Is(err, cause) {
			t.Fatal("live original request lost its exact cancellation/release failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("original request did not promptly refuse at the handoff cut")
	}
	if cut == "caller_cancels" && f.owner.ActiveCount() == 0 {
		t.Fatal("caller cancellation discarded the counted constructor")
	}
	release()
	select {
	case <-original.done:
	case <-join.Done():
		t.Fatal("retained constructor did not settle after release")
	}
	peer.mu.Lock()
	connections := peer.connections
	peer.mu.Unlock()
	// The constructed native connection retains one lifetime use until Close;
	// the additional construction use has already joined before done closes.
	if original.connection == nil || selections.Load() != 1 || releases != 1 || connections != 0 || f.owner.ActiveCount() != 1 {
		t.Fatal("refused handoff lost cleanup ownership or dialed later", selections.Load(), releases, connections, f.owner.ActiveCount())
	}
	if cut == "release_fails" {
		if _, err := f.adapter.ReadSessionPairing(join, f.op, f.principal); !errors.Is(err, cause) {
			t.Fatal("construction readback lost its original release failure", err)
		}
		if err := original.connection.Close(join); err != nil || f.owner.ActiveCount() != 0 {
			t.Fatal("failed construction release lost its joined native cleanup", err, f.owner.ActiveCount())
		}
		return
	}
	if _, err := original.current(join); !errors.Is(err, channelonboarding.ErrSessionPending) {
		t.Fatal("canceled construction automatically launched SDK execution", err)
	}
	// Only a new independently authorized explicit call may connect the retained
	// original after the first caller cancels; construction never does so itself.
	if err := start(join, f.op, f.candidate); err != nil {
		t.Fatal("later explicit request could not continue its retained original", err)
	}
	qr, stopQR := context.WithTimeout(join, 5*time.Second)
	defer stopQR()
	requireConstructionHandoffFirstQR(t, qr, f.adapter, f.op, f.principal)
	f.adapter.mu.Lock()
	retained := f.adapter.connections[f.op.OperationID] == original
	f.adapter.mu.Unlock()
	peer.mu.Lock()
	connections = peer.connections
	peer.mu.Unlock()
	if !retained || selections.Load() != 1 || releases != 1 || connections != 1 {
		t.Fatal("later explicit request replaced or redialed its original", retained, selections.Load(), releases, connections)
	}
	if err := original.connection.Close(join); err != nil || f.owner.ActiveCount() != 0 {
		t.Fatal("continued original did not join all retained work", err, f.owner.ActiveCount())
	}
}

func requireConstructionHandoffFirstQR(t *testing.T, ctx context.Context, adapter *serveSessionBootstrap, op channelonboarding.Operation, principal operatorchannel.Principal) {
	t.Helper()
	// Connect can finish before its peer publishes the first QR. Observe that
	// actual SDK milestone before checking the peer's exact connection count.
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		pairing, err := adapter.ReadSessionPairing(ctx, op, principal)
		if err != nil {
			t.Fatal("original live request pairing readback refused", err)
		}
		if pairing.Status == "awaiting_qr" && pairing.Code != "" && pairing.ExpiresAt.After(time.Now()) {
			if pairing.Paired || pairing.Connected {
				t.Fatal("first QR invented paired or execution authority")
			}
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("original live request did not publish its first QR without retry")
		}
	}
}
