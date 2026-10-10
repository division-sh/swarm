//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

type blockedBootstrapReservationRead struct {
	channelonboarding.Store
	entered, release chan struct{}
	once             sync.Once
}

func (s *blockedBootstrapReservationRead) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	s.once.Do(func() {
		close(s.entered)
		// Model noninterruptible storage work while the retained owner still owns
		// construction. Canceling a caller must not erase or join this work inline.
		<-s.release
	})
	return s.Store.GetChannelOnboarding(ctx, id)
}

func TestServeBootstrapConstructionCallerCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			blocked := &blockedBootstrapReservationRead{Store: f.adapter.store, entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
			t.Cleanup(release)
			f.adapter.store = blocked
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- f.adapter.BootstrapSession(caller, f.op, f.candidate) }()
			select {
			case <-blocked.entered:
			case err := <-finished:
				t.Fatal("construction did not reach the retained reservation read", err)
			case <-time.After(5 * time.Second):
				t.Fatal("construction did not reach its controlled owner barrier")
			}
			cancel()
			returned := false
			select {
			case err := <-finished:
				returned = true
				if !errors.Is(err, context.Canceled) {
					t.Error("construction caller lost cancellation", err)
				}
			case <-time.After(time.Second):
				t.Error("original caller waits for uncancelable retained construction")
			}
			if f.owner.ActiveCount() == 0 {
				t.Error("unresolved construction lost its counted runtime ownership")
			}
			release()
			if !returned {
				select {
				case err := <-finished:
					if !errors.Is(err, context.Canceled) {
						t.Error("joined construction lost cancellation", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("controlled construction did not finish after release")
				}
			}
			if connection := f.adapter.connection(f.op.OperationID); connection != nil {
				if err := connection.Close(context.Background()); err != nil {
					t.Fatal("original partial construction did not join", err)
				}
			}
			if err := f.owner.WaitForQuiescence(f.ctx); err != nil {
				t.Fatal("construction released caller but retained unresolved transient work", err)
			}
			if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err == nil || f.selections.Load() != 1 {
				t.Fatal("canceled construction became success or an automatic replacement", err, f.selections.Load())
			}
		})
	}
}

func TestServeBootstrapOriginalCallerCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			entered := serveBootstrapWireFixture(t, "blocked_handshake", f.owner)
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- f.adapter.BootstrapSession(caller, f.op, f.candidate) }()
			select {
			case <-entered:
			case err := <-finished:
				t.Fatal("did not reach handshake", err)
			}
			cancel()
			prompt := true
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				prompt = false
				t.Error("original bootstrap caller remains blocked after cancellation; cleanup owns its wait")
				select {
				case err := <-finished:
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("cleanup did not join within original fixture safety bound")
				}
			}
			connection := f.adapter.connection(f.op.OperationID)
			if connection == nil {
				t.Fatal("canceled original caller lost cleanup ownership")
			}
			if prompt {
				if f.owner.ActiveCount() == 0 {
					t.Fatal("pending SDK join released counted runtime ownership")
				}
				other, err := sessionprovider.OpenRuntimeBootstrap(f.owned, sessionprovider.RuntimeConnectionOptions{
					Directory: f.adapter.directory, OperationID: f.op.OperationID, Store: f.adapter.store, Plan: f.candidate.Plan})
				if other != nil {
					if closeErr := other.Close(context.Background()); closeErr != nil {
						t.Error(closeErr)
					}
				}
				if err == nil {
					t.Fatal("successor acquired provider state while original SDK cleanup remained pending")
				}
			}
			join, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			if err := connection.Close(join); err != nil {
				t.Fatal("original cleanup failed to join", err)
			}
			if f.owner.ActiveCount() != 0 {
				t.Fatal("completed SDK join retained counted work")
			}
			if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err == nil || f.selections.Load() != 1 {
				t.Fatal("canceled original attempt became success or automatic reconnect", err)
			}
		})
	}
}
