//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

type blockedBootstrapReservationRead struct {
	channelonboarding.Store
	entered, release chan struct{}
	once             sync.Once
	armed            atomic.Bool
}

func (s *blockedBootstrapReservationRead) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	if s.armed.Load() {
		s.once.Do(func() {
			close(s.entered)
			// Model noninterruptible storage work while the retained owner still owns
			// construction. Canceling a caller must not erase or join this work inline.
			<-s.release
		})
	}
	return s.Store.GetChannelOnboarding(ctx, id)
}

func TestServeBootstrapConstructionCallerCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"bootstrap", "resume"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newServeBootstrapTestFixture(t, backend)
				blocked := &blockedBootstrapReservationRead{Store: f.adapter.store, entered: make(chan struct{}), release: make(chan struct{})}
				selectRuntime := f.adapter.selectRuntime
				f.adapter.selectRuntime = func(ctx context.Context, candidate channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, func() error, error) {
					owned, incoming, release, err := selectRuntime(ctx, candidate)
					if err != nil {
						return nil, nil, nil, err
					}
					// Production selection already retains this counted construction use.
					use, err := f.owner.Begin(ctx)
					if err != nil {
						return nil, nil, nil, errors.Join(err, release())
					}
					blocked.armed.Store(true)
					return owned, incoming, func() error { return errors.Join(release(), use.Done()) }, nil
				}
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
				t.Cleanup(release)
				f.adapter.store = blocked
				caller, cancel := context.WithCancel(f.ctx)
				defer cancel()
				finished := make(chan error, 1)
				start := f.adapter.BootstrapSession
				if mode == "resume" {
					start = f.adapter.ResumeSession
				}
				go func() { finished <- start(caller, f.op, f.candidate) }()
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
				reuse, stop := context.WithTimeout(f.ctx, 50*time.Millisecond)
				if err := f.adapter.BootstrapSession(reuse, f.op, f.candidate); !errors.Is(err, context.DeadlineExceeded) || f.selections.Load() != 1 {
					t.Error("pending original construction was replaced or reported success", err, f.selections.Load())
				}
				stop()
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
}

func TestServeBootstrapSelectionFailureReleasesOriginalUseBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"selection_error", "invalid_owner", "canceled"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newServeBootstrapTestFixture(t, backend)
				caller, cancel := context.WithCancel(f.ctx)
				defer cancel()
				selectionFailure, releaseFailure := errors.New("selection failed"), errors.New("release failed")
				var releases atomic.Int64
				f.adapter.selectRuntime = func(ctx context.Context, _ channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, func() error, error) {
					f.selections.Add(1)
					use, err := f.owner.Begin(ctx)
					if err != nil {
						return nil, nil, nil, err
					}
					release := func() error {
						releases.Add(1)
						return errors.Join(use.Done(), releaseFailure)
					}
					if mode == "selection_error" {
						return f.owned, nil, release, selectionFailure
					}
					if mode == "invalid_owner" {
						return nil, nil, release, nil
					}
					cancel()
					return f.owned, nil, release, nil
				}
				err := f.adapter.BootstrapSession(caller, f.op, f.candidate)
				if !errors.Is(err, releaseFailure) || mode == "selection_error" && !errors.Is(err, selectionFailure) ||
					mode == "invalid_owner" && !errors.Is(err, channelonboarding.ErrInvalidRequest) || mode == "canceled" && !errors.Is(err, context.Canceled) {
					t.Fatal("original selection/release evidence lost", err)
				}
				if releases.Load() != 1 || f.owner.ActiveCount() != 0 || f.adapter.connection(f.op.OperationID) != nil {
					t.Fatal("failed selection retained work or constructed an SDK", releases.Load(), f.owner.ActiveCount())
				}
				if cached := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); !errors.Is(cached, releaseFailure) || releases.Load() != 1 || f.selections.Load() != 1 {
					t.Fatal("cache lost original error or repeated selection/release", cached)
				}
			})
		}
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
