package bus_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/lib/pq"
)

type blockedOriginReader struct {
	completeEventDispatchStore
	origins interface {
		LoadRunOrigin(context.Context, string) (runlifecycle.RunOrigin, error)
	}
	enabled  atomic.Bool
	entered  chan context.Context
	release  chan struct{}
	returned chan error
}

func (r *blockedOriginReader) LoadRunOrigin(ctx context.Context, id string) (runlifecycle.RunOrigin, error) {
	if !r.enabled.Load() {
		return r.origins.LoadRunOrigin(ctx, id)
	}
	r.entered <- ctx
	<-r.release
	origin, err := r.origins.LoadRunOrigin(ctx, id)
	r.returned <- err
	return origin, err
}

func waitOriginSQLLock(t *testing.T, selected completeEventDispatchStore) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		blocked, err := originSQLLockCount(selected)
		if err != nil {
			t.Fatal(err)
		}
		if blocked == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("origin query did not reach the PostgreSQL lock barrier")
}

func originSQLLockCount(selected completeEventDispatchStore) (int, error) {
	return storetest.ReadPostgresRunOriginLockCount(context.Background(), selected)
}

func waitPostgresReadLockCount(t *testing.T, selected completeEventDispatchStore, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		blocked, err := storetest.ReadPostgresDatabaseLockCount(context.Background(), selected)
		if err != nil {
			t.Fatal(err)
		}
		if blocked == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("PostgreSQL read lock count did not reach %d", want)
}

func TestOriginSQLLockObserverPostgres(t *testing.T) {
	for _, tc := range []struct {
		name      string
		origin    bool
		unrelated bool
	}{
		{name: "unrelated_only", unrelated: true},
		{name: "origin_only", origin: true},
		{name: "origin_and_unrelated", origin: true, unrelated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCompleteEventDispatchFixtureWithOrigin(t, "postgres", false, runlifecycle.ScenarioSetupRunOrigin())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lock, err := storetest.HoldPostgresRunTableReadBarrier(ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			completed := make(chan error, 2)
			readers := 0
			defer func() {
				if err := lock.Close(); err != nil {
					t.Error(err)
				}
				for range readers {
					select {
					case err := <-completed:
						if err != nil {
							t.Errorf("released read failed: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Error("released read did not drain")
					}
				}
			}()
			if tc.unrelated {
				readers++
				go func() {
					_, err := f.store.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(ctx, f.event.RunID())
					completed <- err
				}()
			}
			want := 0
			if tc.origin {
				readers++
				want = 1
				go func() {
					_, err := f.store.(runtimebus.RunOriginReader).LoadRunOrigin(ctx, f.event.RunID())
					completed <- err
				}()
			}
			waitPostgresReadLockCount(t, f.store, readers)
			blocked, err := originSQLLockCount(f.store)
			if err != nil {
				t.Fatal(err)
			}
			if blocked != want {
				t.Fatalf("origin lock count = %d, want %d with %d actual blocked readers", blocked, want, readers)
			}
			if tc.origin {
				waitOriginSQLLock(t, f.store)
			}
		})
	}
}

func TestContinuationOriginReadPostgresCancellationCausality(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		name := "unowned_read_native_57014_control"
		if coordinated {
			name = "coordinator_drains_origin_read"
		}
		t.Run(name, func(t *testing.T) {
			reader := &blockedOriginReader{entered: make(chan context.Context, 1), release: make(chan struct{}), returned: make(chan error, 1)}
			f := newCompleteEventDispatchFixtureWithOrigin(t, "postgres", false, runlifecycle.ScenarioSetupRunOrigin(), func(store completeEventDispatchStore) runtimebus.RunOriginReader {
				reader.completeEventDispatchStore = store
				reader.origins = store.(interface {
					LoadRunOrigin(context.Context, string) (runlifecycle.RunOrigin, error)
				})
				return reader
			})
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			var c *deliverycontinuation.Coordinator
			var owner *worklifetime.RuntimeOccurrence
			var process *worklifetime.Process
			var barrier *storetest.PostgresRunTableReadBarrier
			var releaseOnce sync.Once
			releaseReader := func() { releaseOnce.Do(func() { close(reader.release) }) }
			completed := make(chan error, 1)
			workerStarted, workerJoined := false, false
			generation := &completeEventDispatchGeneration{}
			t.Cleanup(func() {
				clean := true
				cancel()
				if c != nil {
					wait, stop := context.WithCancel(context.Background())
					stop()
					_ = c.Retire(wait)
				}
				if barrier != nil {
					if err := barrier.Close(); err != nil {
						clean = false
						t.Error(err)
					}
				}
				releaseReader()
				if err := generation.close(); err != nil {
					clean = false
					t.Error(err)
				}
				if workerStarted && !workerJoined {
					select {
					case <-completed:
						workerJoined = true
					case <-time.After(5 * time.Second):
						clean = false
						t.Error("failed origin proof left an unjoined worker")
					}
				}
				if clean && (owner == nil || owner.ActiveCount() == 0) && (!workerStarted || workerJoined) {
					t.Log("origin proof cleanup joined")
				}
			})
			var id string
			if coordinated {
				acknowledgePipelineTestEvent(t, ctx, f.store, f.event.ID())
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
				var err error
				id, err = runtimedelivery.DeliveryID(f.event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := f.store.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.ActivateDeliveryAuthority(ctx, snapshot.Authority); err != nil {
					t.Fatal(err)
				}
				if err := f.bus.SetDeliveryAuthority(snapshot.Authority); err != nil {
					t.Fatal(err)
				}
				process = worklifetime.NewProcess()
				generation.process = process
				owner, err = process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: snapshot.Authority.ExecutionID(), BundleHash: authorActivityTestSourceArtifactFact.BundleHash()})
				if err != nil {
					t.Fatal(err)
				}
				generation.owner = owner
				c, err = deliverycontinuation.New(f.store, f.store, snapshot.Authority, owner, f.bus, nil)
				if err != nil {
					t.Fatal(err)
				}
				generation.coordinator = c
				if err := f.bus.SetDeliveryContinuationOwner(c); err != nil {
					t.Fatal(err)
				}
			}
			reader.enabled.Store(true)
			if coordinated {
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				workerStarted = true
				go func() { _, err := reader.LoadRunOrigin(ctx, f.event.RunID()); completed <- err }()
			}
			var readCtx context.Context
			select {
			case readCtx = <-reader.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("origin not entered")
			}
			var err error
			barrier, err = storetest.HoldPostgresRunTableReadBarrier(context.Background(), f.store)
			if err != nil {
				t.Fatal(err)
			}
			releaseReader()
			waitOriginSQLLock(t, f.store)
			if coordinated {
				wait, stop := context.WithCancel(context.Background())
				stop()
				_ = c.Retire(wait)
				workerStarted = true
				go func() { completed <- c.Retire(context.Background()) }()
				if readCtx.Done() != nil {
					t.Error("retirement can cancel admitted SQL")
				}
				if owner.ActiveCount() == 0 {
					t.Error("origin read lost coordinator lease")
				}
				select {
				case err := <-reader.returned:
					t.Errorf("origin read canceled while lock held: %v", err)
				default:
				}
			} else {
				cancel()
			}
			if !coordinated {
				select {
				case err := <-reader.returned:
					var pg *pq.Error
					if !errors.As(err, &pg) || pg.Code != "57014" {
						t.Fatalf("native cancellation=%T %v", err, err)
					}
					t.Logf("causal native origin reader cancellation: SQLSTATE=%s error=%v", pg.Code, err)
				case <-time.After(5 * time.Second):
					t.Fatal("native origin query did not cancel")
				}
			}
			if err := barrier.Close(); err != nil {
				t.Fatal(err)
			}
			if coordinated {
				select {
				case err := <-reader.returned:
					if err != nil {
						t.Errorf("drained SQL failed: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("SQL did not drain")
				}
			}
			select {
			case err := <-completed:
				workerJoined = true
				if !coordinated && err == nil {
					t.Fatal("retired read continued business work")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not join")
			}
			if coordinated {
				if err := c.Retire(context.Background()); err != nil {
					t.Fatal(err)
				}
				if owner.ActiveCount() != 0 {
					t.Error("leaked read lease")
				}
				if _, err := c.Acquire(id); err == nil {
					t.Error("retired owner accepted carrier")
				}
				if err := generation.close(); err != nil {
					t.Error(err)
				}
				snapshot, err := f.store.Snapshot(context.Background(), id)
				if err != nil || snapshot.Status != runtimedelivery.StatusPending {
					t.Fatalf("read mutated delivery: %+v %v", snapshot, err)
				}
			}
		})
	}
}
