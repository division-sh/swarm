package bus_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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

func waitOriginSQLLock(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		blocked, err := originSQLLockCount(db)
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

func originSQLLockCount(db *sql.DB) (int, error) {
	var blocked int
	// The run-header projection prefix remains visible if pg_stat_activity truncates the query.
	err := db.QueryRow(`
SELECT count(*) FROM pg_stat_activity
WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'
  AND btrim(regexp_replace(query, '[[:space:]]+', ' ', 'g'))
      LIKE 'SELECT r.run_id::text, lower(r.status), r.bundle_hash, r.origin_kind,%'
`).Scan(&blocked)
	return blocked, err
}

func waitPostgresReadLockCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked int
		if err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&blocked); err != nil {
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
			lock, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			completed := make(chan error, 2)
			readers := 0
			defer func() {
				if err := lock.Rollback(); err != nil {
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
			if _, err := lock.Exec(`LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
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
			waitPostgresReadLockCount(t, f.db, readers)
			blocked, err := originSQLLockCount(f.db)
			if err != nil {
				t.Fatal(err)
			}
			if blocked != want {
				t.Fatalf("origin lock count = %d, want %d with %d actual blocked readers", blocked, want, readers)
			}
			if tc.origin {
				waitOriginSQLLock(t, f.db)
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
				owner, err = process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: snapshot.Authority.ExecutionID(), BundleHash: authorActivityTestSourceArtifactFact.BundleHash()})
				if err != nil {
					t.Fatal(err)
				}
				c, err = deliverycontinuation.New(f.store, f.store, snapshot.Authority, owner, f.bus, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.bus.SetDeliveryContinuationOwner(c); err != nil {
					t.Fatal(err)
				}
			}
			reader.enabled.Store(true)
			completed := make(chan error, 1)
			if coordinated {
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				go func() { _, err := reader.LoadRunOrigin(ctx, f.event.RunID()); completed <- err }()
			}
			var readCtx context.Context
			select {
			case readCtx = <-reader.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("origin not entered")
			}
			lock, err := f.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback()
			if _, err := lock.Exec(`LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			close(reader.release)
			waitOriginSQLLock(t, f.db)
			if coordinated {
				wait, stop := context.WithCancel(context.Background())
				stop()
				_ = c.Retire(wait)
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
			if err := lock.Rollback(); err != nil {
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
				if _, err := owner.RetireAndWait(context.Background()); err != nil {
					t.Error(err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
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
