package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAuthorityAcquisitionGracefulCancellation(t *testing.T) {
	for _, scenario := range []string{"before_admission", "after_query", "custom_context", "custom_failure", "custom_panic"} {
		t.Run(scenario, func(t *testing.T) {
			b, probe := newExitProbe(t)
			parent, held, err := AcquireAdvisoryLockLease(context.Background(), b.db, "acquisition-parent")
			if err != nil || !held {
				t.Fatalf("parent held=%t: %v", held, err)
			}
			defer parent.Release(context.Background())
			session := parent.Session()
			conn, err := session.connection()
			if err != nil {
				t.Fatal(err)
			}
			var beforePID int
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&beforePID); err != nil {
				t.Fatal(err)
			}
			release, ok := session.retain()
			if !ok {
				t.Fatal("retain failed")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			primary := errors.New("independent acquisition failure")
			queries, callbacks := 0, 0
			probe.afterQuery = func(sqlCtx context.Context, query string) {
				if !strings.Contains(query, "pg_try_advisory_lock") {
					return
				}
				queries++
				if scenario == "after_query" {
					if sqlCtx.Done() != nil {
						t.Error("built-in acquisition retained logical cancellation")
					}
					// Real stock-pq query has returned its rows; cancel before
					// Scan/attachment. The owner must drain and relinquish only this lock.
					cancel()
				}
			}
			var acquire AdvisoryLockAcquire
			if strings.HasPrefix(scenario, "custom_") {
				acquire = func(callbackCtx context.Context, a *SessionAuthority, key string) (bool, error) {
					callbacks++
					if callbackCtx != ctx {
						t.Error("arbitrary callback context was detached")
					}
					if a.operationMu.TryLock() {
						a.operationMu.Unlock()
						t.Error("acquisition callback not serialized")
					}
					if scenario == "custom_failure" {
						cancel()
						return false, primary
					}
					var held bool
					err := a.queryRowContext(callbackCtx, "SELECT pg_try_advisory_lock(hashtext($1))", key).Scan(&held)
					if scenario == "custom_panic" && err == nil {
						panic(primary)
					}
					return held, err
				}
			}
			if scenario == "before_admission" {
				cancel()
			}
			retired := 0
			parent.InstallTerminalOwner(nil, func() {
				retired++
				if !session.operationMu.TryLock() {
					t.Error("terminal callback ran before operation unlock")
				} else {
					session.operationMu.Unlock()
				}
			}, nil)
			var lease *AdvisoryLockLease
			var recovered any
			acquisitionFinished := false
			func() {
				defer func() { recovered = recover() }()
				lease, held, err = AcquireAdvisoryLockLeaseOnSession(ctx, session, "acquisition-child", acquire, func() error {
					if !acquisitionFinished {
						if !session.operationMu.TryLock() {
							t.Error("acquisition release callback ran before operation unlock")
						} else {
							session.operationMu.Unlock()
						}
					}
					return release()
				})
			}()
			acquisitionFinished = true
			acquisitionErr := err
			probe.afterQuery = nil
			if scenario == "custom_panic" {
				if recovered != primary {
					t.Fatalf("panic identity=%v", recovered)
				}
			} else if recovered != nil {
				t.Fatalf("unexpected panic=%v", recovered)
			}
			if scenario == "custom_context" {
				if err != nil || !held || lease == nil || callbacks != 1 {
					t.Fatalf("custom held=%t calls=%d: %v", held, callbacks, err)
				}
				if err := lease.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if scenario != "custom_panic" {
				if lease != nil || held || !errors.Is(err, context.Canceled) {
					t.Errorf("canceled acquired=%t lease=%v: %v", held, lease, err)
				}
			}
			if scenario == "custom_failure" && !errors.Is(err, primary) {
				t.Errorf("independent failure lost: %v", err)
			}
			if scenario == "before_admission" && queries != 0 {
				t.Error("canceled acquisition admitted SQL")
			}
			if scenario == "after_query" && queries != 1 {
				t.Errorf("queries=%d", queries)
			}
			unsafe := scenario == "custom_failure" || scenario == "custom_panic"
			if unsafe {
				if parent.Current() || retired != 1 {
					t.Errorf("unsafe possession survived: current=%t retired=%d", parent.Current(), retired)
				}
				called := false
				if err := RunAuthorityTransaction(context.Background(), session, func(context.Context, *sql.Tx) error { called = true; return nil }); err == nil || called {
					t.Error("unsafe successor admitted")
				}
			} else {
				var afterPID int
				if err := RunAuthorityReadTransaction(context.Background(), session, func(ctx context.Context, tx *sql.Tx) error {
					return tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&afterPID)
				}); err != nil || beforePID != afterPID {
					t.Errorf("healthy successor pid=%d->%d: %v", beforePID, afterPID, err)
				}
				if err := parent.ProveCurrent(context.Background()); err != nil {
					t.Errorf("independent parent lock lost: %v", err)
				}
			}
			// A different physical socket must acquire the child lock after cancellation,
			// explicit release, or unsafe discard; never rely on same-session reentrancy.
			other, err := sql.Open("postgres", probe.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			competitor, got, err := AcquireAdvisoryLockLease(context.Background(), other, "acquisition-child")
			if err != nil || !got {
				t.Fatalf("competing child acquired=%t: %v", got, err)
			}
			if err := competitor.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Logf("queries=%d callbacks=%d unsafe=%t retired=%d acquisitionError=%v", queries, callbacks, unsafe, retired, acquisitionErr)
		})
	}
}

func TestAuthorityAcquisitionWaitsForTransactionAndRechecksAdmission(t *testing.T) {
	b, _ := newExitProbe(t)
	parent, held, err := AcquireAdvisoryLockLease(context.Background(), b.db, "acquisition-wait-parent")
	if err != nil || !held {
		t.Fatalf("parent held=%t: %v", held, err)
	}
	defer parent.Release(context.Background())
	session := parent.Session()
	release, ok := session.retain()
	if !ok {
		t.Fatal("retain failed")
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	txDone := make(chan error, 1)
	go func() {
		txDone <- RunAuthorityTransaction(context.Background(), session, func(ctx context.Context, tx *sql.Tx) error {
			var n int
			if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
				return err
			}
			close(entered)
			<-finish
			return nil
		})
	}()
	select {
	case <-entered:
	case err := <-txDone:
		t.Fatalf("transaction did not enter: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("transaction barrier timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, called := make(chan struct{}), make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		close(started)
		_, _, err := AcquireAdvisoryLockLeaseOnSession(ctx, session, "acquisition-wait-child", func(context.Context, *SessionAuthority, string) (bool, error) {
			called <- struct{}{}
			return false, nil
		}, release)
		done <- err
	}()
	<-started
	select {
	case <-called:
		t.Error("acquisition entered while transaction owned operation")
	case err := <-done:
		t.Errorf("acquisition completed before transaction: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	close(finish)
	if err := <-txDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("admission not rechecked: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("acquisition did not join")
	}
	select {
	case <-called:
		t.Error("canceled acquisition callback admitted")
	default:
	}
	if err := parent.ProveCurrent(context.Background()); err != nil {
		t.Errorf("parent did not survive: %v", err)
	}
}

func TestBlockingAuthorityAcquisitionWaitsAndJoinsCancellation(t *testing.T) {
	for _, scenario := range []string{"holder_releases", "cancel_before", "cancel_blocked", "cancel_after_acquisition"} {
		t.Run(scenario, func(t *testing.T) {
			b, original := newExitProbe(t)
			key := "blocking-process-lease"
			holder, held, err := AcquireAdvisoryLockLease(context.Background(), b.db, key)
			if err != nil || !held {
				t.Fatalf("holder acquired=%t: %v", held, err)
			}
			defer holder.Release(context.Background())
			probe := &exitProbeConnector{
				driver: original.driver, dsn: original.dsn,
				closedSignal: make(chan struct{}), rollbackDone: make(chan struct{}),
			}
			waiterDB := sql.OpenDB(probe)
			defer waiterDB.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "cancel_before" {
				cancel()
			}
			if scenario == "cancel_after_acquisition" {
				probe.afterExec = func(execCtx context.Context, query string) {
					if query == blockingAdvisoryLockSQL {
						if execCtx != ctx {
							t.Error("blocking acquisition detached cancellation")
						}
						cancel()
					}
				}
			}
			type acquisition struct {
				lease *AdvisoryLockLease
				err   error
			}
			done := make(chan acquisition, 1)
			go func() {
				lease, err := AcquireBlockingAdvisoryLockLease(ctx, waiterDB, key)
				done <- acquisition{lease, err}
			}()
			if scenario != "cancel_before" {
				observeCtx, stopObserve := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopObserve()
				for {
					var waiting bool
					err := holder.Session().queryRowContext(observeCtx, `SELECT EXISTS (
						SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted
						AND classid::bigint=CASE WHEN hashtext($1)<0 THEN 4294967295::bigint ELSE 0::bigint END
						AND objid::bigint=(hashtext($1)::bigint & 4294967295::bigint) AND objsubid=1)`, key).Scan(&waiting)
					if err != nil {
						t.Fatalf("observe actual kernel lock wait: %v", err)
					}
					if waiting {
						break
					}
					select {
					case result := <-done:
						t.Fatalf("waiter returned while holder retained possession: %v", result.err)
					case <-observeCtx.Done():
						t.Fatal("blocking lock wait was not observed")
					case <-time.After(time.Millisecond):
					}
				}
				if scenario == "cancel_blocked" {
					cancel()
				} else if err := holder.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			var result acquisition
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("blocking acquisition did not join")
			}
			if scenario == "holder_releases" {
				if result.err != nil || result.lease == nil {
					t.Fatalf("successor acquisition: %v", result.err)
				}
				if err := result.lease.ProveCurrent(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := result.lease.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if result.lease != nil || !errors.Is(result.err, context.Canceled) {
				t.Fatalf("cancellation published lease=%v err=%v", result.lease, result.err)
			}
			if err := holder.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			competitor, acquired, err := AcquireAdvisoryLockLease(context.Background(), b.db, key)
			if err != nil || !acquired {
				t.Fatalf("waiter leaked lock after joined disposition: acquired=%t err=%v", acquired, err)
			}
			if err := competitor.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
