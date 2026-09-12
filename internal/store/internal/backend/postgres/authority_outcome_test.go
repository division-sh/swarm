package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestAuthorityTransactionOutcomeEvidence(t *testing.T) {
	for _, scenario := range []string{"success", "before_admission", "before_commit", "after_commit_admission", "uncertain_commit", "end_tx_failure", "callback_failure", "rollback_failure", "panic"} {
		t.Run(scenario, func(t *testing.T) {
			b, probe := newExitProbe(t)
			if _, err := b.db.Exec("CREATE TABLE authority_outcome (n integer)"); err != nil {
				t.Fatal(err)
			}
			lease, acquired, err := AcquireAdvisoryLockLease(context.Background(), b.db, "authority-outcome")
			if err != nil || !acquired {
				t.Fatalf("acquire=%t: %v", acquired, err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			session := lease.Session()
			conn, err := session.connection()
			if err != nil {
				t.Fatal(err)
			}
			var pid int
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&pid); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			primary, cleanup := errors.New("independent outcome failure"), errors.New("independent settlement failure")
			switch scenario {
			case "before_admission":
				cancel()
			case "after_commit_admission":
				probe.beforeCommit = cancel
			case "uncertain_commit":
				// Actual COMMIT succeeds, but the driver reports failure. This is
				// an uncertainty boundary injection, not a TCP response-loss proof.
				probe.commitFailure = primary
			case "end_tx_failure":
				session.SetEndTxErrorForTest(func() error {
					if session.operationMu.TryLock() {
						session.operationMu.Unlock()
						t.Error("EndTx failure observed after releasing operation ownership")
					}
					return cleanup
				})
			case "rollback_failure":
				probe.rollbackFailure = cleanup
			}
			var committed bool
			var recovered any
			calls := 0
			func() {
				defer func() { recovered = recover() }()
				committed, err = RunAuthorityTransactionOutcome(ctx, session, func(sqlCtx context.Context, tx *sql.Tx) error {
					calls++
					if _, err := tx.ExecContext(sqlCtx, "INSERT INTO authority_outcome VALUES (1)"); err != nil {
						return err
					}
					switch scenario {
					case "before_commit":
						cancel()
					case "callback_failure":
						cancel()
						return primary
					case "rollback_failure":
						return primary
					case "panic":
						panic(primary)
					}
					return nil
				})
			}()
			wantCommitted := scenario == "success" || scenario == "after_commit_admission" || scenario == "end_tx_failure"
			if committed != wantCommitted {
				t.Errorf("committed=%t want=%t err=%v", committed, wantCommitted, err)
			}
			wantCalls := 1
			if scenario == "before_admission" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Errorf("callbacks=%d want=%d", calls, wantCalls)
			}
			if scenario == "panic" && recovered != primary {
				t.Errorf("panic identity changed: %v", recovered)
			}
			if (scenario == "before_admission" || scenario == "before_commit" || scenario == "callback_failure") && !errors.Is(err, context.Canceled) {
				t.Errorf("lost logical stop: %v", err)
			}
			if (scenario == "uncertain_commit" || scenario == "callback_failure" || scenario == "rollback_failure") && !errors.Is(err, primary) {
				t.Errorf("lost independent primary: %v", err)
			}
			if (scenario == "end_tx_failure" || scenario == "rollback_failure") && !errors.Is(err, cleanup) {
				t.Errorf("lost cleanup: %v", err)
			}
			if (scenario == "success" || scenario == "after_commit_admission") && err != nil {
				t.Errorf("acknowledged commit gained failure: %v", err)
			}
			unsafe := scenario == "uncertain_commit" || scenario == "end_tx_failure" || scenario == "rollback_failure"
			if lease.Current() == unsafe {
				t.Errorf("possession current=%t unsafe=%t", lease.Current(), unsafe)
			}
			next, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			err = RunAuthorityReadTransaction(next, session, func(sqlCtx context.Context, tx *sql.Tx) error {
				if unsafe {
					t.Error("unsafe session admitted successor")
				}
				var nextPID int
				if err := tx.QueryRowContext(sqlCtx, "SELECT pg_backend_pid()").Scan(&nextPID); err != nil {
					return err
				}
				if nextPID != pid {
					t.Errorf("healthy PID changed: %d -> %d", pid, nextPID)
				}
				return nil
			})
			if (err != nil) != unsafe {
				t.Errorf("successor error=%v unsafe=%t", err, unsafe)
			}
			if err := lease.Release(next); err != nil {
				t.Fatal(err)
			}
			var rows int
			if err := b.db.QueryRowContext(next, "SELECT count(*) FROM authority_outcome").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			wantRows := 0
			if wantCommitted || scenario == "uncertain_commit" {
				wantRows = 1
			}
			if rows != wantRows {
				t.Errorf("durable rows=%d want=%d", rows, wantRows)
			}
			// After unsafe disposal, a different physical session must acquire the
			// server lock; a successful local close alone is not release evidence.
			var returnedPID int
			if err := b.db.QueryRowContext(next, "SELECT pg_backend_pid()").Scan(&returnedPID); err != nil {
				t.Fatal(err)
			}
			if unsafe && returnedPID == pid {
				t.Error("unsafe physical session returned to pool")
			}
			if _, err := b.db.ExecContext(next, "SELECT pg_advisory_lock(hashtext('authority-outcome'))"); err != nil {
				t.Fatal(err)
			}
			if _, err := b.db.ExecContext(next, "SELECT pg_advisory_unlock(hashtext('authority-outcome'))"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAuthorityReadGracefulCancellation(t *testing.T) {
	for _, surface := range []string{"before_admission", "query_row", "rows", "prepared_rows", "independent_error", "panic"} {
		t.Run(surface, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			lease, acquired, err := AcquireAdvisoryLockLease(context.Background(), db, "authority-read")
			if err != nil || !acquired {
				t.Fatalf("acquire=%t: %v", acquired, err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			session := lease.Session()
			conn, err := session.connection()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(context.Background(), `CREATE FUNCTION pg_temp.authority_read() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'active'; PERFORM pg_sleep(0.02); RETURN 1; END $$`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			noticed := false
			if err := conn.Raw(func(raw any) error {
				pq.SetNoticeHandler(raw.(driver.Conn), func(*pq.Error) { noticed = true; cancel() })
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if surface == "before_admission" {
				cancel()
			}
			var beforePID, afterPID, count int
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&beforePID); err != nil {
				t.Fatal(err)
			}
			calls := 0
			primary := errors.New("independent read callback")
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err = RunAuthorityReadTransaction(ctx, session, func(sqlCtx context.Context, tx *sql.Tx) error {
					calls++
					var readonly, isolation string
					if err := tx.QueryRowContext(sqlCtx, "SHOW transaction_read_only").Scan(&readonly); err != nil {
						return err
					}
					if err := tx.QueryRowContext(sqlCtx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
						return err
					}
					if readonly != "on" || isolation != "repeatable read" || sqlCtx.Done() != nil {
						t.Errorf("read boundary readonly=%s isolation=%s detached=%t", readonly, isolation, sqlCtx.Done() == nil)
					}
					if surface == "panic" {
						panic(primary)
					}
					if surface == "independent_error" {
						cancel()
						return primary
					}
					const query = "SELECT pg_temp.authority_read()"
					if surface == "query_row" {
						return tx.QueryRowContext(sqlCtx, query).Scan(&count)
					}
					var rows *sql.Rows
					var err error
					if surface == "prepared_rows" {
						stmt, err := tx.PrepareContext(sqlCtx, query)
						if err != nil {
							return err
						}
						defer stmt.Close()
						rows, err = stmt.QueryContext(sqlCtx)
					} else {
						rows, err = tx.QueryContext(sqlCtx, query)
					}
					if err != nil {
						return err
					}
					for rows.Next() {
						count++
					}
					return errors.Join(rows.Err(), rows.Close())
				})
			}()
			if surface == "panic" {
				if recovered != primary {
					t.Errorf("panic changed: %v", recovered)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Errorf("logical cancellation lost: %v", err)
			}
			if surface == "independent_error" && !errors.Is(err, primary) {
				t.Errorf("independent read failure lost: %v", err)
			}
			if surface == "before_admission" {
				if calls != 0 {
					t.Error("canceled read admitted callback")
				}
			} else if surface != "panic" && surface != "independent_error" && (!noticed || count != 1 || !onlyCancellationLeaves(err, context.Canceled)) {
				t.Errorf("admitted read did not drain cleanly: notice=%t rows=%d err=%v", noticed, count, err)
			}
			if !lease.Current() {
				t.Error("healthy read lost possession")
			}
			if err := RunAuthorityReadTransaction(context.Background(), session, func(sqlCtx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(sqlCtx, "SELECT pg_backend_pid()").Scan(&afterPID)
			}); err != nil || beforePID != afterPID {
				t.Errorf("healthy successor: pid %d -> %d err=%v", beforePID, afterPID, err)
			}
			if err := lease.ProveCurrent(context.Background()); err != nil {
				t.Errorf("healthy advisory possession: %v", err)
			}
		})
	}
}

func TestAuthorityReadInvalidSessionFenced(t *testing.T) {
	dsn, _, _ := testutil.StartPostgres(t)
	readFailure := errors.New("injected retained read transport failure")
	dialer := &sessionScopeDialer{readFailure: readFailure}
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	connector.Dialer(dialer)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	defer db.Close()
	lease, acquired, err := AcquireAdvisoryLockLease(context.Background(), db, "invalid-authority-read")
	if err != nil || !acquired {
		t.Fatalf("acquire=%t: %v", acquired, err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	session := lease.Session()
	var retired atomic.Int32
	lease.InstallTerminalOwner(nil, func() {
		retired.Add(1)
		if !session.operationMu.TryLock() {
			t.Error("terminal callback ran under operation ownership")
			return
		}
		session.operationMu.Unlock()
	}, nil)
	dialer.onFirstClose = func() {
		if session.operationMu.TryLock() {
			session.operationMu.Unlock()
			t.Error("physical disposal ran after operation ownership release")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = RunAuthorityReadTransaction(ctx, session, func(sqlCtx context.Context, tx *sql.Tx) error {
		dialer.failRead.Store(true)
		var n int
		err := tx.QueryRowContext(sqlCtx, "SELECT 1").Scan(&n)
		cancel()
		return err
	})
	if !errors.Is(err, readFailure) || !errors.Is(err, context.Canceled) {
		t.Errorf("lost independent read / logical stop: %v", err)
	}
	if lease.Current() || retired.Load() != 1 || dialer.closed.Load() != 1 {
		t.Errorf("invalid disposition: current=%t retired=%d closes=%d", lease.Current(), retired.Load(), dialer.closed.Load())
	}
	if err := RunAuthorityReadTransaction(context.Background(), session, func(context.Context, *sql.Tx) error {
		t.Error("invalid session admitted successor")
		return nil
	}); err == nil {
		t.Error("invalid successor returned success")
	}
	next, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if _, err := db.ExecContext(next, "SELECT pg_advisory_lock(hashtext('invalid-authority-read'))"); err != nil {
		t.Fatalf("competing server possession not released: %v", err)
	}
	if _, err := db.ExecContext(next, "SELECT pg_advisory_unlock(hashtext('invalid-authority-read'))"); err != nil {
		t.Fatal(err)
	}
}
