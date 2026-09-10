package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

// Observe real driver transaction exits; barriers never fabricate query success.
type exitProbeConnector struct {
	driver                           driver.Driver
	dsn                              string
	rollbackEntered, rollbackRelease chan struct{}
	rollbackOnce                     sync.Once
	rollbackDone                     chan struct{}
	rollbackDoneOnce                 sync.Once
	closed                           atomic.Int32
	closedSignal                     chan struct{}
	closeOnce                        sync.Once
	commitFailure, rollbackFailure   error
	beginFailure                     error
	afterCommit                      func()
}

func (p *exitProbeConnector) Driver() driver.Driver { return p.driver }
func (p *exitProbeConnector) Connect(context.Context) (driver.Conn, error) {
	c, err := p.driver.Open(p.dsn)
	if err != nil {
		return nil, err
	}
	return &exitProbeConn{Conn: c, probe: p}, nil
}

type exitProbeConn struct {
	driver.Conn
	probe *exitProbeConnector
}

func (c *exitProbeConn) BindOperationScope(scope *pq.OperationScope) error {
	return pq.BindOperationScope(c.Conn, scope)
}

func (c *exitProbeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *exitProbeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *exitProbeConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *exitProbeConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *exitProbeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.probe.beginFailure != nil {
		return nil, c.probe.beginFailure
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &exitProbeTx{Tx: tx, probe: c.probe}, nil
}
func (c *exitProbeConn) Close() error {
	err := c.Conn.Close()
	c.probe.closed.Add(1)
	c.probe.closeOnce.Do(func() { close(c.probe.closedSignal) })
	return err
}

type exitProbeTx struct {
	driver.Tx
	probe *exitProbeConnector
}

func (t *exitProbeTx) Commit() error {
	err := t.Tx.Commit()
	if t.probe.afterCommit != nil {
		t.probe.afterCommit()
	}
	return errors.Join(err, t.probe.commitFailure)
}
func (t *exitProbeTx) Rollback() error {
	defer t.probe.rollbackDoneOnce.Do(func() { close(t.probe.rollbackDone) })
	if t.probe.rollbackEntered != nil {
		t.probe.rollbackOnce.Do(func() { close(t.probe.rollbackEntered); <-t.probe.rollbackRelease })
	}
	if t.probe.rollbackFailure != nil {
		return t.probe.rollbackFailure
	}
	return t.Tx.Rollback()
}

func newExitProbe(t *testing.T) (*Backend, *exitProbeConnector) {
	t.Helper()
	dsn, original, _ := testutil.StartPostgres(t)
	p := &exitProbeConnector{driver: original.Driver(), dsn: dsn, closedSignal: make(chan struct{}), rollbackDone: make(chan struct{})}
	db := sql.OpenDB(p)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	b, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	return b, p
}

func TestPostgresTransactionCancellationRollbackOrders(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			for _, finished := range []bool{false, true} {
				for _, callbackFailure := range []bool{false, true} {
					t.Run(fmt.Sprintf("read=%t/deadline=%t/finished=%t/callback_error=%t", read, deadline, finished, callbackFailure), func(t *testing.T) {
						b, p := newExitProbe(t)
						p.rollbackEntered, p.rollbackRelease = make(chan struct{}), make(chan struct{})
						var once sync.Once
						release := func() { once.Do(func() { close(p.rollbackRelease) }) }
						defer release()
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						if deadline {
							var stop context.CancelFunc
							ctx, stop = context.WithTimeout(ctx, 50*time.Millisecond)
							defer stop()
						}
						primary := errors.New("independent callback failure")
						callbackReturned := make(chan struct{})
						done := make(chan error, 1)
						go func() {
							run := b.RunTransaction
							if read {
								run = b.RunReadTransaction
							}
							done <- run(ctx, func(ctx context.Context, tx *sql.Tx) error {
								var n int
								if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
									return err
								}
								if !deadline {
									cancel()
								}
								<-ctx.Done()
								<-p.rollbackEntered
								if finished {
									release()
									<-p.rollbackDone
								}
								close(callbackReturned)
								if callbackFailure {
									return primary
								}
								return nil
							})
						}()
						select {
						case <-callbackReturned:
						case <-time.After(3 * time.Second):
							t.Fatal("callback barrier timed out")
						}
						release()
						var err error
						select {
						case err = <-done:
						case <-time.After(3 * time.Second):
							t.Fatal("transaction cleanup did not join")
						}
						if !errors.Is(err, ctx.Err()) {
							t.Fatalf("lost cancellation: %v", err)
						}
						if callbackFailure && !errors.Is(err, primary) {
							t.Fatalf("lost independent failure: %v", err)
						}
						if !callbackFailure && err != ctx.Err() {
							t.Fatalf("manufactured transaction failure: %v", err)
						}
						next, stop := context.WithTimeout(context.Background(), time.Second)
						defer stop()
						if err := b.db.PingContext(next); err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}

func TestPostgresTransactionPanicAndFailureExits(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, scenario := range []string{"success", "panic", "panic_rollback_failure", "callback", "callback_canceled", "callback_tx_done", "commit_canceled", "rollback_failure"} {
			t.Run(fmt.Sprintf("read=%t/%s", read, scenario), func(t *testing.T) {
				b, p := newExitProbe(t)
				if _, err := b.db.Exec("CREATE TABLE exit_probe (n integer NOT NULL); INSERT INTO exit_probe VALUES (0)"); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				primary, cleanup, commit := errors.New("primary"), errors.New("rollback failure"), errors.New("actual commit failure")
				if scenario == "rollback_failure" || scenario == "panic_rollback_failure" {
					p.rollbackFailure = cleanup
				}
				if scenario == "commit_canceled" {
					p.commitFailure = commit
					p.afterCommit = cancel
				}
				var got any
				var err error
				run := b.RunTransaction
				if read {
					run = b.RunReadTransaction
				}
				func() {
					defer func() { got = recover() }()
					err = run(ctx, func(ctx context.Context, tx *sql.Tx) error {
						var n int
						if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
							return err
						}
						if !read {
							if _, err := tx.ExecContext(ctx, "UPDATE exit_probe SET n=n+1"); err != nil {
								return err
							}
						}
						switch scenario {
						case "panic", "panic_rollback_failure":
							panic(primary)
						case "callback_canceled":
							cancel()
							return primary
						case "callback_tx_done":
							cancel()
							return sql.ErrTxDone
						case "callback", "rollback_failure":
							return primary
						}
						return nil
					})
				}()
				switch scenario {
				case "panic", "panic_rollback_failure":
					if got != primary {
						t.Fatalf("panic=%v", got)
					}
				case "callback", "callback_canceled":
					if !errors.Is(err, primary) {
						t.Fatal(err)
					}
				case "callback_tx_done":
					if !errors.Is(err, sql.ErrTxDone) {
						t.Fatal(err)
					}
				case "rollback_failure":
					if !errors.Is(err, primary) || !errors.Is(err, cleanup) {
						t.Fatal(err)
					}
				case "commit_canceled":
					if !errors.Is(err, commit) || !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "success":
					if err != nil {
						t.Fatal(err)
					}
				}
				next, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := b.db.PingContext(next); err != nil {
					t.Fatal(err)
				}
				var persisted int
				if err := b.db.QueryRowContext(next, "SELECT n FROM exit_probe").Scan(&persisted); err != nil {
					t.Fatal(err)
				}
				want := 0
				if !read && (scenario == "success" || scenario == "commit_canceled") {
					want = 1
				}
				if persisted != want {
					t.Fatalf("durable writes=%d want %d; callback must not replay or leak partial work", persisted, want)
				}
			})
		}
	}
}

func TestRetainedPostgresPanicPreservesPossessionAndNextOperation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a := newSessionAuthority(conn)
	defer a.release()
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock(2442)"); err != nil {
		t.Fatal(err)
	}
	primary := errors.New("retained callback panic")
	func() {
		defer func() {
			if recover() != primary {
				t.Error("panic changed")
			}
		}()
		_ = RunAuthorityTransaction(context.Background(), a, func(context.Context, *sql.Tx) error { panic(primary) })
	}()
	a.mu.Lock()
	active := a.activeTx != nil
	a.mu.Unlock()
	if active {
		t.Fatal("transaction ownership retained")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := RunAuthorityTransaction(ctx, a, func(ctx context.Context, tx *sql.Tx) error {
		var held bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND objid=2442)").Scan(&held); err != nil {
			return err
		}
		if !held {
			return errors.New("healthy retained possession was released")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedPostgresTransactionExitMatrix(t *testing.T) {
	for _, scenario := range []string{"success", "cancel", "deadline", "panic", "callback_canceled", "rollback_failure", "commit_failure", "commit_canceled", "externally_rolled_back"} {
		t.Run(scenario, func(t *testing.T) {
			b, p := newExitProbe(t)
			if _, err := b.db.Exec("CREATE TABLE retained_exit (n integer NOT NULL); INSERT INTO retained_exit VALUES (0)"); err != nil {
				t.Fatal(err)
			}
			conn, err := b.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			a := newSessionAuthority(conn)
			defer a.release()
			if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock(2442)"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 100*time.Millisecond)
				defer stop()
			}
			primary := errors.New("independent retained callback")
			cleanup := errors.New("independent retained cleanup")
			commit := errors.New("independent retained commit")
			if scenario == "rollback_failure" {
				p.rollbackFailure = cleanup
			}
			if scenario == "commit_failure" || scenario == "commit_canceled" {
				p.commitFailure = commit
			}
			if scenario == "commit_canceled" {
				p.afterCommit = cancel
			}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err = RunAuthorityTransaction(ctx, a, func(queryCtx context.Context, tx *sql.Tx) error {
					if _, err := tx.ExecContext(queryCtx, "UPDATE retained_exit SET n=n+1"); err != nil {
						return err
					}
					switch scenario {
					case "cancel", "callback_canceled", "panic":
						cancel()
					case "externally_rolled_back":
						cancel()
						if err := tx.Rollback(); err != nil {
							return err
						}
					case "deadline":
						<-ctx.Done()
					}
					if scenario == "panic" {
						panic(primary)
					}
					if scenario == "callback_canceled" || scenario == "rollback_failure" {
						return primary
					}
					return nil
				})
			}()
			if scenario == "panic" && recovered != primary {
				t.Fatalf("panic=%v", recovered)
			}
			if (scenario == "callback_canceled" || scenario == "rollback_failure") && !errors.Is(err, primary) {
				t.Fatalf("lost callback: %v", err)
			}
			if scenario == "rollback_failure" && !errors.Is(err, cleanup) {
				t.Fatalf("lost cleanup: %v", err)
			}
			if (scenario == "commit_failure" || scenario == "commit_canceled") && !errors.Is(err, commit) {
				t.Fatalf("lost commit: %v", err)
			}
			if scenario != "panic" && ctx.Err() != nil && !errors.Is(err, ctx.Err()) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if scenario == "success" && err != nil {
				t.Fatal(err)
			}
			a.mu.Lock()
			active, closed := a.activeTx != nil, a.closed
			a.mu.Unlock()
			if active {
				t.Fatal("active transaction leaked")
			}
			unsafe := scenario == "rollback_failure" || scenario == "commit_failure" || scenario == "commit_canceled" || scenario == "externally_rolled_back"
			if closed != unsafe {
				t.Fatalf("session closed=%t unsafe=%t", closed, unsafe)
			}
			next, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if !unsafe {
				if err := RunAuthorityTransaction(next, a, func(ctx context.Context, tx *sql.Tx) error {
					var held bool
					if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND objid=2442)").Scan(&held); err != nil {
						return err
					}
					if !held {
						return errors.New("healthy advisory possession lost")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.ExecContext(next, "SELECT pg_advisory_unlock(2442)"); err != nil {
					t.Fatal(err)
				}
				if err := a.release(); err != nil {
					t.Fatal(err)
				}
			} else if err := RunAuthorityTransaction(next, a, func(context.Context, *sql.Tx) error { t.Error("unsafe session reused"); return nil }); err == nil {
				t.Fatal("unsafe session admitted")
			}
			var n int
			if err := b.db.QueryRowContext(next, "SELECT n FROM retained_exit").Scan(&n); err != nil {
				t.Fatal(err)
			}
			want := 0
			if scenario == "success" || scenario == "commit_failure" || scenario == "commit_canceled" {
				want = 1
			}
			if n != want {
				t.Fatalf("durable writes=%d want %d", n, want)
			}
		})
	}
}

func TestPostgresTransactionAdmissionExits(t *testing.T) {
	for _, retained := range []bool{false, true} {
		for _, scenario := range []string{"empty", "canceled", "begin_failure", "closed"} {
			t.Run(fmt.Sprintf("retained=%t/%s", retained, scenario), func(t *testing.T) {
				b, p := newExitProbe(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				primary := errors.New("begin failure")
				var a *SessionAuthority
				run := b.RunTransaction
				if retained {
					conn, err := b.db.Conn(ctx)
					if err != nil {
						t.Fatal(err)
					}
					a = newSessionAuthority(conn)
					defer a.release()
					run = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
						return RunAuthorityTransaction(ctx, a, fn)
					}
				}
				if scenario == "canceled" {
					cancel()
				}
				if scenario == "begin_failure" {
					p.beginFailure = primary
				}
				if scenario == "closed" {
					if retained {
						if err := a.release(); err != nil {
							t.Fatal(err)
						}
					} else if err := b.db.Close(); err != nil {
						t.Fatal(err)
					}
				}
				var callback func(context.Context, *sql.Tx) error
				if scenario != "empty" {
					callback = func(context.Context, *sql.Tx) error { t.Error("callback admitted"); return nil }
				}
				err := run(ctx, callback)
				switch scenario {
				case "empty":
					if err != nil {
						t.Fatal(err)
					}
				case "canceled":
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "begin_failure":
					if !errors.Is(err, primary) {
						t.Fatal(err)
					}
				case "closed":
					if err == nil {
						t.Fatal("closed owner admitted")
					}
				}
				if a != nil {
					a.mu.Lock()
					active := a.activeTx != nil
					a.mu.Unlock()
					if active {
						t.Fatal("begin exit leaked operation")
					}
					if err := a.release(); err != nil {
						t.Fatal(err)
					}
				}
				if scenario != "closed" {
					p.beginFailure = nil
					ctx, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					if err := b.db.PingContext(ctx); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
