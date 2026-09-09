package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	modernc "modernc.org/sqlite"
)

var errInjectedCleanup = errors.New("injected rollback failure")
var errInjectedCommit = errors.New("injected uncertain commit: database is locked")

// Wrap a real SQLite driver only at the transaction exit being tested. All
// statements and independent readback use SQLite; no production fault hook.
type exitFaultConnector struct {
	path             string
	rollbackFail     atomic.Bool
	commitAfterError atomic.Bool
	opens, closes    atomic.Int32
	beforeBegin      context.CancelFunc
	afterCommit      context.CancelFunc
}

func (c *exitFaultConnector) Driver() driver.Driver { return &modernc.Driver{} }
func (c *exitFaultConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.path)
	if err != nil {
		return nil, err
	}
	c.opens.Add(1)
	return &exitFaultConn{Conn: conn, owner: c}, nil
}

type exitFaultConn struct {
	driver.Conn
	owner *exitFaultConnector
}

func (c *exitFaultConn) Close() error { c.owner.closes.Add(1); return c.Conn.Close() }
func (c *exitFaultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.owner.beforeBegin != nil {
		c.owner.beforeBegin()
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &exitFaultTx{Tx: tx, owner: c.owner}, nil
}
func (c *exitFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *exitFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type exitFaultTx struct {
	driver.Tx
	owner *exitFaultConnector
}

func (t *exitFaultTx) Rollback() error {
	if t.owner.rollbackFail.Swap(false) {
		return errInjectedCleanup
	}
	return t.Tx.Rollback()
}
func (t *exitFaultTx) Commit() error {
	err := t.Tx.Commit()
	if t.owner.afterCommit != nil {
		t.owner.afterCommit()
	}
	if err == nil && t.owner.commitAfterError.Swap(false) {
		return errInjectedCommit
	}
	return err
}

func TestTransactionExitFaultDisposition(t *testing.T) {
	for _, poolSize := range []int{1, 3} {
		for _, scenario := range []string{"success", "callback", "rollback_failure", "panic_rollback_failure", "committed_error", "cancel_committed_error", "cancel_callback", "cancel_before_acquire"} {
			t.Run(scenario+string(rune('0'+poolSize)), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "fault.db")
				control := &exitFaultConnector{path: "file:" + path}
				db := sql.OpenDB(control)
				db.SetMaxOpenConns(poolSize)
				defer db.Close()
				if _, err := db.Exec("CREATE TABLE counter (value INTEGER); INSERT INTO counter VALUES (0)"); err != nil {
					t.Fatal(err)
				}
				b, err := New(db)
				if err != nil {
					t.Fatal(err)
				}
				var survivor *sql.Conn
				if poolSize > 1 {
					survivor, err = db.Conn(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer survivor.Close()
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				primary := errors.New("primary callback failure: database is locked")
				if scenario == "rollback_failure" || scenario == "panic_rollback_failure" {
					control.rollbackFail.Store(true)
				}
				if scenario == "committed_error" || scenario == "cancel_committed_error" {
					control.commitAfterError.Store(true)
				}
				if scenario == "cancel_committed_error" {
					control.afterCommit = cancel
				}
				if scenario == "cancel_before_acquire" {
					cancel()
				}
				attempts := 0
				func() {
					if scenario == "panic_rollback_failure" {
						defer func() {
							if recover() != primary {
								t.Error("original panic changed")
							}
						}()
					}
					err = b.RunTransaction(ctx, "exit fault", func(ctx context.Context, tx *sql.Tx) error {
						attempts++
						if _, err := tx.ExecContext(ctx, "UPDATE counter SET value=value+1"); err != nil {
							return err
						}
						switch scenario {
						case "callback":
							return errors.New("callback error")
						case "rollback_failure":
							return primary
						case "panic_rollback_failure":
							panic(primary)
						case "cancel_callback":
							cancel()
						}
						return nil
					})
				}()
				wantAttempts := 1
				if scenario == "cancel_before_acquire" {
					wantAttempts = 0
				}
				if attempts != wantAttempts {
					t.Fatalf("callback replayed or never entered: %d", attempts)
				}
				if scenario == "rollback_failure" && (!errors.Is(err, primary) || !errors.Is(err, errInjectedCleanup)) {
					t.Fatalf("lost primary/cleanup errors: %v", err)
				}
				if (scenario == "committed_error" || scenario == "cancel_committed_error") && !errors.Is(err, errInjectedCommit) {
					t.Fatalf("lost uncertain commit: %v", err)
				}
				if (scenario == "cancel_callback" || scenario == "cancel_before_acquire" || scenario == "cancel_committed_error") && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
				if scenario == "success" && err != nil {
					t.Fatal(err)
				}
				want := 0
				if scenario == "success" || scenario == "committed_error" || scenario == "cancel_committed_error" {
					want = 1
				}
				other, err := sql.Open("sqlite", "file:"+path)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				for _, reader := range []*sql.DB{db, other} {
					var value int
					if err := reader.QueryRow("SELECT value FROM counter").Scan(&value); err != nil || value != want {
						t.Fatalf("readback=%d want=%d error=%v", value, want, err)
					}
				}
				if survivor != nil {
					var value int
					if err := survivor.QueryRowContext(context.Background(), "SELECT value FROM counter").Scan(&value); err != nil || value != want {
						t.Fatalf("unrelated pinned connection damaged: %d %v", value, err)
					}
				}
				if scenario == "rollback_failure" || scenario == "panic_rollback_failure" || scenario == "committed_error" {
					if control.closes.Load() != 1 {
						t.Fatalf("uncertain exact connection not discarded: %d", control.closes.Load())
					}
				} else if scenario == "success" || scenario == "callback" {
					if control.closes.Load() != 0 {
						t.Fatalf("healthy connection unnecessarily discarded: %d", control.closes.Load())
					}
				}
				if err := b.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
					var value int
					return tx.QueryRowContext(ctx, "SELECT value FROM counter").Scan(&value)
				}); err != nil {
					t.Fatal(err)
				}
				if err := b.RunTransaction(context.Background(), "next write", func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "UPDATE counter SET value=value+1")
					return err
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestTransactionCancellationCuts(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, cut := range []string{"pool_wait", "before_begin", "callback_error", "before_commit"} {
			t.Run(fmt.Sprintf("read=%t/%s", read, cut), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				control := &exitFaultConnector{path: "file:" + filepath.Join(t.TempDir(), "cancel.db")}
				db := sql.OpenDB(control)
				db.SetMaxOpenConns(1)
				defer db.Close()
				b, err := New(db)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)"); err != nil {
					t.Fatal(err)
				}
				if cut == "before_begin" {
					control.beforeBegin = cancel
				}
				var held *sql.Conn
				if cut == "pool_wait" {
					held, err = db.Conn(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer held.Close()
				}
				primary := errors.New("callback failed during cancellation")
				entered := false
				callback := func(ctx context.Context, tx *sql.Tx) error {
					entered = true
					if _, err := tx.ExecContext(ctx, "UPDATE counter SET value=1"); err != nil {
						return err
					}
					cancel()
					if cut == "callback_error" {
						return primary
					}
					return nil
				}
				done := make(chan error, 1)
				go func() {
					if read {
						done <- b.RunReadTransaction(ctx, callback)
					} else {
						done <- b.RunTransaction(ctx, "cancel cuts", callback)
					}
				}()
				if held != nil {
					deadline := time.Now().Add(time.Second)
					for db.Stats().WaitCount == 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if db.Stats().WaitCount == 0 {
						t.Fatal("runner never attempted pool acquisition")
					}
					cancel()
				}
				select {
				case err = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("cancellation leaked transaction/connection")
				}
				if cut == "callback_error" {
					if !errors.Is(err, primary) {
						t.Fatalf("lost callback error: %v", err)
					}
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
				if entered != (cut == "callback_error" || cut == "before_commit") {
					t.Fatalf("unexpected callback entry at %s", cut)
				}
				if held != nil {
					if err := held.Close(); err != nil {
						t.Fatal(err)
					}
				}
				control.beforeBegin = nil
				progressCtx, progressCancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer progressCancel()
				if err := b.RunTransaction(progressCtx, "next after cancellation", func(ctx context.Context, tx *sql.Tx) error {
					var value int
					if err := tx.QueryRowContext(ctx, "SELECT value FROM counter").Scan(&value); err != nil {
						return err
					}
					if value != 0 {
						return fmt.Errorf("cancelled write survived: %d", value)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				// Pool accounting may lag an already-retired driver connection;
				// single-connection progress above joins that disposal boundary.
				if db.Stats().InUse != 0 {
					t.Fatalf("leaked connection after subsequent progress: %+v", db.Stats())
				}
			})
		}
	}
}
