package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Observe the real rollback and hold only the return from physical Close. This
// separates database/sql's retired-connection bookkeeping from usable state.
type cancellationBarrierConnector struct {
	base                                       *exitFaultConnector
	rollbackStarted, allowRollback, rolledBack chan struct{}
	closed, allowClose                         chan struct{}
	rollbackOnce, closeOnce                    sync.Once
}

func (c *cancellationBarrierConnector) Driver() driver.Driver { return c.base.Driver() }
func (c *cancellationBarrierConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &cancellationBarrierConn{Conn: conn, owner: c}, nil
}

type cancellationBarrierConn struct {
	driver.Conn
	owner *cancellationBarrierConnector
}

func (c *cancellationBarrierConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &cancellationBarrierTx{Tx: tx, owner: c.owner}, nil
}

func (c *cancellationBarrierConn) Close() error {
	err := c.Conn.Close()
	c.owner.closeOnce.Do(func() {
		close(c.owner.closed)
		<-c.owner.allowClose
	})
	return err
}

type cancellationBarrierTx struct {
	driver.Tx
	owner *cancellationBarrierConnector
}

func (t *cancellationBarrierTx) Rollback() error {
	t.owner.rollbackOnce.Do(func() {
		close(t.owner.rollbackStarted)
		<-t.owner.allowRollback
	})
	err := t.Tx.Rollback()
	select {
	case <-t.owner.rolledBack:
	default:
		close(t.owner.rolledBack)
	}
	return err
}

func TestTransactionCancellationRollbackOrders(t *testing.T) {
	t.Run("native_rollback_witness", proveNativeRollbackWitness)
	for _, read := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			for _, rollbackFirst := range []bool{false, true} {
				for _, callbackError := range []bool{false, true} {
					t.Run(fmt.Sprintf("read=%t/deadline=%t/rollback_first=%t/callback_error=%t", read, deadline, rollbackFirst, callbackError), func(t *testing.T) {
						control := &cancellationBarrierConnector{
							base:            &exitFaultConnector{path: "file:" + filepath.Join(t.TempDir(), "cancel.db")},
							rollbackStarted: make(chan struct{}), allowRollback: make(chan struct{}), rolledBack: make(chan struct{}),
							closed: make(chan struct{}), allowClose: make(chan struct{}),
						}
						var releaseRollback, releaseClose sync.Once
						unblockRollback := func() { releaseRollback.Do(func() { close(control.allowRollback) }) }
						unblockClose := func() { releaseClose.Do(func() { close(control.allowClose) }) }
						db := sql.OpenDB(control)
						db.SetMaxOpenConns(1)
						defer db.Close()
						defer unblockClose()
						defer unblockRollback()
						b := originalTransactionTestBackend(t, db, control.base.path)
						var err error
						if _, err := db.Exec("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)"); err != nil {
							t.Fatal(err)
						}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						want := context.Canceled
						if deadline {
							var deadlineCancel context.CancelFunc
							ctx, deadlineCancel = context.WithTimeout(ctx, 50*time.Millisecond)
							defer deadlineCancel()
							want = context.DeadlineExceeded
						}
						primary := errors.New("callback failed during cancellation")
						callbackReturning := make(chan struct{})
						done := make(chan error, 1)
						go func() {
							callback := func(ctx context.Context, tx *sql.Tx) error {
								var value int
								if err := tx.QueryRowContext(ctx, "SELECT value FROM counter").Scan(&value); err != nil {
									return err
								}
								if !read {
									if _, err := tx.ExecContext(ctx, "UPDATE counter SET value=17"); err != nil {
										return err
									}
								}
								if !deadline {
									cancel()
								}
								<-ctx.Done()
								<-control.rollbackStarted
								if rollbackFirst {
									unblockRollback()
									if read {
										<-control.closed
									} else {
										<-control.rolledBack
									}
								}
								close(callbackReturning)
								if callbackError {
									return primary
								}
								return nil
							}
							if read {
								done <- b.RunReadTransaction(ctx, callback)
							} else {
								done <- b.RunTransaction(ctx, "cancel barrier", callback)
							}
						}()
						select {
						case <-callbackReturning:
						case <-time.After(3 * time.Second):
							t.Fatal("callback barrier timed out")
						}
						if !rollbackFirst {
							unblockRollback()
							unblockClose()
						}
						select {
						case err = <-done:
						case <-time.After(3 * time.Second):
							t.Fatal("cancellation owner did not return")
						}
						if !errors.Is(err, want) {
							t.Fatalf("lost caller cancellation: %v, want %v", err, want)
						}
						if callbackError && !errors.Is(err, primary) {
							t.Fatalf("lost callback error: %v", err)
						}
						if !callbackError && errors.Is(err, sql.ErrTxDone) {
							t.Fatalf("automatic rollback manufactured a commit failure: %v", err)
						}
						if before, clean := CleanCancellationBeforeCommit(err); clean != (!read && !deadline && !callbackError) || (clean && before) {
							t.Fatalf("native rollback cancellation evidence: before=%t clean=%t read=%t deadline=%t callback_error=%t err=%v", before, clean, read, deadline, callbackError, err)
						}
						if rollbackFirst && read {
							// The driver is physically closed, although database/sql has not
							// returned from Close to decrement its pool accounting yet.
							if control.base.closes.Load() != 1 || db.Stats().InUse != 1 {
								t.Fatalf("expected retired connection before pool bookkeeping: closes=%d stats=%+v", control.base.closes.Load(), db.Stats())
							}
						}
						unblockClose()
						progressCtx, progressCancel := context.WithTimeout(context.Background(), 3*time.Second)
						defer progressCancel()
						if err := b.RunReadTransaction(progressCtx, func(ctx context.Context, tx *sql.Tx) error {
							var value int
							if err := tx.QueryRowContext(ctx, "SELECT value FROM counter").Scan(&value); err != nil {
								return err
							}
							if value != 0 {
								return fmt.Errorf("cancelled transaction leaked its write: %d", value)
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
						if err := b.RunTransaction(progressCtx, "next write", func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, "UPDATE counter SET value=1")
							return err
						}); err != nil {
							t.Fatal(err)
						}
						wantOpens, wantCloses := int32(2), int32(1)
						if !read {
							// The write owner positively joined rollback and can reuse the
							// healthy connection; reads retain their automatic disposal arm.
							wantOpens, wantCloses = 1, 0
						}
						if control.base.opens.Load() != wantOpens || control.base.closes.Load() != wantCloses || db.Stats().InUse != 0 {
							t.Fatalf("retirement/replacement mismatch: opens=%d closes=%d stats=%+v", control.base.opens.Load(), control.base.closes.Load(), db.Stats())
						}
					})
				}
			}
		}
	}
}

func TestTransactionDoesNotSuppressCallbackTxDone(t *testing.T) {
	t.Run("foreign native cancellation receipt", func(t *testing.T) {
		b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "foreign-cancellation.db"))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, foreign := b.RunTransactionOutcome(ctx, "cancel before native attempt", func(context.Context, *sql.Tx) error {
			t.Fatal("cancelled callback entered")
			return nil
		})
		if before, clean := CleanCancellationBeforeCommit(foreign); !before || !clean {
			t.Fatalf("missing native before-entry control: %v", foreign)
		}
		for _, cancelOuter := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			_, err := b.RunTransactionOutcome(ctx, "foreign callback receipt", func(context.Context, *sql.Tx) error {
				if cancelOuter {
					cancel()
				}
				return foreign
			})
			cancel()
			if _, clean := CleanCancellationBeforeCommit(err); clean || !errors.Is(err, context.Canceled) {
				t.Fatalf("callback supplied another transaction's cancellation evidence: %v", err)
			}
		}
	})
	for _, read := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("read=%t/canceled=%t", read, canceled), func(t *testing.T) {
				b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "callback.db"))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				callback := func(context.Context, *sql.Tx) error {
					if canceled {
						cancel()
					}
					return fmt.Errorf("independent callback transaction: %w", sql.ErrTxDone)
				}
				var err error
				if read {
					err = b.RunReadTransaction(ctx, callback)
				} else {
					err = b.RunTransaction(ctx, "callback result", callback)
				}
				if !errors.Is(err, sql.ErrTxDone) || (canceled && !errors.Is(err, context.Canceled)) {
					t.Fatalf("independent callback result lost: %v", err)
				}
			})
		}
	}
}
