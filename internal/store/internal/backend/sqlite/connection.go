package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"

	modernc "modernc.org/sqlite"
)

// OpenDatabase owns the rollback hook on its private physical connections.
// NewConnector retains the registered driver's functions, hooks and DSN policy.
func OpenDatabase(dsn string) (*sql.DB, error) {
	connector, err := modernc.NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(rollbackConnector{Connector: connector}), nil
}

type nativeRollbackConnection interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
	driver.Validator
	modernc.HookRegisterer
}

type rollbackConnector struct {
	driver.Connector
}

func (c rollbackConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	native, ok := conn.(nativeRollbackConnection)
	if !ok {
		return nil, errors.Join(fmt.Errorf("sqlite native connection lacks owned rollback interfaces"), conn.Close())
	}
	owned := &rollbackConnection{native: native}
	native.RegisterRollbackHook(owned.observeRollback)
	return owned, nil
}

type rollbackEpoch struct {
	owner           *rollbackConnection
	aborted         atomic.Bool
	commitAttempted atomic.Bool
	retired         atomic.Bool
}

type rollbackConnection struct {
	native nativeRollbackConnection
	active atomic.Pointer[rollbackEpoch]
}

// The native hook cannot reenter SQLite. A pointer identifies this one begun
// transaction; bootstrap/autocommit and failed BEGIN have no active epoch.
func (c *rollbackConnection) observeRollback() {
	if epoch := c.active.Load(); epoch != nil && !epoch.retired.Load() {
		epoch.aborted.Store(true)
	}
}

func (c *rollbackConnection) retireActive() {
	if epoch := c.active.Swap(nil); epoch != nil {
		epoch.retired.Store(true)
	}
}

// A later operation must not borrow a prior automatic abort, even if a caller
// ignores the interrupted statement and starts more work on the same sql.Tx.
func (c *rollbackConnection) beforeNativeOperation() {
	if epoch := c.active.Load(); epoch != nil && epoch.aborted.Load() {
		epoch.retired.Store(true)
	}
}

func (c *rollbackConnection) Close() error {
	c.retireActive()
	return c.native.Close()
}

func (c *rollbackConnection) Prepare(query string) (driver.Stmt, error) {
	c.beforeNativeOperation()
	stmt, err := c.native.Prepare(query)
	return c.wrapStatement(stmt, err)
}

func (c *rollbackConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.beforeNativeOperation()
	stmt, err := c.native.PrepareContext(ctx, query)
	return c.wrapStatement(stmt, err)
}

func (c *rollbackConnection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *rollbackConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if c.active.Load() != nil {
		return nil, errors.New("sqlite connection already owns a native transaction")
	}
	tx, err := c.native.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	epoch := &rollbackEpoch{owner: c}
	c.active.Store(epoch)
	return &rollbackTransaction{native: tx, owner: c, epoch: epoch}, nil
}

func (c *rollbackConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.beforeNativeOperation()
	return c.native.ExecContext(ctx, query, args)
}

func (c *rollbackConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.beforeNativeOperation()
	return c.native.QueryContext(ctx, query, args)
}

func (c *rollbackConnection) Ping(ctx context.Context) error {
	c.beforeNativeOperation()
	return c.native.Ping(ctx)
}

func (c *rollbackConnection) ResetSession(ctx context.Context) error {
	c.retireActive()
	return c.native.ResetSession(ctx)
}

func (c *rollbackConnection) IsValid() bool {
	return c.native.IsValid()
}

type rollbackTransaction struct {
	native   driver.Tx
	owner    *rollbackConnection
	epoch    *rollbackEpoch
	finished atomic.Bool
}

func (t *rollbackTransaction) retire() {
	if t.epoch != nil {
		t.epoch.retired.Store(true)
		if t.owner != nil {
			t.owner.active.CompareAndSwap(t.epoch, nil)
		}
	}
}

func (t *rollbackTransaction) Commit() error {
	if !t.finished.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}
	if t.epoch != nil {
		t.epoch.commitAttempted.Store(true)
	}
	defer t.retire()
	return t.native.Commit()
}

func (t *rollbackTransaction) Rollback() error {
	if !t.finished.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}
	defer t.retire()
	// database/sql has serialized this finalizer after the native operation.
	// Its real Tx still retires normally; only a witnessed physical abort is
	// already complete. Missing/crossed evidence retains the native error path.
	if t.epoch != nil && t.owner != nil && t.epoch.owner == t.owner && t.owner.active.Load() == t.epoch &&
		!t.epoch.retired.Load() && !t.epoch.commitAttempted.Load() && t.epoch.aborted.Load() {
		return nil
	}
	return t.native.Rollback()
}
