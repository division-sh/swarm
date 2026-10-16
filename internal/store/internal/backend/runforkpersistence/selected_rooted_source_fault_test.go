package runforkpersistence

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Only this fixture's physical read-COMMIT result is changed. Native SQL and
// transaction finalizers still run; no production fault hook is installed.
type selectedRootReadConnector struct {
	driver      driver.Driver
	dsn         string
	afterCommit func() error
}

func (c *selectedRootReadConnector) Driver() driver.Driver { return c.driver }
func (c *selectedRootReadConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &selectedRootReadConn{Conn: conn, owner: c}, nil
}

type selectedRootReadConn struct {
	driver.Conn
	owner *selectedRootReadConnector
}

func (c *selectedRootReadConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &selectedRootReadTx{Tx: tx, owner: c.owner}, nil
}

func (c *selectedRootReadConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *selectedRootReadConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

type selectedRootReadTx struct {
	driver.Tx
	owner *selectedRootReadConnector
}

func (tx *selectedRootReadTx) Commit() error {
	err := tx.Tx.Commit()
	if tx.owner.afterCommit != nil {
		err = errors.Join(err, tx.owner.afterCommit())
	}
	return err
}
