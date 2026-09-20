package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
	"testing"

	"github.com/lib/pq"
)

const boundedOrderLockRead = `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE`

// The real ordering row is locked before the server notice cancels the caller.
// Only this exact read is decorated; its native value and lock are retained.
const boundedOrderFaultRead = `WITH locked AS MATERIALIZED (
	SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE
)
SELECT bounded_writer_stop_read(last_sequence) FROM locked`

type boundedOrderReadProbe struct {
	graceful pipelineGracefulProbe
	armed    atomic.Bool
	queries  atomic.Int32
}

type boundedOrderReadConnector struct {
	pipelineGracefulConnector
	order *boundedOrderReadProbe
}

func (c boundedOrderReadConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.pipelineGracefulConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &boundedOrderReadConn{pipelineGracefulConn: conn.(*pipelineGracefulConn), order: c.order}, nil
}

type boundedOrderReadConn struct {
	*pipelineGracefulConn
	order *boundedOrderReadProbe
}

func (c *boundedOrderReadConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.order.armed.Load() && query == boundedOrderLockRead {
		c.order.queries.Add(1)
		query = boundedOrderFaultRead
	}
	return c.pipelineGracefulConn.QueryContext(ctx, query, args)
}

func openBoundedOrderReadDB(t *testing.T, dsn string) (*sql.DB, *boundedOrderReadProbe) {
	t.Helper()
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	probe := &boundedOrderReadProbe{}
	db := sql.OpenDB(boundedOrderReadConnector{
		pipelineGracefulConnector: pipelineGracefulConnector{Connector: connector, probe: &probe.graceful},
		order:                     probe,
	})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		probe.armed.Store(false)
		probe.graceful.set(nil)
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, probe
}
