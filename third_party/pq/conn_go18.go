package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lib/pq/internal/pqsql"
	"github.com/lib/pq/internal/proto"
)

const watchCancelDialContextTimeout = 10 * time.Second

// Implement the "QueryerContext" interface
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Rows, err error) {
	op, err := cn.startNative(ctx, "query")
	if err != nil {
		return nil, err
	}
	defer func() {
		if result == nil {
			err = op.finish(err)
		}
	}()
	r, err := cn.query(query, args)
	if err != nil {
		return nil, err
	}
	r.native = op
	return r, nil
}

// Implement the "ExecerContext" interface
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	list := make([]driver.Value, len(args))
	for i, nv := range args {
		list[i] = nv.Value
	}

	op, err := cn.startNative(ctx, "exec")
	if err != nil {
		return nil, err
	}
	defer func() { err = op.finish(err) }()

	return cn.exec(query, list)
}

// Implement the "ConnPrepareContext" interface
func (cn *conn) PrepareContext(ctx context.Context, query string) (result driver.Stmt, err error) {
	if pqsql.StartsWithCopy(query) {
		if s := cn.scopeFor(ctx); s != nil && !s.implicit {
			return nil, errors.New("pq: COPY is outside the owned native operation boundary")
		}
		// COPY has its own streaming producer. It cannot be borrowed by an
		// explicit operation scope; preserve the unbound upstream COPY path.
		return cn.prepare(query)
	}
	op, err := cn.startNative(ctx, "prepare")
	if err != nil {
		return nil, err
	}
	defer func() { err = op.finish(err) }()
	return cn.prepare(query)
}

// Implement the "ConnBeginTx" interface
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (result driver.Tx, err error) {
	var mode string
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
		// Don't touch mode: use the server's default
	case sql.LevelReadUncommitted:
		mode = " ISOLATION LEVEL READ UNCOMMITTED"
	case sql.LevelReadCommitted:
		mode = " ISOLATION LEVEL READ COMMITTED"
	case sql.LevelRepeatableRead:
		mode = " ISOLATION LEVEL REPEATABLE READ"
	case sql.LevelSerializable:
		mode = " ISOLATION LEVEL SERIALIZABLE"
	default:
		return nil, fmt.Errorf("pq: isolation level not supported: %d", opts.Isolation)
	}
	if opts.ReadOnly {
		mode += " READ ONLY"
	} else {
		mode += " READ WRITE"
	}

	op, err := cn.startNative(ctx, "begin")
	if err != nil {
		return nil, err
	}
	defer func() {
		if cn.native == op {
			err = op.finish(err)
		}
	}()
	tx, err := cn.begin(mode)
	if err == nil {
		cn.txnScope = op.scope
		cn.txnScope.work.Add(1)
	}
	err = op.finish(err)
	if err != nil {
		if tx != nil {
			err = errors.Join(err, cn.Rollback())
		}
		return nil, err
	}
	return tx, nil
}

func (cn *conn) Ping(ctx context.Context) (err error) {
	op, err := cn.startNative(ctx, "ping")
	if err != nil {
		return err
	}
	defer func() { err = op.finish(err) }()
	rows, err := cn.simpleQuery(";")
	if err != nil {
		return err
	}
	return rows.Close()
}

func (cn *conn) cancel(ctx context.Context) (err error) {
	// Use a copy since a new connection is created here. This is necessary
	// because cancel is called by the native operation's joined worker.
	cfg := cn.cfg.Clone()

	c, err := dial(ctx, cn.dialer, cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.Close()) }()
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.SetDeadline(deadline); err != nil {
			return err
		}
	}

	cn2 := conn{c: c}
	err = cn2.ssl(cfg)
	if err != nil {
		return err
	}

	w := cn2.writeBuf(0)
	w.int32(proto.CancelRequestCode)
	w.int32(cn.processID)
	w.int32(cn.secretKey)
	if err := cn2.sendStartupPacket(w); err != nil {
		return err
	}

	// Read until EOF to ensure that the server received the cancel.
	_, err = io.Copy(io.Discard, c)
	return err
}

// Implement the "StmtQueryContext" interface
func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (result driver.Rows, err error) {
	op, err := st.cn.startNative(ctx, "prepared query")
	if err != nil {
		return nil, err
	}
	defer func() {
		if result == nil {
			err = op.finish(err)
		}
	}()
	r, err := st.query(args)
	if err != nil {
		return nil, err
	}
	r.native = op
	return r, nil
}

// Implement the "StmtExecContext" interface
func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (result driver.Result, err error) {
	op, err := st.cn.startNative(ctx, "prepared exec")
	if err != nil {
		return nil, err
	}
	defer func() { err = op.finish(err) }()
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}

	err = st.exec(args)
	if err != nil {
		return nil, st.cn.handleError(err)
	}
	res, _, err := st.cn.readExecuteResponse("simple query")
	return res, st.cn.handleError(err)
}
