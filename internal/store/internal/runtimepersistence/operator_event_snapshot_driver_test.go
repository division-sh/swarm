package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

// This test-only adapter delegates real SQL and gates a drained component read.
// The writer uses a separate, unwrapped pool; no production hook is installed.
type operatorSnapshotProbe struct {
	mu             sync.Mutex
	armed          bool
	gateKind       string
	gateOccurrence int
	seen           map[string]int
	entered        chan struct{}
	release        chan struct{}
	fired          bool
	readFailure    error
	commitFailure  error
	options        []driver.TxOptions
	outside        int
	writes         int
	finished       int
	active         int
	candidates     []string
}

func (p *operatorSnapshotProbe) arm(kind string, occurrence int, readFailure, commitFailure error) (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed, p.gateKind, p.gateOccurrence = true, kind, occurrence
	p.readFailure, p.commitFailure = readFailure, commitFailure
	p.seen, p.options, p.candidates = map[string]int{}, nil, nil
	p.outside, p.writes, p.finished, p.active, p.fired = 0, 0, 0, 0, false
	p.entered, p.release = make(chan struct{}), make(chan struct{})
	release := p.release
	var once sync.Once
	return p.entered, func() { once.Do(func() { close(release) }) }
}

func (p *operatorSnapshotProbe) disarm() {
	p.mu.Lock()
	p.armed = false
	p.mu.Unlock()
}

type operatorSnapshotConnector struct {
	driver.Connector
	probe *operatorSnapshotProbe
}

func (c operatorSnapshotConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &operatorSnapshotConn{diagnosticSQLConn: diagnosticSQLConn{Conn: conn}, probe: c.probe}, nil
}

type operatorSnapshotConn struct {
	diagnosticSQLConn
	probe *operatorSnapshotProbe
	inTx  atomic.Bool
}

func (c *operatorSnapshotConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.diagnosticSQLConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	c.inTx.Store(true)
	c.probe.mu.Lock()
	observed := c.probe.armed
	if observed {
		c.probe.options = append(c.probe.options, opts)
		c.probe.active++
	}
	c.probe.mu.Unlock()
	return &operatorSnapshotTx{Tx: tx, conn: c, observed: observed}, nil
}

type operatorSnapshotTx struct {
	driver.Tx
	conn     *operatorSnapshotConn
	observed bool
}

func (tx *operatorSnapshotTx) finish() {
	tx.conn.inTx.Store(false)
	if tx.observed {
		tx.conn.probe.mu.Lock()
		tx.conn.probe.active--
		tx.conn.probe.finished++
		tx.conn.probe.mu.Unlock()
	}
}

func (tx *operatorSnapshotTx) Commit() error {
	err := tx.Tx.Commit()
	tx.finish()
	tx.conn.probe.mu.Lock()
	injected := tx.conn.probe.commitFailure
	tx.conn.probe.mu.Unlock()
	if tx.observed {
		return errors.Join(err, injected)
	}
	return err
}

func (tx *operatorSnapshotTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.finish()
	return err
}

func operatorSnapshotQueryKind(query string) string {
	q := strings.Join(strings.Fields(strings.ToLower(query)), " ")
	switch {
	case strings.HasPrefix(q, "select e.event_id::text, e.created_at from events e"), strings.HasPrefix(q, "select e.event_id, e.created_at from events e"):
		return "candidate"
	case strings.HasPrefix(q, "select e.event_class,"):
		return "event"
	case strings.Contains(q, "from fan_out_outcomes o join fan_out_intents i"):
		return "inherited-owner"
	case strings.HasPrefix(q, "with recursive lineage(run_id) as ("):
		return "inherited-lineage"
	case strings.Contains(q, "from event_deliveries where event_id"):
		return "membership"
	case strings.Contains(q, "from event_deliveries d join events e"):
		return "delivery"
	case strings.Contains(q, "from dead_letters"):
		return "deadletter"
	default:
		return "other"
	}
}

func (c *operatorSnapshotConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	p, kind := c.probe, operatorSnapshotQueryKind(query)
	p.mu.Lock()
	observed := p.armed
	gate := false
	var readFailure error
	var entered, release chan struct{}
	if observed {
		p.seen[kind]++
		if !c.inTx.Load() {
			p.outside++
		}
		gate = !p.fired && kind == p.gateKind && p.seen[kind] == p.gateOccurrence
		if gate {
			p.fired = true
			readFailure, entered, release = p.readFailure, p.entered, p.release
		}
	}
	p.mu.Unlock()
	if gate && readFailure != nil {
		close(entered)
		return nil, readFailure
	}
	rows, err := c.diagnosticSQLConn.QueryContext(ctx, query, args)
	if err != nil {
		return rows, err
	}
	return &operatorSnapshotRows{Rows: rows, probe: p, ctx: ctx, candidate: observed && kind == "candidate", gate: gate, entered: entered, release: release}, nil
}

func (c *operatorSnapshotConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q := strings.ToLower(strings.TrimSpace(query))
	c.probe.mu.Lock()
	if c.probe.armed && (strings.HasPrefix(q, "insert") || strings.HasPrefix(q, "update") || strings.HasPrefix(q, "delete")) {
		c.probe.writes++
	}
	c.probe.mu.Unlock()
	return c.diagnosticSQLConn.ExecContext(ctx, query, args)
}

type operatorSnapshotRows struct {
	driver.Rows
	probe     *operatorSnapshotProbe
	ctx       context.Context
	candidate bool
	gate      bool
	entered   chan struct{}
	release   chan struct{}
}

func (r *operatorSnapshotRows) Next(values []driver.Value) error {
	err := r.Rows.Next(values)
	if err == nil && r.candidate {
		var id string
		switch v := values[0].(type) {
		case string:
			id = v
		case []byte:
			id = string(v)
		}
		r.probe.mu.Lock()
		r.probe.candidates = append(r.probe.candidates, id)
		r.probe.mu.Unlock()
	}
	return err
}

func (r *operatorSnapshotRows) Close() error {
	err := r.Rows.Close()
	if !r.gate || err != nil {
		return err
	}
	r.gate = false
	close(r.entered)
	select {
	case <-r.release:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

var _ driver.ConnBeginTx = (*operatorSnapshotConn)(nil)
var _ driver.QueryerContext = (*operatorSnapshotConn)(nil)
var _ driver.ExecerContext = (*operatorSnapshotConn)(nil)
