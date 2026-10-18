package storetest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/lib/pq"
)

// Disposable test-fixture composition for reviewer-g 6253304321. The supplied
// location is the fixture's original location, never recovered from an owner.
// No pool, transaction, SQL callback or executable authority leaves this helper.
func StartChannelCycleDiagnostic(t *testing.T, location string) (*store.PostgresStore, *ChannelCycleDiagnostic) {
	t.Helper()
	native, err := pq.NewConnector(location)
	if err != nil {
		t.Fatal("diagnostic native connector construction failed")
	}
	p := &ChannelCycleDiagnostic{native: native, binding: make(chan struct{}), principal: make(chan struct{}),
		release: make(chan struct{}), milestones: make(map[string]bool)}
	p.db = sql.OpenDB(p)
	selected := runtimepersistence.NewPostgresStoreForTest(p.db)
	t.Cleanup(func() {
		p.Release()
		if err := selected.Close(); err != nil {
			t.Error("diagnostic selected owner close failed")
		}
	})
	BootstrapPostgresRuntimeStore(t, selected)
	bindTestPayloadAdmitter(selected)
	return selected, p
}

type channelCycleRole struct{}

type ChannelCycleDiagnostic struct {
	native     driver.Connector
	db         *sql.DB
	binding    chan struct{}
	principal  chan struct{}
	release    chan struct{}
	bindOnce   sync.Once
	princOnce  sync.Once
	freeOnce   sync.Once
	mu         sync.Mutex
	milestones map[string]bool
}

func (p *ChannelCycleDiagnostic) RebindContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelCycleRole{}, "rebind")
}
func (p *ChannelCycleDiagnostic) StandingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelCycleRole{}, "standing")
}
func (p *ChannelCycleDiagnostic) BindingAcquired() <-chan struct{}   { return p.binding }
func (p *ChannelCycleDiagnostic) PrincipalAcquired() <-chan struct{} { return p.principal }
func (p *ChannelCycleDiagnostic) Release()                           { p.freeOnce.Do(func() { close(p.release) }) }
func (p *ChannelCycleDiagnostic) mark(label string) {
	p.mu.Lock()
	p.milestones[label] = true
	p.mu.Unlock()
}
func (p *ChannelCycleDiagnostic) Milestones() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	copy := make(map[string]bool, len(p.milestones))
	for k, v := range p.milestones {
		copy[k] = v
	}
	return copy
}
func (p *ChannelCycleDiagnostic) Driver() driver.Driver { return p.native.Driver() }
func (p *ChannelCycleDiagnostic) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := p.native.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &channelCycleConn{Conn: c, probe: p}, nil
}

type channelCycleConn struct {
	driver.Conn
	probe *ChannelCycleDiagnostic
}

func (c *channelCycleConn) before(ctx context.Context, query string) {
	role, _ := ctx.Value(channelCycleRole{}).(string)
	if role == "rebind" && strings.HasPrefix(query, "INSERT INTO operator_channel_operations ") {
		c.probe.mark("rebind_insert_operation_started")
	}
	if role == "standing" && strings.Contains(query, "FROM operator_channel_bindings") && strings.HasSuffix(query, " FOR UPDATE") {
		c.probe.mark("standing_binding_query_started")
	}
}
func (c *channelCycleConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.before(ctx, query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *channelCycleConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.before(ctx, query)
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return rows, err
	}
	role, _ := ctx.Value(channelCycleRole{}).(string)
	label := ""
	if role == "rebind" && strings.Contains(query, "FROM operator_channel_bindings") && strings.HasSuffix(query, " FOR UPDATE") {
		label = "rebind_binding_acquired"
	}
	if role == "standing" && query == "SELECT principal_id FROM operator_principals WHERE singleton_id = 1 FOR UPDATE" {
		label = "standing_principal_acquired"
	}
	return &channelCycleRows{Rows: rows, ctx: ctx, probe: c.probe, label: label}, nil
}
func (c *channelCycleConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c *channelCycleConn) Ping(ctx context.Context) error {
	if native, ok := c.Conn.(driver.Pinger); ok {
		return native.Ping(ctx)
	}
	return nil
}
func (c *channelCycleConn) ResetSession(ctx context.Context) error {
	if native, ok := c.Conn.(driver.SessionResetter); ok {
		return native.ResetSession(ctx)
	}
	return nil
}
func (c *channelCycleConn) IsValid() bool {
	if native, ok := c.Conn.(driver.Validator); ok {
		return native.IsValid()
	}
	return true
}

type channelCycleRows struct {
	driver.Rows
	ctx      context.Context
	probe    *ChannelCycleDiagnostic
	label    string
	observed bool
}

func (r *channelCycleRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err != nil || r.observed || r.label == "" {
		return err
	}
	r.observed = true
	r.probe.mark(r.label)
	if r.label == "rebind_binding_acquired" {
		r.probe.bindOnce.Do(func() { close(r.probe.binding) })
	} else {
		r.probe.princOnce.Do(func() { close(r.probe.principal) })
	}
	select {
	case <-r.probe.release:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}
func (r *channelCycleRows) HasNextResultSet() bool {
	if native, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return native.HasNextResultSet()
	}
	return false
}
func (r *channelCycleRows) NextResultSet() error {
	if native, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return native.NextResultSet()
	}
	return io.EOF
}

func (p *ChannelCycleDiagnostic) PrincipalFKExists(ctx context.Context) (bool, error) {
	var exists bool
	err := p.db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1]
		JOIN pg_attribute b ON b.attrelid=c.confrelid AND b.attnum=c.confkey[1]
		WHERE c.contype='f' AND NOT c.condeferrable AND cardinality(c.conkey)=1
		AND c.conrelid='operator_channel_operations'::regclass AND c.confrelid='operator_principals'::regclass
		AND a.attname='principal_id' AND b.attname='principal_id')`).Scan(&exists)
	return exists, err
}

// Snapshots stay private to the fixture; only comparison booleans are returned.
type ChannelCycleSnapshot struct {
	bindings   map[string]string
	operations map[string]string
	parents    map[string]string
}

func (p *ChannelCycleDiagnostic) Snapshot(ctx context.Context) (ChannelCycleSnapshot, error) {
	var out ChannelCycleSnapshot
	var err error
	out.bindings, err = p.rows(ctx, `SELECT interface_key, row_to_json(t)::text FROM operator_channel_bindings t ORDER BY interface_key`)
	if err != nil {
		return out, err
	}
	out.operations, err = p.rows(ctx, `SELECT operation_id::text, row_to_json(t)::text FROM operator_channel_operations t ORDER BY operation_id`)
	if err != nil {
		return out, err
	}
	out.parents, err = p.rows(ctx, `SELECT operation_id::text, row_to_json(t)::text FROM channel_onboarding_operations t ORDER BY operation_id`)
	return out, err
}
func (p *ChannelCycleDiagnostic) rows(ctx context.Context, query string) (map[string]string, error) {
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		if _, duplicate := out[key]; duplicate {
			return nil, errors.New("duplicate diagnostic row")
		}
		out[key] = value
	}
	return out, rows.Err()
}
func (before ChannelCycleSnapshot) UnchangedExceptNewIdentity(after ChannelCycleSnapshot, identity string, committed bool) bool {
	equal := func(a, b map[string]string) bool {
		if len(a) != len(b) {
			return false
		}
		for k, v := range a {
			if b[k] != v {
				return false
			}
		}
		return true
	}
	if !equal(before.bindings, after.bindings) || !equal(before.parents, after.parents) {
		return false
	}
	expected := len(before.operations)
	if committed {
		expected++
	}
	if len(after.operations) != expected {
		return false
	}
	for k, v := range before.operations {
		if after.operations[k] != v {
			return false
		}
	}
	_, exists := after.operations[identity]
	return exists == committed
}
