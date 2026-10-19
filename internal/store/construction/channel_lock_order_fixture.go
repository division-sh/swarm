package construction

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
)

// ChannelLockOrder observes the fixed native schedules. It owns no database,
// selected store, readback, SQL callback or executable authority.
func NewChannelLockOrder() *ChannelLockOrder {
	return &ChannelLockOrder{binding: make(chan struct{}), principal: make(chan struct{}),
		parent: make(chan struct{}), rebindReady: make(chan struct{}), ackReady: make(chan struct{}), release: make(chan struct{}), milestones: make(map[string]bool)}
}

type channelLockOrderRole struct{}

type ChannelLockOrder struct {
	attached        bool
	binding         chan struct{}
	principal       chan struct{}
	parent          chan struct{}
	rebindReady     chan struct{}
	ackReady        chan struct{}
	release         chan struct{}
	bindOnce        sync.Once
	princOnce       sync.Once
	parentOnce      sync.Once
	rebindReadyOnce sync.Once
	ackReadyOnce    sync.Once
	freeOnce        sync.Once
	mu              sync.Mutex
	milestones      map[string]bool
}

func (p *ChannelLockOrder) RebindContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelLockOrderRole{}, "rebind")
}
func (p *ChannelLockOrder) StandingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelLockOrderRole{}, "standing")
}
func (p *ChannelLockOrder) ACKContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelLockOrderRole{}, "ack")
}
func (p *ChannelLockOrder) ACKStandingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelLockOrderRole{}, "ack_standing")
}
func (p *ChannelLockOrder) ParentAcquired() <-chan struct{}    { return p.parent }
func (p *ChannelLockOrder) RebindReady() <-chan struct{}       { return p.rebindReady }
func (p *ChannelLockOrder) ACKReady() <-chan struct{}          { return p.ackReady }
func (p *ChannelLockOrder) BindingAcquired() <-chan struct{}   { return p.binding }
func (p *ChannelLockOrder) PrincipalAcquired() <-chan struct{} { return p.principal }
func (p *ChannelLockOrder) Release()                           { p.freeOnce.Do(func() { close(p.release) }) }
func (p *ChannelLockOrder) mark(label string) {
	p.mu.Lock()
	p.milestones[label] = true
	p.mu.Unlock()
}
func (p *ChannelLockOrder) Milestones() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	copy := make(map[string]bool, len(p.milestones))
	for k, v := range p.milestones {
		copy[k] = v
	}
	return copy
}

// Reset is called only after both original transactions have joined.
func (p *ChannelLockOrder) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.binding, p.principal, p.parent = make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.rebindReady, p.ackReady, p.release = make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.bindOnce, p.princOnce, p.parentOnce = sync.Once{}, sync.Once{}, sync.Once{}
	p.rebindReadyOnce, p.ackReadyOnce, p.freeOnce = sync.Once{}, sync.Once{}, sync.Once{}
	p.milestones = make(map[string]bool)
}

type channelLockOrderConnector struct {
	native driver.Connector
	probe  *ChannelLockOrder
}

func (c *channelLockOrderConnector) Driver() driver.Driver { return c.native.Driver() }
func (c *channelLockOrderConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.native.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &channelLockOrderConn{Conn: conn, probe: c.probe}, nil
}

type channelLockOrderConn struct {
	driver.Conn
	probe *ChannelLockOrder
}

func (c *channelLockOrderConn) before(ctx context.Context, query string) {
	role, _ := ctx.Value(channelLockOrderRole{}).(string)
	if role == "rebind" && query == "SELECT pg_advisory_xact_lock(hashtext('operator_principal_singleton'))" {
		c.probe.mark("rebind_principal_fence_started")
		c.probe.rebindReadyOnce.Do(func() { close(c.probe.rebindReady) })
	}
	if role == "ack" && query == "SELECT operation_id::text FROM channel_onboarding_operations WHERE operation_id=$1::uuid FOR UPDATE" {
		c.probe.mark("ack_parent_lock_started")
		c.probe.ackReadyOnce.Do(func() { close(c.probe.ackReady) })
	}
	if role == "ack_standing" && query == "SELECT pg_advisory_xact_lock(hashtext('operator_principal_singleton'))" {
		c.probe.mark("standing_principal_fence_started")
	}
	if role == "ack" && strings.Contains(query, "FOR UPDATE OF action, render, plan, receipt, selected, binding, onboarding, activation") {
		c.probe.mark("ack_action_join_started")
	}
	if role == "rebind" && strings.HasPrefix(query, "INSERT INTO operator_channel_operations ") {
		c.probe.mark("rebind_insert_operation_started")
	}
	if role == "standing" && strings.Contains(query, "FROM operator_channel_bindings") && strings.HasSuffix(query, " FOR UPDATE") {
		c.probe.mark("standing_binding_query_started")
	}
}
func (c *channelLockOrderConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.before(ctx, query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *channelLockOrderConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.before(ctx, query)
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return rows, err
	}
	role, _ := ctx.Value(channelLockOrderRole{}).(string)
	label := ""
	if role == "rebind" && strings.Contains(query, "FROM operator_channel_bindings") && strings.HasSuffix(query, " FOR UPDATE") {
		label = "rebind_binding_acquired"
	}
	if role == "standing" && query == "SELECT principal_id FROM operator_principals WHERE singleton_id = 1 FOR UPDATE" {
		label = "standing_principal_acquired"
	}
	if role == "ack" && query == "SELECT principal_id FROM operator_principals WHERE singleton_id = 1 FOR UPDATE" {
		label = "ack_principal_acquired"
	}
	if role == "ack_standing" && strings.Contains(query, "FROM channel_onboarding_operations") && strings.HasSuffix(query, " FOR UPDATE") {
		label = "standing_parent_acquired"
	}
	return &channelLockOrderRows{Rows: rows, ctx: ctx, probe: c.probe, label: label}, nil
}
func (c *channelLockOrderConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c *channelLockOrderConn) Ping(ctx context.Context) error {
	if native, ok := c.Conn.(driver.Pinger); ok {
		return native.Ping(ctx)
	}
	return nil
}
func (c *channelLockOrderConn) ResetSession(ctx context.Context) error {
	if native, ok := c.Conn.(driver.SessionResetter); ok {
		return native.ResetSession(ctx)
	}
	return nil
}
func (c *channelLockOrderConn) IsValid() bool {
	if native, ok := c.Conn.(driver.Validator); ok {
		return native.IsValid()
	}
	return true
}

type channelLockOrderRows struct {
	driver.Rows
	ctx      context.Context
	probe    *ChannelLockOrder
	label    string
	observed bool
}

func (r *channelLockOrderRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err != nil || r.observed || r.label == "" {
		return err
	}
	r.observed = true
	r.probe.mark(r.label)
	if r.label == "rebind_binding_acquired" {
		r.probe.bindOnce.Do(func() { close(r.probe.binding) })
		r.probe.rebindReadyOnce.Do(func() { close(r.probe.rebindReady) })
	} else if r.label == "standing_parent_acquired" {
		r.probe.parentOnce.Do(func() { close(r.probe.parent) })
	} else {
		r.probe.princOnce.Do(func() { close(r.probe.principal) })
		if r.label == "ack_principal_acquired" {
			r.probe.ackReadyOnce.Do(func() { close(r.probe.ackReady) })
		}
	}
	select {
	case <-r.probe.release:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

func (r *channelLockOrderRows) HasNextResultSet() bool {
	if native, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return native.HasNextResultSet()
	}
	return false
}
func (r *channelLockOrderRows) NextResultSet() error {
	if native, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return native.NextResultSet()
	}
	return io.EOF
}
