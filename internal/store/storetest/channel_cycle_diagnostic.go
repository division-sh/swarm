package storetest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
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
		parent: make(chan struct{}), release: make(chan struct{}), milestones: make(map[string]bool)}
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
	parent     chan struct{}
	release    chan struct{}
	bindOnce   sync.Once
	princOnce  sync.Once
	parentOnce sync.Once
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
func (p *ChannelCycleDiagnostic) ACKContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelCycleRole{}, "ack")
}
func (p *ChannelCycleDiagnostic) ACKStandingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, channelCycleRole{}, "ack_standing")
}
func (p *ChannelCycleDiagnostic) ParentAcquired() <-chan struct{}    { return p.parent }
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
	if role == "ack" && query == "SELECT principal_id FROM operator_principals WHERE singleton_id = 1 FOR UPDATE" {
		label = "ack_principal_acquired"
	}
	if role == "ack_standing" && strings.Contains(query, "FROM channel_onboarding_operations") && strings.HasSuffix(query, " FOR UPDATE") {
		label = "standing_parent_acquired"
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
	} else if r.label == "standing_parent_acquired" {
		r.probe.parentOnce.Do(func() { close(r.probe.parent) })
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

// ACK snapshots compare private durable images without exposing payloads or IDs.
type ChannelACKSnapshot struct {
	rows map[string]map[string]string
}

func (p *ChannelCycleDiagnostic) ACKSnapshot(ctx context.Context) (ChannelACKSnapshot, error) {
	out := ChannelACKSnapshot{rows: make(map[string]map[string]string)}
	for _, table := range []struct{ name, key string }{
		{"operator_channel_bindings", "interface_key"},
		{"operator_channel_operations", "operation_id::text"},
		{"channel_onboarding_operations", "operation_id::text"},
		{"connected_channel_activations", "activation_id::text"},
		{"mailbox", "item_id::text"},
		{"operator_channel_action_intents", "publication_id::text"},
		{"operator_channel_text_intents", "publication_id::text"},
		{"channel_delivery_plans", "delivery_id::text"},
		{"channel_delivery_receipts", "effect_operation_id::text"},
		{"api_idempotency", "method || ':' || actor_kind || ':' || actor_id || ':' || idempotency_key"},
	} {
		rows, err := p.rows(ctx, "SELECT "+table.key+", row_to_json(t)::text FROM "+table.name+" t ORDER BY "+table.key)
		if err != nil {
			return ChannelACKSnapshot{}, err
		}
		out.rows[table.name] = rows
	}
	return out, nil
}

func (before ChannelACKSnapshot) ExactExceptACK(after ChannelACKSnapshot, noticeID, publicationID string, committed bool) bool {
	for table, rows := range before.rows {
		got, found := after.rows[table]
		expectedCount := len(rows)
		if table == "api_idempotency" && committed {
			expectedCount++
		}
		if !found || expectedCount != len(got) {
			return false
		}
		for key, value := range rows {
			actual, found := got[key]
			if !found {
				return false
			}
			if committed && (table == "mailbox" && key == noticeID || table == "operator_channel_action_intents" && key == publicationID) {
				if !exactACKMutation(table, value, actual) {
					return false
				}
			} else if actual != value {
				return false
			}
		}
	}
	return true
}

func exactACKMutation(table, before, after string) bool {
	var old, current map[string]json.RawMessage
	if json.Unmarshal([]byte(before), &old) != nil || json.Unmarshal([]byte(after), &current) != nil {
		return false
	}
	if table == "mailbox" {
		if string(old["notified"]) != "false" || string(current["notified"]) != "true" {
			return false
		}
		old["notified"] = current["notified"]
	} else {
		if string(old["state"]) != `"pending"` || string(current["state"]) != `"settled"` || string(current["disposition"]) != `"applied"` {
			return false
		}
		var settled time.Time
		if string(old["settled_at"]) != "null" || json.Unmarshal(current["settled_at"], &settled) != nil || settled.IsZero() {
			return false
		}
		old["state"], old["disposition"] = current["state"], current["disposition"]
		old["settled_at"] = current["settled_at"]
	}
	return reflect.DeepEqual(old, current)
}

func (p *ChannelCycleDiagnostic) ACKCompletionExact(ctx context.Context, req apiidempotency.Request, committed bool) (bool, error) {
	var resource, hash string
	var response []byte
	err := p.db.QueryRowContext(ctx, `SELECT resource_id, request_hash, response FROM api_idempotency
		WHERE method=$1 AND actor_kind=$2 AND actor_id=$3 AND idempotency_key=$4`,
		req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey).Scan(&resource, &hash, &response)
	if !committed {
		return errors.Is(err, sql.ErrNoRows), nil
	}
	if err != nil {
		return false, err
	}
	var actual map[string]any
	if json.Unmarshal(response, &actual) != nil {
		return false, errors.New("diagnostic completion is not valid JSON")
	}
	expected := map[string]any{"ok": true, "mailbox_id": req.ResourceID, "kind": "notice"}
	return resource == req.ResourceID && hash == req.RequestHash && reflect.DeepEqual(actual, expected), nil
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
