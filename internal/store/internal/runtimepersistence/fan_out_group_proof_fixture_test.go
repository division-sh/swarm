package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// Faults wrap actual SQL/COMMIT on this selected database only. The callback
// can block or return a failure, but never supplies a successful SQL result.
type groupProofConnector struct {
	driver driver.Driver
	dsn    string
	mu     sync.Mutex
	hook   func(string, string) error
}

func (c *groupProofConnector) Driver() driver.Driver { return c.driver }
func (c *groupProofConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &groupProofConn{Conn: conn, owner: c}, nil
}
func (c *groupProofConnector) set(h func(string, string) error) {
	c.mu.Lock()
	c.hook = h
	c.mu.Unlock()
}
func (c *groupProofConnector) call(phase, query string) error {
	c.mu.Lock()
	hook := c.hook
	c.mu.Unlock()
	if hook != nil {
		return hook(phase, query)
	}
	return nil
}

type groupProofConn struct {
	driver.Conn
	owner *groupProofConnector
}

func (c *groupProofConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &groupProofStmt{Stmt: stmt, owner: c.owner, query: query}, nil
}

func (c *groupProofConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if prepare, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err := prepare.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return &groupProofStmt{Stmt: stmt, owner: c.owner, query: query}, nil
	}
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = stmt.Close()
		return nil, err
	}
	return stmt, nil
}

// Preparation itself is not an execution boundary. Each execution gets the
// same hooks as the connection path, including fresh first-row observation.
type groupProofStmt struct {
	driver.Stmt
	owner *groupProofConnector
	query string
}

func (s *groupProofStmt) Close() error {
	if err := s.Stmt.Close(); err != nil {
		return err
	}
	return s.owner.call("after_stmt_close", s.query)
}

func (s *groupProofStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.owner.call("before_exec", s.query); err != nil {
		return nil, err
	}
	result, err := s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	if err != nil {
		return result, err
	}
	return result, s.owner.call("after_exec", s.query)
}

func (s *groupProofStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.owner.call("before_query", s.query); err != nil {
		return nil, err
	}
	rows, err := s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	return &groupProofRows{Rows: rows, owner: s.owner, query: s.query}, nil
}

func (s *groupProofStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.owner.call("before_exec", s.query); err != nil {
		return nil, err
	}
	result, err := s.Stmt.Exec(args)
	if err != nil {
		return result, err
	}
	return result, s.owner.call("after_exec", s.query)
}

func (s *groupProofStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.owner.call("before_query", s.query); err != nil {
		return nil, err
	}
	rows, err := s.Stmt.Query(args)
	if err != nil {
		return nil, err
	}
	return &groupProofRows{Rows: rows, owner: s.owner, query: s.query}, nil
}

func (c *groupProofConn) IsValid() bool {
	if valid, ok := c.Conn.(driver.Validator); ok {
		return valid.IsValid()
	}
	return true
}
func (c *groupProofConn) ResetSession(ctx context.Context) error {
	if err := c.owner.call("reset_session", ""); err != nil {
		return err
	}
	if reset, ok := c.Conn.(driver.SessionResetter); ok {
		return reset.ResetSession(ctx)
	}
	return nil
}
func (c *groupProofConn) CheckNamedValue(value *driver.NamedValue) error {
	if check, ok := c.Conn.(driver.NamedValueChecker); ok {
		return check.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func (c *groupProofConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &groupProofTx{Tx: tx, owner: c.owner, readOnly: opts.ReadOnly}, nil
}
func (c *groupProofConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.owner.call("before_exec", q); err != nil {
		return nil, err
	}
	r, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
	if err != nil {
		return r, err
	}
	return r, c.owner.call("after_exec", q)
}
func (c *groupProofConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.owner.call("before_query", q); err != nil {
		return nil, err
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	return &groupProofRows{Rows: rows, owner: c.owner, query: q}, nil
}

type groupProofRows struct {
	driver.Rows
	owner    *groupProofConnector
	query    string
	observed bool
}

func (r *groupProofRows) Next(dest []driver.Value) error {
	if err := r.Rows.Next(dest); err != nil {
		return err
	}
	// A RETURNING mutation has executed before this hook, not at query entry.
	if !r.observed {
		r.observed = true
		return r.owner.call("after_query_row", r.query)
	}
	return nil
}

type groupProofTx struct {
	driver.Tx
	owner    *groupProofConnector
	readOnly bool
}

func (tx *groupProofTx) Commit() error {
	if err := tx.owner.call("before_commit", ""); err != nil {
		return err
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if !tx.readOnly {
		if err := tx.owner.call("after_write_commit", ""); err != nil {
			return err
		}
	}
	return tx.owner.call("after_commit", "")
}

func (tx *groupProofTx) Rollback() error {
	if err := tx.Tx.Rollback(); err != nil {
		return err
	}
	return tx.owner.call("after_rollback", "")
}

type groupProofFixture struct {
	ctx        context.Context
	raw        selectedFanOutLifecycleOwner
	db         *sql.DB
	postgres   bool
	probe      *groupProofConnector
	seed       fanOutOwnerFixture
	owner      pipeline.FanOutObligationOwner
	grant      startupownership.LiveGenerationGrant
	process    startupownership.ProcessCapability
	occurrence *worklifetime.RuntimeOccurrence
	group      pipelineobligation.PublicationGroup
	bus        *bus.EventBus
	intent     fanoutobligation.Intent
	claim      fanoutobligation.Claim
	events     []events.Event
	plans      []engine.DurablePublicationPlan
	claims     []pipelineobligation.Claim
	command    pipeline.FanOutChunkCommand
}

func newGroupProofFixture(t *testing.T, backend string, count int) *groupProofFixture {
	return newGroupProofFixtureOn(t, backend, count, nil)
}

func newGroupProofFixtureOn(t *testing.T, backend string, count int, parent *groupProofFixture) *groupProofFixture {
	t.Helper()
	f := &groupProofFixture{postgres: backend == "postgres", probe: &groupProofConnector{}}
	if parent != nil {
		f.raw, f.db, f.probe = parent.raw, parent.db, parent.probe
	} else {
		if f.postgres {
			f.probe.dsn, _, _ = testutil.StartPostgres(t)
			f.probe.driver = &pq.Driver{}
		} else {
			path := filepath.Join(t.TempDir(), "group-proof.sqlite")
			newBootstrappedSQLiteRuntimeStoreForPath(t, path)
			f.probe.dsn = "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
			f.probe.driver = &sqlite.Driver{}
		}
		f.db = sql.OpenDB(f.probe)
		f.db.SetMaxOpenConns(16)
		t.Cleanup(func() {
			f.probe.set(nil)
			if err := f.db.Close(); err != nil {
				t.Error(err)
			}
		})
		f.raw = newStopCommitStoreOwner(t, backend, f.db)
	}
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 30*time.Second)
	t.Cleanup(cancel)
	artifact, bundle := fanOutMixedRouteSource(t)
	f.seed = seedFanOutOwnerFixtureWithArtifact(t, ctx, f.db, f.raw, f.postgres, count, time.Now().UTC().Truncate(time.Microsecond), artifact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, f.seed.bundleHash))
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, f.seed.runID), mustStoreTestSourceArtifactFact(f.seed.bundleHash))
	route := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()
	source, err := events.NewStaticFlowRoutingSource(route)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := identity.AdmitExecutableNodeDeclaration("producer", "fan-out-producer")
	if err != nil {
		t.Fatal(err)
	}
	var capsuleRaw []byte
	if err := f.db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, f.seed.runID).Scan(&capsuleRaw); err != nil {
		t.Fatal(err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = producer.Key(), "producer", route.EntityID
	capsule.Route, capsule.ProducerSource = flowidentity.StoredRoute("producer", "producer", "producer"), source
	capsuleRaw, err = json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(capsuleRaw), f.seed.runID); err != nil {
		t.Fatal(err)
	}
	if parent == nil {
		process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, f.raw, []fanOutOwnerFixture{f.seed})
		f.process = process
		f.grant, f.occurrence = grants[0], occurrences[0]
	} else {
		f.grant, f.occurrence = parent.grant, parent.occurrence
		f.process = parent.process
	}
	{
		executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
		workers := 1
		registration, err := startupownership.StartFanOutServing(ctx, f.grant, f.occurrence, &workers, executor)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(registration.Close)
		select {
		case turn := <-executor.turns:
			f.owner = turn.owner
			registration.Close()
			<-executor.done
		case err := <-executor.errors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	key := fanoutobligation.IntentKey{RunID: f.seed.runID, TriggeringDeliveryID: f.seed.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: f.seed.flowPath, Family: "fan_out", SemanticPath: f.seed.semanticPath}}
	var found bool
	f.intent, f.claim, found, err = f.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "group-proof", BundleHash: f.seed.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	input, err := f.owner.LoadFanOutEvaluation(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	f.group, err = f.owner.BeginFanOutPublicationGroup(ctx, f.claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.probe.set(nil)
		if err := f.group.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if parent == nil {
		catalog, err := f.raw.(selectedFanOutMixedRouteOwner).RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, f.seed.bundleHash), []authoractivity.EventDescriptor{{EventType: "producer/mixed.none", Disposition: authoractivity.StoryAuthored}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(catalog.Release)
		f.bus, err = newStoreTestEventBus(t, f.raw.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(f.seed.bundleHash), RuntimeInstanceID: authorActivityTestRuntimeInstanceID})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		f.bus = parent.bus
	}
	f.ctx = deliverylifecycle.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(route)})
	f.command = pipeline.FanOutChunkCommand{Claim: f.claim, Now: time.Now().UTC()}
	for ordinal := 0; ordinal < count; ordinal++ {
		projection, err := fanoutobligation.PrepareOrdinalEmission(f.intent, input.Trigger, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "producer/mixed.none", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: producer.Key()}, Payload: []byte(`{}`), ChainDepth: capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, route), RoutingSource: source, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
		f.events = append(f.events, event)
	}
	return f
}
func (f *groupProofFixture) prepare(t *testing.T) {
	t.Helper()
	var requests []pipeline.FanOutPublicationRequest
	for i, event := range f.events {
		requests = append(requests, pipeline.FanOutPublicationRequest{Ordinal: i, Intent: engine.EmitIntent{Event: event}})
	}
	prepared, err := f.bus.PrepareFanOutPublications(f.ctx, f.group, requests)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range prepared {
		if row.Ordinal != i || row.Err != nil || row.Publication == nil {
			t.Fatalf("prepare row%d: %+v", i, row)
		}
		f.plans = append(f.plans, row.Publication)
		f.claims = append(f.claims, row.Publication.(bus.EnginePublicationPlan).PublicationCommand().Commit.PipelineClaim)
		f.command.Outcomes = append(f.command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: i, Publication: row.Publication})
	}
}
func (f *groupProofFixture) seal(t *testing.T) {
	t.Helper()
	if err := f.group.Seal(f.ctx, len(f.events), f.claims); err != nil {
		t.Fatal(err)
	}
}
func (f *groupProofFixture) commit(t *testing.T) {
	t.Helper()
	if _, err := f.owner.CommitFanOutChunk(f.ctx, f.command); err != nil {
		t.Fatal(err)
	}
}
func (f *groupProofFixture) members() []pipelineobligation.PublicationSettlementMember {
	var out []pipelineobligation.PublicationSettlementMember
	for _, claim := range f.claims {
		out = append(out, pipelineobligation.PublicationSettlementMember{Claim: claim, Disposition: pipelineobligation.Acknowledged("pipeline_persisted")})
	}
	return out
}
func (f *groupProofFixture) store() pipelineobligation.Store {
	return f.raw.(interface {
		PipelineObligations() pipelineobligation.Store
	}).PipelineObligations()
}

// Complete table rows, not just counters: refusals must not rewrite an existing
// receipt, candidate, delivery or historical fact while preserving cardinality.
func (f *groupProofFixture) snapshot(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, table := range []string{"runs", "events", "event_deliveries", "event_delivery_attempts", "event_receipts", "committed_replay_scopes", "decision_card_route_obligations", "fan_out_intents", "fan_out_outcomes", "run_fork_revisions", "run_fork_revision_heads", "run_fork_fact_revisions"} {
		rows, err := f.db.QueryContext(f.ctx, "SELECT * FROM "+table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			ptrs := make([]any, len(values))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(raw))
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		sort.Strings(out[table])
	}
	return out
}
func (f *groupProofFixture) unchanged(t *testing.T, before map[string][]string) {
	t.Helper()
	if after := f.snapshot(t); !reflect.DeepEqual(before, after) {
		t.Fatal("refused operation changed domain or history rows")
	}
}
