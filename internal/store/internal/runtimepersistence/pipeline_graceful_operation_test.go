package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	obligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// This connector delegates native SQL unchanged. The hooks place cancellation
// and independent failures at the admitted operation and COMMIT boundaries.
type pipelineGracefulProbe struct {
	mu     sync.Mutex
	hook   func(string, string) error
	closes atomic.Int32
}

func (p *pipelineGracefulProbe) set(hook func(string, string) error) {
	p.mu.Lock()
	p.hook = hook
	p.mu.Unlock()
}

func (p *pipelineGracefulProbe) call(phase, query string) error {
	p.mu.Lock()
	hook := p.hook
	p.mu.Unlock()
	if hook != nil {
		return hook(phase, query)
	}
	return nil
}

type pipelineGracefulConnector struct {
	driver.Connector
	probe *pipelineGracefulProbe
}

func (c pipelineGracefulConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	pq.SetNoticeHandler(conn, func(notice *pq.Error) { _ = c.probe.call("notice", notice.Message) })
	return &pipelineGracefulConn{Conn: conn, probe: c.probe}, nil
}

type pipelineGracefulConn struct {
	driver.Conn
	probe *pipelineGracefulProbe
}

func (c *pipelineGracefulConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pipelineGracefulTx{Tx: tx, probe: c.probe}, nil
}
func (c *pipelineGracefulConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.probe.call("query", query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *pipelineGracefulConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	return result, errors.Join(err, c.probe.call("exec", query))
}
func (c *pipelineGracefulConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}
func (c *pipelineGracefulConn) CheckNamedValue(value *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(value)
}
func (c *pipelineGracefulConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *pipelineGracefulConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }
func (c *pipelineGracefulConn) Ping(ctx context.Context) error {
	return c.Conn.(driver.Pinger).Ping(ctx)
}
func (c *pipelineGracefulConn) Close() error { c.probe.closes.Add(1); return c.Conn.Close() }

type pipelineGracefulTx struct {
	driver.Tx
	probe *pipelineGracefulProbe
}

func (tx *pipelineGracefulTx) Commit() error {
	if err := tx.probe.call("commit_admitted", ""); err != nil {
		return err
	}
	err := tx.Tx.Commit()
	return errors.Join(err, tx.probe.call("commit_returned", ""))
}

func openPipelineGracefulFixture(t *testing.T) (authorActivityReceiptFixture, *PostgresStore, *pipelineGracefulProbe) {
	t.Helper()
	dsn, _, _ := testutil.StartPostgres(t)
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	probe := &pipelineGracefulProbe{}
	db := sql.OpenDB(pipelineGracefulConnector{Connector: connector, probe: probe})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { probe.set(nil); _ = db.Close() })
	store := admitTestPostgresStore(t, db)
	registerTestAuthorActivityCatalog(t, store)
	return authorActivityReceiptFixture{store: store, db: db, dialect: authoractivityfixture.DialectPostgres}, store, probe
}

func TestPipelineGracefulClaimReadCancellation(t *testing.T) {
	for _, phase := range []string{"entry", "parent_fence", "eligibility", "event_hydration", "receipt", "scope", "native_notice"} {
		t.Run(phase, func(t *testing.T) {
			fixture, store, probe := openPipelineGracefulFixture(t)
			base := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, base, runID)
			eventID := commitPipelineParityEvent(t, base, store, runID, time.Now().UTC())
			if phase == "native_notice" {
				for _, statement := range []string{
					`CREATE FUNCTION pipeline_graceful_notice() RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'pipeline active read'; RETURN true; END $$`,
					`ALTER TABLE events RENAME TO pipeline_graceful_events`,
					`CREATE VIEW events AS SELECT * FROM pipeline_graceful_events WHERE pipeline_graceful_notice()`,
				} {
					if _, err := fixture.db.Exec(statement); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			var canceled atomic.Bool
			match := map[string]string{"parent_fence": "pg_advisory_xact_lock", "eligibility": "SELECT EXISTS", "event_hydration": "e.payload_bytes", "receipt": "SELECT outcome FROM event_receipts", "scope": "SELECT scope FROM committed_replay_scopes"}[phase]
			probe.set(func(at, query string) error {
				matches := phase != "native_notice" && at == "query" && strings.Contains(query, match)
				if phase == "native_notice" {
					matches = at == "notice" && query == "pipeline active read"
				}
				if matches && canceled.CompareAndSwap(false, true) {
					cancel()
				}
				return nil
			})
			if phase == "entry" {
				probe.set(nil)
				cancel()
			}
			closes := probe.closes.Load()
			work, err := store.PipelineObligations().ClaimEvent(ctx, eventID, obligation.PurposeRecovery)
			probe.set(nil)
			if !errors.Is(err, context.Canceled) || work.Event.ID() != "" {
				t.Fatalf("canceled claim exposed work: %s, %v", work.Event.ID(), err)
			}
			if phase != "entry" && !canceled.Load() {
				t.Fatal("read boundary was not exercised")
			}
			if got := probe.closes.Load(); got != closes {
				t.Fatalf("healthy cancellation closed native session: %d -> %d", closes, got)
			}
			if n := store.pipelinePostgresOwner.PostgresPipelineClaimsForTest().ClaimCountForTest(); n != 0 {
				t.Fatalf("canceled claim retained %d claims", n)
			}
			next, err := store.PipelineObligations().ClaimEvent(base, eventID, obligation.PurposeRecovery)
			if err != nil {
				t.Fatalf("successor claim: %v", err)
			}
			outcome, err := store.PipelineObligations().Settle(base, next.Claim, obligation.Acknowledged("processed"))
			if err != nil || !outcome.Committed() {
				t.Fatalf("successor settlement: %#v, %v", outcome, err)
			}
		})
	}
}

func TestPipelineGracefulParentFenceDrainsCanceledWait(t *testing.T) {
	fixture, store, probe := openPipelineGracefulFixture(t)
	base := testAuthorActivityContext()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, base, runID)
	eventID := commitPipelineParityEvent(t, base, store, runID, time.Now().UTC())
	fixture.db.SetMaxOpenConns(2)
	holder, err := fixture.db.Conn(base)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	key := "swarm:pipeline-parent-fence:" + eventID
	if _, err := holder.ExecContext(base, `SELECT pg_advisory_lock(hashtext($1))`, key); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, key)
	entered := make(chan struct{})
	var once sync.Once
	probe.set(func(at, query string) error {
		if at == "query" && strings.Contains(query, "pg_advisory_xact_lock") {
			once.Do(func() { close(entered) })
		}
		return nil
	})
	ctx, cancel := context.WithCancel(base)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.PipelineObligations().ClaimEvent(ctx, eventID, obligation.PurposeRecovery)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("claim never entered parent fence")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("admitted fence returned before parent settlement: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := holder.ExecContext(base, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("fence cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim did not settle after parent fence released")
	}
	probe.set(nil)
	var acquired bool
	if err := holder.QueryRowContext(base, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&acquired); err != nil || !acquired {
		t.Fatalf("canceled fence leaked lock: %v, %v", acquired, err)
	}
	if _, err := holder.ExecContext(base, `SELECT pg_advisory_unlock(hashtext($1))`, key); err != nil {
		t.Fatal(err)
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	work, err := store.PipelineObligations().ClaimEvent(base, eventID, obligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("successor claim: %v", err)
	}
	if _, err := store.PipelineObligations().Settle(base, work.Claim, obligation.Acknowledged("processed")); err != nil {
		t.Fatal(err)
	}
}

type pipelineGracefulSink struct {
	submits, cancels atomic.Int32
	failure          error
}

func (s *pipelineGracefulSink) ReserveCompletionCandidate(context.Context) (runlifecycle.CandidateAdmission, error) {
	return s, nil
}
func (s *pipelineGracefulSink) Submit(runlifecycle.Candidate) error {
	s.submits.Add(1)
	return s.failure
}
func (s *pipelineGracefulSink) Cancel() error { s.cancels.Add(1); return nil }

func TestPipelineGracefulWriteOutcome(t *testing.T) {
	for _, method := range []string{"settle", "mark_decision"} {
		for _, phase := range []string{"success", "cancel_entry", "cancel_work", "cancel_commit_admitted", "independent_work", "panic", "endtx", "release", "handoff", "combined", "release_handoff", "commit_refused", "commit_ack_lost"} {
			if method == "mark_decision" && (phase == "release" || phase == "release_handoff") {
				continue
			}
			t.Run(method+"/"+phase, func(t *testing.T) {
				fixture, store, probe := openPipelineGracefulFixture(t)
				base := testAuthorActivityContext()
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, base, runID)
				eventID := commitPipelineParityEvent(t, base, store, runID, time.Now().UTC())
				purpose := obligation.PurposeRecovery
				if method == "mark_decision" {
					insertProducerIdentityDecisionObligation(t, fixture, base, eventID, runID, time.Now().UTC())
					purpose = obligation.PurposeDecisionRoute
				}
				owner := store.PipelineObligations()
				work, err := owner.ClaimEvent(base, eventID, purpose)
				if err != nil {
					t.Fatal(err)
				}
				state, err := store.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(work.Claim)
				if err != nil {
					t.Fatal(err)
				}
				lease := state.LeaseForTest()
				session := lease.Session()
				t.Cleanup(func() { session.SetEndTxErrorForTest(nil); _ = lease.ReleaseTerminal(context.Background()) })
				primary := errors.New("independent pipeline operation failure")
				cleanup := errors.New("independent pipeline EndTx failure")
				release := errors.New("independent pipeline release failure")
				handoff := errors.New("independent pipeline handoff failure")
				if phase == "endtx" || phase == "combined" {
					session.SetEndTxErrorForTest(func() error { return cleanup })
				}
				if phase == "release" || phase == "release_handoff" {
					lease.SetReleaseSessionForTest(func() error { return errors.Join(session.Release(), release) })
				}
				process := worklifetime.NewProcess()
				occurrence := newRunLifecycleExecutorOccurrence(t, process)
				base = worklifetime.WithRuntimeOccurrence(base, occurrence)
				sink := &pipelineGracefulSink{}
				if phase == "handoff" || phase == "combined" || phase == "release_handoff" {
					sink.failure = handoff
				}
				registration, err := store.RegisterCompletionCandidateSink(base, runlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					registration.Release()
					retireRunLifecycleExecutorOccurrence(t, occurrence)
					retireRunLifecycleProcess(t, process)
				})
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				var fired atomic.Bool
				probe.set(func(at, query string) error {
					if at == "exec" && strings.Contains(query, "INSERT INTO event_receipts") && fired.CompareAndSwap(false, true) {
						switch phase {
						case "cancel_work":
							cancel()
						case "independent_work":
							cancel()
							return primary
						case "panic":
							panic(primary)
						}
					}
					if at == "commit_admitted" {
						if phase == "cancel_commit_admitted" {
							cancel()
						}
						if phase == "commit_refused" {
							return primary
						}
					}
					if at == "commit_returned" && phase == "commit_ack_lost" {
						return primary
					}
					return nil
				})
				if phase == "cancel_entry" {
					cancel()
				}
				var outcome obligation.SettlementOutcome
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					if method == "settle" {
						outcome, err = owner.Settle(ctx, work.Claim, obligation.Acknowledged("processed"))
					} else {
						err = owner.MarkDecisionProcessed(ctx, work.Claim)
					}
				}()
				probe.set(nil)
				session.SetEndTxErrorForTest(nil)
				committed := phase == "success" || phase == "cancel_commit_admitted" || phase == "endtx" || phase == "release" || phase == "handoff" || phase == "combined" || phase == "release_handoff"
				if method == "settle" && (outcome.Committed() != committed || outcome.DeliveryHandoffCommitted() != committed) {
					t.Fatalf("commit evidence=%#v, want committed=%v, err=%v", outcome, committed, err)
				}
				if phase == "panic" && recovered != primary {
					t.Fatalf("panic=%v", recovered)
				}
				if (phase == "cancel_entry" || phase == "cancel_work") && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
				if (phase == "independent_work" || phase == "commit_refused" || phase == "commit_ack_lost") && !errors.Is(err, primary) {
					t.Fatalf("lost primary error: %v", err)
				}
				if (phase == "endtx" || phase == "combined") && !errors.Is(err, cleanup) {
					t.Fatalf("lost EndTx error: %v", err)
				}
				if (phase == "release" || phase == "release_handoff") && !errors.Is(err, release) {
					t.Fatalf("lost release error: %v", err)
				}
				if (phase == "handoff" || phase == "combined" || phase == "release_handoff") && !errors.Is(err, handoff) {
					t.Fatalf("lost handoff error: %v", err)
				}
				if (phase == "success" || phase == "cancel_commit_admitted") && err != nil {
					t.Fatalf("acknowledged commit relabeled: %v", err)
				}
				wantSubmits := int32(0)
				if committed {
					wantSubmits = 1
				}
				if sink.submits.Load() != wantSubmits {
					t.Fatalf("handoff submissions=%d, want %d", sink.submits.Load(), wantSubmits)
				}
				count, _, _ := readExactPipelineReceipt(t, context.Background(), fixture, eventID)
				persisted := committed || phase == "commit_ack_lost"
				if (count == 1) != persisted {
					t.Fatalf("durable receipts=%d, expected persisted=%v", count, persisted)
				}
				if phase == "commit_refused" || phase == "commit_ack_lost" {
					if lease.Current() {
						t.Fatal("uncertain commit retained usable claim session")
					}
				} else if !committed {
					if !lease.Current() {
						t.Fatal("safe rollback lost healthy claim session")
					}
					if method == "settle" {
						_, err = owner.Settle(base, work.Claim, obligation.Acknowledged("processed"))
					} else {
						err = owner.MarkDecisionProcessed(base, work.Claim)
					}
					if err != nil {
						t.Fatalf("healthy successor operation: %v", err)
					}
				}
			})
		}
	}
}

func TestPipelineGracefulCanceledAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var fixture authorActivityReceiptFixture
			if backend == "sqlite" {
				fixture = openSQLiteAuthorActivityReceiptFixture(t)
			} else {
				fixture = openPostgresAuthorActivityReceiptFixture(t)
			}
			store := fixture.store.(pipelineObligationParityStore)
			base := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, base, runID)
			eventID := commitPipelineParityEvent(t, base, store, runID, time.Now().UTC())
			ctx, cancel := context.WithCancel(base)
			cancel()
			owner := store.PipelineObligations()
			if _, err := owner.ClaimEvent(ctx, eventID, obligation.PurposeRecovery); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled claim: %v", err)
			}
			work, err := owner.ClaimEvent(base, eventID, obligation.PurposeRecovery)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := owner.Settle(ctx, work.Claim, obligation.Acknowledged("processed"))
			if !errors.Is(err, context.Canceled) || outcome.Committed() {
				t.Fatalf("canceled settlement=%#v, %v", outcome, err)
			}
			outcome, err = owner.Settle(base, work.Claim, obligation.Acknowledged("processed"))
			if err != nil || !outcome.Committed() {
				t.Fatalf("successor settlement=%#v, %v", outcome, err)
			}
		})
	}
}

var _ driver.ConnBeginTx = (*pipelineGracefulConn)(nil)
var _ driver.Validator = (*pipelineGracefulConn)(nil)
