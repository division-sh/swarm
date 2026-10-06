package mutationprotocol

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privatefork "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

const nativeFenceSQL = `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1 FOR UPDATE`

// This connector interleaves real transactions before the real fence statement;
// it never fabricates serialization errors or successful SQL results.
type fenceConflictConnector struct {
	driver                         driver.Driver
	dsn                            string
	competitor                     *sql.DB
	conflicts                      int
	fences, rollbacks, commits     int
	txIDs                          []int64
	onConflict                     func() error
	rollbackFailure, commitFailure error
	afterRollback                  func() error
	conflictQueryPrefix            string
}

func (p *fenceConflictConnector) Driver() driver.Driver { return p.driver }

func (p *fenceConflictConnector) Connect(context.Context) (driver.Conn, error) {
	c, err := p.driver.Open(p.dsn)
	if err != nil {
		return nil, err
	}
	return &fenceConflictConn{Conn: c, probe: p}, nil
}

type fenceConflictConn struct {
	driver.Conn
	probe *fenceConflictConnector
}

func (c *fenceConflictConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *fenceConflictConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	native := c.Conn.(driver.QueryerContext)
	if c.probe.conflictQueryPrefix != "" && strings.HasPrefix(strings.TrimSpace(query), c.probe.conflictQueryPrefix) {
		_, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, `DO $$ BEGIN RAISE EXCEPTION 'revision conflict' USING ERRCODE='40001'; END $$`, nil)
		return nil, err
	}
	if query == nativeFenceSQL {
		p := c.probe
		p.fences++
		rows, err := native.QueryContext(ctx, `SELECT txid_current(), last_sequence FROM author_activity_order WHERE singleton_id=1`, nil)
		if err != nil {
			return nil, err
		}
		values := make([]driver.Value, 2)
		readErr := rows.Next(values)
		if err := errors.Join(readErr, rows.Close()); err != nil {
			return nil, err
		}
		p.txIDs = append(p.txIDs, values[0].(int64))
		if p.conflicts < 0 || p.fences <= p.conflicts {
			if _, err := p.competitor.ExecContext(ctx, `UPDATE author_activity_order SET last_sequence=last_sequence+1 WHERE singleton_id=1`); err != nil {
				return nil, err
			}
			if p.onConflict != nil {
				if err := p.onConflict(); err != nil {
					return nil, err
				}
			}
		}
	}
	return native.QueryContext(ctx, query, args)
}

func (c *fenceConflictConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *fenceConflictConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *fenceConflictConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &fenceConflictTx{Tx: tx, probe: c.probe}, nil
}

type fenceConflictTx struct {
	driver.Tx
	probe *fenceConflictConnector
}

func (tx *fenceConflictTx) Commit() error {
	tx.probe.commits++
	return errors.Join(tx.Tx.Commit(), tx.probe.commitFailure)
}
func (tx *fenceConflictTx) Rollback() error {
	tx.probe.rollbacks++
	if tx.probe.rollbackFailure != nil {
		return tx.probe.rollbackFailure
	}
	err := tx.Tx.Rollback()
	if tx.probe.afterRollback != nil {
		err = errors.Join(err, tx.probe.afterRollback())
	}
	return err
}

func nativeFenceBackend(t *testing.T) (*postgresbackend.Backend, *fenceConflictConnector, *postgresbackend.AdvisoryLockLease) {
	t.Helper()
	dsn, original, _ := testutil.StartEmptyPostgres(t)
	faultMatrixSchema(t, original)
	faultMatrixActivitySchema(t, original)
	if _, err := original.Exec(`INSERT INTO author_activity_order VALUES (1, 0)`); err != nil {
		t.Fatal(err)
	}
	p := &fenceConflictConnector{driver: original.Driver(), dsn: dsn, competitor: original, conflicts: 1}
	db := sql.OpenDB(p)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	b, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(context.Background(), b, "selected-fork-fence-regression")
	if err != nil || !acquired {
		t.Fatalf("retained publication lease: acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return b, p, lease
}

func TestNativeRetainedPostgresFenceConflictRetriesBeforeDomain(t *testing.T) {
	_, p, lease := nativeFenceBackend(t)
	baseline := NewBaseline()
	// Dirty attempt-local revision evidence must not survive the aborted fence.
	p.onConflict = func() error {
		return baseline.effects.AddFact(faultMatrixMissingRun, privatefork.FamilyEvents, "00000000-0000-0000-0000-000000000244")
	}
	calls := 0
	var domainAttempt *Attempt
	result := RunRetainedPostgresWithOptions(context.Background(), lease.Session(), &sql.TxOptions{Isolation: sql.LevelSerializable}, Story, Ordinary, baseline, nil,
		func(ctx context.Context, attempt *Attempt) (string, error) {
			calls++
			domainAttempt = attempt
			var sequence int64
			if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&sequence)
			}); err != nil {
				return "", err
			}
			if sequence != 1 {
				return "", fmt.Errorf("stale authority sequence %d", sequence)
			}
			if err := faultMatrixInsert(ctx, attempt, "selected-fork-output"); err != nil {
				return "", err
			}
			return "selected-fork-output", attempt.Record(ctx, faultMatrixDraft("selected-fork-history"))
		})
	if !result.Acknowledged() || result.Err() != nil || result.Phase() != PostCommit {
		var conflict *pq.Error
		if errors.As(result.Err(), &conflict) {
			t.Logf("native conflict SQLSTATE=%s", conflict.Code)
		}
		t.Fatalf("native retained publication did not recover fence: calls=%d fences=%d rollbacks=%d commits=%d phase=%v err=%v", calls, p.fences, p.rollbacks, p.commits, result.Phase(), result.Err())
	}
	if value, ok := result.Value(); !ok || value != "selected-fork-output" {
		t.Fatalf("output=%q acknowledged=%v", value, ok)
	}
	if calls != 1 || p.fences != 2 || p.rollbacks != 1 || p.commits != 1 || len(p.txIDs) != 2 || p.txIDs[0] == p.txIDs[1] {
		t.Fatalf("domain replay or stale transaction: calls=%d fences=%d rollbacks=%d commits=%d txIDs=%v", calls, p.fences, p.rollbacks, p.commits, p.txIDs)
	}
	if err := domainAttempt.WithSQL(context.Background(), func(context.Context, *sql.Tx) error { return nil }); err == nil {
		t.Fatal("retired domain attempt retained authority")
	}
	if faultMatrixCount(t, p.competitor, "selected-fork-output") != 1 {
		t.Fatal("output was not exactly once")
	}
	var occurrences, sequence int64
	if err := p.competitor.QueryRow(`SELECT COUNT(*), MAX(sequence) FROM author_activity_occurrences WHERE dedup_key='selected-fork-history'`).Scan(&occurrences, &sequence); err != nil || occurrences != 1 || sequence != 2 {
		t.Fatalf("history did not use fresh ordering exactly once: occurrences=%d sequence=%d err=%v", occurrences, sequence, err)
	}
	if err := lease.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("retry lost retained publication possession: %v", err)
	}
}

func TestNativePostgresFenceConsumers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		evidence  Evidence
		kind      Kind
		isolation sql.IsolationLevel
	}{
		{"failure_retention", Story, Ordinary, sql.LevelSerializable},
		{"selected_discard", Story, RetainedForkCleanup, sql.LevelSerializable},
		{"authority_fence", AuthorityFence, Ordinary, sql.LevelSerializable},
		{"read_committed", Story, Ordinary, sql.LevelReadCommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, p, lease := nativeFenceBackend(t)
			if err := lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			calls := 0
			result := RunPostgresWithOptions(context.Background(), b, &sql.TxOptions{Isolation: tc.isolation}, tc.evidence, tc.kind, nil, runhandoff.NewCandidateCoordinator(),
				func(ctx context.Context, attempt *Attempt) (string, error) {
					calls++
					if tc.kind == RetainedForkCleanup {
						retained, err := attempt.SelectForkDiscardRetention(ctx, faultMatrixMissingRun)
						if err != nil || retained {
							return "", fmt.Errorf("discard selection retained=%v err=%v", retained, err)
						}
						if err := attempt.BeginDestructiveCleanup(ctx); err != nil {
							return "", err
						}
					}
					return tc.name, faultMatrixInsert(ctx, attempt, tc.name)
				})
			wantFences := 2
			if tc.isolation == sql.LevelReadCommitted {
				wantFences = 1
			}
			if result.Err() != nil || !result.Acknowledged() || calls != 1 || p.fences != wantFences || p.rollbacks != wantFences-1 || p.commits != 1 || faultMatrixCount(t, p.competitor, tc.name) != 1 {
				t.Fatalf("consumer replay or wrong settlement: calls=%d fences=%d rollbacks=%d commits=%d err=%v", calls, p.fences, p.rollbacks, p.commits, result.Err())
			}
		})
	}
}

func TestNativeRetainedPostgresPersistentFenceConflictCancellation(t *testing.T) {
	_, p, lease := nativeFenceBackend(t)
	p.conflicts = -1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.onConflict = func() error {
		if p.fences == 8 {
			cancel()
		}
		return nil
	}
	calls := 0
	result := RunRetainedPostgresWithOptions(ctx, lease.Session(), &sql.TxOptions{Isolation: sql.LevelSerializable}, Story, Ordinary, nil, nil,
		func(context.Context, *Attempt) (string, error) { calls++; return "must-not-run", nil })
	faultMatrixNoValue(t, result, AcquireFence)
	var pgErr *pq.Error
	if !errors.Is(result.Err(), context.Canceled) || !errors.As(result.Err(), &pgErr) || pgErr.Code != "40001" || calls != 0 || p.fences != 8 || p.rollbacks != 8 || p.commits != 0 {
		t.Fatalf("persistent conflict escaped caller cancellation: calls=%d fences=%d rollbacks=%d commits=%d err=%v", calls, p.fences, p.rollbacks, p.commits, result.Err())
	}
	for i := 1; i < len(p.txIDs); i++ {
		if p.txIDs[i] == p.txIDs[i-1] {
			t.Fatal("persistent conflict reused transaction")
		}
	}
	if err := lease.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("canceled attempt leaked session transaction: %v", err)
	}
}

func TestNativeRetainedPostgresFenceSettlementRefusesRetry(t *testing.T) {
	for _, scenario := range []string{"rollback_failure", "session_cleanup", "revoked_session", "already_settled"} {
		t.Run(scenario, func(t *testing.T) {
			_, p, lease := nativeFenceBackend(t)
			session := lease.Session()
			cleanup := errors.New("independent native cleanup")
			switch scenario {
			case "rollback_failure":
				p.rollbackFailure = cleanup
			case "session_cleanup":
				session.SetEndTxErrorForTest(func() error { return cleanup })
			case "revoked_session":
				p.afterRollback = session.ForceDiscard
			case "already_settled":
				p.afterRollback = func() error { return sql.ErrTxDone }
			}
			calls := 0
			result := RunRetainedPostgresWithOptions(context.Background(), session, &sql.TxOptions{Isolation: sql.LevelSerializable}, Story, Ordinary, nil, nil,
				func(context.Context, *Attempt) (string, error) { calls++; return "must-not-run", nil })
			faultMatrixNoValue(t, result, AcquireFence)
			if calls != 0 || p.fences != 1 || p.rollbacks != 1 || p.commits != 0 || postgresbackend.IsRolledBackSerializationConflict(result.Err()) {
				t.Fatalf("unsafe retry: calls=%d fences=%d rollbacks=%d commits=%d err=%v", calls, p.fences, p.rollbacks, p.commits, result.Err())
			}
			if (scenario == "rollback_failure" || scenario == "session_cleanup") && !errors.Is(result.Err(), cleanup) {
				t.Fatalf("lost cleanup failure: %v", result.Err())
			}
		})
	}
}

func TestNativeRetainedPostgresPostFenceConflictsNeverReplay(t *testing.T) {
	for _, scenario := range []string{"domain", "revision_finalize", "activity_finalize", "unconfirmed_commit", "acknowledged_cleanup"} {
		t.Run(scenario, func(t *testing.T) {
			_, p, lease := nativeFenceBackend(t)
			p.conflicts = 0
			session := lease.Session()
			wantPhase := DomainWrite
			switch scenario {
			case "revision_finalize":
				wantPhase = RevisionFinalize
				p.conflictQueryPrefix = "SELECT CAST(run_id AS TEXT) FROM runs"
			case "activity_finalize":
				wantPhase = ActivityFinalize
				if _, err := p.competitor.Exec(`CREATE FUNCTION fail_activity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'activity conflict' USING ERRCODE='40001'; END $$; CREATE TRIGGER fail_activity BEFORE INSERT ON author_activity_occurrences FOR EACH ROW EXECUTE FUNCTION fail_activity()`); err != nil {
					t.Fatal(err)
				}
			case "unconfirmed_commit":
				wantPhase = CommitAdmission
				p.commitFailure = &pq.Error{Code: "40001", Message: "ack lost after real commit"}
			case "acknowledged_cleanup":
				wantPhase = PostCommit
				session.SetEndTxErrorForTest(func() error { return &pq.Error{Code: "40001", Message: "independent postcommit cleanup"} })
			}
			calls := 0
			result := RunRetainedPostgresWithOptions(context.Background(), session, &sql.TxOptions{Isolation: sql.LevelSerializable}, Story, Ordinary, nil, nil,
				func(ctx context.Context, attempt *Attempt) (string, error) {
					calls++
					if err := faultMatrixInsert(ctx, attempt, scenario); err != nil {
						return "", err
					}
					switch scenario {
					case "domain":
						return "", attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, `DO $$ BEGIN RAISE EXCEPTION 'domain conflict' USING ERRCODE='40001'; END $$`)
							return err
						})
					case "revision_finalize":
						return scenario, attempt.AddWholeFamily(faultMatrixMissingRun, privatefork.FamilyEvents)
					case "activity_finalize":
						return scenario, attempt.Record(ctx, faultMatrixDraft(scenario))
					}
					return scenario, nil
				})
			var pgErr *pq.Error
			if result.Err() == nil || !errors.As(result.Err(), &pgErr) || pgErr.Code != "40001" || result.Phase() != wantPhase || calls != 1 || p.fences != 1 {
				t.Fatalf("post-fence conflict replayed or wrong phase: calls=%d fences=%d phase=%v err=%v", calls, p.fences, result.Phase(), result.Err())
			}
			wantRows := 0
			if scenario == "unconfirmed_commit" || scenario == "acknowledged_cleanup" {
				wantRows = 1
			}
			if faultMatrixCount(t, p.competitor, scenario) != wantRows || p.commits != wantRows || p.rollbacks != 1-wantRows {
				t.Fatalf("wrong native settlement: commits=%d rollbacks=%d want rows=%d", p.commits, p.rollbacks, wantRows)
			}
			if scenario == "acknowledged_cleanup" {
				if value, ok := result.Value(); !ok || value != scenario {
					t.Fatal("acknowledged output was erased")
				}
			} else {
				faultMatrixNoValue(t, result, wantPhase)
			}
		})
	}
}

// Retained native SQL uses WithoutCancel. Done is first observed by the retry
// backoff select, where this real caller cancellation must interrupt the wait.
type fenceBackoffCancellation struct {
	context.Context
	cancel context.CancelFunc
}

func (c fenceBackoffCancellation) Done() <-chan struct{} {
	c.cancel()
	return c.Context.Done()
}

func TestNativeRetainedPostgresFenceRetryRechecksAdmission(t *testing.T) {
	for _, scenario := range []string{"joined_evidence", "wrapped_evidence", "revoked_after_settlement", "backoff_cancel"} {
		t.Run(scenario, func(t *testing.T) {
			_, p, lease := nativeFenceBackend(t)
			session := lease.Session()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "backoff_cancel" {
				ctx = fenceBackoffCancellation{Context: ctx, cancel: cancel}
			}
			nativeCalls, domainCalls := 0, 0
			independent := errors.New("independent wrapper failure")
			native := func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
				nativeCalls++
				ack, err := postgresbackend.RunAuthorityTransactionOutcomeWithOptions(ctx, session, &sql.TxOptions{Isolation: sql.LevelSerializable}, write)
				if nativeCalls == 1 {
					if ack || !postgresbackend.IsRolledBackSerializationConflict(err) {
						t.Fatalf("missing clean native rollback proof: ack=%v err=%v", ack, err)
					}
					switch scenario {
					case "joined_evidence":
						err = errors.Join(err, independent)
					case "wrapped_evidence":
						err = fmt.Errorf("untrusted wrapper: %w", err)
					case "revoked_after_settlement":
						if revokeErr := session.ForceDiscard(); revokeErr != nil {
							t.Fatal(revokeErr)
						}
					}
				}
				return ack, err
			}
			result := run(ctx, privateactivity.DialectPostgres, Story, Ordinary, nil, nil, native,
				func(context.Context, *Attempt) (string, error) { domainCalls++; return "must-not-run", nil })
			wantPhase, wantNative := AcquireFence, 1
			if scenario == "revoked_after_settlement" {
				wantPhase, wantNative = BeforeAttempt, 2
			}
			faultMatrixNoValue(t, result, wantPhase)
			if nativeCalls != wantNative || domainCalls != 0 || p.fences != 1 || p.rollbacks != 1 || p.commits != 0 {
				t.Fatalf("refused boundary replayed: native=%d domain=%d fences=%d rollbacks=%d commits=%d err=%v", nativeCalls, domainCalls, p.fences, p.rollbacks, p.commits, result.Err())
			}
			if scenario == "joined_evidence" && !errors.Is(result.Err(), independent) {
				t.Fatalf("lost independent wrapper failure: %v", result.Err())
			}
			if scenario == "backoff_cancel" && !errors.Is(result.Err(), context.Canceled) {
				t.Fatalf("backoff ignored caller cancellation: %v", result.Err())
			}
		})
	}
}
