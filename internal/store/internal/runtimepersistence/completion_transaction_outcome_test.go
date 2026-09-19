package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

// Only native transaction exit and a post-statement observation are injected.
// The completion mutation, SQL and independent persistence readback are real.
type completionOutcomeConnector struct {
	driver.Connector
	enabled atomic.Bool
	writes  atomic.Int32
	phase   string
	cancel  context.CancelFunc
	failure error
}

func (c *completionOutcomeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &completionOutcomeConn{pipelineCrashConn: &pipelineCrashConn{Conn: conn}, control: c}, nil
}

type completionOutcomeConn struct {
	*pipelineCrashConn
	control *completionOutcomeConnector
}

func (c *completionOutcomeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &completionOutcomeTx{Tx: tx, control: c.control}, nil
}

func (c *completionOutcomeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err == nil && completionOutcomeAgentTurnInsert(query) {
		c.control.observeWrite()
	}
	return result, err
}

func (c *completionOutcomeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err == nil && completionOutcomeAgentTurnInsert(query) {
		// PostgreSQL contributes stored coordinates via INSERT RETURNING. Observe
		// the successful native row, not query entry or an unrelated history read.
		return &completionOutcomeRows{Rows: rows, control: c.control}, nil
	}
	return rows, err
}

func completionOutcomeAgentTurnInsert(query string) bool {
	normalized := strings.Join(strings.Fields(strings.ToUpper(query)), " ")
	return strings.HasPrefix(normalized, "INSERT INTO AGENT_TURNS (")
}

func (c *completionOutcomeConnector) observeWrite() {
	if !c.enabled.Load() {
		return
	}
	c.writes.Add(1)
	if c.phase == "callback_cancel" {
		c.cancel()
	}
}

type completionOutcomeRows struct {
	driver.Rows
	control  *completionOutcomeConnector
	observed bool
}

func (r *completionOutcomeRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil && !r.observed {
		r.observed = true
		r.control.observeWrite()
	}
	return err
}

type completionOutcomeTx struct {
	driver.Tx
	control *completionOutcomeConnector
}

func (tx *completionOutcomeTx) Commit() error {
	if !tx.control.enabled.Load() {
		return tx.Tx.Commit()
	}
	if tx.control.phase == "commit_refused" {
		return tx.control.failure
	}
	if tx.control.phase == "commit_admitted_cancel" {
		tx.control.cancel()
	}
	err := tx.Tx.Commit()
	if err == nil && tx.control.phase == "commit_ack_lost" {
		return tx.control.failure
	}
	return err
}

func openCompletionOutcomeFixture(t *testing.T, backend string) (completionSettlementFixture, *completionOutcomeConnector) {
	t.Helper()
	var native driver.Connector
	if backend == "postgres" {
		dsn, _, _ := testutil.StartPostgres(t)
		var err error
		native, err = pq.NewConnector(dsn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		native = pipelineCrashSQLiteConnector{path: filepath.Join(t.TempDir(), "completion.db")}
	}
	control := &completionOutcomeConnector{Connector: native}
	db := sql.OpenDB(control)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if backend == "postgres" {
		return newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false), control
	}
	store := NewSQLiteRuntimeStoreForTest(db)
	if err := store.BootstrapSchema(testAuthorActivityContext(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
		t.Fatal(err)
	}
	store.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
	registerTestAuthorActivityCatalog(t, store)
	return newCompletionSettlementFixture(t, store, db, true), control
}

func TestCompletionTransactionAcknowledgementBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"entry_cancel", "callback_cancel", "commit_admitted_cancel", "commit_refused", "commit_ack_lost", "healthy"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				fixture, control := openCompletionOutcomeFixture(t, backend)
				handle := beginObservedCompletionForSettlementTest(t, fixture.context, "claude_cli", "completion-outcome")
				attempt := handle.Attempt()
				settlement := completionSettlementForTest(t, attempt.Authority.Target, fixture, "claude_cli", "", "")
				settlement.ProviderHead = nil
				settlement.Settlement.OperationID, settlement.Settlement.AttemptID = attempt.OperationID, attempt.AttemptID
				settlement.Settlement.Authority = attempt.Authority
				settlement.Settlement.Now = settlement.Now
				ctx, cancel := context.WithCancel(fixture.context)
				defer cancel()
				control.phase, control.cancel, control.failure = phase, cancel, errors.New("independent native COMMIT error")
				submits := 0
				sink := &completionHandoffEvidenceProbeSink{submit: func(runlifecycle.Candidate) error { submits++; return nil }}
				registration, err := fixture.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: authorActivityTestBundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				control.enabled.Store(true)
				if phase == "entry_cancel" {
					cancel()
				}
				// The selected operation owns explicit cancellation. Handle's
				// irreversible-provider settlement policy is a separate caller.
				result, err := fixture.store.SettleCompletion(ctx, attempt, settlement)
				control.enabled.Store(false)
				committed := phase == "healthy" || phase == "commit_admitted_cancel"
				if result.Committed != committed {
					t.Fatalf("committed=%v want %v: %v", result.Committed, committed, err)
				}
				if committed && (err != nil || result.AttemptID != attempt.AttemptID || !result.SpendRecorded) {
					t.Fatalf("lost acknowledged result: %#v %v", result, err)
				}
				if (phase == "entry_cancel" || phase == "callback_cancel") && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost explicit cancellation: %v", err)
				}
				if (phase == "commit_refused" || phase == "commit_ack_lost") && !errors.Is(err, control.failure) {
					t.Fatalf("lost COMMIT error: %v", err)
				}
				wantWrites, wantSubmits := int32(1), 0
				if phase == "entry_cancel" {
					wantWrites = 0
				}
				if committed {
					wantSubmits = 1
				}
				if control.writes.Load() != wantWrites || submits != wantSubmits {
					t.Fatalf("replay or unsafe handoff: writes=%d submits=%d", control.writes.Load(), submits)
				}
				if committed || phase == "commit_ack_lost" {
					requireCompletionSettlementRows(t, fixture, attempt.AttemptID, attempt.Authority.Target.ID, runtimeeffects.StateSettled, 1, 0)
				} else {
					requireCompletionSettlementRows(t, fixture, attempt.AttemptID, attempt.Authority.Target.ID, runtimeeffects.StateResponseObserved, 0, 1)
				}
			})
		}
	}
}
