package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// The existing constructor dependency still delegates to the real lifecycle
// owner. Only the final SQL statement introduces native SQLite lock contention.
type llmCandidateRetryProbe struct {
	request func(context.Context, *mutationprotocol.Attempt, *sql.Tx, string, *time.Time) (runlifecycle.CandidateRequestResult, error)
}

func (p llmCandidateRetryProbe) WriteCompletionCandidateTx(ctx context.Context, attempt *mutationprotocol.Attempt, run string, due *time.Time) (result runlifecycle.CandidateRequestResult, err error) {
	err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		result, err = p.request(ctx, attempt, tx, run, due)
		return err
	})
	return result, err
}

func llmSQLiteBusyBlocker(t *testing.T, db *sql.DB, path, phase string, retainedConnections int) *sql.Tx {
	t.Helper()
	if got := db.Stats().InUse; got != retainedConnections {
		t.Fatalf("native BUSY fixture has %d retained connections, want %d", got, retainedConnections)
	}
	// Keep exactly one workload connection for the connection-local PRAGMAs,
	// without starving an already retained ownership-proof connection.
	db.SetMaxOpenConns(1 + retainedConnections)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=1`); err != nil {
		t.Fatal(err)
	}
	attached := phase == "callback" || path == ""
	lockPath := path
	if attached {
		lockPath = filepath.Join(t.TempDir(), "lock.db")
	} else {
		var mode string
		if err := db.QueryRow(`PRAGMA journal_mode=DELETE`).Scan(&mode); err != nil || mode != "delete" {
			t.Fatalf("rollback journal=%s err=%v", mode, err)
		}
	}
	other, err := sql.Open("sqlite", "file:"+lockPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	if attached {
		if _, err := other.Exec(`CREATE TABLE retry_lock (id INTEGER PRIMARY KEY,value INTEGER NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		if _, err := other.Exec(`INSERT INTO retry_lock VALUES (1,0)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`ATTACH DATABASE $1 AS llm_retry_probe`, lockPath); err != nil {
			t.Fatal(err)
		}
	}
	blocker, err := other.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback() })
	if phase == "callback" {
		if _, err := blocker.Exec(`UPDATE retry_lock SET value=1 WHERE id=1`); err != nil {
			t.Fatal(err)
		}
	} else {
		var count int
		query := `SELECT COUNT(*) FROM agent_sessions`
		if attached {
			query = `SELECT COUNT(*) FROM retry_lock`
		}
		if err := blocker.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
	}
	return blocker
}

func newLLMRetryOwner(t *testing.T, store *SQLiteRuntimeStore, blocker *sql.Tx, phase string, perAttempt int, observe func(context.Context, *sql.Tx) error) (*llmpersistence.LLMSQLiteOwner, *int) {
	t.Helper()
	calls := 0
	probe := llmCandidateRetryProbe{request: func(ctx context.Context, attempt *mutationprotocol.Attempt, tx *sql.Tx, run string, due *time.Time) (runlifecycle.CandidateRequestResult, error) {
		calls++
		if calls == perAttempt+1 {
			if err := blocker.Rollback(); err != nil {
				return runlifecycle.CandidateRequestResult{}, err
			}
		}
		result, err := store.runLifecycleSQLiteOwner.WriteCompletionCandidateTx(ctx, attempt, run, due)
		if err != nil {
			return result, err
		}
		if observe != nil {
			if err := observe(ctx, tx); err != nil {
				return result, err
			}
		}
		if phase == "callback" && calls == perAttempt {
			_, err := tx.ExecContext(ctx, `UPDATE llm_retry_probe.retry_lock SET value=2 WHERE id=1`)
			var coded interface{ Code() int }
			if !errors.As(err, &coded) || coded.Code()&255 != 5 {
				return result, fmt.Errorf("expected native SQLITE_BUSY after LLM writes, got %v", err)
			}
			return result, err
		}
		if phase == "commit" && calls == perAttempt {
			_, err := tx.ExecContext(ctx, `UPDATE llm_retry_probe.retry_lock SET value=2 WHERE id=1`)
			return result, err
		}
		return result, nil
	}}
	owner, err := llmpersistence.NewSQLite(store.backend, store.requireCurrentSchema, probe, store.runLifecycleCandidates, store.now)
	if err != nil {
		t.Fatal(err)
	}
	return owner, &calls
}

func assertLLMRetryCounts(t *testing.T, probe *transactiontest.Collector, phase string) {
	t.Helper()
	c := probe.Snapshot()
	commits, failures, finalizations := uint64(1), uint64(0), uint64(1)
	if phase == "commit" {
		commits, failures, finalizations = 2, 1, 2
	}
	if c.Total.Begun != 2 || c.Total.WriteCommits != 1 || c.Total.CommitAttempts != commits || c.Total.CommitFailures != failures || c.Total.RollbackAttempts != 1 || c.Total.Revision.Finalizations != finalizations || c.Active != 0 {
		t.Fatalf("actual LLM retry accounting: %+v", c)
	}
	t.Logf("native %s BUSY: %+v", phase, c.Total)
}

func TestLLMSQLiteResetBusyRetryPreservesExactSummary(t *testing.T) {
	for _, phase := range []string{"callback", "commit"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "reset.db")
			store := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
			s := exactFactStore{db: store.backend.ConstructionHandle(), selected: store}
			f := newLLMResetFixture(t, s)
			terminal := llmTerminalSnapshot(t, s, f.terminal)
			var submitted []runlifecycle.Candidate
			registerLLMResetSink(t, s, f.runs[0], &submitted)
			blocker := llmSQLiteBusyBlocker(t, s.db, path, phase, 1)
			owner, calls := newLLMRetryOwner(t, store, blocker, phase, 2, nil)
			probe, restore, err := store.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), store)
			summary, err := owner.ResetAll(ctx, sessions.ResetMetadata{Source: "exact-reset"})
			if err != nil {
				t.Fatal(err)
			}
			if *calls != 4 {
				t.Fatalf("candidate calls=%d want two runs per attempt", *calls)
			}
			assertLLMResetSummary(t, f, summary)
			assertLLMResetHistory(t, s, f, true)
			assertLLMRetryCounts(t, probe, phase)
			if got := llmTerminalSnapshot(t, s, f.terminal); !reflect.DeepEqual(got, terminal) {
				t.Fatalf("terminal evidence changed: got=%v want=%v", got, terminal)
			}
			seen := make(map[string]int)
			for _, c := range submitted {
				seen[c.RunID]++
			}
			if len(submitted) != 2 || seen[f.runs[0]] != 1 || seen[f.runs[1]] != 1 {
				t.Fatalf("retry leaked candidate handoffs: %+v", submitted)
			}
		})
	}
}
