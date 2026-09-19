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

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

// The existing constructor dependency still delegates to the real lifecycle
// owner. Only the final SQL statement introduces native SQLite lock contention.
type llmCandidateRetryProbe struct {
	request func(context.Context, *sql.Tx, string, *time.Time, *runhandoff.CandidateHandoff) (runlifecycle.CandidateRequestResult, error)
}

func (p llmCandidateRetryProbe) RequestCompletionCandidateTx(ctx context.Context, tx *sql.Tx, run string, due *time.Time, handoff *runhandoff.CandidateHandoff) (runlifecycle.CandidateRequestResult, error) {
	return p.request(ctx, tx, run, due, handoff)
}

func llmSQLiteBusyBlocker(t *testing.T, db *sql.DB, path, phase string) *sql.Tx {
	t.Helper()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=1`); err != nil {
		t.Fatal(err)
	}
	lockPath := path
	if phase == "callback" {
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
	if phase == "callback" {
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
		if err := blocker.QueryRow(`SELECT COUNT(*) FROM agent_sessions`).Scan(&count); err != nil {
			t.Fatal(err)
		}
	}
	return blocker
}

func newLLMRetryOwner(t *testing.T, store *SQLiteRuntimeStore, blocker *sql.Tx, phase string, perAttempt int, observe func(context.Context, *sql.Tx) error) (*llmpersistence.LLMSQLiteOwner, *int) {
	t.Helper()
	calls := 0
	probe := llmCandidateRetryProbe{request: func(ctx context.Context, tx *sql.Tx, run string, due *time.Time, handoff *runhandoff.CandidateHandoff) (runlifecycle.CandidateRequestResult, error) {
		calls++
		if calls == perAttempt+1 {
			if err := blocker.Rollback(); err != nil {
				return runlifecycle.CandidateRequestResult{}, err
			}
		}
		result, err := store.runLifecycleSQLiteOwner.RequestCompletionCandidateTx(ctx, tx, run, due, handoff)
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
			blocker := llmSQLiteBusyBlocker(t, s.db, path, phase)
			owner, calls := newLLMRetryOwner(t, store, blocker, phase, 2, nil)
			probe, restore, err := store.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			summary, err := owner.ResetAll(sessions.ResetMetadata{Source: "exact-reset"})
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

func TestLLMSQLiteGeneratedSessionBusyRetry(t *testing.T) {
	for _, operation := range []string{"acquire", "rotate"} {
		for _, phase := range []string{"callback", "commit"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "generated.db")
				store := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
				s := exactFactStore{db: store.backend.ConstructionHandle(), selected: store}
				f := newExactFactFixture(t, s)
				identity := mustTestAgentIdentityForRun(f.runID, "revision-matrix-agent", "")
				var oldID string
				if operation == "rotate" {
					oldID, identity = seedLLMExactSession(t, s, f.runID, "rotating-agent", "active")
					exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
						effects := exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyAgentSessions, oldID))
						if _, err := runforkrevision.FinalizeSQLite(ctx, tx, effects); err != nil {
							t.Fatal(err)
						}
					})
				}
				var submitted []runlifecycle.Candidate
				registerLLMResetSink(t, s, f.runID, &submitted)
				blocker := llmSQLiteBusyBlocker(t, s.db, path, phase)
				var generated []string
				owner, calls := newLLMRetryOwner(t, store, blocker, phase, 1, func(ctx context.Context, tx *sql.Tx) error {
					var id string
					if err := tx.QueryRowContext(ctx, `SELECT session_id FROM agent_sessions WHERE run_id=$1 AND status='active'`, f.runID).Scan(&id); err != nil {
						return err
					}
					generated = append(generated, id)
					return nil
				})
				probe, restore, err := store.backend.InstallTransactionProbeForTest(transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				ctx, cancel := context.WithTimeout(runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure), 10*time.Second)
				defer cancel()
				var lease *sessions.Lease
				if operation == "rotate" {
					lease, err = owner.Rotate(ctx, identity, "retry-worker", sessions.RotationMetadata{RetryReason: "exact-retry"})
				} else {
					var conversationSession string
					acquired, conversation, acquireErr := owner.AcquireLiveSession(ctx, agentmemory.Identity(identity), "retry-worker")
					lease, err, conversationSession = acquired, acquireErr, conversation.SessionID
					if err == nil && (lease == nil || conversationSession != lease.SessionID) {
						t.Fatalf("acquire lost committed conversation: %+v / %s", lease, conversationSession)
					}
				}
				if err != nil || lease == nil {
					t.Fatalf("generated session retry: lease=%+v err=%v", lease, err)
				}
				if *calls != 2 || len(generated) != 2 || generated[0] == generated[1] || lease.SessionID != generated[1] {
					t.Fatalf("generated attempts=%v calls=%d lease=%+v", generated, *calls, lease)
				}
				assertLLMRetryCounts(t, probe, phase)
				for i, id := range generated {
					for _, query := range []string{`SELECT COUNT(*) FROM agent_sessions WHERE session_id=$1`, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE family='agent_sessions' AND fact_key=$1`} {
						var count int
						if err := s.db.QueryRow(query, id).Scan(&count); err != nil || count != i {
							t.Fatalf("attempt %d ID=%s count=%d want=%d err=%v", i, id, count, i, err)
						}
					}
				}
				if oldID != "" {
					var successor, status string
					if err := s.db.QueryRow(`SELECT successor_session_id,status FROM agent_sessions WHERE session_id=$1`, oldID).Scan(&successor, &status); err != nil || successor != lease.SessionID || status != "terminated" {
						t.Fatalf("rotation predecessor successor=%s status=%s err=%v", successor, status, err)
					}
				}
				exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					if err := runforkrevision.ValidateCompleteSQLite(ctx, tx, f.runID); err != nil {
						t.Fatal(err)
					}
				})
				if len(submitted) != 1 {
					t.Fatalf("retry leaked candidate handoffs: %+v", submitted)
				}
			})
		}
	}
}
