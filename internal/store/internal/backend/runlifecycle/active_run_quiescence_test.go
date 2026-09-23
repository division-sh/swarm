package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func quiescenceTestBackend(t *testing.T, schema string) (*sql.DB, *sqlitebackend.Backend) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return db, backend
}

func TestQuiescenceTerminatesExactSessionsInAttempt(t *testing.T) {
	db, backend := quiescenceTestBackend(t, `CREATE TABLE agent_sessions (
		session_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL,
		termination_reason TEXT, termination_detail TEXT, terminated_at TEXT,
		lease_holder TEXT, lease_expires_at TEXT, updated_at TEXT
	)`)
	runID, sessionID, inactiveID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, row := range []struct{ id, status string }{{sessionID, "active"}, {inactiveID, "terminated"}} {
		if _, err := db.Exec(`INSERT INTO agent_sessions (session_id, run_id, status) VALUES (?, ?, ?)`, row.id, runID, row.status); err != nil {
			t.Fatal(err)
		}
	}
	rollback := errors.New("test rollback")
	result := mutationprotocol.RunSQLite(context.Background(), backend, "test session quiescence", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (int, error) {
		var count int
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
			count, err = sqliteTerminateActiveRunSessionsTx(ctx, tx, attempt, []string{runID}, "test", time.Now().UTC())
			return err
		})
		if err != nil {
			return 0, err
		}
		if count != 1 {
			t.Fatalf("terminated sessions = %d, want 1", count)
		}
		return count, rollback
	})
	if !errors.Is(result.Err(), rollback) || result.Acknowledged() {
		t.Fatalf("result = acknowledged %t, error %v; want rollback", result.Acknowledged(), result.Err())
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM agent_sessions WHERE session_id = ?`, sessionID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "active" {
		t.Fatalf("status after rollback = %q, want active", status)
	}
}

func TestQuiescenceCancelsExactWorkflowTimersInAttempt(t *testing.T) {
	db, backend := quiescenceTestBackend(t, `CREATE TABLE timers (
		timer_id TEXT PRIMARY KEY, timer_name TEXT NOT NULL, fire_at TEXT NOT NULL,
		run_id TEXT NOT NULL, task_type TEXT NOT NULL, status TEXT NOT NULL
	)`)
	runID, timerID, inactiveID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	due := time.Now().UTC().Truncate(time.Second)
	for _, row := range []struct{ id, status string }{{timerID, "active"}, {inactiveID, "cancelled"}} {
		if _, err := db.Exec(`INSERT INTO timers (timer_id, timer_name, fire_at, run_id, task_type, status) VALUES (?, 'task', ?, ?, 'workflow_timer', ?)`, row.id, due.Format(time.RFC3339Nano), runID, row.status); err != nil {
			t.Fatal(err)
		}
	}
	rollback := errors.New("test rollback")
	result := mutationprotocol.RunSQLite(context.Background(), backend, "test timer quiescence", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (int, error) {
		var count int
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			refs, err := cancelActiveRunWorkflowTimersTx(ctx, tx, attempt, false, []string{runID, runID})
			if err != nil {
				return err
			}
			count = len(refs)
			if count != 1 || refs[0].ActivationID != timerID || refs[0].RunID != runID || !refs[0].DueAt.Equal(due) {
				t.Fatalf("cancelled refs = %+v", refs)
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		return count, rollback
	})
	if !errors.Is(result.Err(), rollback) || result.Acknowledged() {
		t.Fatalf("result = acknowledged %t, error %v; want rollback", result.Acknowledged(), result.Err())
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM timers WHERE timer_id = ?`, timerID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "active" {
		t.Fatalf("status after rollback = %q, want active", status)
	}
}

func TestEmptyQuiescenceRollsBackStoryInitialization(t *testing.T) {
	db, backend := quiescenceTestBackend(t, `
		CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY, last_sequence INTEGER NOT NULL);
		CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, bundle_hash TEXT);
	`)
	owner, err := NewSQLite(backend, func() error { return nil }, runhandoff.NewCandidateCoordinator(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := owner.ApplyActiveRunQuiescence(context.Background(), runtimerunquiescence.Request{
		OperationName: "test", ReasonCode: "test", ControlledBy: "test", RunIDs: []string{uuid.NewString()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Runs) != 0 {
		t.Fatalf("quiesced runs = %+v, want none", result.Runs)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_order`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("author activity order rows = %d, want 0", count)
	}
}
