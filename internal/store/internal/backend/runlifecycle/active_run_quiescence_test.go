package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func quiescenceTestBackend(t *testing.T, schema string) (*sql.DB, *sqlitebackend.Backend, context.Context) {
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
	ctx := context.Background()
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := platformschema.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.TableName != "runtime_process_generation" {
			continue
		}
		statements, err := schemastore.SQLiteStatementsForPlan(plan)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range statements {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := platformschema.SeedFreshProcessGeneration(ctx, db, plans); err != nil {
		t.Fatal(err)
	}
	var original processbinding.LeaseStamp
	acknowledged, err := backend.RunProcessAcquisitionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = generationauthority.AdvanceProcessGeneration(txctx, tx, processbinding.LeaseIdentity{
			LeaseID: uuid.NewString(), OwnerID: "quiescence-original", BootID: uuid.NewString(),
			RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend,
		}, true)
		return err
	})
	if err != nil || !acknowledged {
		t.Fatalf("original quiescence acquisition: acknowledged=%v err=%v", acknowledged, err)
	}
	ctx, err = generationauthority.MutationContext(ctx, original, false)
	if err != nil {
		t.Fatal(err)
	}
	return db, backend, ctx
}

func TestQuiescenceTerminatesExactSessionsInAttempt(t *testing.T) {
	db, backend, ctx := quiescenceTestBackend(t, `CREATE TABLE agent_sessions (
		session_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL,
		termination_reason TEXT, termination_detail TEXT, terminated_at TEXT,
		lease_holder TEXT, lease_grant_id TEXT, lease_expires_at TEXT, updated_at TEXT
	)`)
	runID, sessionID, inactiveID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, row := range []struct{ id, status string }{{sessionID, "active"}, {inactiveID, "terminated"}} {
		if _, err := db.Exec(`INSERT INTO agent_sessions (session_id, run_id, status) VALUES (?, ?, ?)`, row.id, runID, row.status); err != nil {
			t.Fatal(err)
		}
	}
	rollback := errors.New("test rollback")
	result := mutationprotocol.RunSQLite(ctx, backend, "test session quiescence", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (int, error) {
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
	db, backend, ctx := quiescenceTestBackend(t, `CREATE TABLE timers (
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
	result := mutationprotocol.RunSQLite(ctx, backend, "test timer quiescence", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (int, error) {
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
