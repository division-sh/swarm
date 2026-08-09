package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

func TestTimerObligationSnapshotObservationBoundaryOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, ctx := tc.open(t)
			reader, ok := selected.(runtimetimerobligation.Reader)
			if !ok {
				t.Fatalf("selected store %T lacks timer obligation reader", selected)
			}
			runID := runtimecorrelationRunID(t, ctx)
			observedAt := time.Now().UTC().Truncate(time.Microsecond)

			insertTimerObligationProofRow(t, ctx, db, selected, runID, "timer", observedAt)
			workflow := newWorkflowTimerDDLProofRow(runID)
			workflow.createdAt = observedAt.Add(-time.Hour)
			workflow.fireAt = observedAt.Add(time.Hour)
			if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, workflow); err != nil {
				t.Fatalf("insert workflow timer obligation: %v", err)
			}

			terminalRunID := uuid.NewString()
			insertTimerObligationProofRun(t, ctx, db, selected, terminalRunID, "cancelled")
			insertTimerObligationProofRow(t, ctx, db, selected, terminalRunID, "deadline", observedAt)
			insertTimerObligationProofRow(t, ctx, db, selected, "", "global_recurring", observedAt.Add(time.Hour))

			snapshot, err := reader.ReadTimerObligations(ctx, runtimetimerobligation.All(), observedAt)
			if err != nil {
				t.Fatalf("read all timer obligations: %v", err)
			}
			if !snapshot.ObservedAt.Equal(observedAt) {
				t.Fatalf("snapshot observed_at = %s, want %s", snapshot.ObservedAt, observedAt)
			}
			running, ok := snapshot.Run(runID)
			if !ok {
				t.Fatalf("snapshot omitted running run %s", runID)
			}
			assertTimerFamilyObligation(t, running.Families, runtimetimerobligation.FamilyTimer, 1, 1, 1)
			assertTimerFamilyObligation(t, running.Families, runtimetimerobligation.FamilyWorkflowTimer, 1, 0, 1)
			completed, ok := snapshot.Run(terminalRunID)
			if !ok {
				t.Fatalf("snapshot omitted terminal run %s", terminalRunID)
			}
			assertTimerFamilyObligation(t, completed.Families, runtimetimerobligation.FamilyDeadline, 1, 1, 0)
			assertTimerFamilyObligation(t, snapshot.GlobalFamilies, runtimetimerobligation.FamilyGlobalRecurring, 1, 0, 1)

			scope, err := runtimetimerobligation.Run(runID)
			if err != nil {
				t.Fatalf("construct run timer scope: %v", err)
			}
			before, err := reader.ReadTimerObligations(ctx, scope, observedAt)
			if err != nil {
				t.Fatalf("read scoped obligations before transaction: %v", err)
			}
			assertTimerFamilyObligation(t, mustTimerRun(t, before, runID).Families, runtimetimerobligation.FamilyScheduledTask, 0, 0, 0)
			assertTimerFamilyObligation(t, before.GlobalFamilies, runtimetimerobligation.FamilyGlobalRecurring, 0, 0, 0)

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin timer obligation proof transaction: %v", err)
			}
			insertTimerObligationProofRowTx(t, ctx, tx, selected, runID, "scheduled_task", observedAt)
			during, err := reader.ReadTimerObligations(ctx, scope, observedAt)
			if err != nil {
				_ = tx.Rollback()
				t.Fatalf("read timer obligations while ambient transaction is open: %v", err)
			}
			assertTimerFamilyObligation(t, mustTimerRun(t, during, runID).Families, runtimetimerobligation.FamilyScheduledTask, 0, 0, 0)
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback timer obligation proof transaction: %v", err)
			}
			after, err := reader.ReadTimerObligations(ctx, scope, observedAt)
			if err != nil {
				t.Fatalf("read scoped obligations after rollback: %v", err)
			}
			assertTimerFamilyObligation(t, mustTimerRun(t, after, runID).Families, runtimetimerobligation.FamilyScheduledTask, 0, 0, 0)
		})
	}
}

func TestTimerObligationSnapshotDrivesRunDiagnosisOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, ctx := tc.open(t)
			runID := runtimecorrelationRunID(t, ctx)
			observedAt := time.Now().UTC().Truncate(time.Microsecond)
			workflow := newWorkflowTimerDDLProofRow(runID)
			workflow.createdAt = observedAt.Add(-time.Hour)
			workflow.fireAt = observedAt
			if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, workflow); err != nil {
				t.Fatalf("insert due workflow timer: %v", err)
			}
			insertTimerObligationProofRow(t, ctx, db, selected, runID, "scheduled_task", observedAt.Add(time.Hour))

			var (
				quiescence RunTestQuiescence
				err        error
			)
			switch store := selected.(type) {
			case *PostgresStore:
				quiescence, err = store.loadRunTestQuiescence(ctx, runID, observedAt)
			case *SQLiteRuntimeStore:
				quiescence, err = store.sqliteRunTestQuiescence(ctx, runID, observedAt)
			default:
				t.Fatalf("unsupported selected store %T", selected)
			}
			if err != nil {
				t.Fatalf("load run diagnosis quiescence: %v", err)
			}
			if quiescence.DueTimers != 1 || quiescence.Ready {
				t.Fatalf("run diagnosis quiescence = %#v, want one due workflow timer and not ready", quiescence)
			}
		})
	}
}

func runtimecorrelationRunID(t *testing.T, ctx context.Context) string {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if runID == "" {
		t.Fatal("timer obligation proof context lacks run_id")
	}
	return runID
}

func insertTimerObligationProofRun(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	selected runtimepipeline.SchedulePersistence,
	runID, status string,
) {
	t.Helper()
	sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		t.Fatal("timer obligation proof context lacks bundle source fact")
	}
	state, err := storerunlifecycle.ParseState(status)
	if err != nil {
		t.Fatalf("parse %s timer obligation run state: %v", status, err)
	}
	bundleHash, bundleSource := sourceFact.StorageValues()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: runID, State: state, BundleHash: bundleHash, BundleSource: bundleSource,
		StartedAt: time.Now().UTC().Add(-time.Hour), EndedAt: time.Now().UTC(),
	})
}

func insertTimerObligationProofRow(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	selected runtimepipeline.SchedulePersistence,
	runID, family string,
	fireAt time.Time,
) {
	t.Helper()
	insertTimerObligationProofRowTx(t, ctx, db, selected, runID, family, fireAt)
}

type timerObligationProofExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertTimerObligationProofRowTx(
	t *testing.T,
	ctx context.Context,
	exec timerObligationProofExecutor,
	selected runtimepipeline.SchedulePersistence,
	runID, family string,
	fireAt time.Time,
) {
	t.Helper()
	timerID := uuid.NewString()
	query := `
		INSERT INTO timers (
			timer_id, run_id, timer_name, fire_event, routing_source, fire_at, recurring,
			owner_agent, owner_kind, task_type, status, created_at
		) VALUES (?, NULLIF(?, ''), ?, 'timer.proof', '{"kind":"platform_control","route":{}}', ?, ?, 'timer-proof', 'system', ?, 'active', ?)
	`
	args := []any{timerID, runID, "proof-" + timerID, fireAt, family == "scheduled_task" || family == "global_recurring", family, fireAt.Add(-time.Hour)}
	if _, ok := selected.(*PostgresStore); ok {
		query = `
			INSERT INTO timers (
				timer_id, run_id, timer_name, fire_event, routing_source, fire_at, recurring,
				owner_agent, owner_kind, task_type, status, created_at
			) VALUES ($1::uuid, NULLIF($2, '')::uuid, $3, 'timer.proof', '{"kind":"platform_control","route":{}}'::jsonb, $4, $5, 'timer-proof', 'system', $6, 'active', $7)
		`
	}
	if _, err := exec.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("insert %s timer obligation row: %v", family, err)
	}
}

func mustTimerRun(t *testing.T, snapshot runtimetimerobligation.Snapshot, runID string) runtimetimerobligation.RunObligations {
	t.Helper()
	run, ok := snapshot.Run(runID)
	if !ok {
		t.Fatalf("timer obligation snapshot omitted requested run %s", runID)
	}
	return run
}

func assertTimerFamilyObligation(
	t *testing.T,
	families []runtimetimerobligation.FamilyObligation,
	family runtimetimerobligation.Family,
	active, due, recoverable int,
) {
	t.Helper()
	for _, got := range families {
		if got.Family != family {
			continue
		}
		if got.ActiveCount != active || got.DueCount != due || got.RecoverableCount != recoverable {
			t.Fatalf("%s obligation = %#v, want active=%d due=%d recoverable=%d", family, got, active, due, recoverable)
		}
		return
	}
	t.Fatalf("timer obligation family %s is missing", family)
}
