package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
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

			insertTimerObligationProofRow(t, ctx, selected, runID, "timer", observedAt)
			workflow := newWorkflowTimerDDLProofRow(runID)
			workflow.createdAt = observedAt.Add(-time.Hour)
			workflow.fireAt = observedAt.Add(time.Hour)
			if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, workflow); err != nil {
				t.Fatalf("insert workflow timer obligation: %v", err)
			}

			terminalRunID := uuid.NewString()
			requireRunningRunForTest(t, ctx, selected, terminalRunID, observedAt.Add(-time.Hour))
			insertTimerObligationProofRow(t, ctx, selected, terminalRunID, "timer", observedAt)
			transitionGenericScheduleRun(t, selected, terminalRunID, true)
			insertTimerObligationProofRow(t, ctx, selected, "", "global_recurring", observedAt.Add(time.Hour))

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
			assertTimerFamilyObligation(t, completed.Families, runtimetimerobligation.FamilyTimer, 0, 0, 0)
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
			insertTimerObligationProofRow(t, ctx, selected, runID, "scheduled_task", observedAt.Add(time.Hour))

			var (
				quiescence operatorread.RunTestQuiescence
				err        error
			)
			switch store := selected.(type) {
			case *PostgresStore:
				quiescence, err = store.LoadRunTestQuiescence(ctx, runID, observedAt)
			case *SQLiteRuntimeStore:
				quiescence, err = store.LoadRunTestQuiescence(ctx, runID, observedAt)
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

func insertTimerObligationProofRow(
	t *testing.T,
	ctx context.Context,
	selected any,
	runID, family string,
	fireAt time.Time,
) {
	t.Helper()
	store, ok := selected.(runtimegenericschedule.Store)
	if !ok {
		t.Fatalf("selected store %T lacks generic schedule admission", selected)
	}
	command := timerObligationProofCommand(t, runID, family, fireAt)
	setGenericScheduleClock(t, store, func() time.Time { return fireAt.Add(-time.Hour) })
	if admitted, err := store.AdmitGenericSchedule(ctx, command); err != nil || admitted.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("admit %s timer obligation row = %#v, %v", family, admitted, err)
	}
}

func insertTimerObligationProofRowTx(
	t *testing.T,
	ctx context.Context,
	tx *sql.Tx,
	selected any,
	runID, family string,
	fireAt time.Time,
) {
	t.Helper()
	command := timerObligationProofCommand(t, runID, family, fireAt)
	var (
		admitted runtimegenericschedule.AdmissionResult
		err      error
	)
	switch store := selected.(type) {
	case *PostgresStore:
		store.genericSchedulePostgresOwner.SetNowFnForTest(func() time.Time { return fireAt.Add(-time.Hour) })
		admitted, err = store.genericSchedulePostgresOwner.AdmitTx(ctx, tx, command)
	case *SQLiteRuntimeStore:
		store.genericScheduleSQLiteOwner.SetNowFnForTest(func() time.Time { return fireAt.Add(-time.Hour) })
		admitted, err = store.genericScheduleSQLiteOwner.AdmitTx(ctx, tx, command)
	default:
		t.Fatalf("unsupported selected store %T", selected)
	}
	if err != nil || admitted.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("admit transactional %s timer obligation row = %#v, %v", family, admitted, err)
	}
}

func timerObligationProofCommand(t *testing.T, runID, family string, fireAt time.Time) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	key := "obligation-" + family + "-" + uuid.NewString()
	switch family {
	case "timer":
		return testAgentGenericScheduleCommand(t, runID, "obligation-agent", "obligation/instance", uuid.NewString(), key, runtimegenericschedule.AbsoluteDue(fireAt))
	case "scheduled_task":
		return testAgentGenericScheduleCommand(t, runID, "obligation-agent", "obligation/instance", uuid.NewString(), key, runtimegenericschedule.EveryDue(time.Hour))
	case "global_recurring":
		return runtimegenericschedule.AdmissionCommand{
			ScheduleKey: key, OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "runtime",
			EventType: "platform.timer_obligation_proof", Payload: semanticvalue.EmptyObject(),
			RoutingSource: events.NewPlatformControlRoutingSource(), ExecutionMode: executionmode.Live,
			Due: runtimegenericschedule.EveryDue(time.Hour), TaskID: key,
		}
	default:
		t.Fatalf("unsupported timer obligation proof family %q", family)
		return runtimegenericschedule.AdmissionCommand{}
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
