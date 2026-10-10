package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestGenericScheduleExactEffectsAdmissionPreparationCancellationBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			postgres := tc.name == "postgres"
			at := time.Now().UTC().Truncate(time.Microsecond)
			var owner interface {
				SetNowFnForTest(func() time.Time)
				AdmitTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error)
				PrepareOccurrenceTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.Wakeup, time.Time) (runtimegenericschedule.PreparedOccurrence, error)
				CancelAdmissionTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.AdmissionCommand, string, time.Time) (runtimegenericschedule.CancelResult, error)
				CancelActivationTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelResult, error)
			}
			switch selected := selected.(type) {
			case *PostgresStore:
				owner = selected.genericSchedulePostgresOwner
			case *SQLiteRuntimeStore:
				owner = selected.genericScheduleSQLiteOwner
			default:
				t.Fatalf("unsupported exact-effects store %T", selected)
			}
			owner.SetNowFnForTest(func() time.Time { return at })
			head := func() int64 {
				t.Helper()
				var revision int64
				if err := db.QueryRowContext(ctx, `SELECT COALESCE((SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1),0)`, runID).Scan(&revision); err != nil {
					t.Fatal(err)
				}
				return revision
			}
			run := func(label string, factID func() string, write func(context.Context, *mutationprotocol.Attempt) error) {
				t.Helper()
				before := head()
				if err := runSelectedFixtureMutation(ctx, selected, label, write); err != nil {
					t.Fatalf("%s: %v", label, err)
				}
				after := head()
				if factID == nil {
					if after != before {
						t.Fatalf("%s no-op advanced revision from %d to %d", label, before, after)
					}
					return
				}
				if after != before+1 {
					t.Fatalf("%s revision=%d, want %d", label, after, before+1)
				}
				var total, exact int
				if err := db.QueryRowContext(ctx, `
					SELECT COUNT(*), COALESCE(SUM(CASE WHEN family='timers' AND fact_key=$3 AND present THEN 1 ELSE 0 END),0)
					FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2
				`, runID, after, factID()).Scan(&total, &exact); err != nil {
					t.Fatal(err)
				}
				if total != 1 || exact != 1 {
					t.Fatalf("%s revision %d timer facts total=%d exact=%d, want 1/1", label, after, total, exact)
				}
			}
			command := testRootGenericScheduleCommand(t, runID, uuid.NewString(), "exact-timer", runtimegenericschedule.DelayDue(time.Minute))
			var created runtimegenericschedule.AdmissionResult
			run("timer admission", func() string { return created.Activation.ID }, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				var err error
				created, err = owner.AdmitTx(txctx, attempt, command)
				if err == nil && created.Outcome != runtimegenericschedule.AdmissionCreated {
					t.Fatalf("admit: %+v", created)
				}
				return err
			})
			run("timer exact replay", nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				owner.SetNowFnForTest(func() time.Time { return at.Add(time.Hour) })
				replayed, err := owner.AdmitTx(txctx, attempt, command)
				owner.SetNowFnForTest(func() time.Time { return at })
				if err == nil && (replayed.Outcome != runtimegenericschedule.AdmissionExactReplay || replayed.Activation.ID != created.Activation.ID) {
					t.Fatalf("admit replay: %+v", replayed)
				}
				return err
			})
			wakeup, err := created.Activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			run("timer occurrence preparation", func() string { return created.Activation.ID }, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				prepared, err := owner.PrepareOccurrenceTx(txctx, attempt, wakeup, at.Add(time.Minute))
				if err == nil && prepared.Activation.ID != created.Activation.ID {
					t.Fatalf("prepare: %+v", prepared)
				}
				return err
			})
			run("timer cancellation", func() string { return created.Activation.ID }, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				cancelled, err := owner.CancelAdmissionTx(txctx, attempt, command, "operator_cancelled", at.Add(2*time.Minute))
				if err == nil && cancelled.Outcome != runtimegenericschedule.CancelChanged {
					t.Fatalf("cancel immutable admission: %+v", cancelled)
				}
				return err
			})
			run("timer cancel replay", nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				_, err := owner.CancelActivationTx(txctx, attempt, runtimegenericschedule.CancelCommand{
					ActivationID: created.Activation.ID, Cause: "operator_cancelled", CancelledAt: at.Add(2 * time.Minute),
				})
				return err
			})
			if !postgres {
				return
			}
			command.ScheduleKey, command.TaskID = "malformed-exact-timer", "malformed-exact-timer"
			var malformed runtimegenericschedule.AdmissionResult
			run("malformed timer admission", func() string { return malformed.Activation.ID }, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				var err error
				malformed, err = owner.AdmitTx(txctx, attempt, command)
				return err
			})
			if _, err := db.ExecContext(ctx, `UPDATE timers SET immutable_hash='corrupt' WHERE timer_id=$1::uuid`, malformed.Activation.ID); err != nil {
				t.Fatal(err)
			}
			variantWakeup, err := runtimegenericschedule.NewWakeup(strings.ToUpper(malformed.Activation.ID), malformed.Activation.CurrentDueAt)
			if err != nil {
				t.Fatal(err)
			}
			run("malformed timer preparation", func() string { return malformed.Activation.ID }, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				prepared, err := owner.PrepareOccurrenceTx(txctx, attempt, variantWakeup, at.Add(time.Minute))
				if err == nil && prepared.Outcome != runtimegenericschedule.PrepareTerminal {
					t.Fatalf("malformed variant prepare: %+v", prepared)
				}
				return err
			})
			run("malformed timer prepare replay", nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				_, err := owner.PrepareOccurrenceTx(txctx, attempt, variantWakeup, at.Add(time.Minute))
				return err
			})
			for _, spelling := range []string{strings.ToUpper(runID), strings.ReplaceAll(runID, "-", "")} {
				variantCommand := testRootGenericScheduleCommand(t, spelling, command.EntityID, "run-spelling-"+spelling, runtimegenericschedule.DelayDue(time.Minute))
				var admitted runtimegenericschedule.AdmissionResult
				run("run spelling admission", func() string { return admitted.Activation.ID }, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
					var err error
					admitted, err = owner.AdmitTx(txctx, attempt, variantCommand)
					if err == nil && admitted.Outcome != runtimegenericschedule.AdmissionCreated {
						t.Fatalf("run spelling admission: %+v", admitted)
					}
					return err
				})
			}
		})
	}
}
