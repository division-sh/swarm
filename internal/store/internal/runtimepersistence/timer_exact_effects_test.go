package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	privategenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestGenericScheduleExactEffectsAdmissionPreparationCancellationBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			postgres := tc.name == "postgres"
			at := time.Now().UTC().Truncate(time.Microsecond)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			assertEffects := func(got *runforkrevision.Effects, ids ...string) {
				t.Helper()
				want := runforkrevision.NewEffects()
				for _, id := range ids {
					if err := want.AddFact(runID, runforkrevision.FamilyTimers, id); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("generic timer contributions are not exactly %v", ids)
				}
				// Use the same verified collector for the actual history cut.
				if postgres {
					_, err = runforkrevision.FinalizePostgres(ctx, tx, got)
				} else {
					_, err = runforkrevision.FinalizeSQLite(ctx, tx, got)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			command := testRootGenericScheduleCommand(t, runID, uuid.NewString(), "exact-timer", runtimegenericschedule.DelayDue(time.Minute))
			effects := runforkrevision.NewEffects()
			created, err := privategenericschedule.AdmitTx(ctx, tx, postgres, effects, command, func() time.Time { return at })
			if err != nil || created.Outcome != runtimegenericschedule.AdmissionCreated {
				t.Fatalf("admit: %+v: %v", created, err)
			}
			assertEffects(effects, created.Activation.ID)
			effects = runforkrevision.NewEffects()
			replayed, err := privategenericschedule.AdmitTx(ctx, tx, postgres, effects, command, func() time.Time { return at.Add(time.Hour) })
			if err != nil || replayed.Outcome != runtimegenericschedule.AdmissionExactReplay || replayed.Activation.ID != created.Activation.ID {
				t.Fatalf("admit replay: %+v: %v", replayed, err)
			}
			assertEffects(effects)
			wakeup, err := created.Activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			effects = runforkrevision.NewEffects()
			prepared, err := privategenericschedule.PrepareOccurrenceTx(ctx, tx, postgres, effects, wakeup, at.Add(time.Minute))
			if err != nil || prepared.Activation.ID != created.Activation.ID {
				t.Fatalf("prepare: %+v: %v", prepared, err)
			}
			assertEffects(effects, created.Activation.ID)
			effects = runforkrevision.NewEffects()
			cancelled, err := privategenericschedule.CancelAdmissionTx(ctx, tx, postgres, effects, command, "operator_cancelled", at.Add(2*time.Minute))
			if err != nil || cancelled.Outcome != runtimegenericschedule.CancelChanged {
				t.Fatalf("cancel immutable admission: %+v: %v", cancelled, err)
			}
			assertEffects(effects, created.Activation.ID)
			effects = runforkrevision.NewEffects()
			_, err = privategenericschedule.CancelTx(ctx, tx, postgres, effects, runtimegenericschedule.CancelCommand{
				ActivationID: created.Activation.ID, Cause: "operator_cancelled", CancelledAt: at.Add(2 * time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			assertEffects(effects)
			if postgres {
				command.ScheduleKey, command.TaskID = "malformed-exact-timer", "malformed-exact-timer"
				effects = runforkrevision.NewEffects()
				malformed, err := privategenericschedule.AdmitTx(ctx, tx, postgres, effects, command, func() time.Time { return at })
				if err != nil {
					t.Fatal(err)
				}
				assertEffects(effects, malformed.Activation.ID)
				if _, err := tx.ExecContext(ctx, `UPDATE timers SET immutable_hash='corrupt' WHERE timer_id=$1::uuid`, malformed.Activation.ID); err != nil {
					t.Fatal(err)
				}
				variantWakeup, err := runtimegenericschedule.NewWakeup(strings.ToUpper(malformed.Activation.ID), malformed.Activation.CurrentDueAt)
				if err != nil {
					t.Fatal(err)
				}
				effects = runforkrevision.NewEffects()
				prepared, err := privategenericschedule.PrepareOccurrenceTx(ctx, tx, postgres, effects, variantWakeup, at.Add(time.Minute))
				if err != nil || prepared.Outcome != runtimegenericschedule.PrepareTerminal {
					t.Fatalf("malformed variant prepare: %+v: %v", prepared, err)
				}
				assertEffects(effects, malformed.Activation.ID)
				effects = runforkrevision.NewEffects()
				if _, err := privategenericschedule.PrepareOccurrenceTx(ctx, tx, postgres, effects, variantWakeup, at.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				assertEffects(effects)
				for _, spelling := range []string{strings.ToUpper(runID), strings.ReplaceAll(runID, "-", "")} {
					variantCommand := testRootGenericScheduleCommand(t, spelling, command.EntityID, "run-spelling-"+spelling, runtimegenericschedule.DelayDue(time.Minute))
					effects = runforkrevision.NewEffects()
					admitted, err := privategenericschedule.AdmitTx(ctx, tx, postgres, effects, variantCommand, func() time.Time { return at })
					if err != nil || admitted.Outcome != runtimegenericschedule.AdmissionCreated {
						t.Fatalf("run spelling admission: %+v: %v", admitted, err)
					}
					assertEffects(effects, admitted.Activation.ID)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
