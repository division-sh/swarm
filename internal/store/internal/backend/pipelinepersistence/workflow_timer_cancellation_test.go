package pipelinepersistence

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestWorkflowTimerRuleRemovalNativeCancellationAndReplayBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, name := range []string{"rule_removed", "ordinary", "matching_replay", "conflicting_time", "conflicting_cause", "ordinary_after_removal", "fired_rule_target", "ordinary_fired_target", "changed_owner", "write_failure"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectBegin()
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				expected := nativeInheritedWorkflowTimer(t)
				actual := expected
				cause, at := pipeline.WorkflowTimerCancelCauseRuleRemoved, expected.CreatedAt
				wantChanged, wantError := false, false
				switch name {
				case "rule_removed":
					wantChanged = true
				case "ordinary":
					cause, at, wantChanged = "", time.Time{}, true
				case "matching_replay", "conflicting_time", "ordinary_after_removal":
					actual.Status, actual.CancelCause, actual.CancelledAt = "cancelled", cause, at
					if name == "conflicting_time" {
						at, wantError = at.Add(time.Microsecond), true
					} else if name == "ordinary_after_removal" {
						cause, at = "", time.Time{}
					}
				case "conflicting_cause":
					actual.Status, wantError = "cancelled", true
				case "fired_rule_target", "ordinary_fired_target":
					actual.Status, actual.FiredAt = "fired", expected.CreatedAt
					wantError = name == "fired_rule_target"
					if !wantError {
						cause, at = "", time.Time{}
					}
				case "changed_owner":
					actual.OwnerAgent, wantError = "foreign-owner", true
				case "write_failure":
					wantError = true
				}
				mock.ExpectQuery(`(?s)^SELECT timer_name, .*`).WithArgs(expected.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, actual, true))
				if wantChanged || name == "write_failure" {
					query := `UPDATE timers SET status = 'cancelled', cancel_cause = NULLIF(?, ''), cancelled_at = ? WHERE timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
					if postgres {
						query = `UPDATE timers SET status = 'cancelled', cancel_cause = NULLIF($1, ''), cancelled_at = $2 WHERE timer_id = $3::uuid AND task_type = 'workflow_timer' AND status = 'active' RETURNING CAST(run_id AS TEXT), CAST(timer_id AS TEXT)`
						update := mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(string(cause), nullableWorkflowTimerSourceArmedAt(at), expected.Ref.ActivationID)
						if name == "write_failure" {
							update.WillReturnError(errors.New("write refused"))
						} else {
							update.WillReturnRows(sqlmock.NewRows([]string{"run_id", "timer_id"}).AddRow(expected.RunID, expected.Ref.ActivationID))
						}
					} else {
						update := mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(string(cause), nullableWorkflowTimerSourceArmedAt(at), expected.Ref.ActivationID)
						if name == "write_failure" {
							update.WillReturnError(errors.New("write refused"))
						} else {
							update.WillReturnResult(sqlmock.NewResult(0, 1))
						}
					}
				}
				effects := runforkrevision.NewEffects()
				changed, err := cancelWorkflowEngineTimerActivation(context.Background(), tx, postgres, effects, expected, cause, at)
				if changed != wantChanged || (err != nil) != wantError {
					t.Fatalf("cancellation changed=%v want=%v error=%v wantError=%v", changed, wantChanged, err, wantError)
				}
				want := runforkrevision.NewEffects()
				if wantChanged {
					if err := want.AddFact(expected.RunID, runforkrevision.FamilyTimers, expected.Ref.ActivationID); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(effects, want) {
					t.Fatal("cancellation contributed a missing, duplicate, or failed-write fact")
				}
				mock.ExpectRollback()
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestWorkflowTimerRuleRemovalNativeReadersRejectInvalidHistoryBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, name := range []string{"valid", "ordinary", "missing_time", "missing_cause", "unknown_cause", "active_with_cause", "before_birth"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				actual := nativeInheritedWorkflowTimer(t)
				actual.Status, actual.CancelCause, actual.CancelledAt = "cancelled", pipeline.WorkflowTimerCancelCauseRuleRemoved, actual.CreatedAt
				switch name {
				case "ordinary":
					actual.CancelCause, actual.CancelledAt = "", time.Time{}
				case "missing_time":
					actual.CancelledAt = time.Time{}
				case "missing_cause":
					actual.CancelCause = ""
				case "unknown_cause":
					actual.CancelCause = "unknown"
				case "active_with_cause":
					actual.Status = "active"
				case "before_birth":
					actual.CancelledAt = actual.CreatedAt.Add(-time.Microsecond)
				}
				mock.ExpectQuery(`(?s)^SELECT.*FROM timers t.*`).WithArgs(actual.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, actual, false))
				got, found, err := loadWorkflowTimerActivation(context.Background(), db, !postgres, actual.Ref.ActivationID)
				valid := name == "valid" || name == "ordinary"
				if valid && (err != nil || !found || !reflect.DeepEqual(got, actual.Canonical())) {
					t.Fatalf("cancellation history readback lost facts: %+v: %v", got, err)
				}
				if !valid && err == nil {
					t.Fatal("invalid cancellation history admitted")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
