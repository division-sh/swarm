package runforkpersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestSelectedRecoveryPausedUnkeyedRequiresSettledEffects(t *testing.T) {
	for _, state := range []string{"prepared", "running"} {
		for _, unsafe := range []int{0, 1} {
			t.Run(state+map[int]string{0: "/settled", 1: "/unsettled"}[unsafe], func(t *testing.T) {
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
				record := selectedRecoveryRecord{hasExecution: true, state: state}
				record.RunID, record.ExecutionID = "child", "predecessor"
				mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runtime_external_effect_operations").WithArgs(record.RunID).
					WillReturnRows(sqlmock.NewRows([]string{"unsafe_effects"}).AddRow(unsafe))
				plan, err := planSelectedRecoveryTx(context.Background(), tx, runlifecycle.Snapshot{State: runlifecycle.StatePaused}, record, false, false)
				want := runfork.SelectedForkRecoveryControlOnly
				if unsafe != 0 {
					want = runfork.SelectedForkRecoveryFailed
				}
				if err != nil || plan.Disposition != want || plan.Operation != nil || plan.Continuation != nil || plan.ExecutionID != record.ExecutionID {
					t.Fatalf("paused manual child gained replay or wrong disposition: %+v %v", plan, err)
				}
				if unsafe == 0 && plan.failure != nil {
					t.Fatalf("unreleased attachment gained failure evidence: %+v", plan.failure)
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

func TestSelectedRecoveryPausedUnkeyedPreservesFailureAndReadError(t *testing.T) {
	envelope, ok := failures.EnvelopeFromError(failures.New(failures.ClassOutcomeUncertain, "selected-test-uncertain", "selected-fork", "test", nil))
	if !ok {
		t.Fatal("missing typed test failure")
	}
	record := selectedRecoveryRecord{hasExecution: true, state: "running", failure: &envelope}
	record.RunID = "child"
	plan, err := planInterruptedSelectedRecoveryTx(context.Background(), nil, runlifecycle.Snapshot{State: runlifecycle.StatePaused}, record)
	if err != nil || plan.Disposition != runfork.SelectedForkRecoveryFailed || plan.failure != record.failure {
		t.Fatalf("paused recovery erased original uncertainty: %+v %v", plan, err)
	}
	record.failure = nil
	plan, err = planInterruptedSelectedRecoveryTx(context.Background(), nil, runlifecycle.Snapshot{State: runlifecycle.StateRunning}, record)
	if err != nil || plan.Disposition != runfork.SelectedForkRecoveryFailed {
		t.Fatalf("unkeyed active execution gained control-only admission: %+v %v", plan, err)
	}
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
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runtime_external_effect_operations").WithArgs(record.RunID).WillReturnError(context.Canceled)
	if _, err := planInterruptedSelectedRecoveryTx(context.Background(), tx, runlifecycle.Snapshot{State: runlifecycle.StatePaused}, record); !errors.Is(err, context.Canceled) {
		t.Fatalf("failed inventory read gained control admission: %v", err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedRecoveryPredecessorRetirementUsesExactDispositionAndCAS(t *testing.T) {
	now := time.Unix(1700002400, 0).UTC()
	for _, tc := range []struct {
		name, state, next string
		disposition       runfork.SelectedForkRecoveryDisposition
		rows              int64
		wantError         bool
	}{
		{"paused_running", "running", "quiesced", runfork.SelectedForkRecoveryControlOnly, 1, false},
		{"paused_prepared", "prepared", "quiesced", runfork.SelectedForkRecoveryControlOnly, 1, false},
		{"already_quiesced", "quiesced", "", runfork.SelectedForkRecoveryControlOnly, 0, false},
		{"already_closed", "closed", "", runfork.SelectedForkRecoveryControlOnly, 0, false},
		{"keyed_resume", "running", "closed", runfork.SelectedForkRecoveryResume, 1, false},
		{"keyed_activation", "prepared", "closed", runfork.SelectedForkRecoveryActivate, 1, false},
		{"failed_not_reinterpreted", "running", "", runfork.SelectedForkRecoveryFailed, 0, false},
		{"changed_predecessor", "running", "quiesced", runfork.SelectedForkRecoveryControlOnly, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			if tc.next != "" {
				mock.ExpectExec(`UPDATE run_fork_selected_contract_runtime_executions SET state=\$4,fence_generation=fence_generation\+1,lease_expires_at=NULL,terminal_at=\$2,updated_at=\$2 WHERE execution_id=\$1 AND state=\$3 AND failure IS NULL`).
					WithArgs("predecessor", now, tc.state, tc.next).WillReturnResult(sqlmock.NewResult(0, tc.rows))
			}
			err = retireSelectedRecoveryPredecessorTx(context.Background(), tx, runfork.SelectedForkRecoveryResult{ExecutionID: "predecessor", Disposition: tc.disposition}, tc.state, now)
			if (err != nil) != tc.wantError {
				t.Fatalf("predecessor retirement err=%v wantError=%t", err, tc.wantError)
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

func TestSelectedRecoveryContinuationClassifiesAllTypedCuts(t *testing.T) {
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointRunStart, runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
		for _, state := range []string{"", "prepared", "running", "quiesced", "closed"} {
			t.Run(string(kind)+"/materialized/"+state, func(t *testing.T) {
				record := selectedRecoveryRecord{hasExecution: state != "", state: state}
				record.Operation = &runfork.ForkOperationRecord{Status: runfork.ForkOperationMaterialized,
					Request: runfork.ForkOperationRequest{ResolvedPoint: &runfork.RunForkPoint{Kind: kind, Revision: 3}}}
				disposition, eligible, err := selectedRecoveryContinuationDisposition(runlifecycle.StatePaused, record)
				want := runfork.SelectedForkRecoveryResume
				if state == "quiesced" {
					want = runfork.SelectedForkRecoveryActivate
				}
				if err != nil || !eligible || disposition != want {
					t.Fatalf("materialized child: disposition=%s eligible=%t err=%v", disposition, eligible, err)
				}
			})
		}
		for _, state := range []string{"prepared", "running", "quiesced", "closed"} {
			t.Run(string(kind)+"/activated/"+state, func(t *testing.T) {
				record := selectedRecoveryRecord{hasExecution: true, state: state}
				record.Operation = &runfork.ForkOperationRecord{Status: runfork.ForkOperationActivated,
					Request: runfork.ForkOperationRequest{ResolvedPoint: &runfork.RunForkPoint{Kind: kind, Revision: 3}}}
				disposition, eligible, err := selectedRecoveryContinuationDisposition(runlifecycle.StateRunning, record)
				if err != nil || !eligible || disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("activated child cannot rely on closed executor as completion: disposition=%s eligible=%t err=%v", disposition, eligible, err)
				}
			})
		}
	}
}

func TestSelectedRecoveryClosedAcknowledgmentCannotHideUnsettledEffects(t *testing.T) {
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
	record := selectedRecoveryRecord{hasExecution: true, state: "closed"}
	record.RunID = "child"
	record.Operation = &runfork.ForkOperationRecord{Status: runfork.ForkOperationActivated}
	snapshot := runlifecycle.Snapshot{State: runlifecycle.StateRunning}
	if _, complete, err := settledSelectedRecoveryTx(context.Background(), tx, snapshot, record, true, false); err != nil || complete {
		t.Fatalf("closed predecessor silently hid owed work: complete=%t err=%v", complete, err)
	}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runtime_external_effect_operations").WithArgs(record.RunID).
		WillReturnRows(sqlmock.NewRows([]string{"unsafe_effects"}).AddRow(1))
	plan, err := planSelectedRecoveryTx(context.Background(), tx, snapshot, record, true, false)
	if err != nil || plan.Disposition != runfork.SelectedForkRecoveryFailed || plan.Continuation != nil || plan.Operation != record.Operation {
		t.Fatalf("unsafe closed predecessor acquired successor or rewrote acknowledgment: plan=%+v err=%v", plan, err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedRecoveryContinuationPreservesTerminalAndManualDisposition(t *testing.T) {
	for _, status := range []runfork.ForkOperationStatus{runfork.ForkOperationMaterialized, runfork.ForkOperationActivated, runfork.ForkOperationFailed, runfork.ForkOperationUncertain} {
		record := selectedRecoveryRecord{hasExecution: true, state: "closed"}
		record.Operation = &runfork.ForkOperationRecord{Status: status}
		for _, state := range []runlifecycle.State{runlifecycle.StateCompleted, runlifecycle.StateFailed, runlifecycle.StateCancelled} {
			if disposition, eligible, err := selectedRecoveryContinuationDisposition(state, record); err != nil || eligible || disposition != "" {
				t.Fatalf("terminal child acquired continuation: disposition=%s eligible=%t err=%v", disposition, eligible, err)
			}
		}
	}
	manual := selectedRecoveryRecord{hasExecution: true, state: "closed"}
	if _, eligible, err := selectedRecoveryContinuationDisposition(runlifecycle.StateRunning, manual); err != nil || eligible {
		t.Fatalf("unkeyed generic execution was promoted: eligible=%t err=%v", eligible, err)
	}
	for _, tc := range []struct {
		operation runfork.ForkOperationStatus
		state     runlifecycle.State
		execution bool
	}{
		{runfork.ForkOperationMaterialized, runlifecycle.StateRunning, true},
		{runfork.ForkOperationActivated, runlifecycle.StateRunning, false},
		{runfork.ForkOperationActivated, runlifecycle.StatePaused, false},
	} {
		record := selectedRecoveryRecord{hasExecution: tc.execution, state: "running"}
		record.Operation = &runfork.ForkOperationRecord{Status: tc.operation}
		if _, _, err := selectedRecoveryContinuationDisposition(tc.state, record); err == nil {
			t.Fatalf("contradictory continuation admitted: %+v", tc)
		}
	}
	paused := selectedRecoveryRecord{hasExecution: true, state: "closed"}
	paused.Operation = &runfork.ForkOperationRecord{Status: runfork.ForkOperationActivated}
	if disposition, eligible, err := selectedRecoveryContinuationDisposition(runlifecycle.StatePaused, paused); err != nil || !eligible || disposition != runfork.SelectedForkRecoveryResume {
		t.Fatalf("paused-after-activation child lost its original operation: disposition=%s eligible=%t err=%v", disposition, eligible, err)
	}
}
