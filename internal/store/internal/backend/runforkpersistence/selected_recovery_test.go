package runforkpersistence

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

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
