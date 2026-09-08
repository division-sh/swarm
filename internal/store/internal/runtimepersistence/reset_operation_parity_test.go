package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestResetOperationIdentityAndPendingRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			if backend == "sqlite" {
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			}
			ctx := context.Background()
			acquire := func() startupownership.ProcessCapability {
				t.Helper()
				cap, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-journal"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := cap.Release(ctx); err != nil {
						t.Error(err)
					}
				})
				return cap
			}
			cap := acquire()
			req := destructivereset.Request{OperationID: uuid.NewString(), ActorTokenID: "operator", IdempotencyKey: "reset-key", RequestHash: "original", RequestedAt: time.Now().UTC()}
			op, err := cap.AdmitResetOperation(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			retry := req
			retry.OperationID = uuid.NewString()
			retry.RequestedAt = req.RequestedAt.Add(48 * time.Hour)
			replayed, err := cap.AdmitResetOperation(ctx, retry)
			if err != nil || !reflect.DeepEqual(op, replayed) {
				t.Fatalf("key replay = %+v, %v", replayed, err)
			}
			conflict := retry
			conflict.RequestHash = "different"
			if _, err := cap.AdmitResetOperation(ctx, conflict); !errors.Is(err, destructivereset.ErrOperationConflict) {
				t.Fatalf("conflict = %v", err)
			}
			other := retry
			other.IdempotencyKey = ""
			if _, err := cap.AdmitResetOperation(ctx, other); !errors.Is(err, destructivereset.ErrOperationInProgress) {
				t.Fatalf("overlap = %v", err)
			}
			planned := op
			planned.Revision++
			planned.Phase = destructivereset.PhasePlanned
			planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, IncludeSourceArtifacts: true, PlannedAt: op.Request.RequestedAt, Plan: destructivereset.Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: true}}
			if err := cap.AdvanceResetOperation(ctx, op, planned); err != nil {
				t.Fatal(err)
			}
			if err := cap.AdvanceResetOperation(ctx, op, planned); !errors.Is(err, destructivereset.ErrOperationChanged) {
				t.Fatalf("stale advance = %v", err)
			}
			if err := cap.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := cap.ReadResetOperation(ctx, op.Request.OperationID); err == nil {
				t.Fatal("released authority read journal")
			}
			cap = acquire()
			pending, err := cap.PendingResetOperations(ctx)
			if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], planned) {
				t.Fatalf("pending after reacquisition = %+v, %v", pending, err)
			}
			forged := planned
			forged.Phase = destructivereset.PhaseCleanupCommitted
			forged.Revision++
			if err := cap.AdvanceResetOperation(ctx, planned, forged); err == nil {
				t.Fatal("caller forged cleanup commit")
			}
		})
	}
}

func TestResetQuiescenceReceiptCommitsWithRunCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				startupownership.Store
				destructivereset.QuiescenceStore
			}
			var db *sql.DB
			if backend == "sqlite" {
				s := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = s, s.backend.ConstructionHandle()
			} else {
				_, database, _ := testutil.StartPostgres(t)
				selected, db = newTestPostgresStore(t, database), database
			}
			ctx := testAuthorActivityContext()
			cap, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-quiescence"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := cap.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			runID := uuid.NewString()
			fixture := runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin()}
			if backend == "sqlite" {
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
			}
			op, err := cap.AdmitResetOperation(ctx, destructivereset.Request{OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "quiescence", IncludeSourceArtifactsSet: true, RequestedAt: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			planned := op
			planned.Phase, planned.Revision = destructivereset.PhasePlanned, op.Revision+1
			planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: op.Request.RequestedAt,
				Plan: destructivereset.Plan{CleanupRunSetKnown: true, ActiveRuns: []destructivereset.RunRef{{RunID: runID, Status: "running"}}, CleanupRuns: []destructivereset.RunRef{{RunID: runID, Status: "running"}}}}
			if err := cap.AdvanceResetOperation(ctx, op, planned); err != nil {
				t.Fatal(err)
			}
			req := destructivereset.QuiescenceRequest{OperationID: op.Request.OperationID, ActorTokenID: "operator", RequestedAt: op.Request.RequestedAt, Result: *planned.Plan}
			dropFault := installResetReceiptFault(t, db, backend, "quiesced")
			if _, err := selected.ApplyDestructiveResetQuiescence(ctx, req); err == nil {
				t.Fatal("receipt fault did not abort quiescence")
			}
			var status string
			if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1`, runID).Scan(&status); err != nil || status != "running" {
				t.Fatalf("rollback run = %s, %v", status, err)
			}
			unchanged, err := cap.ReadResetOperation(ctx, op.Request.OperationID)
			if err != nil || !reflect.DeepEqual(unchanged, planned) {
				t.Fatalf("rollback receipt = %+v, %v", unchanged, err)
			}
			dropFault()
			result, err := selected.ApplyDestructiveResetQuiescence(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			committed, err := cap.ReadResetOperation(ctx, op.Request.OperationID)
			if err != nil || committed.Phase != destructivereset.PhaseQuiesced || !reflect.DeepEqual(committed.Quiescence, &result) {
				t.Fatalf("commit receipt = %+v, %v", committed, err)
			}
			if len(result.Runs) != 1 || result.Runs[0].PreviousStatus != "running" || result.Runs[0].Status != "cancelled" {
				t.Fatalf("exact cancellation = %+v", result)
			}
			if backend == "postgres" {
				proveResetCleanupCommitAndReplay(t, ctx, cap, db, committed)
			}
		})
	}
}

func installResetReceiptFault(t *testing.T, db *sql.DB, backend, phase string) func() {
	t.Helper()
	ddl := fmt.Sprintf(`CREATE TRIGGER reset_receipt_fault BEFORE UPDATE ON runtime_reset_operations WHEN NEW.phase = '%s' BEGIN SELECT RAISE(ABORT, 'reset receipt fault'); END`, phase)
	drop := `DROP TRIGGER reset_receipt_fault`
	if backend == "postgres" {
		ddl = fmt.Sprintf(`CREATE FUNCTION reset_receipt_fault_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase = '%s' THEN RAISE EXCEPTION 'reset receipt fault'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER reset_receipt_fault BEFORE UPDATE ON runtime_reset_operations FOR EACH ROW EXECUTE FUNCTION reset_receipt_fault_fn()`, phase)
		drop = `DROP TRIGGER reset_receipt_fault ON runtime_reset_operations; DROP FUNCTION reset_receipt_fault_fn()`
	}
	if _, err := db.Exec(ddl); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if _, err := db.Exec(drop); err != nil {
			t.Fatal(err)
		}
	}
}

func proveResetCleanupCommitAndReplay(t *testing.T, ctx context.Context, cap startupownership.ProcessCapability, db *sql.DB, op destructivereset.Operation) {
	t.Helper()
	req := destructivereset.CleanupRequest{OperationID: op.Request.OperationID, ActorTokenID: op.Request.ActorTokenID, RequestedAt: op.Request.RequestedAt, Result: *op.Plan, Quiescence: *op.Quiescence}
	drop := installResetReceiptFault(t, db, "postgres", "cleanup_committed")
	if _, err := cap.ApplyDestructiveResetCleanup(ctx, req, nil); err == nil {
		t.Fatal("cleanup receipt fault did not roll back deletion")
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("cleanup rollback count = %d, %v", count, err)
	}
	drop()
	result, err := cap.ApplyDestructiveResetCleanup(ctx, req, nil)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := cap.ReadResetOperation(ctx, req.OperationID)
	if err != nil || committed.Phase != destructivereset.PhaseCleanupCommitted || !reflect.DeepEqual(committed.Cleanup, &result) {
		t.Fatalf("cleanup receipt = %+v, %v", committed, err)
	}
	laterRun := uuid.NewString()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{RunID: laterRun, Origin: runlifecyclefixture.ScenarioSetupOrigin()})
	replay, err := cap.ApplyDestructiveResetCleanup(ctx, req, nil)
	if err != nil || !reflect.DeepEqual(replay, result) {
		t.Fatalf("cleanup replay = %+v, %v", replay, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id = $1`, laterRun).Scan(&count); err != nil || count != 1 {
		t.Fatalf("replay altered later work: %d, %v", count, err)
	}
}
