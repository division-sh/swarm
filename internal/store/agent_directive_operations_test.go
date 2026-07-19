package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/testutil"
)

const directiveOperationTestRunID = "00000000-0000-0000-0000-000000001000"

func seedDirectiveOperationRun(t *testing.T, db *sql.DB, postgres bool) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`
	if postgres {
		query = `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`
	}
	if _, err := db.Exec(query, directiveOperationTestRunID, time.Now().UTC()); err != nil {
		t.Fatalf("seed directive operation run: %v", err)
	}
}

func TestSQLiteDirectiveOperationOwnsReservationExecutionAndCompletion(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedDirectiveOperationRun(t, store.DB, false)
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001001", "00000000-0000-0000-0000-000000001002", "idem-1", "hash-1", now)

	reserved, err := store.ReserveDirectiveOperation(ctx, req)
	if err != nil {
		t.Fatalf("ReserveDirectiveOperation: %v", err)
	}
	if !reserved.Created || reserved.Operation.State != runtimeagentcontrol.DirectiveOperationPrepared {
		t.Fatalf("reservation = %#v", reserved)
	}
	missing, err := store.ListEventsMissingPipelineReceipt(ctx, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("ListEventsMissingPipelineReceipt: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("generic pipeline recovery saw operation-owned directive events: %#v", missing)
	}
	replay, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001003", "00000000-0000-0000-0000-000000001004", "idem-1", "hash-1", now.Add(time.Second)))
	if err != nil {
		t.Fatalf("ReserveDirectiveOperation replay: %v", err)
	}
	if replay.Created || replay.Operation.OperationID != reserved.Operation.OperationID {
		t.Fatalf("replay reservation = %#v", replay)
	}
	_, err = store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001005", "00000000-0000-0000-0000-000000001006", "idem-1", "hash-2", now.Add(2*time.Second)))
	var conflict *runtimeagentcontrol.DirectiveIdempotencyConflictError
	if !errors.As(err, &conflict) || conflict.OperationID != reserved.Operation.OperationID {
		t.Fatalf("conflict = %T %v", err, err)
	}

	ownerID := "00000000-0000-0000-0000-000000001007"
	admitted, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, ownerID, now.Add(3*time.Second), time.Minute)
	if err != nil {
		t.Fatalf("AdmitDirectiveExecution: %v", err)
	}
	if admitted.State != runtimeagentcontrol.DirectiveOperationExecuting {
		t.Fatalf("admitted state = %s", admitted.State)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, "other", now, time.Minute); !errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress) {
		t.Fatalf("second admission error = %v", err)
	}

	response := directiveOperationResponseForTest(reserved.Operation)
	executed, err := store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, response, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("RecordDirectiveExecuted: %v", err)
	}
	if executed.State != runtimeagentcontrol.DirectiveOperationExecuted {
		t.Fatalf("executed state = %s", executed.State)
	}
	finalized, err := store.FinalizeDirectiveSuccess(ctx, reserved.Operation.OperationID, now.Add(5*time.Second), 24*time.Hour)
	if err != nil {
		t.Fatalf("FinalizeDirectiveSuccess: %v", err)
	}
	if finalized.State != runtimeagentcontrol.DirectiveOperationSucceeded {
		t.Fatalf("finalized state = %s", finalized.State)
	}
	assertDirectiveOperationCompletionRows(t, store.DB, reserved.Operation.OperationID, reserved.Operation.DirectiveEventID)
	if _, ok, err := store.ReconcileDirectiveOperation(ctx, reserved.Operation.OperationID, now.Add(25*time.Hour), 24*time.Hour); err != nil || ok {
		t.Fatalf("expired terminal reconciliation ok=%v err=%v, want deleted", ok, err)
	}
	replacement, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001008", "00000000-0000-0000-0000-000000001009", "idem-1", "hash-after-expiry", now.Add(25*time.Hour)))
	if err != nil || !replacement.Created || replacement.Operation.OperationID == reserved.Operation.OperationID {
		t.Fatalf("replacement reservation = %#v err=%v", replacement, err)
	}
}

func TestSQLiteDirectiveOperationRecoveryNeverReadmitsUncertainExecution(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedDirectiveOperationRun(t, store.DB, false)
	now := time.Date(2026, 7, 10, 13, 0, 0, 0, time.UTC)

	executingReq := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001101", "00000000-0000-0000-0000-000000001102", "idem-recovery", "hash-recovery", now)
	reserved, err := store.ReserveDirectiveOperation(ctx, executingReq)
	if err != nil {
		t.Fatalf("reserve executing: %v", err)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, "owner", now, time.Second); err != nil {
		t.Fatalf("admit executing: %v", err)
	}

	preparedReq := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001103", "00000000-0000-0000-0000-000000001104", "", "hash-keyless", now)
	prepared, err := store.ReserveDirectiveOperation(ctx, preparedReq)
	if err != nil {
		t.Fatalf("reserve keyless prepared: %v", err)
	}

	executedReq := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001105", "00000000-0000-0000-0000-000000001106", "idem-executed", "hash-executed", now)
	executedReservation, err := store.ReserveDirectiveOperation(ctx, executedReq)
	if err != nil {
		t.Fatalf("reserve executed: %v", err)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, executedReservation.Operation.OperationID, "executed-owner", now, time.Minute); err != nil {
		t.Fatalf("admit executed: %v", err)
	}
	if _, err := store.RecordDirectiveExecuted(ctx, executedReservation.Operation.OperationID, "executed-owner", directiveOperationResponseForTest(executedReservation.Operation), now); err != nil {
		t.Fatalf("record executed: %v", err)
	}

	result, err := store.ReconcileDirectiveOperations(ctx, now.Add(2*time.Second), 24*time.Hour)
	if err != nil {
		t.Fatalf("ReconcileDirectiveOperations: %v", err)
	}
	if result.Finalized != 1 || result.Indeterminate != 1 || result.Failed != 1 {
		t.Fatalf("reconcile result = %#v", result)
	}
	assertDirectiveOperationCompletionRows(t, store.DB, executedReservation.Operation.OperationID, executedReservation.Operation.DirectiveEventID)
	uncertain, _, err := store.LoadDirectiveOperation(ctx, reserved.Operation.OperationID)
	if err != nil {
		t.Fatalf("load uncertain: %v", err)
	}
	if uncertain.State != runtimeagentcontrol.DirectiveOperationIndeterminate {
		t.Fatalf("uncertain state = %s", uncertain.State)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, uncertain.OperationID, "new-owner", now, time.Minute); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("indeterminate readmission error = %v", err)
	}
	abandoned, _, err := store.LoadDirectiveOperation(ctx, prepared.Operation.OperationID)
	if err != nil {
		t.Fatalf("load abandoned: %v", err)
	}
	if abandoned.State != runtimeagentcontrol.DirectiveOperationFailed || abandoned.Failure == nil || abandoned.Failure.Detail.Code != runtimeagentcontrol.DirectiveExecutionNotAdmittedDetail {
		t.Fatalf("abandoned operation = %#v", abandoned)
	}
}

func TestSQLiteDirectiveOperationConcurrentSameKeyHasOneReservation(t *testing.T) {
	ctx := testAuthorActivityContext()
	path := filepath.Join(t.TempDir(), "directive.db")
	first := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	seedDirectiveOperationRun(t, first.DB, false)
	second := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	now := time.Date(2026, 7, 10, 14, 0, 0, 0, time.UTC)
	requests := []runtimeagentcontrol.ReserveDirectiveOperationRequest{
		directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001201", "00000000-0000-0000-0000-000000001202", "same-key", "same-hash", now),
		directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001203", "00000000-0000-0000-0000-000000001204", "same-key", "same-hash", now),
	}
	stores := []*SQLiteRuntimeStore{first, second}
	results := make([]runtimeagentcontrol.DirectiveOperationReservation, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = stores[i].ReserveDirectiveOperation(ctx, requests[i])
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
	}
	if results[0].Operation.OperationID != results[1].Operation.OperationID {
		t.Fatalf("operation ids = %s / %s", results[0].Operation.OperationID, results[1].Operation.OperationID)
	}
	var operationCount, eventCount int
	if err := first.DB.QueryRow(`SELECT COUNT(*) FROM agent_directive_operations`).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if err := first.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'platform.agent_directive'`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if operationCount != 1 || eventCount != 1 {
		t.Fatalf("operation/event counts = %d/%d", operationCount, eventCount)
	}
}

func TestSQLiteDirectiveOperationFinalizationFailuresRollbackToExecuted(t *testing.T) {
	for _, tc := range []struct {
		name        string
		triggerName string
		triggerSQL  string
	}{
		{
			name:        "receipt failure",
			triggerName: "fail_directive_receipt",
			triggerSQL:  `CREATE TRIGGER fail_directive_receipt BEFORE INSERT ON event_receipts BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END`,
		},
		{
			name:        "api projection failure",
			triggerName: "fail_directive_projection",
			triggerSQL:  `CREATE TRIGGER fail_directive_projection BEFORE INSERT ON api_idempotency BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			store := newBootstrappedSQLiteRuntimeStoreForTest(t)
			seedDirectiveOperationRun(t, store.DB, false)
			now := time.Date(2026, 7, 10, 14, 30, 0, 0, time.UTC)
			reserved := reserveAndRecordExecutedDirectiveForTest(t, ctx, store, now)
			if _, err := store.DB.Exec(tc.triggerSQL); err != nil {
				t.Fatalf("create failure trigger: %v", err)
			}
			if _, err := store.FinalizeDirectiveSuccess(ctx, reserved.OperationID, now.Add(time.Second), 24*time.Hour); err == nil {
				t.Fatal("FinalizeDirectiveSuccess error = nil")
			}
			persisted, _, err := store.LoadDirectiveOperation(ctx, reserved.OperationID)
			if err != nil {
				t.Fatalf("load operation after failed finalization: %v", err)
			}
			if persisted.State != runtimeagentcontrol.DirectiveOperationExecuted {
				t.Fatalf("state after failed finalization = %s, want executed", persisted.State)
			}
			var receiptCount, projectionCount int
			if err := store.DB.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id = ?`, reserved.DirectiveEventID).Scan(&receiptCount); err != nil {
				t.Fatal(err)
			}
			if err := store.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE resource_id = ?`, reserved.OperationID).Scan(&projectionCount); err != nil {
				t.Fatal(err)
			}
			if receiptCount != 0 || projectionCount != 0 {
				t.Fatalf("failed finalization receipt/projection = %d/%d, want 0/0", receiptCount, projectionCount)
			}
			if _, err := store.DB.Exec(`DROP TRIGGER ` + tc.triggerName); err != nil {
				t.Fatalf("drop failure trigger: %v", err)
			}
			if _, err := store.FinalizeDirectiveSuccess(ctx, reserved.OperationID, now.Add(2*time.Second), 24*time.Hour); err != nil {
				t.Fatalf("repair finalization: %v", err)
			}
			assertDirectiveOperationCompletionRows(t, store.DB, reserved.OperationID, reserved.DirectiveEventID)
		})
	}
}

func TestSQLiteDirectiveOperationResultPersistenceFailureBecomesIndeterminate(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedDirectiveOperationRun(t, store.DB, false)
	now := time.Date(2026, 7, 10, 14, 45, 0, 0, time.UTC)
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001251", "00000000-0000-0000-0000-000000001252", "result-failure", "result-hash", now)
	reserved, err := store.ReserveDirectiveOperation(ctx, req)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	ownerID := "00000000-0000-0000-0000-000000001253"
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, ownerID, now, time.Second); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := store.DB.Exec(`CREATE TRIGGER fail_directive_executed BEFORE UPDATE OF state ON agent_directive_operations WHEN NEW.state = 'executed' BEGIN SELECT RAISE(ABORT, 'injected result failure'); END`); err != nil {
		t.Fatalf("create result trigger: %v", err)
	}
	if _, err := store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, directiveOperationResponseForTest(reserved.Operation), now.Add(500*time.Millisecond)); err == nil {
		t.Fatal("RecordDirectiveExecuted error = nil")
	}
	if _, err := store.DB.Exec(`DROP TRIGGER fail_directive_executed`); err != nil {
		t.Fatalf("drop result trigger: %v", err)
	}
	if _, err := store.ReconcileDirectiveOperations(ctx, now.Add(2*time.Second), 24*time.Hour); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	operation, _, err := store.LoadDirectiveOperation(ctx, reserved.Operation.OperationID)
	if err != nil {
		t.Fatalf("load operation: %v", err)
	}
	if operation.State != runtimeagentcontrol.DirectiveOperationIndeterminate {
		t.Fatalf("operation state = %s, want indeterminate", operation.State)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, operation.OperationID, "new-owner", now, time.Minute); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("readmission error = %v", err)
	}
}

func TestSQLiteDirectiveOperationReservationFailureRollsBackEveryFact(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	seedDirectiveOperationRun(t, store.DB, false)
	now := time.Date(2026, 7, 10, 14, 50, 0, 0, time.UTC)
	if _, err := store.DB.Exec(`CREATE TRIGGER fail_directive_event BEFORE INSERT ON events WHEN NEW.event_name = 'platform.agent_directive' BEGIN SELECT RAISE(ABORT, 'injected event failure'); END`); err != nil {
		t.Fatalf("create event trigger: %v", err)
	}
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001261", "00000000-0000-0000-0000-000000001262", "reservation-failure", "reservation-hash", now)
	if _, err := store.ReserveDirectiveOperation(ctx, req); err == nil {
		t.Fatal("ReserveDirectiveOperation error = nil")
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM agent_directive_operations`,
		`SELECT COUNT(*) FROM events WHERE event_name = 'platform.agent_directive'`,
	} {
		var count int
		if err := store.DB.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("query %q count = %d, want 0", query, count)
		}
	}
}

func TestPostgresDirectiveOperationOwnsReservationExecutionAndCompletion(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	store := admitTestPostgresStore(t, db)
	seedDirectiveOperationRun(t, db, true)
	now := time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001301", "00000000-0000-0000-0000-000000001302", "pg-idem", "pg-hash", now)
	reserved, err := store.ReserveDirectiveOperation(ctx, req)
	if err != nil {
		t.Fatalf("ReserveDirectiveOperation: %v", err)
	}
	ownerID := "00000000-0000-0000-0000-000000001303"
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, ownerID, now.Add(time.Second), time.Minute); err != nil {
		t.Fatalf("AdmitDirectiveExecution: %v", err)
	}
	if _, err := store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, directiveOperationResponseForTest(reserved.Operation), now.Add(2*time.Second)); err != nil {
		t.Fatalf("RecordDirectiveExecuted: %v", err)
	}
	finalized, err := store.FinalizeDirectiveSuccess(ctx, reserved.Operation.OperationID, now.Add(3*time.Second), 24*time.Hour)
	if err != nil {
		t.Fatalf("FinalizeDirectiveSuccess: %v", err)
	}
	if finalized.State != runtimeagentcontrol.DirectiveOperationSucceeded {
		t.Fatalf("finalized state = %s", finalized.State)
	}
	assertDirectiveOperationCompletionRows(t, db, reserved.Operation.OperationID, reserved.Operation.DirectiveEventID)
	if _, ok, err := store.ReconcileDirectiveOperation(ctx, reserved.Operation.OperationID, now.Add(25*time.Hour), 24*time.Hour); err != nil || ok {
		t.Fatalf("expired terminal reconciliation ok=%v err=%v, want deleted", ok, err)
	}
	replacement, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001304", "00000000-0000-0000-0000-000000001305", "pg-idem", "pg-hash-after-expiry", now.Add(25*time.Hour)))
	if err != nil || !replacement.Created || replacement.Operation.OperationID == reserved.Operation.OperationID {
		t.Fatalf("replacement reservation = %#v err=%v", replacement, err)
	}
}

func TestPostgresDirectiveOperationConcurrentSameKeyHasOneReservationAndAdmission(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	stores := []*PostgresStore{admitTestPostgresStore(t, db), admitTestPostgresStore(t, db)}
	seedDirectiveOperationRun(t, db, true)
	now := time.Date(2026, 7, 10, 15, 15, 0, 0, time.UTC)
	requests := []runtimeagentcontrol.ReserveDirectiveOperationRequest{
		directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001311", "00000000-0000-0000-0000-000000001312", "pg-same-key", "pg-same-hash", now),
		directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001313", "00000000-0000-0000-0000-000000001314", "pg-same-key", "pg-same-hash", now),
	}
	results := make([]runtimeagentcontrol.DirectiveOperationReservation, 2)
	errs := make([]error, 2)
	runConcurrently(2, func(i int) {
		results[i], errs[i] = stores[i].ReserveDirectiveOperation(ctx, requests[i])
	})
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
	}
	if results[0].Operation.OperationID != results[1].Operation.OperationID || results[0].Operation.DirectiveEventID != results[1].Operation.DirectiveEventID {
		t.Fatalf("reservation identities = %#v / %#v", results[0], results[1])
	}
	var operationCount, eventCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM agent_directive_operations`).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'platform.agent_directive'`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if operationCount != 1 || eventCount != 1 {
		t.Fatalf("operation/event counts = %d/%d, want 1/1", operationCount, eventCount)
	}

	admissionErrs := make([]error, 2)
	runConcurrently(2, func(i int) {
		_, admissionErrs[i] = stores[i].AdmitDirectiveExecution(ctx, results[0].Operation.OperationID, fmt.Sprintf("owner-%d", i), now, time.Minute)
	})
	var admitted, inProgress int
	for _, err := range admissionErrs {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress):
			inProgress++
		default:
			t.Fatalf("unexpected admission error: %v", err)
		}
	}
	if admitted != 1 || inProgress != 1 {
		t.Fatalf("admission outcomes admitted=%d in_progress=%d, want 1/1", admitted, inProgress)
	}
}

func TestPostgresDirectiveOperationRecoveryNeverReadmitsUncertainExecution(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	store := admitTestPostgresStore(t, db)
	seedDirectiveOperationRun(t, db, true)
	now := time.Date(2026, 7, 10, 15, 30, 0, 0, time.UTC)

	executing, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001321", "00000000-0000-0000-0000-000000001322", "pg-recovery", "pg-recovery-hash", now))
	if err != nil {
		t.Fatalf("reserve executing: %v", err)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, executing.Operation.OperationID, "owner", now, time.Second); err != nil {
		t.Fatalf("admit executing: %v", err)
	}
	prepared, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001323", "00000000-0000-0000-0000-000000001324", "", "pg-keyless-hash", now))
	if err != nil {
		t.Fatalf("reserve keyless: %v", err)
	}
	executed, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001325", "00000000-0000-0000-0000-000000001326", "pg-executed", "pg-executed-hash", now))
	if err != nil {
		t.Fatalf("reserve executed: %v", err)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, executed.Operation.OperationID, "executed-owner", now, time.Minute); err != nil {
		t.Fatalf("admit executed: %v", err)
	}
	if _, err := store.RecordDirectiveExecuted(ctx, executed.Operation.OperationID, "executed-owner", directiveOperationResponseForTest(executed.Operation), now); err != nil {
		t.Fatalf("record executed: %v", err)
	}

	result, err := store.ReconcileDirectiveOperations(ctx, now.Add(2*time.Second), 24*time.Hour)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Finalized != 1 || result.Indeterminate != 1 || result.Failed != 1 {
		t.Fatalf("reconcile result = %#v", result)
	}
	uncertain, _, err := store.LoadDirectiveOperation(ctx, executing.Operation.OperationID)
	if err != nil || uncertain.State != runtimeagentcontrol.DirectiveOperationIndeterminate {
		t.Fatalf("uncertain operation = %#v err=%v", uncertain, err)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, uncertain.OperationID, "new-owner", now, time.Minute); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("indeterminate readmission error = %v", err)
	}
	abandoned, _, err := store.LoadDirectiveOperation(ctx, prepared.Operation.OperationID)
	if err != nil || abandoned.State != runtimeagentcontrol.DirectiveOperationFailed || abandoned.Failure == nil || abandoned.Failure.Detail.Code != runtimeagentcontrol.DirectiveExecutionNotAdmittedDetail {
		t.Fatalf("abandoned operation = %#v err=%v", abandoned, err)
	}
	assertDirectiveOperationCompletionRows(t, db, executed.Operation.OperationID, executed.Operation.DirectiveEventID)
}

func TestPostgresDirectiveOperationFinalizationFailuresRollbackToExecuted(t *testing.T) {
	for _, tc := range []struct {
		name        string
		triggerName string
		table       string
	}{
		{name: "receipt failure", triggerName: "fail_pg_directive_receipt", table: "event_receipts"},
		{name: "api projection failure", triggerName: "fail_pg_directive_projection", table: "api_idempotency"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			ctx := testAuthorActivityContext()
			store := admitTestPostgresStore(t, db)
			seedDirectiveOperationRun(t, db, true)
			now := time.Date(2026, 7, 10, 15, 45, 0, 0, time.UTC)
			reserved := reserveAndRecordExecutedPostgresDirectiveForTest(t, ctx, store, now)
			dropTrigger := installPostgresDirectiveRejectInsertTrigger(t, db, tc.triggerName, tc.table)
			if _, err := store.FinalizeDirectiveSuccess(ctx, reserved.OperationID, now.Add(time.Second), 24*time.Hour); err == nil {
				t.Fatal("FinalizeDirectiveSuccess error = nil")
			}
			persisted, _, err := store.LoadDirectiveOperation(ctx, reserved.OperationID)
			if err != nil || persisted.State != runtimeagentcontrol.DirectiveOperationExecuted {
				t.Fatalf("operation after failed finalization = %#v err=%v", persisted, err)
			}
			var receiptCount, projectionCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, reserved.DirectiveEventID).Scan(&receiptCount); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE resource_id = $1`, reserved.OperationID).Scan(&projectionCount); err != nil {
				t.Fatal(err)
			}
			if receiptCount != 0 || projectionCount != 0 {
				t.Fatalf("failed finalization receipt/projection = %d/%d, want 0/0", receiptCount, projectionCount)
			}
			dropTrigger()
			if _, err := store.FinalizeDirectiveSuccess(ctx, reserved.OperationID, now.Add(2*time.Second), 24*time.Hour); err != nil {
				t.Fatalf("repair finalization: %v", err)
			}
			assertDirectiveOperationCompletionRows(t, db, reserved.OperationID, reserved.DirectiveEventID)
		})
	}
}

func TestPostgresDirectiveOperationResultPersistenceFailureBecomesIndeterminate(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	store := admitTestPostgresStore(t, db)
	seedDirectiveOperationRun(t, db, true)
	now := time.Date(2026, 7, 10, 16, 0, 0, 0, time.UTC)
	reserved, err := store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001341", "00000000-0000-0000-0000-000000001342", "pg-result-failure", "pg-result-hash", now))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	ownerID := "00000000-0000-0000-0000-000000001343"
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, ownerID, now, time.Second); err != nil {
		t.Fatalf("admit: %v", err)
	}
	dropTrigger := installPostgresDirectiveRejectExecutedTrigger(t, db)
	if _, err := store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, directiveOperationResponseForTest(reserved.Operation), now.Add(500*time.Millisecond)); err == nil {
		t.Fatal("RecordDirectiveExecuted error = nil")
	}
	dropTrigger()
	if _, err := store.ReconcileDirectiveOperations(ctx, now.Add(2*time.Second), 24*time.Hour); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	operation, _, err := store.LoadDirectiveOperation(ctx, reserved.Operation.OperationID)
	if err != nil || operation.State != runtimeagentcontrol.DirectiveOperationIndeterminate {
		t.Fatalf("operation = %#v err=%v", operation, err)
	}
	if _, err := store.AdmitDirectiveExecution(ctx, operation.OperationID, "new-owner", now, time.Minute); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("indeterminate readmission error = %v", err)
	}
}

func TestPostgresDirectiveOperationReservationFailureRollsBackEveryFact(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := testAuthorActivityContext()
	store := admitTestPostgresStore(t, db)
	seedDirectiveOperationRun(t, db, true)
	dropTrigger := installPostgresDirectiveRejectInsertTrigger(t, db, "fail_pg_directive_event", "events")
	defer dropTrigger()
	now := time.Date(2026, 7, 10, 16, 15, 0, 0, time.UTC)
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001351", "00000000-0000-0000-0000-000000001352", "pg-reservation-failure", "pg-reservation-hash", now)
	if _, err := store.ReserveDirectiveOperation(ctx, req); err == nil {
		t.Fatal("ReserveDirectiveOperation error = nil")
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM agent_directive_operations`,
		`SELECT COUNT(*) FROM events WHERE event_name = 'platform.agent_directive'`,
	} {
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("query %q count = %d, want 0", query, count)
		}
	}
}

func directiveOperationReservationForTest(t *testing.T, operationID, eventID, key, hash string, now time.Time) runtimeagentcontrol.ReserveDirectiveOperationRequest {
	t.Helper()
	runID := directiveOperationTestRunID
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: "agent-1", Directive: "continue", RunID: runID, Source: runtimeagentcontrol.DirectiveSourceV1RPC, OperatorID: "actor-1"}
	event, err := runtimeagentcontrol.NewDirectiveEvent(req, runtimeagentcontrol.RunTargetResolution{RunID: runID, Mode: runtimeagentcontrol.RunResolutionSpecified}, operationID, eventID, now)
	if err != nil {
		t.Fatalf("NewDirectiveEvent: %v", err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("AdmitForPersistence: %v", err)
	}
	return runtimeagentcontrol.ReserveDirectiveOperationRequest{
		Operation: runtimeagentcontrol.DirectiveOperation{
			OperationID:      operationID,
			Method:           runtimeagentcontrol.DirectiveOperationMethod,
			ActorTokenID:     "actor-1",
			IdempotencyKey:   key,
			RequestHash:      hash,
			AgentID:          req.AgentID,
			Directive:        req.Directive,
			RequestedRunID:   runID,
			ResolvedRunID:    runID,
			RunIDResolution:  runtimeagentcontrol.RunResolutionSpecified,
			Source:           req.Source,
			OperatorID:       req.OperatorID,
			DirectiveEventID: eventID,
			State:            runtimeagentcontrol.DirectiveOperationPrepared,
		},
		Event: admitted,
		Now:   now,
	}
}

func directiveOperationResponseForTest(op runtimeagentcontrol.DirectiveOperation) json.RawMessage {
	response, _ := json.Marshal(runtimeagentcontrol.SendDirectiveResult{
		OK:                 true,
		OperationID:        op.OperationID,
		Response:           "accepted",
		RunID:              op.ResolvedRunID,
		RunIDResolution:    op.RunIDResolution,
		DirectiveEventID:   op.DirectiveEventID,
		DirectiveEventType: runtimeagentcontrol.DirectiveEventType,
	})
	return response
}

func reserveAndRecordExecutedDirectiveForTest(t *testing.T, ctx context.Context, store *SQLiteRuntimeStore, now time.Time) runtimeagentcontrol.DirectiveOperation {
	t.Helper()
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001241", "00000000-0000-0000-0000-000000001242", "finalize-failure", "finalize-hash", now)
	reserved, err := store.ReserveDirectiveOperation(ctx, req)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	ownerID := "00000000-0000-0000-0000-000000001243"
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, ownerID, now, time.Minute); err != nil {
		t.Fatalf("admit: %v", err)
	}
	executed, err := store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, directiveOperationResponseForTest(reserved.Operation), now)
	if err != nil {
		t.Fatalf("record executed: %v", err)
	}
	return executed
}

func reserveAndRecordExecutedPostgresDirectiveForTest(t *testing.T, ctx context.Context, store *PostgresStore, now time.Time) runtimeagentcontrol.DirectiveOperation {
	t.Helper()
	req := directiveOperationReservationForTest(t, "00000000-0000-0000-0000-000000001361", "00000000-0000-0000-0000-000000001362", "pg-finalize-failure", "pg-finalize-hash", now)
	reserved, err := store.ReserveDirectiveOperation(ctx, req)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	ownerID := "00000000-0000-0000-0000-000000001363"
	if _, err := store.AdmitDirectiveExecution(ctx, reserved.Operation.OperationID, ownerID, now, time.Minute); err != nil {
		t.Fatalf("admit: %v", err)
	}
	executed, err := store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, ownerID, directiveOperationResponseForTest(reserved.Operation), now)
	if err != nil {
		t.Fatalf("record executed: %v", err)
	}
	return executed
}

func runConcurrently(count int, run func(int)) {
	ready := make(chan struct{}, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			run(i)
		}(i)
	}
	for i := 0; i < count; i++ {
		<-ready
	}
	close(start)
	wg.Wait()
}

func installPostgresDirectiveRejectInsertTrigger(t *testing.T, db *sql.DB, triggerName, table string) func() {
	t.Helper()
	functionName := triggerName + "_fn"
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'injected directive persistence failure';
		END;
		$$ LANGUAGE plpgsql`, functionName)); err != nil {
		t.Fatalf("create %s function: %v", functionName, err)
	}
	if _, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, table, functionName)); err != nil {
		t.Fatalf("create %s trigger: %v", triggerName, err)
	}
	return func() {
		if _, err := db.Exec(fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, triggerName, table)); err != nil {
			t.Fatalf("drop %s trigger: %v", triggerName, err)
		}
		if _, err := db.Exec(fmt.Sprintf(`DROP FUNCTION IF EXISTS %s()`, functionName)); err != nil {
			t.Fatalf("drop %s function: %v", functionName, err)
		}
	}
}

func installPostgresDirectiveRejectExecutedTrigger(t *testing.T, db *sql.DB) func() {
	t.Helper()
	const triggerName = "fail_pg_directive_executed"
	const functionName = triggerName + "_fn"
	if _, err := db.Exec(`
		CREATE FUNCTION fail_pg_directive_executed_fn() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'injected directive result persistence failure';
		END;
		$$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create %s function: %v", functionName, err)
	}
	if _, err := db.Exec(`
		CREATE TRIGGER fail_pg_directive_executed
		BEFORE UPDATE OF state ON agent_directive_operations
		FOR EACH ROW WHEN (NEW.state = 'executed')
		EXECUTE FUNCTION fail_pg_directive_executed_fn()`); err != nil {
		t.Fatalf("create %s trigger: %v", triggerName, err)
	}
	return func() {
		if _, err := db.Exec(`DROP TRIGGER IF EXISTS fail_pg_directive_executed ON agent_directive_operations`); err != nil {
			t.Fatalf("drop %s trigger: %v", triggerName, err)
		}
		if _, err := db.Exec(`DROP FUNCTION IF EXISTS fail_pg_directive_executed_fn()`); err != nil {
			t.Fatalf("drop %s function: %v", functionName, err)
		}
	}
}

func assertDirectiveOperationCompletionRows(t *testing.T, db interface {
	QueryRow(string, ...any) *sql.Row
}, operationID, eventID string) {
	t.Helper()
	var operationState, projectionResource string
	if err := db.QueryRow(`SELECT state FROM agent_directive_operations WHERE operation_id = ?`, operationID).Scan(&operationState); err != nil {
		// Postgres does not accept SQLite placeholders.
		if err := db.QueryRow(`SELECT state FROM agent_directive_operations WHERE operation_id = $1::uuid`, operationID).Scan(&operationState); err != nil {
			t.Fatalf("load operation state: %v", err)
		}
	}
	if operationState != "succeeded" {
		t.Fatalf("operation state = %s", operationState)
	}
	var producedByType string
	if err := db.QueryRow(`SELECT COALESCE(produced_by_type, '') FROM events WHERE event_id = ?`, eventID).Scan(&producedByType); err != nil {
		if err := db.QueryRow(`SELECT COALESCE(produced_by_type, '') FROM events WHERE event_id = $1::uuid`, eventID).Scan(&producedByType); err != nil {
			t.Fatalf("load directive event producer classification: %v", err)
		}
	}
	if producedByType != "platform" {
		t.Fatalf("directive event produced_by_type = %q, want platform", producedByType)
	}
	var receiptCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&receiptCount); err != nil {
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&receiptCount); err != nil {
			t.Fatalf("count directive receipts: %v", err)
		}
	}
	if receiptCount != 1 {
		t.Fatalf("receipt count = %d", receiptCount)
	}
	if err := db.QueryRow(`SELECT resource_id FROM api_idempotency WHERE method = 'agent.send_directive'`).Scan(&projectionResource); err != nil {
		t.Fatalf("load directive projection: %v", err)
	}
	if projectionResource != operationID {
		t.Fatalf("projection resource = %s, want %s", projectionResource, operationID)
	}
}
