package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type forkedPipelineBackend struct {
	db       *sql.DB
	store    *WorkflowInstanceStore
	ctx      context.Context
	runID    string
	sqlite   bool
	frozenAt time.Time
}

func newForkedPipelineBackend(t *testing.T, backend string) forkedPipelineBackend {
	t.Helper()
	runID := uuid.NewString()
	frozenAt := time.Now().UTC().Truncate(time.Microsecond)
	if backend == "sqlite" {
		db := newSQLiteWorkflowInstanceStoreTestDB(t)
		store := newSQLiteWorkflowInstanceStoreForTest(t, db)
		ensurePipelineTestRun(t, store, runID)
		return forkedPipelineBackend{
			db: db, store: store, ctx: runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID),
			runID: runID, sqlite: true, frozenAt: frozenAt,
		}
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, frozenAt.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	return forkedPipelineBackend{
		db: db, store: NewWorkflowInstanceStore(db), ctx: runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID),
		runID: runID, frozenAt: frozenAt,
	}
}

func (b forkedPipelineBackend) freeze(t *testing.T) {
	t.Helper()
	if b.sqlite {
		if _, err := b.db.ExecContext(context.Background(), `UPDATE runs SET status = 'forked' WHERE run_id = ?`, b.runID); err != nil {
			t.Fatal(err)
		}
		return
	}
	continuedRunID := uuid.NewString()
	if _, err := b.db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, continuedRunID, b.frozenAt); err != nil {
		t.Fatal(err)
	}
	if _, err := b.db.ExecContext(context.Background(), `UPDATE runs SET status = 'forked', ended_at = $2, continued_as_run_id = $3::uuid WHERE run_id = $1::uuid`, b.runID, b.frozenAt, continuedRunID); err != nil {
		t.Fatal(err)
	}
}

func requireForkedPipelineRefusal(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
		t.Fatalf("%s error = %v, want run-not-active", label, err)
	}
}

func TestForkedSourceWorkflowInstanceMutationsRefuseAndSelectorsExclude(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedPipelineBackend(t, backend)
			storageRef := "freeze/" + uuid.NewString()
			instance := WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: storageRef, WorkflowName: "freeze", WorkflowVersion: "1",
				CurrentState: "active", EnteredStageAt: fixture.frozenAt.Add(-time.Minute),
				Metadata: map[string]any{"marker": "source"},
			}
			if err := fixture.store.Create(fixture.ctx, instance); err != nil {
				t.Fatal(err)
			}
			before, err := fixture.store.SelectActiveByFields(fixture.ctx, "freeze", []WorkflowInstanceFieldSelector{{Field: "marker", Value: "source"}}, nil)
			if err != nil || len(before) != 1 {
				t.Fatalf("active selector before freeze = %d, %v", len(before), err)
			}
			fixture.freeze(t)

			late := instance
			late.CurrentState = "changed"
			requireForkedPipelineRefusal(t, "upsert workflow", fixture.store.Upsert(fixture.ctx, late))
			late.InstanceID = uuid.NewString()
			late.StorageRef = "freeze/" + uuid.NewString()
			requireForkedPipelineRefusal(t, "create workflow", fixture.store.Create(fixture.ctx, late))
			requireForkedPipelineRefusal(t, "mutate workflow", fixture.store.Mutate(fixture.ctx, storageRef, func(item *WorkflowInstance) { item.CurrentState = "changed" }))
			requireForkedPipelineRefusal(t, "mutate workflow with error", fixture.store.MutateE(fixture.ctx, storageRef, func(item *WorkflowInstance) error {
				item.CurrentState = "changed"
				return nil
			}))
			requireForkedPipelineRefusal(t, "terminate workflow", fixture.store.MarkTerminated(fixture.ctx, storageRef, fixture.frozenAt))

			after, err := fixture.store.SelectActiveByFields(fixture.ctx, "freeze", []WorkflowInstanceFieldSelector{{Field: "marker", Value: "source"}}, nil)
			if err != nil || len(after) != 0 {
				t.Fatalf("active selector after freeze = %d, %v", len(after), err)
			}
			preserved, ok, err := fixture.store.Load(fixture.ctx, storageRef)
			if err != nil || !ok || preserved.CurrentState != "active" {
				t.Fatalf("preserved workflow = %#v found=%v err=%v", preserved, ok, err)
			}
		})
	}
}

func TestForkedSourceActivityAttemptMutationsRefuseAndPreserveJournal(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedPipelineBackend(t, backend)
			intent := testNonIdempotentActivityIntent(fixture.runID, uuid.NewString(), uuid.NewString())
			start := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
			started, inserted, err := fixture.store.StartActivityAttempt(fixture.ctx, start)
			if err != nil || !inserted {
				t.Fatalf("start activity before freeze = %#v inserted=%v err=%v", started, inserted, err)
			}
			fixture.freeze(t)

			lateIntent := testNonIdempotentActivityIntent(fixture.runID, uuid.NewString(), uuid.NewString())
			lateStart := activityAttemptStartRecord(lateIntent, activityInputHash(lateIntent.Input))
			_, _, err = fixture.store.StartActivityAttempt(fixture.ctx, lateStart)
			requireForkedPipelineRefusal(t, "start activity", err)
			_, _, err = fixture.store.ClaimActivityAttemptForLoopGeneration(fixture.ctx, lateStart)
			requireForkedPipelineRefusal(t, "claim activity", err)

			success := started.withTerminal(
				ActivityAttemptStatusSucceeded,
				activityResultEventID(intent, intent.SuccessEvent),
				intent.SuccessEvent,
				activitySuccessPayload(intent, map[string]any{"ok": true}),
				nil,
			)
			_, err = fixture.store.CompleteActivityAttempt(fixture.ctx, success)
			requireForkedPipelineRefusal(t, "complete activity", err)
			failure := runtimefailures.Normalize(errors.New("provider outcome is unknown"), "pipeline-test", "freeze_activity")
			uncertain := started.withTerminal(
				ActivityAttemptStatusUncertain,
				uuid.NewString(),
				intent.FailureEvent,
				map[string]any{"uncertain": true},
				&failure,
			)
			_, err = fixture.store.MarkActivityAttemptUncertain(fixture.ctx, uncertain)
			requireForkedPipelineRefusal(t, "mark activity uncertain", err)

			preserved, ok, err := fixture.store.LoadActivityAttempt(fixture.ctx, started.RequestEventID)
			if err != nil || !ok || preserved.Status != ActivityAttemptStatusStarted {
				t.Fatalf("preserved activity = %#v found=%v err=%v", preserved, ok, err)
			}
		})
	}
}
