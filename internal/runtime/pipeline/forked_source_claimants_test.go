package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type forkedPipelineBackend struct {
	db             *sql.DB
	store          *workflowInstanceStore
	runner         *recordingRuntimeMutationRunner
	ctx            context.Context
	runID          string
	continuedRunID string
	sqlite         bool
	frozenAt       time.Time
}

func newForkedPipelineBackend(t *testing.T, backend string) forkedPipelineBackend {
	t.Helper()
	runID := uuid.NewString()
	continuedRunID := uuid.NewString()
	frozenAt := time.Now().UTC().Truncate(time.Microsecond)
	if backend == "sqlite" {
		db := newSQLiteWorkflowInstanceStoreTestDB(t)
		store := newSQLiteWorkflowInstanceStoreForTest(t, db)
		runlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: runID, StartedAt: frozenAt.Add(-time.Hour),
		})
		runlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
			RunID: continuedRunID, StartedAt: frozenAt,
		})
		return forkedPipelineBackend{
			db: db, store: store, ctx: runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID),
			runner: &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite},
			runID:  runID, continuedRunID: continuedRunID, sqlite: true, frozenAt: frozenAt,
		}
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: frozenAt.Add(-time.Hour)})
	runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: continuedRunID, StartedAt: frozenAt})
	return forkedPipelineBackend{
		db: db, store: newPostgresWorkflowInstanceStoreForTest(db), ctx: runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID),
		runner: &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectPostgres},
		runID:  runID, continuedRunID: continuedRunID, frozenAt: frozenAt,
	}
}

func (b forkedPipelineBackend) freeze(t *testing.T) {
	t.Helper()
	if err := b.runner.RunRuntimeMutationContext(b.ctx, func(txctx context.Context) error {
		_, _, err := b.store.runLifecycle.ForkRunSource(txctx, storerunlifecycle.ForkSourceRequest{
			RunID: b.runID, ContinuedAsRunID: b.continuedRunID, EndedAt: b.frozenAt,
		})
		return err
	}); err != nil {
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
			instanceID := uuid.NewString()
			storageRef := "freeze/" + instanceID
			entityID := uuid.NewString()
			instance := WorkflowInstance{
				InstanceID: instanceID, StorageRef: storageRef, EntityID: entityID, WorkflowName: "freeze", WorkflowVersion: "1",
				CurrentState: "active", EnteredStageAt: fixture.frozenAt.Add(-time.Minute),
				Fields: map[string]any{"marker": "source"},
			}
			if err := fixture.store.create(fixture.ctx, instance); err != nil {
				t.Fatal(err)
			}
			before, err := fixture.store.selectActiveByFieldsExported(fixture.ctx, "freeze", []WorkflowInstanceFieldSelector{{Field: "marker", Value: "source"}}, nil)
			if err != nil || len(before) != 1 {
				t.Fatalf("active selector before freeze = %d, %v", len(before), err)
			}
			fixture.freeze(t)

			late := instance
			late.CurrentState = "changed"
			requireForkedPipelineRefusal(t, "upsert workflow", fixture.store.upsert(fixture.ctx, late))
			late.InstanceID = uuid.NewString()
			late.StorageRef = "freeze/" + late.InstanceID
			late.Fields = cloneStringAnyMap(late.Fields)
			requireForkedPipelineRefusal(t, "create workflow", fixture.store.create(fixture.ctx, late))
			requireForkedPipelineRefusal(t, "mutate workflow", fixture.store.mutate(fixture.ctx, testWorkflowInstanceRoute(storageRef), func(item *WorkflowInstance) { item.CurrentState = "changed" }))
			requireForkedPipelineRefusal(t, "mutate workflow with error", fixture.store.mutateE(fixture.ctx, testWorkflowInstanceRoute(storageRef), func(item *WorkflowInstance) error {
				item.CurrentState = "changed"
				return nil
			}))
			requireForkedPipelineRefusal(t, "terminate workflow", fixture.store.MarkTerminated(fixture.ctx, testWorkflowInstanceRoute(storageRef), identity.NormalizeEntityID(entityID), fixture.frozenAt))

			after, err := fixture.store.selectActiveByFieldsExported(fixture.ctx, "freeze", []WorkflowInstanceFieldSelector{{Field: "marker", Value: "source"}}, nil)
			if err != nil || len(after) != 0 {
				t.Fatalf("active selector after freeze = %d, %v", len(after), err)
			}
			preserved, ok, err := fixture.store.Load(fixture.ctx, testWorkflowInstanceRoute(storageRef))
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
