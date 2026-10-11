package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func VerifySQLiteWorkflowInstanceStore_PreservesCreateEntityInitialValueMutationRowsForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: initial-value-proof\nstages:\n  created: {}\n",
		"entities.yaml": "test_entity:\n  region: {type: text, initial: \"west\"}\n  tier: {type: integer, initial: 1}\n",
	})
	fixture, _, ctx := nativePilotPipelineForTest(t, "sqlite", bundle, open)
	runID := runtimecorrelation.RunIDFromContext(ctx)
	storageRef, entityID := runID, runID

	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      runID,
		StorageRef:      storageRef,
		EntityID:        entityID,
		WorkflowName:    semanticview.RootExecutionFlowID(semanticview.Wrap(bundle)),
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "created",
		EnteredStageAt:  time.Now().UTC(),
		Fields: map[string]any{
			"region": "west",
			"tier":   int64(2),
		},
		InitialFieldValues: map[string]any{
			"region": "west",
			"tier":   int64(1),
		},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	rows := fixture.MutationHistory(ctx, runID, entityID)
	assertNativeInitialValueMutationCountForTest(t, rows, "region", "entity_initial_value", "create_entity", "null", `"west"`, 1)
	assertNativeInitialValueMutationCountForTest(t, rows, "region", "workflow_instance_store", "create", "", "", 0)
	assertNativeInitialValueMutationCountForTest(t, rows, "tier", "entity_initial_value", "create_entity", "null", "1", 1)
	assertNativeInitialValueMutationCountForTest(t, rows, "tier", "workflow_engine", "create", "1", "2", 1)
}

func VerifyNativeEntityStateDiffRequiresExistingCanonicalRunBeforeMutationForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativeMutationLoggingFixtureForTest(t, backend, open)
			run := runtimecorrelation.RunIDFromContext(ctx)
			instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, run))
			if err != nil || !found {
				t.Fatalf("load native diff target: found=%t err=%v", found, err)
			}
			before, err := fixture.PhysicalCounts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			missingRun := uuid.NewString()
			missingCtx := runtimecorrelation.WithRunID(ctx, missingRun)
			instance.Fields["status"] = "ready"
			record, err := workflowEngineStateRecord(testRunScopedWorkflowInstanceFromContext(missingCtx, run), instance, instance.CurrentState, instance.Revision, WorkflowEngineStateTransitionUpdateStateAndCompanion, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			_, err = pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(missingCtx, WorkflowEngineMutationCommand{State: record})
			if !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
				t.Fatalf("native diff err=%v, want ErrRunNotFound", err)
			}
			assertNativeMissingRunLeftNoMutationForTest(t, fixture, ctx, missingCtx, missingRun, before)
		})
	}
}

func VerifyNativeInitialValueMutationRequiresExistingCanonicalRunBeforeMutationForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: missing-initial-run\nstages:\n  ready: {}\n",
				"entities.yaml": "test_entity:\n  region: {type: text, initial: \"west\"}\n",
			})
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			before, err := fixture.PhysicalCounts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			missingRun := uuid.NewString()
			missingCtx := runtimecorrelation.WithRunID(ctx, missingRun)
			instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), missingCtx, ".")
			if instance.InitialFieldValues["region"] != "west" {
				t.Fatal("missing-run proof must retain compiled initial-value work")
			}
			if err := fixture.Construct(missingCtx, instance); !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
				t.Fatalf("native initial-value construction err=%v, want ErrRunNotFound", err)
			}
			assertNativeMissingRunLeftNoMutationForTest(t, fixture, ctx, missingCtx, missingRun, before)
		})
	}
}

func assertNativeMissingRunLeftNoMutationForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx, missingCtx context.Context, missingRun string, before WorkflowEnginePhysicalCountsForTest) {
	t.Helper()
	if err := fixture.Runs.RequirePresentRun(missingCtx, missingRun); !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
		t.Fatalf("failed writer materialized absent run: %v", err)
	}
	after, err := fixture.PhysicalCounts(ctx)
	if err != nil || after != before {
		t.Fatalf("missing run changed native persistence: %+v -> %+v err=%v", before, after, err)
	}
	if counts := fixture.Transactions(); counts.Active != 0 {
		t.Fatalf("missing-run refusal leaked native transaction: %+v", counts)
	}
}

func VerifySQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadataForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	fixture := open(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(fixture.Context, runID)
	if err := fixture.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	storageRef := "review/inst-1"

	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:         "inst-1",
		StorageRef:         storageRef,
		ParentFlowID:       "operating",
		ParentFlowInstance: "operating/root",
		ParentEntityID:     "parent-ent",
		WorkflowName:       "review",
		WorkflowVersion:    "v1",
		CurrentState:       "created",
		EnteredStageAt:     time.Now().UTC(),
		Fields:             map[string]any{},
		EntityType:         "test_entity",
	})); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	loaded, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, storageRef))
	if err != nil {
		t.Fatalf("Load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if loaded.ParentFlowID != "operating" || loaded.ParentFlowInstance != "operating/root" || loaded.ParentEntityID != "parent-ent" {
		t.Fatalf("loaded parent identity = %q/%q/%q", loaded.ParentFlowID, loaded.ParentFlowInstance, loaded.ParentEntityID)
	}
	identity, err := workflowInstancePersistedIdentity(nil, loaded)
	if err != nil {
		t.Fatalf("workflowInstancePersistedIdentity: %v", err)
	}
	if identity.ParentRoute.FlowID != "operating" || identity.ParentRoute.FlowInstance != "operating/root" || identity.ParentRoute.EntityID != "parent-ent" {
		t.Fatalf("ParentRoute = %#v, want operating/operating/root/parent-ent", identity.ParentRoute)
	}
}

func VerifySQLiteWorkflowInstanceStore_MarkTerminatedUsesRuntimeMutationRunnerForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: termination-proof\nstages:\n  running: {}\n",
		"entities.yaml": "test_entity: {}\n",
	})
	fixture, coordinator, ctx := nativePilotPipelineForTest(t, "sqlite", bundle, open)
	runID := runtimecorrelation.RunIDFromContext(ctx)
	storageRef, entityID := runID, runID
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: runID, StorageRef: storageRef, EntityID: entityID, WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState: "running", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	before := fixture.Transactions()
	terminatedAt := time.Now().UTC()
	if err := coordinator.MarkTerminated(ctx, testRunScopedWorkflowInstanceFromContext(ctx, storageRef), identity.NormalizeEntityID(entityID), terminatedAt); err != nil {
		t.Fatalf("MarkTerminated: %v", err)
	}
	after := fixture.Transactions()
	if after.WorkflowCommits != before.WorkflowCommits+1 || after.Active != 0 {
		t.Fatalf("native termination must acknowledge one selected mutation: %+v -> %+v", before, after)
	}
	loaded, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, storageRef))
	if err != nil || !found || loaded.Status != "terminated" || loaded.TerminatedAt.IsZero() {
		t.Fatalf("load terminated native flow: %+v found=%t err=%v", loaded, found, err)
	}
}

func VerifySQLiteWorkflowInstanceStore_MutateERollsBackCallbackFailureForTest(t *testing.T, open func(*testing.T, string) WorkflowProjectionNativeFixtureForTest) {
	runID := uuid.NewString()
	fixture := open(t, runID)
	store, ctx := fixture.Persistence.store, fixture.Context
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: "item", StorageRef: "root/item", WorkflowName: "root", WorkflowVersion: "1.0.0", CurrentState: "queued", Fields: map[string]any{},
		EntityType: "test_entity"})
	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sentinel := errors.New("supersession failed")
	if err := store.mutateE(ctx, testRunScopedWorkflowInstanceForRun(runID, instance.StorageRef), func(item *WorkflowInstance) error {
		item.CurrentState = "must_not_commit"
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("MutateE error = %v, want sentinel", err)
	}
	loaded, ok, err := store.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, instance.StorageRef))
	if err != nil || !ok {
		t.Fatalf("Load = found %v err %v", ok, err)
	}
	if loaded.CurrentState != "queued" {
		t.Fatalf("CurrentState = %q, want queued", loaded.CurrentState)
	}
}

func assertNativeInitialValueMutationCountForTest(t *testing.T, rows []PipelineNativeMutationRowForTest, field, writerID, handlerStep, oldValue, newValue string, want int) {
	t.Helper()
	got := 0
	for _, row := range rows {
		if row.Domain == "authored_field" && row.Path == field && row.WriterID == writerID && row.HandlerStep == handlerStep &&
			(oldValue == "" || string(row.OldValue) == oldValue) && (newValue == "" || string(row.NewValue) == newValue) {
			got++
		}
	}
	if got != want {
		t.Fatalf("mutation count for field=%s writer=%s step=%s old=%s new=%s = %d, want %d", field, writerID, handlerStep, oldValue, newValue, got, want)
	}
}
