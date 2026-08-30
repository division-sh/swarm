package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type workflowInitialMaterializationTestOwner struct {
	effects   int
	emissions int
}

type concurrentWorkflowInitialMaterializationTestOwner struct {
	mu      sync.Mutex
	effects int
}

func (o *workflowInitialMaterializationTestOwner) PrepareWorkflowLifecycleMutation(_ context.Context, _ *WorkflowInstance, _ []runtimeworkflowlifecycle.Effect, _ bool) (PreparedWorkflowLifecycleMutation, error) {
	return PreparedWorkflowLifecycleMutation{Emissions: make([]runtimeengine.EmitIntent, o.emissions)}, nil
}

func TestWorkflowInitialMaterializationRejectsUnownedLifecycleEmissions(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	store.lifecycleOwner = &workflowInitialMaterializationTestOwner{emissions: 1}
	ctx := withLiveWorkflowInitialEntry(sqliteExactOnceRunContext(t, db))
	instance := WorkflowInstance{
		InstanceID: "inst-1", StorageRef: "review/inst-1", WorkflowName: "review",
		WorkflowVersion: "1.0.0", CurrentState: "pending",
		Fields:     map[string]any{},
		EntityType: "test_entity",
	}
	if _, err := store.MaterializeInitialEntry(ctx, instance, time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("initial materialization accepted lifecycle emissions outside its atomic commit")
	} else if !strings.Contains(err.Error(), "lifecycle emissions outside its atomic commit") {
		t.Fatalf("initial materialization error = %v", err)
	}
	if _, found, err := store.Load(ctx, testWorkflowInstanceRoute(instance.StorageRef)); err != nil || found {
		t.Fatalf("rejected initial materialization persisted state: found=%v err=%v", found, err)
	}
}

func (o *workflowInitialMaterializationTestOwner) FinalizeWorkflowLifecycleMutation(_ context.Context, _ CommittedWorkflowLifecycleMutation) error {
	o.effects++
	return nil
}

func (*concurrentWorkflowInitialMaterializationTestOwner) PrepareWorkflowLifecycleMutation(_ context.Context, _ *WorkflowInstance, _ []runtimeworkflowlifecycle.Effect, _ bool) (PreparedWorkflowLifecycleMutation, error) {
	return PreparedWorkflowLifecycleMutation{}, nil
}

func (o *concurrentWorkflowInitialMaterializationTestOwner) FinalizeWorkflowLifecycleMutation(_ context.Context, _ CommittedWorkflowLifecycleMutation) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.effects++
	return nil
}

func (o *concurrentWorkflowInitialMaterializationTestOwner) effectCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.effects
}

func (*workflowInitialMaterializationTestOwner) ArmInitialEntryTimers(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (*concurrentWorkflowInitialMaterializationTestOwner) ArmInitialEntryTimers(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (*workflowInitialMaterializationTestOwner) ReconcileInitialEntryTimers(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (*concurrentWorkflowInitialMaterializationTestOwner) ReconcileInitialEntryTimers(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (*workflowInitialMaterializationTestOwner) RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (*concurrentWorkflowInitialMaterializationTestOwner) RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func TestWorkflowInitialMaterializationReportsExactReplayWithoutReapplyingEffectsSQLitePostgres(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				return newSQLiteWorkflowInstanceStoreForTest(t, db), withLiveWorkflowInitialEntry(sqliteExactOnceRunContext(t, db))
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return newPostgresWorkflowInstanceStoreForTest(db), withLiveWorkflowInitialEntry(testPipelineRunContext(t, db))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.setup(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			owner := &workflowInitialMaterializationTestOwner{}
			store.lifecycleOwner = owner
			occurredAt := time.Date(2026, time.July, 26, 12, 0, 0, 987654321, time.UTC)
			instance := WorkflowInstance{
				InstanceID:      "inst-1",
				StorageRef:      "review/inst-1",
				WorkflowName:    "review",
				WorkflowVersion: "1.0.0",
				CurrentState:    "pending",
				Config: map[string]any{
					"attempt_limit": 3,
					"policy":        map[string]any{"weights": []any{1, 2, 3}},
				},
				StateBuckets: map[string]any{
					"totals": map[string]any{"accepted": 1},
				},
				Fields: map[string]any{
					"priority": 2,
					"routing":  map[string]any{"shards": []any{1, 4}},
				},
				EntityType: "test_entity",
			}

			first, err := store.MaterializeInitialEntry(ctx, instance, occurredAt)
			if err != nil {
				t.Fatalf("first MaterializeInitialEntry: %v", err)
			}
			if first != WorkflowInitialMaterializationCreated {
				t.Fatalf("first materialization = %d, want created", first)
			}
			persisted, found, err := store.Load(ctx, testWorkflowInstanceRoute(instance.StorageRef))
			if err != nil {
				t.Fatalf("load persisted initial materialization: %v", err)
			}
			if !found {
				t.Fatal("persisted initial materialization not found")
			}
			rejectVersionOne := `UPDATE workflow_instance_initial_materializations SET projection_version = 1 WHERE run_id = ? AND instance_id = ?`
			if !store.isSQLite() {
				rejectVersionOne = `UPDATE workflow_instance_initial_materializations SET projection_version = 1 WHERE run_id = $1::uuid AND instance_id = $2`
			}
			if _, err := store.testDB().ExecContext(ctx, rejectVersionOne, runID, instance.StorageRef); err == nil {
				t.Fatal("fresh selected-store schema accepted retired initial materialization projection version 1")
			}
			wantPersistedAt := occurredAt.UTC().Truncate(time.Microsecond)
			if !persisted.EnteredStageAt.Equal(wantPersistedAt) || !persisted.CreatedAt.Equal(wantPersistedAt) {
				t.Fatalf("persisted timestamps = entered %s created %s, want %s", persisted.EnteredStageAt, persisted.CreatedAt, wantPersistedAt)
			}
			progressed := persisted
			progressed.CurrentState = "active"
			progressed.EnteredStageAt = occurredAt.Add(time.Minute)
			progressed.Fields = cloneStringAnyMap(persisted.Fields)
			progressed.Fields["priority"] = 9
			progressed.Gates = map[string]bool{"approved": true}
			progressed.StateBuckets = map[string]any{"totals": map[string]any{"accepted": 4}}
			progressed.TransitionHistory = append(progressed.TransitionHistory, WorkflowTransitionRecord{
				TransitionID:   "activate",
				From:           "pending",
				To:             "active",
				TriggerEventID: "event-2",
				FiredAt:        occurredAt.Add(time.Minute),
			})
			if err := store.upsert(ctx, progressed); err != nil {
				t.Fatalf("persist legitimate workflow progress: %v", err)
			}
			replay, err := store.MaterializeInitialEntry(ctx, instance, occurredAt)
			if err != nil {
				t.Fatalf("replay initial creation after workflow progress: %v", err)
			}
			if replay != WorkflowInitialMaterializationAlreadyExists {
				t.Fatalf("replay materialization = %d, want already exists", replay)
			}
			if owner.effects != 1 {
				t.Fatalf("initial-entry effects = %d, want exactly 1", owner.effects)
			}
			afterReplay, found, err := store.Load(ctx, testWorkflowInstanceRoute(instance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load progressed workflow after replay: found=%v err=%v", found, err)
			}
			if afterReplay.CurrentState != "active" || asInt(afterReplay.Fields["priority"]) != 9 {
				t.Fatalf("creation replay rewrote progressed workflow: %#v", afterReplay)
			}

			conflict := instance
			conflict.CurrentState = "active"
			if _, err := store.MaterializeInitialEntry(ctx, conflict, occurredAt); err == nil {
				t.Fatal("conflicting replay succeeded")
			} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
				t.Fatalf("conflicting replay failure = %#v, want conflicting duplicate", failure)
			}
			contractConflict := instance
			contractConflict.EntityType = "different_entity_contract"
			if _, err := store.MaterializeInitialEntry(ctx, contractConflict, occurredAt); err == nil {
				t.Fatal("conflicting entity contract replay succeeded")
			} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
				t.Fatalf("conflicting entity contract replay failure = %#v, want conflicting duplicate", failure)
			}
			if owner.effects != 1 {
				t.Fatalf("conflicting entity contract replay reapplied effects: %d", owner.effects)
			}

			deleteEntityQuery := `DELETE FROM entity_state WHERE run_id = ? AND flow_instance = ?`
			if !store.isSQLite() {
				deleteEntityQuery = `DELETE FROM entity_state WHERE run_id = $1::uuid AND flow_instance = $2`
			}
			if _, err := store.testDB().ExecContext(ctx, deleteEntityQuery, runID, instance.StorageRef); err != nil {
				t.Fatalf("remove materialized entity state: %v", err)
			}
			if _, err := store.MaterializeInitialEntry(ctx, instance, occurredAt); err == nil {
				t.Fatal("creation replay accepted an incomplete persisted snapshot")
			} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
				t.Fatalf("incomplete snapshot failure = %#v, want conflicting duplicate", failure)
			}

			deleteQuery := `DELETE FROM workflow_instance_initial_materializations WHERE run_id = ? AND instance_id = ?`
			if !store.isSQLite() {
				deleteQuery = `DELETE FROM workflow_instance_initial_materializations WHERE run_id = $1::uuid AND instance_id = $2`
			}
			if _, err := store.testDB().ExecContext(ctx, deleteQuery, runID, instance.StorageRef); err != nil {
				t.Fatalf("remove immutable creation record: %v", err)
			}
			if _, err := store.MaterializeInitialEntry(ctx, instance, occurredAt); err == nil {
				t.Fatal("creation replay inferred identity after immutable record was removed")
			} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
				t.Fatalf("missing creation record failure = %#v, want conflicting duplicate", failure)
			}
		})
	}
}

func TestWorkflowInitialMaterializationConcurrentExactReplayPostgres(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	db.SetMaxOpenConns(6)
	db.SetMaxIdleConns(6)
	ctx, cancel := context.WithTimeout(withLiveWorkflowInitialEntry(testPipelineRunContext(t, db)), 10*time.Second)
	defer cancel()
	store := newPostgresWorkflowInstanceStoreForTest(db)
	owner := &concurrentWorkflowInitialMaterializationTestOwner{}
	store.lifecycleOwner = owner
	occurredAt := time.Date(2026, time.July, 28, 14, 0, 0, 123456000, time.UTC)
	const storageRef = "review/inst-1"
	newInstance := func() WorkflowInstance {
		return WorkflowInstance{
			InstanceID:      "inst-1",
			StorageRef:      storageRef,
			WorkflowName:    "review",
			WorkflowVersion: "1.0.0",
			CurrentState:    "pending",
			Config:          map[string]any{"attempt_limit": 3},
			Fields:          map[string]any{},
			EntityType:      "test_entity",
		}
	}

	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin instance lock transaction: %v", err)
	}
	defer lockTx.Rollback()
	if _, err := lockTx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		fmt.Sprintf("%d:%s%s", len(runtimecorrelation.RunIDFromContext(ctx)), runtimecorrelation.RunIDFromContext(ctx), storageRef),
	); err != nil {
		t.Fatalf("hold instance lock: %v", err)
	}

	type materializationResult struct {
		result WorkflowInitialMaterializationResult
		err    error
	}
	results := make(chan materializationResult, 2)
	for range 2 {
		go func() {
			result, err := store.MaterializeInitialEntry(ctx, newInstance(), occurredAt)
			results <- materializationResult{result: result, err: err}
		}()
	}
	waitForWorkflowInstanceAdvisoryWaiters(t, ctx, db, 1)
	if err := lockTx.Commit(); err != nil {
		t.Fatalf("release instance lock: %v", err)
	}

	counts := map[WorkflowInitialMaterializationResult]int{}
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("concurrent exact materialization: %v", got.err)
			}
			counts[got.result]++
		case <-ctx.Done():
			t.Fatalf("concurrent exact materialization did not finish: %v", context.Cause(ctx))
		}
	}
	if counts[WorkflowInitialMaterializationCreated] != 1 ||
		counts[WorkflowInitialMaterializationAlreadyExists] != 1 {
		t.Fatalf("concurrent materialization dispositions = %#v, want one created and one exact replay", counts)
	}
	if got := owner.effectCount(); got != 1 {
		t.Fatalf("concurrent initial-entry effects = %d, want exactly 1", got)
	}
}

func waitForWorkflowInstanceAdvisoryWaiters(t *testing.T, ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, want int) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND query LIKE '%pg_advisory_xact_lock%'
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect workflow-instance lock waiters: %v", err)
		}
		if waiting >= want {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("workflow-instance lock waiters = %d, want at least %d: %v", waiting, want, context.Cause(ctx))
		}
	}
}

func TestDynamicFlowRuntimeReadinessPersistsAndReplaysExactlyOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				return newSQLiteWorkflowInstanceStoreForTest(t, db), withLiveWorkflowInitialEntry(sqliteExactOnceRunContext(t, db))
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return newPostgresWorkflowInstanceStoreForTest(db), withLiveWorkflowInitialEntry(testPipelineRunContext(t, db))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.setup(t)
			store.lifecycleOwner = &workflowInitialMaterializationTestOwner{}
			runID := runtimecorrelation.RunIDFromContext(ctx)
			occurredAt := time.Date(2026, time.July, 26, 13, 0, 0, 123456000, time.UTC)
			sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
			if !ok {
				t.Fatal("initial readiness context is missing bundle source fact")
			}
			bundleHash := sourceFact.BundleHash()
			plan := DynamicFlowRuntimeReadinessPlan{
				Identity: runtimeflowidentity.Instance{
					TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
					InstancePath: "review/inst-1", EntityID: uuid.NewString(), HasStoredPath: true,
				},
				RunID:           runID,
				BundleHash:      bundleHash,
				WorkflowVersion: "1.0.0",
				ExecutionMode:   executionmode.Live,
				Agents: []DynamicFlowRuntimeAgentExpectation{
					{
						Identity: runtimeagentidentity.Identity{
							Name: runtimeagentidentity.Name{
								AgentID: "reviewer-inst-1",
								Owner:   "agent://review/reviewer",
								Source:  runtimeagentidentity.NameSourceDeclared,
							},
							Route: runtimeagentidentity.Route{
								Presence:     runtimeagentidentity.RoutePresent,
								ScopeKey:     "review",
								InstanceID:   "inst-1",
								InstancePath: "review/inst-1",
							},
						},
						ConfigRevision: strings.Repeat("a", 64),
					},
					{
						Identity: runtimeagentidentity.Identity{
							Name: runtimeagentidentity.Name{
								AgentID: "writer-inst-1",
								Owner:   "agent://review/writer",
								Source:  runtimeagentidentity.NameSourceDeclared,
							},
							Route: runtimeagentidentity.Route{
								Presence:     runtimeagentidentity.RoutePresent,
								ScopeKey:     "review",
								InstanceID:   "inst-1",
								InstancePath: "review/inst-1",
							},
						},
						ConfigRevision: strings.Repeat("b", 64),
					},
				},
				CreationEvent: &DynamicFlowRuntimeCreationEventPlan{
					EventID: uuid.NewString(), EventType: "review/inst-1/review.created",
					RunID: runID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live,
					Payload: []byte(`{"name":"alpha"}`), CreatedAt: occurredAt,
				},
			}
			missingMode := plan
			missingMode.ExecutionMode = ""
			if _, err := missingMode.Normalized(); err == nil {
				t.Fatal("readiness plan without execution mode was accepted")
			}
			mismatchedCreationMode := plan
			mismatchedCreation := *plan.CreationEvent
			mismatchedCreation.ExecutionMode = executionmode.Mock
			mismatchedCreationMode.CreationEvent = &mismatchedCreation
			if _, err := mismatchedCreationMode.Normalized(); err == nil {
				t.Fatal("readiness plan accepted a creation event with conflicting execution mode")
			}
			instance := WorkflowInstance{
				InstanceID: "inst-1", StorageRef: "review/inst-1", WorkflowName: "review",
				WorkflowVersion: "1.0.0", RuntimeReadiness: &plan, CurrentState: "pending",
				Config:     map[string]any{"name": "alpha"},
				EntityID:   plan.Identity.EntityID,
				Fields:     map[string]any{},
				EntityType: "test_entity",
			}

			result, err := store.MaterializeInitialEntry(ctx, instance, occurredAt)
			if err != nil || result != WorkflowInitialMaterializationCreated {
				t.Fatalf("first materialization: result=%d err=%v", result, err)
			}
			readiness, found, err := store.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load readiness: found=%v err=%v", found, err)
			}
			if readiness.Plan.Identity.Route() != plan.Identity.Route() || len(readiness.Plan.Agents) != 2 {
				t.Fatalf("readiness plan = %#v", readiness.Plan)
			}
			if !readiness.TopologyReadyAt.IsZero() || !readiness.CreationEventEmittedAt.IsZero() {
				t.Fatalf("new readiness already completed: %#v", readiness)
			}
			result, err = store.MaterializeInitialEntry(ctx, instance, occurredAt)
			if err != nil || result != WorkflowInitialMaterializationAlreadyExists {
				t.Fatalf("exact replay: result=%d err=%v", result, err)
			}
			readyAt := occurredAt.Add(time.Second)
			if err := store.MarkDynamicFlowRuntimeTopologyReady(ctx, plan, readyAt); err != nil {
				t.Fatalf("mark topology ready: %v", err)
			}
			creationEvent := workflowReadinessCreationEventForTest(t, plan)
			if err := (DynamicFlowRuntimeCreationOccurrenceRequest{
				RunID:        runID,
				InstancePath: instance.StorageRef,
				Plan:         plan,
				Event:        creationEvent,
				OccurredAt:   readyAt.Add(time.Second),
			}).Validate(); err != nil {
				t.Fatalf("validate creation occurrence: %v", err)
			}
			projection, err := store.InspectDynamicFlowRuntimeReadinessForSource(ctx, sourceFact)
			items := append([]DynamicFlowRuntimeReadiness(nil), projection.CurrentCompleted...)
			items = append(items, projection.CurrentPending...)
			if err != nil || len(items) != 1 {
				t.Fatalf("inspect readiness: items=%#v err=%v", items, err)
			}
			if items[0].TopologyReadyAt.IsZero() || !items[0].CreationEventEmittedAt.IsZero() {
				t.Fatalf("topology-only readiness = %#v", items[0])
			}
			if err := store.markDynamicFlowRuntimeReadiness(
				ctx,
				runID,
				instance.StorageRef,
				"creation_event_emitted_at",
				nil,
				readyAt.Add(time.Second),
			); err != nil {
				t.Fatalf("mark creation occurrence emitted: %v", err)
			}
			revisedSourceFact, err := runtimecorrelation.NewSourceArtifactFact(
				"bundle-v2:sha256:" + strings.Repeat("c", 64),
			)
			if err != nil {
				t.Fatalf("revised bundle source fact: %v", err)
			}
			revisedCtx := runtimecorrelation.WithSourceArtifactFact(ctx, revisedSourceFact)
			revisedPlan := plan
			revisedPlan.BundleHash = revisedSourceFact.BundleHash()
			reviseWorkflowActivationRunSourceForTest(t, store, revisedCtx, runID, revisedSourceFact)
			observed, found, err := store.LoadDynamicFlowRuntimeReadiness(revisedCtx, runID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load source-transition readiness: found=%v err=%v", found, err)
			}
			reconciled, err := store.ReconcileDynamicFlowRuntimeReadinessPlan(revisedCtx, observed, revisedPlan, readyAt.Add(2*time.Second))
			if err != nil || !reconciled {
				t.Fatalf("reconcile same-version revised-source readiness plan: changed=%v err=%v", reconciled, err)
			}
			revised, found, err := store.LoadDynamicFlowRuntimeReadiness(revisedCtx, runID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load revised readiness: found=%v err=%v", found, err)
			}
			if revised.Plan.WorkflowVersion != plan.WorkflowVersion ||
				revised.Plan.BundleHash != revisedPlan.BundleHash ||
				!revised.TopologyReadyAt.IsZero() ||
				revised.CreationEventEmittedAt.IsZero() ||
				!revised.Pending() {
				t.Fatalf("revised readiness = %#v", revised)
			}
			conflictingCreation := revisedPlan
			conflictingCreationEvent := *revisedPlan.CreationEvent
			conflictingCreationEvent.EventID = uuid.NewString()
			conflictingCreation.CreationEvent = &conflictingCreationEvent
			if _, err := store.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, revised, conflictingCreation, readyAt.Add(3*time.Second)); err == nil {
				t.Fatal("revised readiness replaced an emitted creation occurrence")
			}
			if err := store.MarkDynamicFlowRuntimeTopologyReady(revisedCtx, plan, readyAt.Add(4*time.Second)); err == nil {
				t.Fatal("stale topology plan marked revised readiness complete")
			}
			stillRevised, found, err := store.LoadDynamicFlowRuntimeReadiness(revisedCtx, runID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load readiness after stale topology completion: found=%v err=%v", found, err)
			}
			if stillRevised.Plan.BundleHash != revisedPlan.BundleHash ||
				stillRevised.Plan.WorkflowVersion != revisedPlan.WorkflowVersion ||
				!stillRevised.TopologyReadyAt.IsZero() {
				t.Fatalf("stale topology completion changed revised readiness: %#v", stillRevised)
			}
			if err := store.MarkDynamicFlowRuntimeTopologyReady(revisedCtx, revisedPlan, readyAt.Add(4*time.Second)); err != nil {
				t.Fatalf("mark revised topology ready: %v", err)
			}

			noAutoPlan := plan
			noAutoPlan.Identity = runtimeflowidentity.Instance{
				TemplateID: "review", ScopeKey: "review", InstanceID: "inst-no-auto",
				InstancePath: "review/inst-no-auto", EntityID: uuid.NewString(), HasStoredPath: true,
			}
			noAutoPlan.BundleHash = revisedSourceFact.BundleHash()
			noAutoPlan.CreationEvent = nil
			noAutoInstance := instance
			noAutoInstance.InstanceID = noAutoPlan.Identity.InstanceID
			noAutoInstance.StorageRef = noAutoPlan.Identity.InstancePath
			noAutoInstance.EntityID = noAutoPlan.Identity.EntityID
			noAutoInstance.RuntimeReadiness = &noAutoPlan
			noAutoInstance.Fields = map[string]any{}
			if result, err := store.MaterializeInitialEntry(revisedCtx, noAutoInstance, occurredAt); err != nil || result != WorkflowInitialMaterializationCreated {
				t.Fatalf("no-auto materialization: result=%d err=%v", result, err)
			}
			if err := store.MarkDynamicFlowRuntimeTopologyReady(revisedCtx, noAutoPlan, readyAt); err != nil {
				t.Fatalf("mark no-auto topology ready: %v", err)
			}
			changedMode := noAutoPlan
			changedMode.ExecutionMode = executionmode.Mock
			observedNoAuto, found, err := store.LoadDynamicFlowRuntimeReadiness(revisedCtx, runID, runtimeflowidentity.RouteForInstancePath(noAutoInstance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load no-auto readiness: found=%v err=%v", found, err)
			}
			if _, err := store.ReconcileDynamicFlowRuntimeReadinessPlan(ctx, observedNoAuto, changedMode, readyAt.Add(5*time.Second)); err == nil {
				t.Fatal("readiness reconciliation changed immutable execution mode")
			}
			revisedNoAutoPlan := noAutoPlan
			revisedNoAutoPlan.WorkflowVersion += "-revised"
			if changed, err := store.ReconcileDynamicFlowRuntimeReadinessPlan(revisedCtx, observedNoAuto, revisedNoAutoPlan, readyAt.Add(5*time.Second)); err != nil || !changed {
				t.Fatalf("reconcile revised no-auto readiness: changed=%v err=%v", changed, err)
			}
			revisedNoAuto, found, err := store.LoadDynamicFlowRuntimeReadiness(revisedCtx, runID, runtimeflowidentity.RouteForInstancePath(noAutoInstance.StorageRef))
			if err != nil || !found {
				t.Fatalf("load revised no-auto readiness: found=%v err=%v", found, err)
			}
			if !revisedNoAuto.TopologyReadyAt.IsZero() || !revisedNoAuto.CreationEventEmittedAt.IsZero() || !revisedNoAuto.Pending() {
				t.Fatalf("revised no-auto readiness = %#v", revisedNoAuto)
			}

			nextRunID := uuid.NewString()
			transitionWorkflowActivationRunForTest(
				t,
				store,
				ctx,
				runID,
				runtimerunlifecycle.StateCancelled,
			)
			if store.isSQLite() {
				runlifecyclefixture.RequireSQLite(t, ctx, store.testDB(), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: nextRunID, StartedAt: occurredAt.Add(time.Hour), BundleHash: bundleHash})
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, store.testDB(), runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: nextRunID, BundleHash: bundleHash})
			}
			nextPlan := plan
			nextPlan.RunID = nextRunID
			nextCreationEvent := *plan.CreationEvent
			nextCreationEvent.EventID = uuid.NewString()
			nextCreationEvent.RunID = nextRunID
			nextCreationEvent.ParentEventID = uuid.NewString()
			nextCreationEvent.CreatedAt = occurredAt.Add(time.Hour)
			nextPlan.CreationEvent = &nextCreationEvent
			nextInstance := instance
			nextInstance.RuntimeReadiness = &nextPlan
			nextContext := runtimecorrelation.WithRunID(ctx, nextRunID)
			result, err = store.MaterializeInitialEntry(nextContext, nextInstance, occurredAt.Add(time.Hour))
			if err != nil || result != WorkflowInitialMaterializationCreated {
				t.Fatalf("successor generation materialization: result=%d err=%v", result, err)
			}
			if prior, found, err := store.LoadDynamicFlowRuntimeReadiness(ctx, runID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef)); err != nil || !found || prior.CreationEventEmittedAt.IsZero() {
				t.Fatalf("retired generation readiness changed: found=%v readiness=%#v err=%v", found, prior, err)
			} else if prior.Eligible() {
				t.Fatal("retired generation readiness remained eligible")
			}
			if successor, found, err := store.LoadDynamicFlowRuntimeReadiness(nextContext, nextRunID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef)); err != nil || !found || !successor.TopologyReadyAt.IsZero() {
				t.Fatalf("successor generation readiness: found=%v readiness=%#v err=%v", found, successor, err)
			}
			projection, err = store.InspectDynamicFlowRuntimeReadinessForSource(nextContext, sourceFact)
			items = append(items[:0], projection.CurrentCompleted...)
			items = append(items, projection.CurrentPending...)
			if err != nil || len(items) != 1 || items[0].Plan.RunID != nextRunID {
				t.Fatalf("active generation readiness: items=%#v err=%v", items, err)
			}
			transitionWorkflowActivationRunForTest(
				t,
				store,
				nextContext,
				nextRunID,
				runtimerunlifecycle.StatePaused,
			)
			projection, err = store.InspectDynamicFlowRuntimeReadinessForSource(nextContext, sourceFact)
			items = append(items[:0], projection.CurrentCompleted...)
			items = append(items, projection.CurrentPending...)
			if err != nil || len(items) != 1 || items[0].Plan.RunID != nextRunID {
				t.Fatalf("paused generation readiness: items=%#v err=%v", items, err)
			}
			transitionWorkflowActivationRunForTest(
				t,
				store,
				nextContext,
				nextRunID,
				runtimerunlifecycle.StateCancelled,
			)
			if err := store.MarkDynamicFlowRuntimeTopologyReady(nextContext, nextPlan, occurredAt.Add(2*time.Hour)); err == nil {
				t.Fatal("terminal successor accepted topology completion")
			}
			if successor, found, err := store.LoadDynamicFlowRuntimeReadiness(nextContext, nextRunID, runtimeflowidentity.RouteForInstancePath(instance.StorageRef)); err != nil || !found {
				t.Fatalf("load terminal successor readiness: found=%v err=%v", found, err)
			} else if successor.Eligible() {
				t.Fatal("terminal successor readiness remained eligible")
			}
			projection, err = store.InspectDynamicFlowRuntimeReadinessForSource(nextContext, sourceFact)
			items = append(items[:0], projection.CurrentCompleted...)
			items = append(items, projection.CurrentPending...)
			if err != nil || len(items) != 0 {
				t.Fatalf("terminal generation readiness: items=%#v err=%v", items, err)
			}
			changed := plan
			changed.Agents = append([]DynamicFlowRuntimeAgentExpectation(nil), plan.Agents...)
			changed.Agents[0].ConfigRevision = strings.Repeat("c", 64)
			instance.RuntimeReadiness = &changed
			if _, err := store.MaterializeInitialEntry(ctx, instance, occurredAt); err == nil {
				t.Fatal("changed readiness plan replay succeeded")
			}
		})
	}
}

func transitionWorkflowActivationRunForTest(
	t *testing.T,
	store *workflowInstanceStore,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
) {
	t.Helper()
	if store == nil || store.testRuntimeMutation() == nil {
		t.Fatal("workflow activation run transition requires runtime mutation owner")
	}
	if err := store.testRuntimeMutation().RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		if state == runtimerunlifecycle.StateCancelled {
			_, _, err := store.runLifecycle.MarkTerminalRun(txctx, runtimerunlifecycle.TerminalRequest{
				RunID: runID, State: state, EndedAt: time.Now().UTC(),
			})
			return err
		}
		_, err := store.runLifecycle.TransitionActiveRun(txctx, runtimerunlifecycle.ActiveTransitionRequest{
			RunID: runID, State: state,
		})
		return err
	}); err != nil {
		t.Fatalf("transition workflow activation run %s to %s: %v", runID, state, err)
	}
}

func reviseWorkflowActivationRunSourceForTest(
	t *testing.T,
	store *workflowInstanceStore,
	ctx context.Context,
	runID string,
	source runtimecorrelation.SourceArtifactFact,
) {
	t.Helper()
	if store == nil || store.testRuntimeMutation() == nil {
		t.Fatal("workflow activation source revision requires runtime mutation owner")
	}
	if err := store.testRuntimeMutation().RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		_, err := store.runLifecycle.ReviseRunSource(txctx, runtimerunlifecycle.SourceRevisionRequest{
			RunID:  runID,
			Source: source,
		})
		return err
	}); err != nil {
		t.Fatalf("revise workflow activation run source: %v", err)
	}
}

func workflowReadinessCreationEventForTest(t testing.TB, plan DynamicFlowRuntimeReadinessPlan) events.Event {
	t.Helper()
	creation := plan.CreationEvent
	if creation == nil {
		t.Fatal("creation event plan is required")
	}
	return eventtest.PersistedChildForProducer(
		creation.EventID,
		events.EventType(creation.EventType),
		eventtest.Producer(events.EventProducerPlatform, "flow-instance-activator"),
		"",
		creation.Payload,
		0,
		creation.RunID,
		creation.ParentEventID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{
			EntityID: plan.Identity.EntityID, FlowInstance: plan.Identity.InstancePath,
		}, events.RouteIdentity{
			FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID,
		}),
		creation.CreatedAt,
	)
}

func testCreateFlowInstanceContext(trigger workflowTriggerContext) values.Context {
	payload := parsePayloadMap(trigger.Event.Payload())
	entity := map[string]any{
		"entity_id": workflowEventEntityID(trigger.Event),
	}
	ctx := createFlowInstanceHandlerContext(trigger, payload, entity)
	ctx.PlatformEntity = values.Wrap(map[string]any{"id": workflowEventEntityID(trigger.Event)})
	return ctx
}

func TestCreateFlowInstanceResolvesInstanceIDFromPayloadPath(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.triggered"),
		"",
		testPipelineRunID,
		[]byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1"}),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	ok := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if ok != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", ok)
	}
	if captured.Instance.InstanceID != "inst-42" {
		t.Fatalf("instance id = %q, want inst-42", captured.Instance.InstanceID)
	}
	if captured.Instance.InstancePath != "review/inst-42" {
		t.Fatalf("instance path = %q, want review/inst-42", captured.Instance.InstancePath)
	}
	if captured.Instance.EntityID != FlowInstanceEntityID("review/inst-42") {
		t.Fatalf("entity id = %q, want %q", captured.Instance.EntityID, FlowInstanceEntityID("review/inst-42"))
	}
}

func TestCreateFlowInstancePreservesMockAuthorityInInitialStageTimers(t *testing.T) {
	runID := uuid.NewString()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ensurePipelineTestRun(t, store, runID)
	source := semanticview.Wrap(stageTimerTemplateLifecycleBundle())
	pc := &PipelineCoordinator{
		module:        &pipelineFixtureWorkflowModule{source: source},
		workflowStore: store,
		instanceActivator: func(ctx context.Context, req FlowInstanceActivationRequest) error {
			_, err := store.MaterializeInitialEntry(ctx, WorkflowInstance{
				InstanceID:      req.Instance.InstanceID,
				StorageRef:      req.Instance.InstancePath,
				EntityID:        req.Instance.EntityID,
				ParentEntityID:  req.Instance.ParentEntityID,
				WorkflowName:    req.Instance.TemplateID,
				WorkflowVersion: "1.0.0",
				CurrentState:    "awaiting_review",
				Fields:          map[string]any{},
				EntityType:      "test_entity",
			}, req.OccurredAt)
			return err
		},
	}
	pc.workflowTimers = newWorkflowTimerLifecycle(store, pc.SemanticSource(), pc.bus, pc.workOwner, pc.timerScheduler, executionposture.Live)
	store.lifecycleOwner = pipelineWorkflowLifecycleOwner{coordinator: pc}
	trigger := eventtest.RunCreatingRootIngressWithMode(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		runID,
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1"}),
		time.Now().UTC(),
		executionmode.Mock,
	)
	triggerCtx := workflowTriggerContext{Event: trigger}
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Mock)
	err := pc.createFlowInstance(ctx, triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{"name": "payload.name"},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("createFlowInstance: %v", err)
	}
	entityID := FlowInstanceEntityID("review/inst-42")
	activations, err := store.listWorkflowTimerActivations(ctx, runID, entityID, true)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	if len(activations) != 1 {
		t.Fatalf("workflow timer activations = %d, want 1: %#v", len(activations), activations)
	}
	if activations[0].ExecutionMode != executionmode.Mock {
		t.Fatalf("workflow timer execution mode = %q, want mock", activations[0].ExecutionMode)
	}
	activation := activations[0]
	if activation.Ref.DeclarationKey != "stage:review:review.awaiting_review.expired" {
		t.Fatalf("timer declaration = %q, want stage:review:review.awaiting_review.expired", activation.Ref.DeclarationKey)
	}
	scheduledAt := activation.FireAt
	if want := trigger.CreatedAt().Add(2 * time.Hour); !scheduledAt.Equal(want) {
		t.Fatalf("schedule At = %s, want exact trigger-relative time %s", scheduledAt, want)
	}
}

func TestCreateFlowInstanceResolvesConfigFromBindings(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha","priority":1}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1"}),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name":     "payload.name",
				"priority": "payload.priority",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", err)
	}
	if captured.Config["name"] != "alpha" {
		t.Fatalf("config name = %#v, want alpha", captured.Config["name"])
	}
	if captured.Config["priority"] != float64(1) && captured.Config["priority"] != 1 {
		t.Fatalf("config priority = %#v, want 1", captured.Config["priority"])
	}
	if captured.Instance.ParentEntityID != "ent-1" {
		t.Fatalf("parent entity id = %q, want ent-1", captured.Instance.ParentEntityID)
	}
}

func TestCreateFlowInstancePreservesNullConfigFromValues(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","optional_setting":null}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"optional_setting": "payload.optional_setting",
				"bare_optional":    "optional_setting",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("expected createFlowInstance to preserve explicit null config values: %v", err)
	}
	for _, key := range []string{"optional_setting", "bare_optional"} {
		value, ok := captured.Config[key]
		if !ok {
			t.Fatalf("config[%s] missing; want explicit nil value", key)
		}
		if value != nil {
			t.Fatalf("config[%s] = %#v, want nil", key, value)
		}
	}
}

func TestCreateFlowInstanceResolvesConfigFromHandlerEventContext(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := eventtest.ChildWithLineage(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		events.EventLineage{RunID: testPipelineRunID, ParentEventID: "source-evt-1", ExecutionMode: executionmode.Live},
		events.EventEnvelope{
			EntityID: "ent-1",
			Source: events.RouteIdentity{
				FlowID:       "parent-flow",
				FlowInstance: "parent-flow/source-1",
				EntityID:     "ent-parent",
			},
			Target: events.RouteIdentity{EntityID: "ent-1"},
		},
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"source_event_id": "event.id",
				"event_type":      "event.type",
				"source_flow":     "event.source.flow_id",
				"correlation_id":  "event.source_event_id",
				"name":            "payload.name",
				"parent_entity":   "_entity.id",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", err)
	}
	for key, want := range map[string]any{
		"source_event_id": "evt-123",
		"event_type":      "spawn.requested",
		"source_flow":     "parent-flow",
		"correlation_id":  "source-evt-1",
		"name":            "alpha",
		"parent_entity":   "ent-1",
	} {
		if got := captured.Config[key]; got != want {
			t.Fatalf("config[%s] = %#v, want %#v", key, got, want)
		}
	}
}

func TestCreateFlowInstanceRejectsUnknownEventConfigRef(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"missing_payload": "payload.missing",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	var refErr flowInstanceConfigRefError
	if !errors.As(err, &refErr) {
		t.Fatalf("createFlowInstance error = %T %v, want flowInstanceConfigRefError", err, err)
	}
	for _, want := range []string{`config_from "missing_payload"`, `ref "payload.missing"`, "resolved empty"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("createFlowInstance error = %v, want %q", err, want)
		}
	}
}

func TestCreateFlowInstanceRejectsUnsupportedConfigRefRoot(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"policy_value": "policy.value",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	var refErr flowInstanceConfigRefError
	if !errors.As(err, &refErr) {
		t.Fatalf("createFlowInstance error = %T %v, want flowInstanceConfigRefError", err, err)
	}
	for _, want := range []string{`config_from "policy_value"`, `ref "policy.value"`, `unsupported root "policy"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("createFlowInstance error = %v, want %q", err, want)
		}
	}
}

func TestCreateFlowInstanceDoesNotResolveInstanceIDFromEventRef(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"evt-123",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)

	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "event.id",
		InstanceIDPath: paths.Parse("event.id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), "create_flow_instance instance_id_from resolved empty") {
		t.Fatalf("createFlowInstance error = %v, want instance_id_from split behavior", err)
	}
}

func TestCreateFlowInstanceRejectsMissingRequiredSiblingFields(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template: "review",
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), "requires non-empty instance_id_from and config_from") {
		t.Fatalf("createFlowInstance error = %v, want missing required siblings", err)
	}
}

func TestCreateFlowInstanceRejectsGeneratedFallbackWithoutInstanceIDFrom(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template: "review",
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), "requires non-empty instance_id_from and config_from") {
		t.Fatalf("createFlowInstance error = %v, want missing instance_id_from failure", err)
	}
}

func TestCreateFlowInstanceRejectsEmptyConfigFromBindings(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), "requires non-empty instance_id_from and config_from") {
		t.Fatalf("createFlowInstance error = %v, want missing config_from failure", err)
	}
}

func TestCreateFlowInstanceRejectsEmptyResolvedConfig(t *testing.T) {
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			t.Fatalf("unexpected activation request: %#v", req)
			return nil
		},
	}
	trigger := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("spawn.requested"),
		"",
		"",
		[]byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
		time.Time{},
	)
	triggerCtx := workflowTriggerContext{Event: trigger}

	err := pc.createFlowInstance(testAuthorActivityContext(t, context.Background()), triggerCtx, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name": "payload.missing_name",
			},
		},
	}, testCreateFlowInstanceContext(triggerCtx))
	if err == nil || !strings.Contains(err.Error(), `config_from "name" ref "payload.missing_name" resolved empty`) {
		t.Fatalf("createFlowInstance error = %v, want missing config ref failure", err)
	}
}

func TestHandlerEmitEnvelope_KeepsLocalEntityAcrossOutputBoundaries(t *testing.T) {
	bundle := loadWorkflowFixtureBundle(t, "test-child-flow-local-events")
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	pc := &PipelineCoordinator{module: module}
	trigger := workflowTriggerContext{
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("child/child.start"),
			"",
			"",
			[]byte(`{"entity_id":"ent-child"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-child"),
			time.Time{},
		),

		State: WorkflowState{
			EntityID: "ent-child",
			Control: runtimeengine.StateControl{
				FlowPath:       "child/inst-1",
				ParentEntityID: "ent-parent",
			},
		},
	}

	internalPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "child"), trigger, "child/child.internal")
	if got := asString(internalPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("internal payload entity_id = %q, want ent-child", got)
	}

	outputPayload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "child"), trigger, "child/child.done")
	if got := asString(outputPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("output payload entity_id = %q, want ent-child", got)
	}

	pinBundle := loadWorkflowFixtureBundle(t, "test-child-flow-pin-wiring")
	pinModule, err := newPipelineFixtureWorkflowModule(pinBundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule(pin wiring): %v", err)
	}
	pinPC := &PipelineCoordinator{module: pinModule}
	pinPayload := pinPC.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "child"), trigger, "child/work.completed")
	if got := asString(pinPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("pin output payload entity_id = %q, want ent-child", got)
	}
}

func TestHandlerEmitEnvelope_RootFlowOutputUsesLocalEntity(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"scoring/schema.yaml": `name: scoring
pins:
  outputs:
    events:
      - scoring.requested
`,
		"scoring/nodes.yaml": `scoring-node:
  id: scoring-node
  execution_type: system_node
  event_handlers: {}
`,
	})
	pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}}
	trigger := workflowTriggerContext{
		Event: eventtest.RunCreatingRootIngress(
			"",
			events.EventType("vertical.discovered"),
			"",
			"",
			[]byte(`{"entity_id":"ent-root"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-root"),
			time.Time{},
		),

		State: WorkflowState{
			EntityID: "ent-child",
			Control: runtimeengine.StateControl{
				ParentEntityID: "ent-root",
			},
		},
	}

	payload := pc.handlerEmitEnvelope(withPipelineFlowScope(testAuthorActivityContext(t, context.Background()), "scoring"), trigger, "scoring/scoring.requested")
	if got := asString(payload["entity_id"]); got != "ent-child" {
		t.Fatalf("root flow output payload entity_id = %q, want ent-child", got)
	}
}

func TestTemplateInstanceSystemNodeDeliveryUsesExactLocalHandlerKey(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"operating/entities.yaml": "test_entity: {}\n",
		"operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"operating/events.yaml": `opco.product_initialization_requested:
  entity_id: string
opco.ceo_ready:
  entity_id: string?
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [opco.ceo_ready]
  event_handlers:
    opco.product_initialization_requested:
      emit: opco.ceo_ready
`,
	})
	entityID := FlowInstanceEntityID("operating/inst-1")
	evt := handlerTestRootIngress(
		uuid.NewString(),
		events.EventType("opco.product_initialization_requested"),
		"",
		"",
		mustJSON(map[string]any{"entity_id": entityID}),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "operating/inst-1"),
		time.Time{},
	)
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: "11111111-1111-1111-1111-111111111111"})

	node := pipelineNode(t, "operating", "lifecycle-orchestrator")
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, node, evt)
	if !resolved.Matched {
		t.Fatal("expected exact local event to resolve to lifecycle-orchestrator handler")
	}
	if got := resolved.HandlerEventKey; got != "opco.product_initialization_requested" {
		t.Fatalf("handler event key = %q, want opco.product_initialization_requested", got)
	}

	_, db, _ := testutil.StartPostgres(t)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		workflowStore:  newPostgresWorkflowInstanceStoreForTest(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         staticSemanticWorkflowModule{source: source},
	}
	configurePipelineTestDeliveryOwner(t, pc)
	if err := pc.workflowStore.upsert(testPipelineCoordinatorRunContext(t, pc), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "inst-1", StorageRef: "operating/inst-1", EntityID: "11111111-1111-1111-1111-111111111111",
		WorkflowName: "operating", WorkflowVersion: "1.0.0", CurrentState: "initializing", Fields: map[string]any{},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed exact selected template owner: %v", err)
	}
	route := seedPipelineNodeDeliveryAuthority(t, db, evt, pipelineNode(t, "operating", "lifecycle-orchestrator"))
	handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(testPipelineCoordinatorRunContext(t, pc), route), node, evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("executeNodeHandlerPlanResult handled = false, want true")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "operating/opco.ceo_ready" {
		t.Fatalf("published event type = %q, want operating/opco.ceo_ready", got)
	}
}

func TestTemplateInstanceRecordEvidenceUsesExactLocalHandlerEvidenceTarget(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{

		"operating/entities.yaml": "test_entity: {}\n",
		"operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
`,
		"operating/events.yaml": `build_progress:
  entity_id: string
  summary: string
`,
		"operating/nodes.yaml": `build-orchestrator:
  id: build-orchestrator
  execution_type: system_node
  subscribes_to: [build_progress]
  event_handlers:
    build_progress:
      action: record_evidence
      evidence_target: build_evidence
`,
	})
	const entityID = "11111111-1111-1111-1111-111111111111"
	evt := handlerTestRootIngress(
		uuid.NewString(),
		events.EventType("build_progress"),
		"",
		"",
		mustJSON(map[string]any{"entity_id": entityID, "summary": "compile complete"}),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "operating/inst-1"),
		time.Time{},
	)
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: entityID})

	node := pipelineNode(t, "operating", "build-orchestrator")
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, node, evt)
	if !resolved.Matched {
		t.Fatal("expected exact local event to resolve to build-orchestrator handler")
	}
	if got := resolved.HandlerEventKey; got != "build_progress" {
		t.Fatalf("handler event key = %q, want build_progress", got)
	}
	if got := strings.TrimSpace(resolved.Handler.EvidenceTarget); got != "build_evidence" {
		t.Fatalf("resolved evidence target = %q, want build_evidence", got)
	}

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	workflowStore := newPostgresWorkflowInstanceStoreForTest(db)
	pc := &PipelineCoordinator{
		bus:            &recordingPipelineBus{},
		workflowStore:  workflowStore,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         staticSemanticWorkflowModule{source: source},
	}
	ctx := testPipelineCoordinatorRunContext(t, pc)
	if err := workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      "operating/inst-1",
		EntityID:        entityID,
		WorkflowName:    "operating",
		WorkflowVersion: "1.0.0",
		CurrentState:    "initializing",
		Fields:          map[string]any{},
		StateBuckets:    map[string]any{},
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	configurePipelineTestDeliveryOwner(t, pc)
	route := seedPipelineNodeDeliveryAuthority(t, db, evt, pipelineNode(t, "operating", "build-orchestrator"))

	handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(ctx, route), node, evt)
	if err != nil {
		t.Fatalf("executeNodeHandlerPlanResult: %v", err)
	}
	if !handled {
		t.Fatal("executeNodeHandlerPlanResult handled = false, want true")
	}

	instance, ok, err := workflowStore.Load(ctx, testWorkflowInstanceRoute("operating/inst-1"))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to exist")
	}
	entries := workflowEvidenceEntries(t, instance, "build_evidence")
	if len(entries) != 1 {
		t.Fatalf("build_evidence entries = %d, want 1", len(entries))
	}
	if got := entries[0]["summary"]; got != "compile complete" {
		t.Fatalf("evidence summary = %#v, want compile complete", got)
	}
}

func loadWorkflowFixtureSource(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowFixtureBundle(t, fixture))
}

func loadWorkflowFixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixture)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	return bundle
}

func loadWorkflowTempSource(t *testing.T, files map[string]string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowTempBundle(t, files))
}

func loadWorkflowTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return bundle
}

type staticSemanticWorkflowModule struct {
	source semanticview.Source
}

func (m staticSemanticWorkflowModule) SemanticSource() semanticview.Source   { return m.source }
func (staticSemanticWorkflowModule) WorkflowDefinition() *WorkflowDefinition { return nil }
func (staticSemanticWorkflowModule) WorkflowNodes() []WorkflowNode           { return nil }
func (staticSemanticWorkflowModule) GuardRegistry() GuardRegistry            { return nil }
func (staticSemanticWorkflowModule) ActionRegistry() ActionRegistry          { return nil }
