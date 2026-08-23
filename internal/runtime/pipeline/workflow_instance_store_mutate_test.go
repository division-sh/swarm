package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowEngineCompleteCarrierPreservesBookkeepingOnBothStores(t *testing.T) {
	setups := []struct {
		name string
		open func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				return newSQLiteWorkflowInstanceStoreForTest(t, db), sqliteExactOnceRunContext(t, db)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := newPostgresWorkflowInstanceStoreForTest(db)
				return store, testWorkflowStoreRunContext(t, store)
			},
		},
	}
	for _, setup := range setups {
		setup := setup
		t.Run(setup.name, func(t *testing.T) {
			store, ctx := setup.open(t)
			entityID := uuid.NewString()
			route := testWorkflowInstanceRoute("bookkeeping/root")
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: "root", StorageRef: "bookkeeping/root", EntityID: entityID,
				WorkflowName: "bookkeeping", WorkflowVersion: "v1", CurrentState: "ready",
				EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"status": "before"},
				Bookkeeping: map[string]any{}, Gates: map[string]bool{"ready": true},
				StateBuckets: map[string]any{"join": map[string]any{"count": float64(1)}},
				EntityType:   "test_entity",
			})
			if err := store.create(ctx, instance); err != nil {
				t.Fatalf("create workflow instance: %v", err)
			}
			update := `UPDATE entity_state SET bookkeeping = '{"platform_fact":"preserve"}' WHERE flow_instance = ?`
			if setup.name == "postgres" {
				update = `UPDATE entity_state SET bookkeeping = '{"platform_fact":"preserve"}'::jsonb WHERE flow_instance = $1`
			}
			if _, err := store.testDB().ExecContext(ctx, update, route.InstancePath); err != nil {
				t.Fatalf("seed existing platform bookkeeping: %v", err)
			}
			created, ok, err := store.Load(ctx, route)
			if err != nil || !ok || created.Bookkeeping["platform_fact"] != "preserve" {
				t.Fatalf("seeded workflow bookkeeping = %#v found=%v err=%v", created.Bookkeeping, ok, err)
			}
			if err := store.mutateE(ctx, route, func(current *WorkflowInstance) error {
				carrier, err := runtimeengine.StateCarrierFromPersisted(
					map[string]any{"status": "after"}, current.Bookkeeping, current.Gates, current.StateBuckets,
				)
				if err != nil {
					return err
				}
				return applyEngineStateMutation(current, runtimeengine.StateMutation{StateCarrier: carrier}, nil, nil, "")
			}); err != nil {
				t.Fatalf("commit workflow engine mutation: %v", err)
			}
			loaded, ok, err := store.Load(ctx, route)
			if err != nil || !ok {
				t.Fatalf("reload workflow instance found=%v err=%v", ok, err)
			}
			if loaded.Fields["status"] != "after" || loaded.Bookkeeping["platform_fact"] != "preserve" || !loaded.Gates["ready"] {
				t.Fatalf("reloaded workflow state = %#v", loaded)
			}
		})
	}
}

func TestWorkflowInstanceStoreMutateE_RollsBackCallbackFailure(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := newPostgresWorkflowInstanceStoreForTest(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)
	ctx := testWorkflowStoreRunContext(t, store)
	sentinel := errors.New("supersession failed")
	if err := store.mutateE(ctx, testWorkflowInstanceRoute("mutation-flow"), func(instance *WorkflowInstance) error {
		instance.CurrentState = "must_not_commit"
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("MutateE error = %v, want sentinel", err)
	}
	loaded, ok, err := store.Load(ctx, testWorkflowInstanceRoute("mutation-flow"))
	if err != nil || !ok {
		t.Fatalf("Load = found %v err %v", ok, err)
	}
	if loaded.CurrentState == "must_not_commit" {
		t.Fatalf("callback failure committed state: %#v", loaded)
	}
}

func TestWorkflowInstanceLookupMissIsTypedAndExactOnBothStores(t *testing.T) {
	setups := []struct {
		name string
		open func(*testing.T) (*sql.DB, *workflowInstanceStore, context.Context)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*sql.DB, *workflowInstanceStore, context.Context) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				store := newSQLiteWorkflowInstanceStoreForTest(t, db)
				return db, store, sqliteExactOnceRunContext(t, db)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*sql.DB, *workflowInstanceStore, context.Context) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := newPostgresWorkflowInstanceStoreForTest(db)
				return db, store, testWorkflowStoreRunContext(t, store)
			},
		},
	}

	for _, setup := range setups {
		setup := setup
		t.Run(setup.name, func(t *testing.T) {
			db, store, ctx := setup.open(t)
			for _, requestedKey := range []string{" \t ", uuid.NewString()} {
				requestedKey := requestedKey
				t.Run(fmt.Sprintf("key_%q", requestedKey), func(t *testing.T) {
					before := workflowInstanceRowCount(t, ctx, db)
					callbackRan := false
					err := store.mutateE(ctx, testWorkflowInstanceRoute(requestedKey), func(*WorkflowInstance) error {
						callbackRan = true
						return nil
					})
					var miss *WorkflowInstanceLookupMiss
					if !errors.As(err, &miss) {
						t.Fatalf("mutateE error = %T %v, want *WorkflowInstanceLookupMiss", err, err)
					}
					wantKey := strings.Trim(strings.TrimSpace(requestedKey), "/")
					if miss.RequestedKey != wantKey {
						t.Fatalf("RequestedKey = %q, want canonical %q", miss.RequestedKey, wantKey)
					}
					if callbackRan {
						t.Fatal("workflow mutation callback ran after lookup miss")
					}
					if after := workflowInstanceRowCount(t, ctx, db); after != before {
						t.Fatalf("workflow rows after miss = %d, want unchanged %d", after, before)
					}
				})
			}
		})
	}
}

func TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores(t *testing.T) {
	setups := []struct {
		name string
		open func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				store := newSQLiteWorkflowInstanceStoreForTest(t, db)
				return store, sqliteExactOnceRunContext(t, db)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) (*workflowInstanceStore, context.Context) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := newPostgresWorkflowInstanceStoreForTest(db)
				return store, testWorkflowStoreRunContext(t, store)
			},
		},
	}

	for _, setup := range setups {
		setup := setup
		t.Run(setup.name, func(t *testing.T) {
			store, ctx := setup.open(t)
			cases := []struct {
				name         string
				instancePath string
			}{
				{name: "singleton", instancePath: "scout"},
				{name: "template_instance", instancePath: "review/" + uuid.NewString()},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					entityID := uuid.NewString()
					if err := store.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
						InstanceID:      runtimeflowidentity.LogicalInstanceID(tc.instancePath),
						StorageRef:      tc.instancePath,
						EntityID:        entityID,
						WorkflowName:    tc.instancePath,
						WorkflowVersion: "v1",
						CurrentState:    "active",
						EnteredStageAt:  time.Now().UTC(),
						Fields:          map[string]any{},
						EntityType:      "test_entity",
					})); err != nil {
						t.Fatalf("create workflow instance: %v", err)
					}

					if _, found, err := store.Load(ctx, testWorkflowInstanceRoute(tc.instancePath)); err != nil || !found {
						t.Fatalf("load exact route = found %v err %v", found, err)
					}
					if _, found, err := store.Load(ctx, testWorkflowInstanceRoute(entityID)); err != nil {
						t.Fatalf("load entity identity: %v", err)
					} else if found {
						t.Fatalf("entity identity %q addressed workflow route %q", entityID, tc.instancePath)
					}
				})
			}
		})
	}
}

func workflowInstanceRowCount(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances`).Scan(&count); err != nil {
		t.Fatalf("count workflow instances: %v", err)
	}
	return count
}

func TestWorkflowInstanceStoreMutate_RejectsOverlappingStaleSnapshots(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := newPostgresWorkflowInstanceStoreForTest(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	ctx := testWorkflowStoreRunContext(t, store)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	errCh := make(chan error, 2)

	go func() {
		errCh <- store.mutate(ctx, testWorkflowInstanceRoute("mutation-flow"), func(instance *WorkflowInstance) {
			setWorkflowGate(instance, "g_first")
			appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "first"})
			close(firstEntered)
			<-releaseFirst
		})
	}()

	<-firstEntered
	go func() {
		errCh <- store.mutate(ctx, testWorkflowInstanceRoute("mutation-flow"), func(instance *WorkflowInstance) {
			close(secondEntered)
			setWorkflowGate(instance, "g_second")
			appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "second"})
			instance.CurrentState = "done"
		})
	}()

	<-secondEntered
	if err := <-errCh; err != nil {
		t.Fatalf("second mutation commit: %v", err)
	}
	close(releaseFirst)
	if err := <-errCh; err == nil || !strings.Contains(err.Error(), "changed before commit") {
		t.Fatalf("stale first mutation error = %v, want optimistic conflict", err)
	}

	instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute("mutation-flow"))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := instance.CurrentState; got != "done" {
		t.Fatalf("current_state = %q, want done", got)
	}
	gates := instance.Gates
	if gates["g_first"] || !gates["g_second"] {
		t.Fatalf("gates = %#v, want only the committed snapshot", gates)
	}
	evidence := workflowEvidenceEntries(t, instance, "audit")
	if len(evidence) != 1 {
		t.Fatalf("evidence entries = %d, want 1 (%#v)", len(evidence), evidence)
	}
}

func TestUpdateEntityState_RejectsCompetingStaleCallbackSnapshot(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := newPostgresWorkflowInstanceStoreForTest(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	pc := &PipelineCoordinator{
		workflowStore: store,
		module:        NewGenericTestWorkflowModule(),
		entityLocks:   map[string]*sync.Mutex{},
	}

	ctx := testWorkflowStoreRunContext(t, store)
	transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, testWorkflowInstanceRoute("mutation-flow"), entityID, "workflow.completed")
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	callbackErr := make(chan error, 1)
	transitionErr := make(chan error, 1)

	go func() {
		callbackErr <- store.mutate(ctx, testWorkflowInstanceRoute("mutation-flow"), func(instance *WorkflowInstance) {
			setWorkflowGate(instance, "g_ready")
			close(firstEntered)
			<-releaseFirst
		})
	}()

	<-firstEntered
	go func() {
		transitionErr <- pc.persistWorkflowStateForTest(transitionCtx, testWorkflowInstanceRoute("mutation-flow"), entityID, "done", "workflow.completed")
	}()

	if err := <-transitionErr; err != nil {
		t.Fatalf("closed transition commit: %v", err)
	}
	close(releaseFirst)
	if err := <-callbackErr; err == nil || !strings.Contains(err.Error(), "changed before commit") {
		t.Fatalf("stale callback commit error = %v, want optimistic conflict", err)
	}

	instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute("mutation-flow"))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := instance.CurrentState; got != "done" {
		t.Fatalf("current_state = %q, want done", got)
	}
	gates := instance.Gates
	if gates["g_ready"] {
		t.Fatalf("gates = %#v, stale callback mutation survived", gates)
	}
	if len(instance.TransitionHistory) == 0 {
		t.Fatal("expected transition history to be recorded")
	}
}

func TestWorkflowInstanceStoreMutate_PersistsSingleWriterUpdates(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := newPostgresWorkflowInstanceStoreForTest(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	if err := store.mutate(testWorkflowStoreRunContext(t, store), testWorkflowInstanceRoute("mutation-flow"), func(instance *WorkflowInstance) {
		setWorkflowGate(instance, "g_single")
		appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "single"})
		instance.CurrentState = "processing"
	}); err != nil {
		t.Fatalf("mutate: %v", err)
	}

	instance, ok, err := store.Load(testWorkflowStoreRunContext(t, store), testWorkflowInstanceRoute("mutation-flow"))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := instance.CurrentState; got != "processing" {
		t.Fatalf("current_state = %q, want processing", got)
	}
	gates := instance.Gates
	if !gates["g_single"] {
		t.Fatalf("gates = %#v, want g_single=true", gates)
	}
	evidence := workflowEvidenceEntries(t, instance, "audit")
	if len(evidence) != 1 {
		t.Fatalf("evidence entries = %d, want 1 (%#v)", len(evidence), evidence)
	}
}

func TestWorkflowInstanceStoreMutate_IgnoresSchedulerOwnedTimerRows(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := newPostgresWorkflowInstanceStoreForTest(db)
	entityID := uuid.NewString()
	storageRef := "mutation-flow"
	now := time.Now().UTC().Round(time.Microsecond)
	if err := store.upsert(testWorkflowStoreRunContext(t, store), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      storageRef,
		StorageRef:      storageRef,
		EntityID:        entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Fields:          map[string]any{},
		StateBuckets:    map[string]any{},
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	ctx := testWorkflowStoreRunContext(t, store)
	routing, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
		FlowID: "mutation-flow", FlowInstance: storageRef, EntityID: entityID,
	})
	if err != nil {
		t.Fatal(err)
	}
	insertGenericSchedulePersistenceFixture(t, ctx, db, true, runtimegenericschedule.AdmissionCommand{
		ScheduleKey: "task_timer", RunID: runtimecorrelation.RunIDFromContext(ctx), EntityID: entityID, FlowInstance: storageRef,
		OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: runtimeWorkflowID,
		EventType: "timer.task_timeout", Payload: semanticvalue.EmptyObject(), RoutingSource: routing,
		ExecutionMode: executionmode.Live,
		Due:           runtimegenericschedule.AbsoluteDue(now.Add(2 * time.Hour)), TaskID: "task_timer",
	})

	if err := store.mutate(testWorkflowStoreRunContext(t, store), testWorkflowInstanceRoute(storageRef), func(instance *WorkflowInstance) {
		instance.CurrentState = "active"
	}); err != nil {
		t.Fatalf("mutate with scheduler-owned timer row present: %v", err)
	}

	_, ok, err := store.Load(testWorkflowStoreRunContext(t, store), testWorkflowInstanceRoute(storageRef))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	var schedulerRows int
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COUNT(*)
		FROM timers
		WHERE entity_id = $1::uuid
		  AND flow_instance = $2
		  AND owner_agent = $3
	`, entityID, storageRef, runtimeWorkflowID).Scan(&schedulerRows); err != nil {
		t.Fatalf("count scheduler-owned timers: %v", err)
	}
	if schedulerRows != 1 {
		t.Fatalf("scheduler-owned timer rows = %d, want 1", schedulerRows)
	}
}

func seedWorkflowInstanceForMutationTest(t *testing.T, store *workflowInstanceStore, entityID string) {
	t.Helper()
	if err := store.upsert(testWorkflowStoreRunContext(t, store), materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "mutation-flow",
		StorageRef:      "mutation-flow",
		EntityID:        entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Fields:          map[string]any{},
		StateBuckets:    map[string]any{},
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
}

func setWorkflowGate(instance *WorkflowInstance, gate string) {
	if instance.Gates == nil {
		instance.Gates = map[string]bool{}
	}
	instance.Gates[gate] = true
}

func appendWorkflowEvidence(instance *WorkflowInstance, bucketID string, payload map[string]any) {
	bucket := workflowMutableStateBucket(instance, "evidence")
	workflowAppendEvidence(bucket, bucketID, payload)
	workflowSetStateBucket(instance, "evidence", bucket)
}

func workflowEvidenceEntries(t *testing.T, instance WorkflowInstance, bucketID string) []map[string]any {
	t.Helper()
	evidence, ok := workflowStateBucketObject(instance, "evidence")
	if !ok {
		return nil
	}
	raw, ok := evidence[bucketID].([]any)
	if !ok {
		t.Fatalf("evidence[%s] = %#v, want []any", bucketID, evidence[bucketID])
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("evidence entry = %#v, want map[string]any", item)
		}
		out = append(out, entry)
	}
	return out
}
