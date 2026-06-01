package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStoreMutate_SerializesOverlappingMutations(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	ctx := testWorkflowStoreRunContext(t, store)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	errCh := make(chan error, 2)

	go func() {
		errCh <- store.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
			setWorkflowGate(instance, "g_first")
			appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "first"})
			close(firstEntered)
			<-releaseFirst
		})
	}()

	<-firstEntered
	go func() {
		errCh <- store.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
			close(secondEntered)
			setWorkflowGate(instance, "g_second")
			appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "second"})
			instance.CurrentState = "done"
		})
	}()

	select {
	case <-secondEntered:
		t.Fatal("second mutation entered callback before first mutation committed")
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseFirst)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("mutate[%d]: %v", i, err)
		}
	}

	instance, ok, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := instance.CurrentState; got != "done" {
		t.Fatalf("current_state = %q, want done", got)
	}
	gates := workflowStateGatesAsBools(instance.Metadata)
	if !gates["g_first"] || !gates["g_second"] {
		t.Fatalf("gates = %#v, want both concurrent mutations preserved", gates)
	}
	evidence := workflowEvidenceEntries(t, instance, "audit")
	if len(evidence) != 2 {
		t.Fatalf("evidence entries = %d, want 2 (%#v)", len(evidence), evidence)
	}
}

func TestUpdateEntityState_PreservesMutationCommittedWhileTransitionWaits(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	pc := &PipelineCoordinator{
		workflowStore: store,
		module:        NewGenericTestWorkflowModule(),
		entityLocks:   map[string]*sync.Mutex{},
	}

	ctx := testWorkflowStoreRunContext(t, store)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	errCh := make(chan error, 2)

	go func() {
		errCh <- store.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
			setWorkflowGate(instance, "g_ready")
			close(firstEntered)
			<-releaseFirst
		})
	}()

	<-firstEntered
	go func() {
		errCh <- pc.updateEntityState(ctx, entityID, "done", "workflow.completed")
	}()

	time.Sleep(100 * time.Millisecond)
	close(releaseFirst)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("update[%d]: %v", i, err)
		}
	}

	instance, ok, err := store.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := instance.CurrentState; got != "done" {
		t.Fatalf("current_state = %q, want done", got)
	}
	gates := workflowStateGatesAsBools(instance.Metadata)
	if !gates["g_ready"] {
		t.Fatalf("gates = %#v, want concurrent gate mutation preserved", gates)
	}
	if len(instance.TransitionHistory) == 0 {
		t.Fatal("expected transition history to be recorded")
	}
}

func TestWorkflowInstanceStoreMutate_PersistsSingleWriterUpdates(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	if err := store.Mutate(testWorkflowStoreRunContext(t, store), entityID, func(instance *WorkflowInstance) {
		setWorkflowGate(instance, "g_single")
		appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "single"})
		instance.CurrentState = "processing"
	}); err != nil {
		t.Fatalf("mutate: %v", err)
	}

	instance, ok, err := store.Load(testWorkflowStoreRunContext(t, store), entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := instance.CurrentState; got != "processing" {
		t.Fatalf("current_state = %q, want processing", got)
	}
	gates := workflowStateGatesAsBools(instance.Metadata)
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

	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()
	storageRef := entityID
	now := time.Now().UTC().Round(time.Microsecond)
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      storageRef,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata:        map[string]any{},
		StateBuckets:    map[string]any{},
		TimerState: []WorkflowTimerState{{
			TimerID:   "task_timer",
			EventType: "timer.task_timeout",
			CreatedAt: now,
			FiresAt:   now.Add(time.Hour),
		}},
	}); err != nil {
		t.Fatalf("seed workflow instance with timer state: %v", err)
	}

	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO timers (
			timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, recurring, recurrence_cron, recurrence_interval,
			owner_node, owner_agent, task_type, status
		)
		VALUES (
			$1, $2::uuid, $3, $4, '{}'::jsonb,
			$5, false, NULL, NULL,
			NULL, $6, 'timer', 'active'
		)
	`, "task_timer", entityID, storageRef, "timer.task_timeout", now.Add(2*time.Hour), runtimeWorkflowID); err != nil {
		t.Fatalf("insert scheduler-owned timer row: %v", err)
	}

	if err := store.Mutate(testWorkflowStoreRunContext(t, store), entityID, func(instance *WorkflowInstance) {
		instance.CurrentState = "active"
	}); err != nil {
		t.Fatalf("mutate with scheduler-owned timer row present: %v", err)
	}

	instance, ok, err := store.Load(testWorkflowStoreRunContext(t, store), entityID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if len(instance.TimerState) != 1 {
		t.Fatalf("timer state count = %d, want 1", len(instance.TimerState))
	}
	if got := instance.TimerState[0].TimerID; got != "task_timer" {
		t.Fatalf("timer state id = %q, want task_timer", got)
	}

	var schedulerRows int
	if err := db.QueryRowContext(context.Background(), `
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

func seedWorkflowInstanceForMutationTest(t *testing.T, store *WorkflowInstanceStore, entityID string) {
	t.Helper()
	if err := store.Upsert(testWorkflowStoreRunContext(t, store), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
		Metadata:        map[string]any{},
		StateBuckets:    map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
}

func setWorkflowGate(instance *WorkflowInstance, gate string) {
	metadata := cloneStringAnyMap(instance.Metadata)
	gates := workflowStateGatesAsBools(metadata)
	gates[gate] = true
	metadata["gates"] = workflowBoolGatesAsMap(gates)
	instance.Metadata = metadata
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
