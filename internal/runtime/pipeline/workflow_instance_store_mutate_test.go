package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestWorkflowInstanceStoreMutate_SerializesOverlappingMutations(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	store := NewWorkflowInstanceStore(db)
	entityID := uuid.NewString()
	seedWorkflowInstanceForMutationTest(t, store, entityID)

	ctx := context.Background()
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

	ctx := context.Background()
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

	if err := store.Mutate(context.Background(), entityID, func(instance *WorkflowInstance) {
		setWorkflowGate(instance, "g_single")
		appendWorkflowEvidence(instance, "audit", map[string]any{"writer": "single"})
		instance.CurrentState = "processing"
	}); err != nil {
		t.Fatalf("mutate: %v", err)
	}

	instance, ok, err := store.Load(context.Background(), entityID)
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

func seedWorkflowInstanceForMutationTest(t *testing.T, store *WorkflowInstanceStore, entityID string) {
	t.Helper()
	if err := store.Upsert(context.Background(), WorkflowInstance{
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
