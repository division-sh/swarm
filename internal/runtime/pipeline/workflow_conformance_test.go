package pipeline

import (
	"context"
	"sync"
	"testing"
)

func TestPipelineStateStore_DefaultsAndProcessedTracking(t *testing.T) {
	store := NewPipelineStateStore(nil, &sync.Mutex{})
	snapshot := store.Load(context.Background())
	if len(snapshot.Scans) != 0 || len(snapshot.PendingDedup) != 0 || len(snapshot.Validations) != 0 || len(snapshot.Processed) != 0 {
		t.Fatalf("expected empty snapshot, got %+v", snapshot)
	}
	processed := map[string]struct{}{}
	if !store.MarkProcessed(context.Background(), processed, "evt-1") {
		t.Fatal("expected first event to be marked processed")
	}
	if store.MarkProcessed(context.Background(), processed, "evt-1") {
		t.Fatal("expected duplicate processed event to be ignored")
	}
}

func TestWorkflowRuntime_NodesOwnRegisteredPolicies(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	nodes := pc.WorkflowNodes()
	if len(nodes) == 0 {
		t.Fatal("expected workflow nodes")
	}
	executors := pc.workflowNodeExecutors()
	executorByID := make(map[string]WorkflowNodeExecutor, len(executors))
	for _, executor := range executors {
		executorByID[executor.NodeID()] = executor
	}
	for _, node := range nodes {
		executor, ok := executorByID[node.ID]
		if !ok {
			t.Fatalf("missing executor for node %s", node.ID)
		}
		if len(executor.Subscriptions()) == 0 {
			t.Fatalf("executor %s missing subscriptions", node.ID)
		}
		for _, sub := range node.Subscriptions {
			if _, ok := workflowNodeEventPolicy("", string(sub)); !ok {
				t.Fatalf("subscription %s is missing from runtime workflow policies", sub)
			}
		}
	}
}
