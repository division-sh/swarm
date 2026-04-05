package pipeline

import (
	"context"
	"testing"

	"swarm/internal/events"
	runtimeengine "swarm/internal/runtime/engine"
)

type pipelineTestBus struct{}

func (pipelineTestBus) Publish(context.Context, events.Event) error { return nil }
func (pipelineTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (pipelineTestBus) DirectSubscribe(string) <-chan events.Event { return make(chan events.Event) }
func (pipelineTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (pipelineTestBus) SubscribeAll(string) <-chan events.Event     { return make(chan events.Event) }
func (pipelineTestBus) ResetSubscribers()                           {}
func (pipelineTestBus) LogRuntime(context.Context, RuntimeLogEntry) {}
func (pipelineTestBus) ResolveSubscribedRecipients(string) []string { return nil }
func (pipelineTestBus) EngineOutbox() runtimeengine.OutboxWriter    { return noOpEngineOutbox{} }
func (pipelineTestBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func TestWorkflowRuntime_NodesOwnRegisteredPolicies(t *testing.T) {
	pc := NewPipelineCoordinatorWithOptions(pipelineTestBus{}, nil, PipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
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
			if node.ID == "build-orchestrator" {
				continue
			}
			t.Fatalf("missing executor for node %s", node.ID)
		}
		if len(executor.Subscriptions()) == 0 {
			t.Fatalf("executor %s missing subscriptions", node.ID)
		}
		subscriptions := make(map[string]struct{}, len(node.Subscriptions))
		for _, sub := range node.Subscriptions {
			subscriptions[string(sub)] = struct{}{}
		}
		if len(node.Policies) == 0 {
			t.Fatalf("workflow node %s missing runtime policies", node.ID)
		}
		for eventType := range node.Policies {
			if _, ok := subscriptions[eventType]; !ok {
				t.Fatalf("policy %s for node %s is not backed by a node subscription", eventType, node.ID)
			}
		}
	}
}
