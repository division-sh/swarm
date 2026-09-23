package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type pipelineTestBus struct{}

func (pipelineTestBus) Publish(context.Context, events.Event) error { return nil }
func (pipelineTestBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (pipelineTestBus) DirectSubscribe(string) <-chan events.Event { return make(chan events.Event) }
func (pipelineTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (pipelineTestBus) SubscribeAll(string) <-chan events.Event           { return make(chan events.Event) }
func (pipelineTestBus) ResetSubscribers()                                 {}
func (pipelineTestBus) LogRuntime(context.Context, RuntimeLogEntry) error { return nil }
func (pipelineTestBus) ResolveSubscribedRecipients(string) []string       { return nil }
func (pipelineTestBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func TestWorkflowRuntime_NodesOwnRegisteredPolicies(t *testing.T) {
	pc := newPreviewPipelineCoordinatorForTest(pipelineTestBus{}, PipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
	nodes := pc.WorkflowNodes()
	if len(nodes) == 0 {
		t.Fatal("expected workflow nodes")
	}
	source := pc.SemanticSource()
	for _, node := range nodes {
		record, ok := source.ExecutableNode(node.Node)
		if !ok {
			if node.Node.NodeID() == "build-orchestrator" {
				continue
			}
			t.Fatalf("missing compiled executable node %s", node.Node.Key())
		}
		admittedSubscriptions := 0
		for _, eventType := range runtimecontracts.EffectiveSystemNodeSubscriptions(record.Entry) {
			aliases, err := workflowNodeSubscriptionAliases(source, node.Node, eventType)
			if err != nil {
				t.Fatalf("node %s subscription %s: %v", node.Node.Key(), eventType, err)
			}
			admittedSubscriptions += len(aliases)
		}
		if admittedSubscriptions == 0 {
			t.Fatalf("compiled node %s missing subscriptions", node.Node.Key())
		}
		subscriptions := make(map[string]struct{}, len(node.Subscriptions))
		for _, sub := range node.Subscriptions {
			subscriptions[string(sub)] = struct{}{}
		}
		if len(node.Policies) == 0 {
			t.Fatalf("workflow node %s missing runtime policies", node.Node.Key())
		}
		for eventType := range node.Policies {
			if _, ok := subscriptions[eventType]; !ok {
				t.Fatalf("policy %s for node %s is not backed by a node subscription", eventType, node.Node.Key())
			}
		}
	}
}
