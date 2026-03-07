package pipeline

import (
	"context"
	"testing"

	"empireai/internal/events"
)

func TestFactoryPipelineCoordinator_ImplementsWorkflowRuntime(t *testing.T) {
	var rt WorkflowRuntime = NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	if rt == nil {
		t.Fatal("expected workflow runtime")
	}
	if rt.WorkflowDefinition() == nil {
		t.Fatal("expected workflow definition")
	}
	if rt.TransitionEvaluator() == nil {
		t.Fatal("expected transition evaluator")
	}
	if rt.GuardRegistry() == nil {
		t.Fatal("expected guard registry")
	}
	if rt.ActionRegistry() == nil {
		t.Fatal("expected action registry")
	}
	if len(rt.WorkflowNodes()) == 0 {
		t.Fatal("expected workflow nodes")
	}
}

func TestEmpireWorkflowRegistries_ResolveKnownIDs(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	if !pc.GuardRegistry().HasGuard("has_vertical_id") {
		t.Fatal("expected platform guard has_vertical_id")
	}
	if !pc.GuardRegistry().HasGuard("signal_above_threshold") {
		t.Fatal("expected empire guard signal_above_threshold")
	}
	if !pc.ActionRegistry().HasAction("emit_validation_started") {
		t.Fatal("expected empire action emit_validation_started")
	}
	if !pc.ActionRegistry().HasAction("spinup_opco_org") {
		t.Fatal("expected platform action spinup_opco_org")
	}
}

func TestFactoryPipelineCoordinator_WorkflowNodesHaveExecutors(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	executors := pc.workflowNodeExecutors()
	if len(executors) == 0 {
		t.Fatal("expected workflow node executors")
	}
	execByID := make(map[string]struct{}, len(executors))
	for _, executor := range executors {
		execByID[executor.NodeID()] = struct{}{}
	}
	for _, node := range pc.WorkflowNodes() {
		if _, ok := execByID[node.ID]; !ok {
			t.Fatalf("workflow node %s missing runtime executor", node.ID)
		}
	}
}

type pipelineTestBus struct{}

func (pipelineTestBus) Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (pipelineTestBus) Publish(ctx context.Context, evt events.Event) error { return nil }

func (pipelineTestBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) error {
	return nil
}

func (pipelineTestBus) ResolveSubscribedRecipients(eventType string) []string { return nil }

func (pipelineTestBus) LogRuntime(ctx context.Context, entry RuntimeLogEntry) {}
