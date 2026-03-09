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
	if rt.ContractBundle() == nil {
		t.Fatal("expected workflow contract bundle")
	}
	if rt.WorkflowDefinition() == nil {
		t.Fatal("expected workflow definition")
	}
	if rt.WorkflowInstanceStore() == nil {
		t.Fatal("expected workflow instance store")
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
	if got := rt.ContractBundle().Workflow.Workflow.Name; got != "empire_vertical_pipeline" {
		t.Fatalf("unexpected workflow bundle name: %s", got)
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
	if !pc.GuardRegistry().IsExecutable("signal_above_threshold") {
		t.Fatal("expected signal_above_threshold to be executable")
	}
	if !pc.ActionRegistry().HasAction("emit_validation_started") {
		t.Fatal("expected empire action emit_validation_started")
	}
	if !pc.ActionRegistry().HasAction("spinup_opco_org") {
		t.Fatal("expected platform action spinup_opco_org")
	}
	if !pc.ActionRegistry().IsExecutable("spinup_opco_org") {
		t.Fatal("expected spinup_opco_org to be executable")
	}
	guard, ok := pc.GuardRegistry().Guard("signal_above_threshold")
	if !ok {
		t.Fatal("expected signal_above_threshold guard definition")
	}
	if guard.Category != "empire" {
		t.Fatalf("unexpected guard category: %+v", guard)
	}
	action, ok := pc.ActionRegistry().Action("emit_validation_started")
	if !ok {
		t.Fatal("expected emit_validation_started action definition")
	}
	if action.Emits != "validation.started" {
		t.Fatalf("unexpected action emits: %+v", action)
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
		if node.ID == "scan-orchestrator" || node.ID == "discovery-aggregator" {
			continue
		}
		if len(node.OwnedTransitions) == 0 {
			t.Fatalf("workflow node %s missing owned transitions", node.ID)
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
