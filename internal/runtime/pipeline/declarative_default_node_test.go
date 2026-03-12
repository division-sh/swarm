package pipeline

import (
	"context"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

func TestCoordinatorHandlerExecutionEngineUsesRuntimeEnginePath(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewFactoryPipelineCoordinatorWithOptions(bus, nil, FactoryPipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})
	pc.validationGate.states["ent-1"] = &validationPipelineState{}

	engine := newCoordinatorHandlerExecutionEngine(pc, "node-a")
	if engine == nil {
		t.Fatal("expected engine")
	}
	outcome, err := engine.ExecuteHandlerSteps(context.Background(), runtimecontracts.SystemNodeEventHandler{
		Emits:  runtimecontracts.EventEmission{Single: "custom.emitted"},
		Action: runtimecontracts.ActionSpec{ID: "increment_revision_count"},
	}, events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-1"))
	if err != nil {
		t.Fatalf("ExecuteHandlerSteps: %v", err)
	}
	if outcome == nil || !outcome.Handled {
		t.Fatalf("handled outcome = %#v", outcome)
	}
	if len(outcome.ActionsExecuted) == 0 || outcome.ActionsExecuted[0] != "increment_revision_count" {
		t.Fatalf("actions executed = %#v", outcome.ActionsExecuted)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "custom.emitted" {
		t.Fatalf("published event type = %q, want custom.emitted", got)
	}
}
