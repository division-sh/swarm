package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestRetainedNodeContractHandlerUsesRuntimeEnginePath(t *testing.T) {
	pc, bus, ctx := newConstructorHandlerUnitCoordinator(t, handlerEngineProjectNodeModule(t))

	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		"00000000-0000-0000-0000-000000000001", events.EventType("custom.trigger"), "", "", nil, 0, testPipelineRunID, "",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID}), eventtest.StaticFlowRoutingSource(".", testPipelineRunID, ""), time.Unix(1, 0).UTC(),
	)
	ctx, state := prepareConstructorUnitDelivery(t, pc, ctx, ".", "node-a", evt)
	outcome, err := executeNodeContractHandlerWithHandoff(t, pc, ctx, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{Event: evt, State: state, HandlerEventKey: "custom.trigger"}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !outcome.Handled {
		t.Fatalf("handled outcome = %#v", outcome)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "custom.emitted" {
		t.Fatalf("published event type = %q, want custom.emitted", got)
	}
}

func TestHandlerExecutionStateSnapshotKeepsAuthoredGatesFieldSeparate(t *testing.T) {
	snapshot, err := handlerExecutionStateSnapshot(runtimecontracts.SystemNodeEventHandler{}, "ent-1", WorkflowState{
		EntityID: "ent-1",
		Stage:    WorkflowStateID("queued"),
		Metadata: map[string]any{"gates": "authored"},
	}, "default", "v-test")
	if err != nil {
		t.Fatalf("handlerExecutionStateSnapshot: %v", err)
	}
	if got := snapshot.Fields["gates"]; got != "authored" {
		t.Fatalf("authored gates field = %#v, want authored", got)
	}
	if len(snapshot.Gates) != 0 {
		t.Fatalf("typed gates = %#v, want empty", snapshot.Gates)
	}
}
