package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func VerifyRetainedNodeContractHandlerUsesRuntimeEnginePathForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityForTest(t, open, testPipelineRunID)

	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		"00000000-0000-0000-0000-000000000001", events.EventType("custom.trigger"), "", "", nil, 0, testPipelineRunID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID}), eventtest.StaticFlowRoutingSource(".", testPipelineRunID, ""), time.Unix(1, 0).UTC(),
	)
	node := pipelineNode(t, ".", "node-a")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: ".", FlowInstance: testPipelineRunID, EntityID: testPipelineRunID,
	})}
	fixture.Publish(ctx, evt, route)
	ctx, stop := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, evt, route)
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	state := mustCurrentWorkflowState(t, pc, ctx, flowidentity.StoredRoute(".", testPipelineRunID, testPipelineRunID), testPipelineRunID)
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
