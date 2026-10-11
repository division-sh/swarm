package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func VerifyNativeCreateEntityHandlerEffectsAreExactOnceAcrossStoreMutationsForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativeExactOnceCoordinatorForTest(t, backend, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			eventID := uuid.NewString()
			evt := eventtest.ExistingRunRootIngress(eventID,
				events.EventType("thing.created"), "", "", mustJSON(map[string]any{"amount": 250, "who": "alice"}), 0, runtimecorrelation.RunIDFromContext(ctx), events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "validation", FlowInstance: "validation", EntityID: FlowInstanceEntityID("validation")}), time.Now().UTC())

			initial := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, "validation")
			ctx, _ = nativeExactOncePublicationForTest(t, fixture, pc, ctx, evt, initial)
			initial = constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "validation")
			node := pipelineSourceNode(t, pc.SemanticSource(), "validation", "w-node")
			// Settle a real preliminary accepted-event occurrence while the
			// instance is non-final. The component deliberately has no business
			// mutation; the later full handler must preserve this timer identity.
			arm := eventtest.ExistingRunRootIngress(uuid.NewString(), evt.Type(), "", "", evt.Payload(), 0, evt.RunID(), evt.Envelope(), time.Now().UTC())
			armCtx, armRoute := nativeExactOncePublicationForTest(t, fixture, pc, ctx, arm, initial)
			if _, err := executeNativeClaimedPipelineHandlerForTest(t, pc, armCtx, node, runtimecontracts.SystemNodeEventHandler{}, workflowTriggerContext{Event: arm, HandlerEventKey: "thing.created", State: WorkflowState{Stage: WorkflowStateID(initial.CurrentState), Metadata: cloneMap(initial.Fields)}}); err != nil {
				t.Fatalf("settle preliminary accepted timer occurrence: %v", err)
			}
			armID, err := runtimedelivery.DeliveryID(arm.ID(), armRoute)
			if err != nil {
				t.Fatal(err)
			}
			armSnapshot, err := fixture.Store.Snapshot(ctx, armID)
			if err != nil || armSnapshot.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("preliminary accepted timer was not settled: %+v/%v", armSnapshot, err)
			}
			accepted := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, initial.EntityID)
			if len(accepted) != 1 || accepted[0].Status != workflowTimerStatusActive {
				t.Fatalf("pre-final accepted timers = %#v", accepted)
			}
			result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, node, exactOnceCreateEntityHandler(), workflowTriggerContext{
				Event:           evt,
				HandlerEventKey: "thing.created",
				State: WorkflowState{
					Stage:    WorkflowStateID("new"),
					Metadata: map[string]any{},
				},
			})
			if err != nil {
				t.Fatalf("executeNodeContractHandler: %v", err)
			}
			if !result.Handled {
				t.Fatal("expected handler to run")
			}
			if got := bus.publishedCount(); got != 1 {
				t.Fatalf("published event count = %d, want 1", got)
			}
			if got := bus.committedCount(); got != 1 {
				t.Fatalf("outbox intent count = %d, want 1", got)
			}
			entityID := bus.publishedEvent(0).EntityID()
			if entityID == "" {
				t.Fatal("expected emitted event to carry created entity id")
			}
			instance, ok, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, bus.publishedEvent(0).FlowInstance()))
			if err != nil {
				t.Fatalf("load created entity: %v", err)
			}
			if !ok {
				t.Fatal("created entity missing")
			}
			activations := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, entityID)
			if len(activations) != 1 || strings.TrimSpace(activations[0].EventType) != "validation/timer.check" {
				t.Fatalf("canonical workflow timers = %#v, want one validation/timer.check activation", activations)
			}
			if activations[0].Ref != accepted[0].Ref {
				t.Fatal("final entry replaced the already accepted timer")
			}
			if got := strings.TrimSpace(instance.CurrentState); got != "done" {
				t.Fatalf("current state = %q, want done", got)
			}
			assertMetadataNumber(t, instance.Fields, "amount", 250)
			assertMetadataString(t, instance.Fields, "who", "alice")
			assertMetadataNumber(t, instance.Fields, "counter", 1)
			if !instance.Gates["validation/ready"] {
				t.Fatalf("ready gate = false, want true (all=%v)", instance.Gates)
			}

			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "amount", "entity_initial_value", "create_entity", 1)
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "who", "entity_initial_value", "create_entity", 1)
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "counter", "entity_initial_value", "create_entity", 1)
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "amount", "workflow_instance_store", "upsert", 0)
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "who", "workflow_instance_store", "upsert", 0)
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "counter", "workflow_instance_store", "upsert", 0)
		})
	}
}

func VerifyNativeDispatchWorkflowNodeEventSkipsAlreadyProcessedCreateEntityHandlerForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativeExactOnceCoordinatorForTest(t, backend, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			eventID := uuid.NewString()
			evt := eventtest.ExistingRunRootIngress(eventID,
				events.EventType("thing.created"), "", "", mustJSON(map[string]any{"amount": 250, "who": "alice"}), 0, runtimecorrelation.RunIDFromContext(ctx), events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "validation", FlowInstance: "validation", EntityID: FlowInstanceEntityID("validation")}), time.Now().UTC())
			initial := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, "validation")
			deliveryCtx, route := nativeExactOncePublicationForTest(t, fixture, pc, ctx, evt, initial)
			constructNativePipelineScenarioForTest(t, fixture, pc, deliveryCtx, "validation")

			handled, err := pc.dispatchWorkflowNodeEventResult(deliveryCtx, evt)
			if err != nil {
				t.Fatalf("first dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("first dispatch handled = false, want true")
			}
			handled, err = pc.dispatchWorkflowNodeEventResult(deliveryCtx, evt)
			if err != nil {
				t.Fatalf("second dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("second dispatch handled = false, want true for already processed node event")
			}

			if got := bus.publishedCount(); got != 1 {
				t.Fatalf("published event count after duplicate dispatch = %d, want 1", got)
			}
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := fixture.Store.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 1 {
				t.Fatalf("exact outcomes = %v/%v", outcomes, err)
			}
			snapshot, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("exact delivered obligation = %+v/%v", snapshot, err)
			}
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "amount", "entity_initial_value", "create_entity", 1)
			assertNativePipelineMutationCountForTest(t, fixture, ctx, eventID, "amount", "workflow_instance_store", "upsert", 0)
		})
	}
}

func exactOnceCreateEntityHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			SourceEvent: "thing.created",
			Writes: []runtimecontracts.WorkflowDataWrite{
				{SourceField: "amount", TargetField: "amount"},
				{SourceField: "who", TargetField: "who"},
				{TargetField: "counter", Value: runtimecontracts.CELExpression("entity.counter + 1")},
			},
		},
		SetsGate:   &runtimecontracts.GateSpec{Name: "ready"},
		AdvancesTo: "done",
		Emit: runtimecontracts.EmitSpec{
			Event: "thing.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"amount": runtimecontracts.CELExpression("entity.amount"),
				"who":    runtimecontracts.CELExpression("entity.who"),
			},
		},
	}
}

func assertMetadataNumber(t *testing.T, metadata map[string]any, key string, want float64) {
	t.Helper()
	got := metadata[key]
	switch v := got.(type) {
	case int:
		if float64(v) == want {
			return
		}
	case int64:
		if float64(v) == want {
			return
		}
	case float64:
		if v == want {
			return
		}
	}
	t.Fatalf("metadata[%s] = %#v (%T), want %v", key, got, got, want)
}

func assertMetadataString(t *testing.T, metadata map[string]any, key, want string) {
	t.Helper()
	if got := strings.TrimSpace(asString(metadata[key])); got != want {
		t.Fatalf("metadata[%s] = %#v, want %q", key, metadata[key], want)
	}
}
