package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func VerifyNativeNodeContractFirstEventTransitionsFromCanonicalInitialStateOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: first-event-transition\nstages:\n  waiting: {}\n  done: {final: true}\n",
		"entities.yaml": "first_event_entity: {}\n",
		"events.yaml":   "request.accepted:\n",
		"nodes.yaml":    "acceptor:\n  execution_type: system_node\n  subscribes_to: [request.accepted]\n  event_handlers:\n    request.accepted:\n      advances_to: done\n",
	})

	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx := nativePilotPipelineForTest(t, tc.name, bundle, open)
			store := pc.workflowStore
			runID := correlation.RunIDFromContext(ctx)

			entityID := runID
			eventID := uuid.NewString()
			occurredAt := time.Now().UTC()
			evt := eventtest.ExistingRunRootIngress(
				eventID,
				events.EventType("request.accepted"),
				"",
				"",
				[]byte(`{}`),
				0,
				runID,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}),
				occurredAt,
			)
			node := pipelineSourceNode(t, pc.SemanticSource(), ".", "acceptor")
			ctx, state := prepareNativeConstructorHandlerDeliveryForTest(t, fixture, pc, ctx, ".", "acceptor", evt)
			outcome, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, node, runtimecontracts.SystemNodeEventHandler{
				AdvancesTo: "done",
			}, workflowTriggerContext{Event: evt, State: state, HandlerEventKey: "request.accepted"})
			if err != nil {
				t.Fatalf("execute first event transition: %v", err)
			}
			if !outcome.Handled {
				t.Fatalf("first event outcome = %#v, want handled", outcome)
			}

			instance, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
			if err != nil {
				t.Fatalf("load first-event workflow instance: %v", err)
			}
			if !found {
				t.Fatal("first-event workflow instance was not materialized")
			}
			if instance.CurrentState != "done" {
				t.Fatalf("current state = %q, want done", instance.CurrentState)
			}
			if instance.EntityType != "first_event_entity" {
				t.Fatalf("entity type = %q, want first_event_entity", instance.EntityType)
			}
			if len(instance.TransitionHistory) != 1 {
				t.Fatalf("transition history = %#v, want one transition", instance.TransitionHistory)
			}
			transition := instance.TransitionHistory[0]
			if transition.From != "waiting" || transition.To != "done" || transition.TriggerEventID != eventID {
				t.Fatalf("first-event transition = %#v, want waiting -> done from %s", transition, eventID)
			}
		})
	}
}
