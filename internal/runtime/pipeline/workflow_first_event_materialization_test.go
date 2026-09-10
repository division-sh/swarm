package pipeline

import (
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestDeclarativeFirstEventTransitionsFromCanonicalInitialStateOnBothStores(t *testing.T) {
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: first-event-transition\ninitial_state: waiting\nstates: [waiting, done]\nterminal_states: [done]\n",
		"entities.yaml": "first_event_entity: {}\n",
		"events.yaml":   "request.accepted: {}\n",
		"nodes.yaml":    "acceptor:\n  execution_type: system_node\n  subscribes_to: [request.accepted]\n  event_handlers:\n    request.accepted:\n      advances_to: done\n",
	})
	module := &previewWorkflowModule{bundle: bundle}

	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			runID := correlation.RunIDFromContext(ctx)
			pc := &PipelineCoordinator{
				bus:            &recordingPipelineBus{},
				workflowStore:  store,
				expressionEval: newWorkflowExpressionEvaluator(),
				entityLocks:    map[string]*sync.Mutex{},
				module:         module,
			}
			configureWorkflowLifecycleForTest(t, pc)

			entityID := uuid.NewString()
			eventID := uuid.NewString()
			occurredAt := time.Now().UTC()
			evt := eventtest.RunCreatingRootIngress(
				eventID,
				events.EventType("request.accepted"),
				"",
				"",
				[]byte(`{}`),
				0,
				runID,
				"",
				testWorkflowSourceEnvelope(".", runID, entityID),
				occurredAt,
			)
			dialect := authoractivityfixture.DialectPostgres
			if store.isSQLite() {
				dialect = authoractivityfixture.DialectSQLite
			}
			seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
			engine := newCoordinatorHandlerExecutionEngine(pc, pipelineSourceNode(t, pc.SemanticSource(), ".", "acceptor"))
			outcome, err := engine.ExecuteHandlerSteps(ctx, runtimecontracts.SystemNodeEventHandler{
				AdvancesTo: "done",
			}, evt, "request.accepted")
			if err != nil {
				t.Fatalf("execute first event transition: %v", err)
			}
			if outcome == nil || !outcome.Handled {
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
