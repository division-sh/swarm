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
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"acceptor": {ID: "acceptor", ExecutionType: "system_node"},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:         "first-event-transition",
			Version:      "1",
			InitialStage: "waiting",
			Stages: []runtimecontracts.WorkflowStageContract{
				{ID: "waiting"},
				{ID: "done"},
			},
			TerminalStages: []string{"done"},
		},
	}
	workflow := NewWorkflowDefinition("first-event-transition", []WorkflowStage{
		{Name: "waiting"},
		{Name: "done", Terminal: true},
	}, []WorkflowTransition{{
		Name: "accept",
		From: []WorkflowStateID{"waiting"},
		To:   "done",
	}})
	module := handlerTestWorkflowModule("first-event-transition", "acceptor").(*previewWorkflowModule)
	module.bundle.Semantics = bundle.Semantics
	module.bundle.RootEntities = testEntityContractsForType("first_event_entity")
	module.workflow = workflow

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
				testWorkflowSourceEnvelope("first-event-transition", runID, entityID),
				occurredAt,
			)
			dialect := authoractivityfixture.DialectPostgres
			if store.isSQLite() {
				dialect = authoractivityfixture.DialectSQLite
			}
			seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
			engine := newCoordinatorHandlerExecutionEngine(pc, pipelineSourceNode(t, pc.SemanticSource(), "first-event-transition", "acceptor"))
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
