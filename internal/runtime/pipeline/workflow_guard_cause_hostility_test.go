package pipeline

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func TestPipelineRejectsFabricatedGuardCauseOnBothStores(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "name: guard-cause-hostility\nstages:\n  ready: {initial: true}\n  done: {terminal: true}\n  killed: {terminal: true}\n",
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "observe: {}\nkill: {}\nkill_chain: {}\n",
		"nodes.yaml": `router:
  event_handlers:
    observe:
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'observed'"}
    kill:
      guard: {id: declared-kill, check: "false", on_fail: kill}
    kill_chain:
      guard:
        checks:
          - {id: leading-pass, check: "true"}
          - {id: declared-kill, check: "false"}
        on_fail: kill
`,
	})
	graph, found := semanticview.WorkflowStageTopology(source, ".")
	if !found || len(graph.Edges) != 0 {
		t.Fatalf("fixture must have declared stages but no ordinary transition edges: %#v", graph)
	}
	for _, backend := range workflowJoinStoreCases() {
		for _, tc := range []struct {
			name, node, handler, guard, target string
			evaluated                          []string
			wantError                          string
		}{
			{"nonexistent_guard", "router", "observe", "fabricated-guard", "killed", []string{"fabricated-guard"}, "kill disposition"},
			{"declared_guard_wrong_target", "router", "kill", "declared-kill", "done", []string{"declared-kill"}, "kill target"},
			{"nonexistent_node_and_handler", "missing", "observe", "fabricated-guard", "killed", []string{"fabricated-guard"}, "no exact compiled handler"},
			{"declared_handler_wrong_guard_name", "router", "kill", "fabricated-guard", "killed", []string{"fabricated-guard"}, "guard checks"},
			{"declared_handler_extra_prefix", "router", "kill", "declared-kill", "killed", []string{"extra-label", "declared-kill"}, "guard checks"},
			{"declared_handler_wrong_prefix", "router", "kill_chain", "declared-kill", "killed", []string{"extra-label", "declared-kill"}, "guard checks"},
			{"failed_guard_not_last", "router", "kill_chain", "leading-pass", "killed", []string{"leading-pass", "declared-kill"}, "guard checks"},
		} {
			t.Run(backend.name+"/"+tc.name, func(t *testing.T) {
				store, ctx := backend.open(t)
				pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
					Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
				})
				runID := correlation.RunIDFromContext(ctx)
				entityID := eventtest.UUID("guard-cause-" + backend.name + "-" + tc.name)
				address := testEngineStateAddress(".", runID, entityID)
				if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "1",
					Mode: contracts.FlowModeStatic, CurrentState: "ready", EntityType: "test_entity", Fields: map[string]any{"marker": "unchanged"},
				})); err != nil {
					t.Fatal(err)
				}
				before, found, err := store.Load(ctx, address.Route)
				if err != nil || !found {
					t.Fatalf("load before: found=%v err=%v", found, err)
				}
				node, err := identity.AdmitExecutableNodeDeclaration(".", tc.node)
				if err != nil {
					t.Fatal(err)
				}
				cause, err := workflowlifecycle.NewGuardTermination(graph, node, tc.handler, tc.guard, "ready", tc.target, tc.evaluated)
				if err != nil {
					t.Fatalf("construct hostile cause: %v", err)
				}
				state := testEngineStateMutation(map[string]any{"marker": "unauthorized"}, nil, nil)
				state.NextState, state.Transition = tc.target, &cause
				state.TriggerEventID, state.TriggerEventType = eventtest.UUID("guard-trigger-"+tc.name), tc.handler
				state.TriggeredAt = time.Now().UTC()
				persistWorkflowTimerEvent(t, store, ctx, state.TriggerEventID, state.TriggerEventType, runID, entityID, nil, state.TriggeredAt)
				effect, err := workflowlifecycle.NewAcceptedEvent(address.Route, identity.NormalizeEntityID(entityID), state.TriggerEventID, state.TriggerEventType, executionmode.Live, state.TriggeredAt, &cause)
				if err != nil {
					t.Fatal(err)
				}
				mutation := engine.EngineMutation{Address: address, State: state, HandlerRuleSelection: handlerselection.NotApplicable(), LifecycleEffects: []workflowlifecycle.Effect{effect}}
				if err := mutation.ValidateTransitionEvidence(); err != nil {
					t.Fatalf("projection-consistent hostile fixture rejected before source ownership check: %v", err)
				}
				owner := pipelineEngineMutationOwner{store: store, state: pipelineEngineStateRepo{coordinator: pc}}
				_, commitErr := owner.CommitEngineMutation(ctx, mutation)
				after, found, err := store.Load(ctx, address.Route)
				if err != nil || !found {
					t.Fatalf("load after: found=%v err=%v", found, err)
				}
				if commitErr == nil {
					t.Errorf("FABRICATED GUARD COMMITTED: stage=%s->%s revision=%d->%d history=%d->%d marker=%v cause=%s",
						before.CurrentState, after.CurrentState, before.Revision, after.Revision, len(before.TransitionHistory), len(after.TransitionHistory), after.Fields["marker"], cause.ID())
				} else if !strings.Contains(commitErr.Error(), tc.wantError) {
					t.Errorf("rejected at wrong boundary: got %v, want %q", commitErr, tc.wantError)
				}
				if !reflect.DeepEqual(before, after) {
					t.Errorf("fabricated guard changed durable instance despite absent source authority")
				}
			})
		}
	}
}
