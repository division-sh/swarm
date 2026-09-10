package pipeline

import (
	"context"
	"encoding/json"
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

func transitionMutationSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "name: transition-proof\nstages:\n  ready: {initial: true}\n  other: {}\n  done: {terminal: true}\n",
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "advance: {}\nforeign: {}\n",
		"nodes.yaml": `router:
  event_handlers:
    advance:
      advances_to: done
      rules:
        - {id: chosen, condition: else}
    foreign:
      advances_to: other
      rules:
        - {id: other, condition: else}
`,
	})
}

func TestPipelineCompiledTransitionRejectsContradictoryEvidenceOnBothStores(t *testing.T) {
	source := transitionMutationSource(t)
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("missing compiled root graph")
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "router")
	if err != nil {
		t.Fatal(err)
	}
	handlers := source.ExecutableNodeEventHandlers(node)
	site := contracts.WorkflowTransitionSite{Node: node, HandlerEvent: "advance", AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler}
	compiled, err := graph.AdmitTransition(site, "ready", "done")
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := handlers["advance"].Rules[0].DeclarationIdentity()
	if !ok {
		t.Fatal("missing selected rule reference")
	}
	selection, err := handlerselection.Selected(handlerselection.ContextRules, ref, "chosen")
	if err != nil {
		t.Fatal(err)
	}
	cause, err := workflowlifecycle.NewCompiledTransition(compiled, selection, nil)
	if err != nil {
		t.Fatal(err)
	}
	foreignRef, _ := handlers["foreign"].Rules[0].DeclarationIdentity()
	foreignSelection, err := handlerselection.Selected(handlerselection.ContextRules, foreignRef, "other")
	if err != nil {
		t.Fatal(err)
	}
	foreignCause, err := workflowlifecycle.NewCompiledTransition(compiled, foreignSelection, nil)
	if err != nil {
		t.Fatal(err)
	}
	wrongContext, err := handlerselection.Selected(handlerselection.ContextOnComplete, ref, "chosen")
	if err != nil {
		t.Fatal(err)
	}
	contextCause, err := workflowlifecycle.NewCompiledTransition(compiled, wrongContext, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A structurally valid carrier from another admitted declaration set is not
	// permission in this selected source, even when every stage name is local.
	alternateGraph := contracts.BuildWorkflowStageTopology(".", "ready", graph.Stages, graph.TerminalStages,
		[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "advance", AdvancesTo: "other"}}, nil, nil)
	alternate, err := alternateGraph.AdmitTransition(site, "ready", "other")
	if err != nil {
		t.Fatal(err)
	}
	alternateCause, err := workflowlifecycle.NewCompiledTransition(alternate, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	gateGraph := contracts.BuildWorkflowStageTopology(".", "ready", graph.Stages, graph.TerminalStages, nil, nil, nil,
		[]contracts.WorkflowGatePlan{{FlowID: ".", Stage: "ready", Decision: "forged", Outcomes: map[string]contracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done"}}}})
	gate, err := gateGraph.AdmitTransition(contracts.WorkflowTransitionSite{DecisionID: "forged", Verdict: "approve"}, "ready", "done")
	if err != nil {
		t.Fatal(err)
	}
	gateCause, err := workflowlifecycle.NewCompiledTransition(gate, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	projectedCause := func(flow string, edge contracts.WorkflowStageTopologyEdge) *workflowlifecycle.Transition {
		t.Helper()
		projection := graph
		projection.FlowID, projection.Edges = flow, []contracts.WorkflowStageTopologyEdge{edge}
		admitted, err := projection.AdmitTransition(edge.Site(), edge.From, edge.To)
		if err != nil {
			t.Fatal(err)
		}
		result, err := workflowlifecycle.NewCompiledTransition(admitted, handlerselection.NotApplicable(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return &result
	}
	foreignNode, err := identity.AdmitExecutableNodeDeclaration("child", "router")
	if err != nil {
		t.Fatal(err)
	}
	foreignEdge := compiled.Edge()
	foreignEdge.Node = foreignNode
	foreignFlowCause := projectedCause("child", foreignEdge)
	unknownNode, err := identity.AdmitExecutableNodeDeclaration(".", "other_router")
	if err != nil {
		t.Fatal(err)
	}
	unknownNodeEdge := compiled.Edge()
	unknownNodeEdge.Node = unknownNode
	unknownNodeCause := projectedCause(".", unknownNodeEdge)
	unknownHandlerEdge := compiled.Edge()
	unknownHandlerEdge.HandlerEvent, unknownHandlerEdge.EventType = "other_event", "other_event"
	unknownHandlerCause := projectedCause(".", unknownHandlerEdge)
	wrongSourceEdge := compiled.Edge()
	wrongSourceEdge.From = "other"
	wrongSourceCause := projectedCause(".", wrongSourceEdge)
	loopEdge := compiled.Edge()
	loopEdge.Source, loopEdge.LoopID, loopEdge.LoopOperation = "loop.admit", "unrelated", contracts.LoopOperationAdmit
	unknownLoopCause := projectedCause(".", loopEdge)
	unknownTimerCause := projectedCause(".", contracts.WorkflowStageTopologyEdge{
		From: "ready", To: "done", Source: "timer", InternalOwner: "runtime", TimerID: "foreign", EventType: "timer:foreign", Timed: true, After: "1h",
	})
	forgedGuard, err := workflowlifecycle.NewGuardTermination(graph, node, "advance", "never_evaluated", "ready", "done", []string{"never_evaluated"})
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			entityID := eventtest.UUID("transition-hostility-" + backend)
			address := testEngineStateAddress(".", testPipelineRunID, entityID)
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: testPipelineRunID, StorageRef: testPipelineRunID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: "1", Mode: contracts.FlowModeStatic,
				CurrentState: "ready", Fields: map[string]any{"marker": "unchanged"}, EntityType: "test_entity",
			})
			if err := store.upsert(ctx, instance); err != nil {
				t.Fatal(err)
			}
			accepted := handlerTestRootIngress(eventtest.UUID("accepted-"+backend), "advance", "", "", nil, 0,
				testPipelineRunID, "", handlerTestWorkflowEnvelope(".", testPipelineRunID, entityID), time.Now().UTC())
			seedExactOnceEvent(t, store, ctx, accepted)
			ctx = correlation.WithInboundEvent(ctx, accepted)
			before, found, err := store.Load(ctx, address.Route)
			if err != nil || !found {
				t.Fatalf("initial read: found=%v err=%v", found, err)
			}
			rowCounts := func() map[string]int {
				out := map[string]int{}
				for _, table := range []string{
					"entity_state", "flow_instances", "entity_mutations", "workflow_instance_initial_materializations",
					"events", "event_deliveries", "event_receipts", "timers", "activity_attempts",
					"event_delivery_handler_rule_selections", "event_delivery_attempts", "event_delivery_outcomes",
					"author_activity_occurrences", "fan_out_intents", "fan_out_outcomes",
				} {
					var n int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
						t.Fatal(err)
					}
					out[table] = n
				}
				return out
			}
			counts := rowCounts()
			owner := pipelineEngineMutationOwner{store: store, state: pipelineEngineStateRepo{coordinator: pc}}
			completeMutation := func(state engine.StateMutation) engine.EngineMutation {
				t.Helper()
				result := engine.EngineMutation{Address: address, State: state, HandlerRuleSelection: handlerselection.NotApplicable()}
				if state.Transition != nil {
					result.HandlerRuleSelection = state.Transition.RuleSelection()
					effect, err := workflowlifecycle.NewAcceptedEvent(address.Route, identity.NormalizeEntityID(entityID), state.TriggerEventID, state.TriggerEventType, executionmode.Live, state.TriggeredAt, state.Transition)
					if err != nil {
						t.Fatal(err)
					}
					result.LifecycleEffects = []workflowlifecycle.Effect{effect}
				}
				return result
			}
			for _, tc := range []struct {
				name, next string
				evidence   *workflowlifecycle.Transition
				wantError  string
			}{
				{"missing", "done", nil, "requires exact admitted carrier"},
				{"foreign_rule", "done", &foreignCause, "selected rule is not owned"},
				{"wrong_context", "done", &contextCause, "selected rule is not owned"},
				{"other_selected_source", "other", &alternateCause, "no selected compiled carrier"},
				{"forged_gate", "done", &gateCause, "no authoritative activation/card"},
				{"foreign_flow", "done", foreignFlowCause, "belongs to another flow"},
				{"unknown_node", "done", unknownNodeCause, "no selected compiled carrier"},
				{"unknown_handler", "done", unknownHandlerCause, "no selected compiled carrier"},
				{"wrong_source", "done", wrongSourceCause, "requires exact admitted carrier"},
				{"unknown_loop", "done", unknownLoopCause, "no selected compiled carrier"},
				{"unknown_timer", "done", unknownTimerCause, "no selected compiled carrier"},
				{"forged_guard", "done", &forgedGuard, "guard"},
				{"wrong_target", "other", &cause, "disagrees with state"},
				{"noop_cannot_smuggle_evidence", "ready", &cause, "disagrees with state"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					mutation := testEngineStateMutation(map[string]any{"marker": "must-not-persist"}, nil, nil)
					mutation.NextState, mutation.Transition = tc.next, tc.evidence
					mutation.TriggerEventID, mutation.TriggerEventType = accepted.ID(), string(accepted.Type())
					mutation.TriggeredAt = accepted.CreatedAt()
					if _, err := owner.CommitEngineMutation(ctx, completeMutation(mutation)); err == nil || !strings.Contains(err.Error(), tc.wantError) {
						t.Fatalf("wanted %q rejection, got %v", tc.wantError, err)
					}
					after, found, err := store.Load(ctx, address.Route)
					if err != nil || !found || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(counts, rowCounts()) {
						t.Fatalf("rejection mutated durable state: found=%v err=%v before=%#v after=%#v", found, err, before, after)
					}
				})
			}
			mutation := testEngineStateMutation(map[string]any{"marker": "committed"}, nil, nil)
			mutation.NextState, mutation.Transition = "done", &cause
			mutation.TriggerEventID, mutation.TriggerEventType = accepted.ID(), string(accepted.Type())
			mutation.TriggeredAt = accepted.CreatedAt()
			for _, tc := range []struct {
				name  string
				alter func(*engine.EngineMutation)
			}{
				{"selected_fact", func(m *engine.EngineMutation) { m.HandlerRuleSelection = foreignSelection }},
				{"missing_effect", func(m *engine.EngineMutation) { m.LifecycleEffects = nil }},
				{"duplicate_effect", func(m *engine.EngineMutation) { m.LifecycleEffects = append(m.LifecycleEffects, m.LifecycleEffects[0]) }},
				{"event_id", func(m *engine.EngineMutation) { m.State.TriggerEventID = eventtest.UUID("different-event") }},
				{"event_type", func(m *engine.EngineMutation) { m.State.TriggerEventType = "foreign" }},
				{"event_time", func(m *engine.EngineMutation) { m.State.TriggeredAt = m.State.TriggeredAt.Add(time.Second) }},
				{"missing_state_cause", func(m *engine.EngineMutation) { m.State.Transition = nil }},
			} {
				t.Run("projection_"+tc.name, func(t *testing.T) {
					candidate := completeMutation(mutation)
					tc.alter(&candidate)
					if _, err := owner.CommitEngineMutation(ctx, candidate); err == nil {
						t.Fatal("contradictory projections committed")
					}
					after, found, err := store.Load(ctx, address.Route)
					if err != nil || !found || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(counts, rowCounts()) {
						t.Fatalf("projection rejection changed state: found=%v err=%v", found, err)
					}
				})
			}
			if _, err := owner.CommitEngineMutation(ctx, completeMutation(mutation)); err != nil {
				t.Fatal(err)
			}
			after, found, err := store.Load(ctx, address.Route)
			if err != nil || !found || after.CurrentState != "done" || after.Revision != before.Revision+1 || len(after.TransitionHistory) != 1 {
				t.Fatalf("accepted transition not persisted: %#v found=%v err=%v", after, found, err)
			}
			record := after.TransitionHistory[0]
			if !record.Evidence.RuleSelection().Equal(selection) || record.TransitionID != cause.ID() || record.TriggerEventID != mutation.TriggerEventID {
				t.Fatalf("selected evidence lost: %#v", record)
			}
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			var hydrated WorkflowTransitionRecord
			if err := json.Unmarshal(raw, &hydrated); err != nil || !reflect.DeepEqual(record, hydrated) {
				t.Fatalf("history roundtrip differs: %v", err)
			}
		})
	}
}
