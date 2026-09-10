package pipeline

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/google/uuid"
)

func TestWorkflowExpressionAdapterCapturedLoop(t *testing.T) {
	evaluator := newWorkflowExpressionEvaluator()
	context := workflowExpressionContext{
		Loop: map[string]any{"revision_id": "captured", "attempt": 1, "flow_id": "private"},
		Join: map[string]any{"completed": 1},
	}
	if ok, err := evaluator.EvalBool(`loop.revision_id == "captured" && join.completed == 1`, context); err != nil || !ok {
		t.Fatalf("adapter lost public captured context before lowering: %v %v", ok, err)
	}
	for _, expression := range []string{`_loop.revision_id == "captured"`, `loop.flow_id == "private"`, `loop.attempt.startsWith("1")`} {
		if _, err := evaluator.EvalBool(expression, context); err == nil {
			t.Fatalf("adapter admitted unsupported loop expression %s", expression)
		}
	}
}

func TestJoinCapturedLoopOutcomeAfterRestartBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			for _, complete := range []bool{false, true} {
				name := "timeout"
				if complete {
					name = "pending-complete"
				}
				t.Run(storeCase.name+"/"+pipelineDeclarationFlowPath(flowID)+"/"+name, func(t *testing.T) {
					members := []any{"original-member"}
					if complete {
						members = []any{}
					}
					h := newExactWorkflowJoinHarness(t, storeCase, flowID, "dispatching", members)
					start := runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{Start: "revision", From: "dispatching"}, AdvancesTo: "awaiting"}
					repeat := runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "awaiting"}, AdvancesTo: "awaiting"}
					observer := runtimecontracts.SystemNodeContract{ID: "observer", ExecutionType: "system_node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"loop.start": start, "loop.repeat": repeat}}
					h.bundle.Nodes["observer"] = observer
					h.bundle.FlowTree.ByID["orders"].Nodes["observer"] = observer
					declarationFlowID := pipelineDeclarationFlowPath(flowID)
					h.bundle.Semantics.Loops = []runtimecontracts.WorkflowLoopPlan{{FlowID: declarationFlowID, ID: "revision", RevisionField: "revision_id", MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3}, EntryStage: "awaiting", RegionStages: []string{"awaiting"}}}
					handler := h.bundle.Nodes["join-node"].EventHandlers["item.completed"]
					handler.Loop = &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "awaiting"}
					outcome := runtimecontracts.HandlerRuleEntry{AdvancesTo: "awaiting", DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
						{TargetField: "expected", Value: runtimecontracts.CELExpression("[loop.revision_id]")},
					}}}
					handler.Join.OnComplete, handler.Join.Timeout.Outcome = outcome, outcome
					h.bundle.Nodes["join-node"].EventHandlers["item.completed"] = handler
					h.bundle.FlowTree.ByID["orders"].Nodes["join-node"].EventHandlers["item.completed"] = handler
					h.source.plans[0].Spec = *handler.Join
					h.source.Source = workflowJoinLifecycleRootAndFlowSource(h.bundle)
					h.restart()
					execute := func(eventType string, handler runtimecontracts.SystemNodeEventHandler, payload map[string]any) {
						t.Helper()
						event := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType(eventType), "operator", "", mustJSON(payload), 0, runtimecorrelation.RunIDFromContext(h.ctx), "", h.envelope(), time.Now().UTC())
						persistExactJoinEvent(t, h.store, h.ctx, event)
						if _, err := h.pc.executeNodeContractHandler(h.ctx, pipelineNode(t, flowID, "observer"), handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: eventType}, false); err != nil {
							t.Fatal(err)
						}
					}
					execute("loop.start", start, map[string]any{})
					readLoop := func() loopruntime.Activation {
						t.Helper()
						carrier, err := workflowInstanceStateCarrier(h.instance())
						if err != nil {
							t.Fatal(err)
						}
						owner, found, err := loopruntime.Load(carrier.StateBuckets, declarationFlowID, "revision")
						if err != nil || !found {
							t.Fatalf("real loop writer missing: %v", err)
						}
						return owner
					}
					captured := readLoop()
					schedules, _ := committedWorkflowSchedulesForTest(t, h.store)
					if len(schedules) != 1 {
						t.Fatalf("real stage entry scheduled %d joins", len(schedules))
					}
					_, ref, ok := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, schedules[0])))
					if !ok || ref.Generation() != captured.Generation() {
						t.Fatalf("schedule lost captured generation: %+v", ref)
					}
					h.restart()
					event := h.scheduleEvent(schedules[0], "captured-outcome-after-restart")
					if _, err := h.fire(event); err != nil {
						t.Fatal(err)
					}
					if got := h.instance().Fields["expected"]; !reflect.DeepEqual(got, []any{captured.RevisionID}) {
						t.Fatalf("reconstructed outcome got=%#v captured=%s", got, captured.RevisionID)
					}
					if got := readLoop(); got != captured {
						t.Fatalf("outcome rewrote owning activation: %+v", got)
					}
					execute("loop.repeat", repeat, map[string]any{"revision_id": captured.RevisionID})
					if current := readLoop(); current.Attempt != 2 || current.RevisionID == captured.RevisionID {
						t.Fatalf("real repeat did not advance: %+v", current)
					}
					before := h.instance()
					h.restart()
					if _, err := h.fire(event); err != nil {
						t.Fatalf("duplicate completed outcome after replacement: %v", err)
					}
					if !exactJoinSemanticStateEqual(before, h.instance()) {
						t.Fatal("old outcome borrowed or mutated replacement generation")
					}
				})
			}
		}
	}
}
