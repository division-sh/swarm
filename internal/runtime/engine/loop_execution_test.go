package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestExecutorBoundedLoopEscapesAtStampedCapAndRejectsPriorRevision(t *testing.T) {
	plan := runtimecontracts.WorkflowLoopPlan{
		FlowID: "validation", ID: "revision", RevisionField: "revision_id",
		MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 2},
		Escape:      runtimecontracts.LoopEscapeSpec{AdvancesTo: "escalated"},
		EntryStage:  "drafting", RegionStages: []string{"drafting", "review"},
		Operations: []runtimecontracts.WorkflowLoopOperationPlan{{Kind: runtimecontracts.LoopOperationRepeat, Node: testFlowExecutableNode(t, "validation", "loop-node"), HandlerEvent: "loop.event", From: "review", AdvancesTo: "drafting"}},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		FlowStates: map[string][]string{"validation": {"queued", "drafting", "review", "escalated"}},
		Loops:      []runtimecontracts.WorkflowLoopPlan{plan},
		Stages:     []runtimecontracts.WorkflowStageContract{{ID: "queued"}, {ID: "drafting"}, {ID: "review"}, {ID: "escalated"}},
	}})
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	state := testStateSnapshot("queued", map[string]any{}, nil, map[string]map[string]any{})
	start := runtimecontracts.SystemNodeEventHandler{
		Loop:       &runtimecontracts.LoopOperationSpec{Start: "revision", From: "queued"},
		AdvancesTo: "drafting",
		Emit: runtimecontracts.EmitSpec{Event: "draft.requested", Fields: map[string]runtimecontracts.ExpressionValue{
			"revision_id": runtimecontracts.CELExpression("loop.revision_id"),
		}},
	}
	result := executeLoopTestHandler(t, exec, state, start, "00000000-0000-0000-0000-000000000101", nil)
	if result.NextState != "drafting" || result.LoopTrace == nil || result.LoopTrace.Attempt != 1 || result.LoopTrace.MaxAttempts != 2 {
		t.Fatalf("start result = %#v", result)
	}
	firstRevision := emittedLoopRevision(t, result)
	state = loopTestNextState(result)

	admit := runtimecontracts.SystemNodeEventHandler{
		Loop:       &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"},
		AdvancesTo: "review",
	}
	result = executeLoopTestHandler(t, exec, state, admit, "00000000-0000-0000-0000-000000000102", map[string]any{"revision_id": firstRevision})
	state = loopTestNextState(result)

	repeat := runtimecontracts.SystemNodeEventHandler{
		Loop:       &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "review"},
		AdvancesTo: "drafting",
		Emit: runtimecontracts.EmitSpec{Event: "draft.requested", Fields: map[string]runtimecontracts.ExpressionValue{
			"revision_id": runtimecontracts.CELExpression("loop.revision_id"),
		}},
	}
	result = executeLoopTestHandler(t, exec, state, repeat, "00000000-0000-0000-0000-000000000103", map[string]any{"revision_id": firstRevision})
	if result.NextState != "drafting" || result.LoopTrace.Attempt != 2 || result.LoopTrace.Status != loopruntime.StatusOpen {
		t.Fatalf("repeat result = %#v", result)
	}
	secondRevision := emittedLoopRevision(t, result)
	if secondRevision == firstRevision {
		t.Fatalf("repeat reused revision %s", firstRevision)
	}
	state = loopTestNextState(result)

	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(t, state, repeat, "00000000-0000-0000-0000-000000000104", map[string]any{"revision_id": firstRevision}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassStaleArrival {
		t.Fatalf("prior revision at wrong stage error = %v, want stale_arrival", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(t, state, repeat, "00000000-0000-0000-0000-000000000107", map[string]any{"revision_id": secondRevision}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassEarlyArrival {
		t.Fatalf("current revision at wrong stage error = %v, want early_arrival", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(t, state, repeat, "00000000-0000-0000-0000-000000000109", map[string]any{"revision_id": "00000000-0000-0000-0000-999999999999"}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassUnexpectedArrival {
		t.Fatalf("unknown revision error = %v, want unexpected_arrival", err)
	}

	result = executeLoopTestHandler(t, exec, state, admit, "00000000-0000-0000-0000-000000000105", map[string]any{"revision_id": secondRevision})
	state = loopTestNextState(result)
	result = executeLoopTestHandler(t, exec, state, repeat, "00000000-0000-0000-0000-000000000106", map[string]any{"revision_id": secondRevision})
	if result.NextState != "escalated" || result.LoopTrace.Attempt != 2 || result.LoopTrace.Status != loopruntime.StatusClosed || result.LoopTrace.CloseReason != loopruntime.CloseReasonEscaped {
		t.Fatalf("cap escape result = %#v", result)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("cap escape emitted ordinary repeat work: %#v", result.EmitIntents)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextNone || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionNotApplicable || result.HandlerRuleSelection.Ref().Valid() {
		t.Fatalf("synthetic loop escape fabricated authored rule identity: %#v", result.HandlerRuleSelection)
	}
	closedState := loopTestNextState(result)
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(t, closedState, repeat, "00000000-0000-0000-0000-000000000108", map[string]any{"revision_id": secondRevision}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassStaleArrival {
		t.Fatalf("post-close revision error = %v, want stale_arrival", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(t, closedState, repeat, "00000000-0000-0000-0000-000000000110", map[string]any{"revision_id": "00000000-0000-0000-0000-999999999999"}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassUnexpectedArrival {
		t.Fatalf("post-close unknown revision error = %v, want unexpected_arrival", err)
	}
}

func executeLoopTestHandler(t *testing.T, exec *Executor, state StateSnapshot, handler runtimecontracts.SystemNodeEventHandler, eventID string, payload map[string]any) ExecutionResult {
	t.Helper()
	result, err := exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(t, state, handler, eventID, payload))
	if err != nil {
		t.Fatalf("execute %s: %v", eventID, err)
	}
	return result
}

func loopTestRequest(t testing.TB, state StateSnapshot, handler runtimecontracts.SystemNodeEventHandler, eventID string, payload map[string]any) ExecutionRequest {
	t.Helper()
	raw, _ := json.Marshal(payload)
	return ExecutionRequest{
		EntityID: identity.NormalizeEntityID("00000000-0000-0000-0000-000000000001"),
		Node:     testFlowExecutableNode(t, "validation", "loop-node"), Handler: handler, State: state,
		Event: eventtest.RunCreatingRootIngress(eventID, events.EventType("loop.event"), "", "", raw, 0,
			"00000000-0000-0000-0000-000000000010", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "00000000-0000-0000-0000-000000000001"),
			time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)),
	}
}

func loopTestNextState(result ExecutionResult) StateSnapshot {
	return StateSnapshot{CurrentState: result.NextState, StateCarrier: NewStateCarrier(
		result.StateMutation.Fields, result.StateMutation.Gates, result.StateMutation.StateBuckets,
	)}
}

func emittedLoopRevision(t *testing.T, result ExecutionResult) string {
	t.Helper()
	if len(result.EmitIntents) != 1 {
		t.Fatalf("emit intents = %d, want 1", len(result.EmitIntents))
	}
	payload := eventPayloadMap(t, result.EmitIntents[0].Event)
	revision, _ := payload["revision_id"].(string)
	if revision == "" {
		t.Fatalf("emitted revision_id = %#v", payload["revision_id"])
	}
	return revision
}

func TestPositiveLoopMaxAttemptsRejectsRuntimeBypassValues(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  int
		ok    bool
	}{
		{value: 3, want: 3, ok: true},
		{value: float64(3), want: 3, ok: true},
		{value: float64(2.5)},
		{value: 0},
		{value: "3"},
	} {
		got, ok := positiveLoopMaxAttempts(tc.value)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("positiveLoopMaxAttempts(%#v) = (%d, %v), want (%d, %v)", tc.value, got, ok, tc.want, tc.ok)
		}
	}
}

func TestLoopReturningCarrierAdmissionRejectsPriorAndAcceptsCurrentGeneration(t *testing.T) {
	plan := runtimecontracts.WorkflowLoopPlan{
		FlowID: "validation", ID: "revision", RevisionField: "revision_id",
		MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3}, EntryStage: "drafting",
		RegionStages: []string{"drafting", "review"}, Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escalated"},
	}
	source := fanOutSourceWithBundleIdentity(t, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Loops: []runtimecontracts.WorkflowLoopPlan{plan}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"line_item.completed": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"items": {Type: "[json]"},
				}},
			},
		},
	})
	exec, err := NewExecutor(RuntimeDependencies{
		Source: sourceWithFixtureStages(source, "validation", "drafting", "drafting", "review", "escalated"), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	activation, err := loopruntime.New("run", "00000000-0000-0000-0000-000000000001", "validation", "revision", "revision_id", "start", "drafting", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	priorRevision := activation.RevisionID
	if err := activation.AdvanceWithin("review", "advance", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if escaped, err := activation.Repeat("drafting", "repeat", time.Now().UTC()); err != nil || escaped {
		t.Fatalf("repeat = escaped:%v err:%v", escaped, err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	baseState := testStateSnapshot("drafting", map[string]any{}, nil, buckets)

	for _, tc := range []struct {
		name      string
		eventType string
		handler   runtimecontracts.SystemNodeEventHandler
	}{
		{
			name: "fan_out_result", eventType: "line_item.completed",
			handler: runtimecontracts.SystemNodeEventHandler{
				Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"},
				FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", As: "line_item", Identity: "line_item.id", Emit: runtimecontracts.EmitSpec{
					Event: "line_item.follow_up", Fields: map[string]runtimecontracts.ExpressionValue{"revision_id": runtimecontracts.RefExpression("loop.revision_id")},
				}},
			},
		},
		{
			name: "child_result", eventType: "child_flow.completed",
			handler: runtimecontracts.SystemNodeEventHandler{
				Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"},
				Emit: runtimecontracts.EmitSpec{Event: "child_flow.accepted", Fields: map[string]runtimecontracts.ExpressionValue{"revision_id": runtimecontracts.RefExpression("loop.revision_id")}},
			},
		},
		{
			name: "agent_result", eventType: "agent.review_completed",
			handler: runtimecontracts.SystemNodeEventHandler{
				Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"},
				Emit: runtimecontracts.EmitSpec{Event: "agent.review_accepted", Fields: map[string]runtimecontracts.ExpressionValue{"revision_id": runtimecontracts.RefExpression("loop.revision_id")}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stalePayload := map[string]any{"revision_id": priorRevision, "items": []any{map[string]any{"id": "one"}}}
			_, err := exec.ExecuteSemanticFixture(context.Background(), loopCarrierTestRequest(t, baseState, tc.handler, tc.eventType, uuidForLoopCarrier(tc.name, 1), stalePayload))
			if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassStaleArrival {
				t.Fatalf("prior generation error = %v, want stale_arrival", err)
			}
			currentPayload := map[string]any{"revision_id": activation.RevisionID, "items": []any{map[string]any{"id": "one"}}}
			result, err := exec.ExecuteSemanticFixture(context.Background(), loopCarrierTestRequest(t, baseState, tc.handler, tc.eventType, uuidForLoopCarrier(tc.name, 2), currentPayload))
			if err != nil {
				t.Fatalf("current generation: %v", err)
			}
			if tc.name == "fan_out_result" {
				if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 1 || len(result.EmitIntents) != 0 {
					t.Fatalf("current generation fan-out = intent:%#v immediate:%d, want one durable item and no eager event", result.FanOutIntent, len(result.EmitIntents))
				}
			} else {
				if len(result.EmitIntents) != 1 {
					t.Fatalf("current generation emit intents = %d, want 1", len(result.EmitIntents))
				}
				if got := eventPayloadMap(t, result.EmitIntents[0].Event)["revision_id"]; got != activation.RevisionID {
					t.Fatalf("current generation emitted revision = %#v, want %s", got, activation.RevisionID)
				}
			}
		})
	}
}

func loopCarrierTestRequest(t testing.TB, state StateSnapshot, handler runtimecontracts.SystemNodeEventHandler, eventType, eventID string, payload map[string]any) ExecutionRequest {
	t.Helper()
	req := loopTestRequest(t, state, handler, eventID, payload)
	raw, _ := json.Marshal(payload)
	req.Event = eventtest.RunCreatingRootIngress(eventID, events.EventType(eventType), "", "", raw, 0,
		"00000000-0000-0000-0000-000000000010", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "00000000-0000-0000-0000-000000000001"),
		time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	return req
}

func uuidForLoopCarrier(name string, ordinal int) string {
	return activityidentity.ForkLineageEventID("00000000-0000-0000-0000-000000000010", fmt.Sprintf("%s:%d", name, ordinal))
}

func TestExecutorCompiledLoopOperationsRetainExactCarrier(t *testing.T) {
	node := testFlowExecutableNode(t, "validation", "loop-node")
	plan := runtimecontracts.WorkflowLoopPlan{
		FlowID: "validation", ID: "revision", RevisionField: "revision_id", MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 2},
		EntryStage: "drafting", RegionStages: []string{"drafting", "review"}, Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escalated"},
		Operations: []runtimecontracts.WorkflowLoopOperationPlan{{Node: node, HandlerEvent: "work.repeat", Kind: runtimecontracts.LoopOperationRepeat, LoopID: "revision", From: "review", AdvancesTo: "drafting"}},
	}
	handlers := map[string]runtimecontracts.SystemNodeEventHandler{
		"work.start":    {Loop: &runtimecontracts.LoopOperationSpec{Start: "revision", From: "queued"}, AdvancesTo: "drafting"},
		"work.rule":     {Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"}, Rules: []runtimecontracts.HandlerRuleEntry{{ID: "review", Condition: "else", AdvancesTo: "review"}}},
		"work.complete": {Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"}, OnComplete: []runtimecontracts.HandlerRuleEntry{{ID: "review", Condition: "else", AdvancesTo: "review"}}},
		"work.repeat":   {Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "review"}, AdvancesTo: "drafting", Emit: runtimecontracts.EmitSpec{Event: "draft.requested"}},
		"work.close":    {Loop: &runtimecontracts.LoopOperationSpec{Close: "revision", From: "review"}, AdvancesTo: "done"},
	}
	var transitions []runtimecontracts.HandlerTransitionSemantic
	for event, handler := range handlers {
		qualified, err := completeSemanticFixtureHandlerRuleIdentity(node, event, handler)
		if err != nil {
			t.Fatal(err)
		}
		handlers[event] = qualified
		transitions = append(transitions, runtimecontracts.HandlerTransitionSemantic{Node: node, EventType: event, Loop: qualified.Loop, AdvancesTo: qualified.AdvancesTo, Rules: qualified.Rules, OnComplete: qualified.OnComplete})
	}
	graph := runtimecontracts.BuildWorkflowStageTopology("validation", "queued", []string{"queued", "drafting", "review", "done", "escalated"}, []string{"done", "escalated"}, transitions, nil, []runtimecontracts.WorkflowLoopPlan{plan})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Loops: []runtimecontracts.WorkflowLoopPlan{plan}, StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{"validation": graph},
	}})
	exec, recorder := transitionTestExecutor(t, source)
	execute := func(state StateSnapshot, event string, ordinal int, revision string) ExecutionResult {
		t.Helper()
		req := loopCarrierTestRequest(t, state, handlers[event], event, uuidForLoopCarrier(event, ordinal), map[string]any{"revision_id": revision})
		req.Route = flowidentity.DeriveRoute("validation", req.Event.RunID())
		result, err := exec.ExecuteSemanticFixture(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		cause := result.StateMutation.Transition
		if cause == nil {
			t.Fatalf("%s has no transition", event)
		}
		compiled, ok := cause.Compiled()
		kind, _, _ := handlers[event].Loop.Operation()
		if !ok || compiled.Edge().Node != node || compiled.Edge().HandlerEvent != event || compiled.Edge().LoopID != "revision" || compiled.Edge().LoopOperation != kind {
			t.Fatalf("%s carrier = %#v", event, compiled.Edge())
		}
		if err := cause.ValidateAgainst(graph); err != nil {
			t.Fatal(err)
		}
		if result.HandlerRuleSelection.Ref().Valid() && !compiled.Edge().RuleRef.Equal(result.HandlerRuleSelection.Ref()) {
			t.Fatalf("%s lost underlying selected rule: %#v", event, compiled.Edge())
		}
		return result
	}
	started := execute(testStateSnapshot("queued", nil, nil, nil), "work.start", 1, "")
	revision := started.LoopTrace.RevisionID
	for _, event := range []string{"work.rule", "work.complete"} {
		// The activation is valid, but cannot authorize a graph source different
		// from the compiled loop.from even when the target is otherwise declared.
		wrongStage := loopTestNextState(started)
		wrongStage.CurrentState = "review"
		before := len(recorder.mutations)
		_, err := exec.ExecuteSemanticFixture(context.Background(), loopCarrierTestRequest(t, wrongStage, handlers[event], event, uuidForLoopCarrier(event, 9), map[string]any{"revision_id": revision}))
		if !errors.Is(err, ErrInvalidTransition) || len(recorder.mutations) != before {
			t.Fatalf("%s wrong source: err=%v mutations=%d/%d", event, err, len(recorder.mutations), before)
		}
	}
	admitted := execute(loopTestNextState(started), "work.rule", 1, revision)
	repeated := execute(loopTestNextState(admitted), "work.repeat", 1, revision)
	if len(repeated.EmitIntents) != 1 || repeated.LoopTrace.Attempt != 2 {
		t.Fatalf("ordinary repeat = %#v", repeated)
	}
	revision = repeated.LoopTrace.RevisionID
	completed := execute(loopTestNextState(repeated), "work.complete", 1, revision)
	closed := execute(loopTestNextState(completed), "work.close", 1, revision)
	if closed.LoopTrace.Status != loopruntime.StatusClosed || closed.NextState != "done" {
		t.Fatalf("close = %#v", closed)
	}
	// Independent cap branch uses the same fixed graph and accepted revision.
	escaped := execute(loopTestNextState(completed), "work.repeat", 2, revision)
	compiled, _ := escaped.StateMutation.Transition.Compiled()
	if compiled.Edge().Source != "loop.escape" || compiled.Edge().AdvanceCarrier != "" || compiled.Edge().RuleRef.Valid() || escaped.NextState != "escalated" || len(escaped.EmitIntents) != 0 || escaped.LoopTrace.CloseReason != loopruntime.CloseReasonEscaped {
		t.Fatalf("cap selected ordinary work instead of escape: %#v", escaped)
	}
}
