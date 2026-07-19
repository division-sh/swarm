package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
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
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Loops:  []runtimecontracts.WorkflowLoopPlan{plan},
		Stages: []runtimecontracts.WorkflowStageContract{{ID: "queued"}, {ID: "drafting"}, {ID: "review"}, {ID: "escalated"}},
	}})
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, TxRunner: stubRunner{}, Locker: stubLocker{},
		Outbox: stubOutbox{}, Dispatcher: stubDispatcher{},
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

	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(state, repeat, "00000000-0000-0000-0000-000000000104", map[string]any{"revision_id": firstRevision}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassStaleArrival {
		t.Fatalf("prior revision at wrong stage error = %v, want stale_arrival", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(state, repeat, "00000000-0000-0000-0000-000000000107", map[string]any{"revision_id": secondRevision}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassEarlyArrival {
		t.Fatalf("current revision at wrong stage error = %v, want early_arrival", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(state, repeat, "00000000-0000-0000-0000-000000000109", map[string]any{"revision_id": "00000000-0000-0000-0000-999999999999"}))
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
	closedState := loopTestNextState(result)
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(closedState, repeat, "00000000-0000-0000-0000-000000000108", map[string]any{"revision_id": secondRevision}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassStaleArrival {
		t.Fatalf("post-close revision error = %v, want stale_arrival", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(closedState, repeat, "00000000-0000-0000-0000-000000000110", map[string]any{"revision_id": "00000000-0000-0000-0000-999999999999"}))
	if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassUnexpectedArrival {
		t.Fatalf("post-close unknown revision error = %v, want unexpected_arrival", err)
	}
}

func executeLoopTestHandler(t *testing.T, exec *Executor, state StateSnapshot, handler runtimecontracts.SystemNodeEventHandler, eventID string, payload map[string]any) ExecutionResult {
	t.Helper()
	result, err := exec.ExecuteSemanticFixture(context.Background(), loopTestRequest(state, handler, eventID, payload))
	if err != nil {
		t.Fatalf("execute %s: %v", eventID, err)
	}
	return result
}

func loopTestRequest(state StateSnapshot, handler runtimecontracts.SystemNodeEventHandler, eventID string, payload map[string]any) ExecutionRequest {
	raw, _ := json.Marshal(payload)
	return ExecutionRequest{
		EntityID: identity.NormalizeEntityID("00000000-0000-0000-0000-000000000001"),
		NodeID:   "loop-node", FlowID: "validation", Handler: handler, State: state,
		Event: eventtest.RunCreatingRootIngress(eventID, events.EventType("loop.event"), "", "", raw, 0,
			"00000000-0000-0000-0000-000000000010", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "00000000-0000-0000-0000-000000000001"),
			time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)),
	}
}

func loopTestNextState(result ExecutionResult) StateSnapshot {
	return StateSnapshot{CurrentState: result.NextState, StateCarrier: NewStateCarrier(
		result.StateMutation.Metadata, result.StateMutation.Gates, result.StateMutation.StateBuckets,
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
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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
		Source: source, StateRepo: stubStateRepo{}, TxRunner: stubRunner{}, Locker: stubLocker{}, Outbox: stubOutbox{}, Dispatcher: stubDispatcher{},
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
			_, err := exec.ExecuteSemanticFixture(context.Background(), loopCarrierTestRequest(baseState, tc.handler, tc.eventType, uuidForLoopCarrier(tc.name, 1), stalePayload))
			if envelope, ok := failures.As(err); !ok || envelope.Failure.Class != failures.ClassStaleArrival {
				t.Fatalf("prior generation error = %v, want stale_arrival", err)
			}
			currentPayload := map[string]any{"revision_id": activation.RevisionID, "items": []any{map[string]any{"id": "one"}}}
			result, err := exec.ExecuteSemanticFixture(context.Background(), loopCarrierTestRequest(baseState, tc.handler, tc.eventType, uuidForLoopCarrier(tc.name, 2), currentPayload))
			if err != nil {
				t.Fatalf("current generation: %v", err)
			}
			if len(result.EmitIntents) != 1 {
				t.Fatalf("current generation emit intents = %d, want 1", len(result.EmitIntents))
			}
			if got := eventPayloadMap(t, result.EmitIntents[0].Event)["revision_id"]; got != activation.RevisionID {
				t.Fatalf("current generation emitted revision = %#v, want %s", got, activation.RevisionID)
			}
		})
	}
}

func loopCarrierTestRequest(state StateSnapshot, handler runtimecontracts.SystemNodeEventHandler, eventType, eventID string, payload map[string]any) ExecutionRequest {
	req := loopTestRequest(state, handler, eventID, payload)
	raw, _ := json.Marshal(payload)
	req.Event = eventtest.RunCreatingRootIngress(eventID, events.EventType(eventType), "", "", raw, 0,
		"00000000-0000-0000-0000-000000000010", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "00000000-0000-0000-0000-000000000001"),
		time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	return req
}

func uuidForLoopCarrier(name string, ordinal int) string {
	return activityidentity.ForkLineageEventID("00000000-0000-0000-0000-000000000010", fmt.Sprintf("%s:%d", name, ordinal))
}
