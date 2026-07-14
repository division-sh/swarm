package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

type persistentStateRepo struct {
	snapshot StateSnapshot
	found    bool
}

func cloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (r *persistentStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	if !r.found {
		return StateSnapshot{}, false, nil
	}
	return StateSnapshot{
		EntityID:     r.snapshot.EntityID,
		CurrentState: r.snapshot.CurrentState,
		StateCarrier: NewStateCarrier(
			r.snapshot.StateCarrier.Metadata,
			r.snapshot.StateCarrier.Gates,
			r.snapshot.StateCarrier.StateBuckets,
		),
		TimerState: append([]TimerState(nil), r.snapshot.TimerState...),
	}, true, nil
}

func (r *persistentStateRepo) SaveState(_ context.Context, entityID identity.EntityID, mutation StateMutation) error {
	if !r.found {
		r.found = true
		r.snapshot = StateSnapshot{
			EntityID:     entityID,
			StateCarrier: NewStateCarrier(map[string]any{}, map[string]bool{}, map[string]map[string]any{}),
		}
	}
	if !entityID.IsZero() {
		r.snapshot.EntityID = entityID
	}
	if next := mutation.NextState; next != "" {
		r.snapshot.CurrentState = next
	}
	if mutation.StateCarrier.Metadata != nil {
		r.snapshot.StateCarrier.Metadata = cloneStringAnyMap(mutation.StateCarrier.Metadata)
	}
	if mutation.StateCarrier.StateBuckets != nil {
		r.snapshot.StateCarrier.StateBuckets = cloneStateBucketSet(mutation.StateCarrier.StateBuckets)
	}
	if mutation.StateCarrier.Gates != nil {
		r.snapshot.StateCarrier.Gates = cloneBoolMap(mutation.StateCarrier.Gates)
	}
	return nil
}

type rejectingTransitionValidator struct{}

func (rejectingTransitionValidator) ValidateTransition(_, _ string) error {
	return ErrInvalidTransition
}

type terminalGuardRunner struct{}

func (terminalGuardRunner) EvaluateGuard(context.Context, identity.GuardKey, runtimeregistry.GuardInstruction, ExecutionContext) (bool, bool, error) {
	return false, true, nil
}

func TestExecutorRejectsAccumulateWithHandlerOnCompleteWithoutBootverify(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source: stubSource(), StateRepo: stubStateRepo{}, TxRunner: stubRunner{}, Locker: stubLocker{}, Outbox: stubOutbox{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-1", "item.arrived", "", "", json.RawMessage(`{"item_id":"a"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
			OnComplete: []runtimecontracts.HandlerRuleEntry{{Condition: "accumulated.count >= 2", AdvancesTo: "complete"}},
		},
	})
	if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "cannot be combined with handler.on_complete") {
		t.Fatalf("Execute error = %v, want shared accumulator isolation rejection", err)
	}
}

func TestExecutor_RejectsInvalidAdvancesToTransition(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			CurrentState: "pending",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:              stubSource(),
		StateRepo:           repo,
		TxRunner:            stubRunner{},
		Locker:              stubLocker{},
		Outbox:              stubOutbox{},
		TimerApplier:        stubTimerApplier{},
		Dispatcher:          stubDispatcher{},
		TransitionValidator: rejectingTransitionValidator{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "unreachable_state",
		},
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Execute error = %v, want %v", err, ErrInvalidTransition)
	}
	if result.Status != OutcomeRejected {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeRejected)
	}
	if result.Failure == nil || result.Failure.Class != failures.ClassInternalFailure || result.FailureDisposition != FailureDispositionTerminal {
		t.Fatalf("failure = %#v disposition=%q", result.Failure, result.FailureDisposition)
	}
}

func TestExecutor_GuardBlocksTransitionForTerminalState(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			CurrentState: "done",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       stubSource(),
		StateRepo:    repo,
		TxRunner:     stubRunner{},
		Locker:       stubLocker{},
		Outbox:       stubOutbox{},
		TimerApplier: stubTimerApplier{},
		Dispatcher:   stubDispatcher{},
		GuardRegistry: stubGuardRegistry{entries: map[identity.GuardKey]runtimeregistry.GuardInstruction{
			identity.NormalizeGuardKey("not_in_terminal_state"): {
				Key:     identity.NormalizeGuardKey("not_in_terminal_state"),
				Builtin: "not_in_terminal_state",
			},
		}},
		GuardRunner: terminalGuardRunner{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard:      &runtimecontracts.GuardSpec{ID: "not_in_terminal_state"},
			AdvancesTo: "reopened",
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeRejected {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeRejected)
	}
	if repo.snapshot.CurrentState != "done" {
		t.Fatalf("CurrentState = %q, want done", repo.snapshot.CurrentState)
	}
}

func TestExecutor_CELGuardEvaluatesAgainstEntityState(t *testing.T) {
	newExecutor := func(score int, allowed bool) *Executor {
		exec, err := NewExecutor(RuntimeDependencies{
			Source:       stubSource(),
			StateRepo:    &persistentStateRepo{found: true, snapshot: StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{"score": score}, nil, map[string]map[string]any{})}},
			TxRunner:     stubRunner{},
			Locker:       stubLocker{},
			Outbox:       stubOutbox{},
			TimerApplier: stubTimerApplier{},
			Dispatcher:   stubDispatcher{},
		}, stubEvaluator{bools: map[string]bool{
			"entity.score >= 75": allowed,
		}})
		if err != nil {
			t.Fatalf("NewExecutor error: %v", err)
		}
		return exec
	}

	rejected, err := newExecutor(50, false).Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "entity.score >= 75",
				OnFail: "reject",
			},
			AdvancesTo: "approved",
		},
	})
	if err != nil {
		t.Fatalf("reject Execute error: %v", err)
	}
	if rejected.Status != OutcomeRejected {
		t.Fatalf("rejected Status = %q, want %q", rejected.Status, OutcomeRejected)
	}

	passed, err := newExecutor(80, true).Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-2", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "entity.score >= 75",
				OnFail: "reject",
			},
			AdvancesTo: "approved",
		},
	})
	if err != nil {
		t.Fatalf("passed Execute error: %v", err)
	}
	if passed.Status != OutcomeCompleted {
		t.Fatalf("passed Status = %q, want %q", passed.Status, OutcomeCompleted)
	}
	if passed.NextState != "approved" {
		t.Fatalf("NextState = %q, want approved", passed.NextState)
	}
}

func mustEncodeJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal(%#v): %v", value, err)
	}
	return encoded
}

func TestExecutor_OnCompleteRuleComputeAppliesValue(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			CurrentState: "pending",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       stubSource(),
		StateRepo:    repo,
		TxRunner:     stubRunner{},
		Locker:       stubLocker{},
		Outbox:       stubOutbox{},
		TimerApplier: stubTimerApplier{},
		Dispatcher:   stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score >= 70": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "ent-1",
		NodeID:   "node-1",
		Event: eventtest.RootIngress("evt-1",
			"item.evaluated", "", "", json.RawMessage(`{"entity_id":"ent-1","score":80}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),

		Handler: runtimecontracts.SystemNodeEventHandler{
			OnComplete: []runtimecontracts.HandlerRuleEntry{{
				Condition:  "payload.score >= 70",
				AdvancesTo: "passed",
				Compute: &runtimecontracts.ComputeSpec{
					Operation:   runtimecontracts.ComputeOpWeightedAverage,
					StoreAs:     "entity.composite",
					ValueField:  "score",
					WeightField: "weight",
				},
			}},
		},
		State: StateSnapshot{
			EntityID:     "ent-1",
			CurrentState: "pending",
			StateCarrier: NewStateCarrier(nil, nil, map[string]map[string]any{}),
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.NextState != "passed" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	state, ok, err := repo.LoadState(context.Background(), "ent-1")
	if err != nil || !ok {
		t.Fatalf("LoadState = %v, ok=%v", err, ok)
	}
	got, ok := state.StateCarrier.Metadata["composite"].(float64)
	if !ok {
		t.Fatalf("composite type = %T, want float64", state.StateCarrier.Metadata["composite"])
	}
	if got != 0 {
		t.Fatalf("composite = %v, want 0 for empty accumulator compute", got)
	}
}

func TestExecutor_AccumulationDuplicateStopsBeforeDownstreamEffects(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			CurrentState: "active",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       stubSource(),
		StateRepo:    repo,
		TxRunner:     stubRunner{},
		Locker:       stubLocker{},
		Outbox:       stubOutbox{},
		TimerApplier: stubTimerApplier{},
		Dispatcher:   stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:    "items",
			From:    "payload",
			DedupBy: "payload.item_id",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				SourceField: "marker",
				TargetField: "marker",
			}},
		},
		Emit: runtimecontracts.EmitSpec{Event: "task.recorded"},
	}
	first := ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event: eventtest.RootIngress("evt-1",
			"task.completed", "", "", json.RawMessage(`{"item_id":"item-1","marker":"first"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: handler,
	}
	firstResult, err := exec.Execute(context.Background(), first)
	if err != nil {
		t.Fatalf("first Execute error: %v", err)
	}
	if len(firstResult.EmitIntents) != 1 {
		t.Fatalf("first emit intents = %d, want 1", len(firstResult.EmitIntents))
	}
	duplicate := first
	duplicate.Event = eventtest.RootIngress("evt-2",
		"task.completed", "", "", json.RawMessage(`{"item_id":"item-1","marker":"duplicate"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	duplicateResult, err := exec.Execute(context.Background(), duplicate)
	if err != nil {
		t.Fatalf("second Execute error: %v", err)
	}
	if duplicateResult.Status != OutcomeDiscarded {
		t.Fatalf("duplicate status = %s, want discarded", duplicateResult.Status)
	}
	if len(duplicateResult.EmitIntents) != 0 {
		t.Fatalf("duplicate emit intents = %#v, want none", duplicateResult.EmitIntents)
	}
	if got := duplicateResult.ExecutedSteps[len(duplicateResult.ExecutedSteps)-1]; got != StepAccumulate {
		t.Fatalf("duplicate final executed step = %s, want %s", got, StepAccumulate)
	}
	if got := repo.snapshot.StateCarrier.Metadata["marker"]; got != "first" {
		t.Fatalf("marker after duplicate = %#v, want first arrival value", got)
	}
	acc, ok := loadAccumulator(repo.snapshot, "node-1", events.EventType("task.completed"))
	if !ok {
		t.Fatal("expected accumulator state")
	}
	if got := len(acc.Received); got != 1 {
		t.Fatalf("received count = %d, want 1", got)
	}
	if got := len(acc.Items); got != 1 {
		t.Fatalf("item count = %d, want 1", got)
	}
}

func TestExecutor_FanInInputOwnsWindowAndDedupAtRuntime(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{})
	handler, ok := source.NodeEventHandler(templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent)
	if !ok {
		t.Fatalf("missing fixture handler %s.%s", templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent)
	}
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			EntityID:     templatefanin.ReceiverFlowInstance,
			CurrentState: "active",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       source,
		StateRepo:    repo,
		TxRunner:     stubRunner{},
		Locker:       stubLocker{},
		Outbox:       stubOutbox{},
		TimerApplier: stubTimerApplier{},
		Dispatcher:   stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	execute := func(eventID, operatingID, periodID string) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"period_id":    periodID,
			"operating_id": operatingID,
			"revenue":      42,
		})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		_, err = exec.Execute(context.Background(), ExecutionRequest{
			EntityID:        templatefanin.ReceiverFlowInstance,
			NodeID:          templatefanin.ReceiverNodeID,
			FlowID:          templatefanin.ReceiverFlowID,
			HandlerEventKey: templatefanin.ReceiverEvent,
			Handler:         handler,
			Event: eventtest.RootIngress(
				eventID,
				events.EventType(templatefanin.ReceiverEvent),
				"operating",
				"",
				payload,
				0,
				"",
				"",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, templatefanin.ReceiverFlowInstance), templatefanin.ReceiverFlowInstance),
				time.Now().UTC(),
			),
		})
		if err != nil {
			t.Fatalf("Execute(%s, %s, %s): %v", eventID, operatingID, periodID, err)
		}
	}

	execute("evt-q1-a", "operating-a", "2026-Q1")
	execute("evt-q1-duplicate", "operating-a", "2026-Q1")
	execute("evt-q2-a", "operating-a", "2026-Q2")

	for _, periodID := range []string{"2026-Q1", "2026-Q2"} {
		bucket := timeridentity.NewAccumulatorWindowBucketRef(templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent, periodID)
		acc, ok := loadAccumulatorForBucket(repo.snapshot, bucket)
		if !ok {
			t.Fatalf("missing fan-in accumulator window %s in %#v", periodID, repo.snapshot.StateCarrier.StateBuckets)
		}
		if got := len(acc.Items); got != 1 {
			t.Fatalf("window %s item count = %d, want 1 after pin-owned operating_id dedup", periodID, got)
		}
		if !acc.Received["operating-a"] {
			t.Fatalf("window %s received keys = %#v, want operating-a", periodID, acc.Received)
		}
	}
	if _, ok := loadAccumulatorForBucket(repo.snapshot, timeridentity.NewAccumulatorBucketRef(templatefanin.ReceiverNodeID, templatefanin.ReceiverEvent)); ok {
		t.Fatalf("unwindowed accumulator survived: %#v", repo.snapshot.StateCarrier.StateBuckets)
	}
}
