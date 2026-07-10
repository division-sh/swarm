package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
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

func TestExecutor_AccumulateOnTimeoutAppliesRule(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			EntityID:     "entity-1",
			CurrentState: "collecting",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	acc := &Accumulator{
		ExpectedCount: 3,
		Received: map[string]bool{
			"evt-a": true,
			"evt-b": true,
		},
		Items: []map[string]any{
			{"event_id": "evt-a"},
			{"event_id": "evt-b"},
		},
		StartedAt: "2026-03-15T00:00:00Z",
	}
	storeAccumulator(&repo.snapshot, "node-1", events.EventType("item.arrived"), acc)
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  repo,
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		Event: eventtest.RootIngress("timeout-1",
			events.EventType("accumulate.timeout"), "", "", mustEncodeJSON(t, map[string]any{
				"timer_handle": map[string]any{
					"kind": "accumulation_timeout",
					"bucket": map[string]any{
						"node_id":    "node-1",
						"event_type": "item.arrived",
					},
				},
			}), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{
				Completion: runtimecontracts.ParseAccumulateCompletion("all"),
				OnTimeout: &runtimecontracts.HandlerRuleEntry{
					AdvancesTo: "partial",
					Emit:       runtimecontracts.EmitSpec{Event: "collection.partial"},
				},
			},
		},
		State: repo.snapshot,
	})
	if err != nil {
		t.Fatalf("Execute timeout accumulate: %v", err)
	}
	if result.NextState != "partial" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "collection.partial" {
		t.Fatalf("EmitIntents = %#v", result.EmitIntents)
	}
	if repo.snapshot.CurrentState != "partial" {
		t.Fatalf("persisted CurrentState = %q", repo.snapshot.CurrentState)
	}
}

func TestExecutor_AccumulateOnTimeoutComputeReadsWindowedTimerBucket(t *testing.T) {
	bucket := timeridentity.NewAccumulatorWindowBucketRef("node-1", "item.arrived", "2026-W10")
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			EntityID:     "entity-1",
			CurrentState: "collecting",
			StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{}),
		},
	}
	storeAccumulatorForBucket(&repo.snapshot, bucket, &Accumulator{
		ExpectedCount: 3,
		Received: map[string]bool{
			"evt-a": true,
			"evt-b": true,
		},
		Items: []map[string]any{
			{"event_id": "evt-a"},
			{"event_id": "evt-b"},
		},
		StartedAt: "2026-03-15T00:00:00Z",
	})
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  repo,
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:        "entity-1",
		NodeID:          "node-1",
		HandlerEventKey: "item.arrived",
		Event: eventtest.RootIngress("timeout-1",
			events.EventType("accumulate.timeout"), "", "", mustEncodeJSON(t, map[string]any{
				"timer_handle": map[string]any{
					"kind":   string(timeridentity.TimerHandleAccumulationTimeout),
					"bucket": bucket.PayloadValue(),
				},
			}), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{
				Completion: runtimecontracts.ParseAccumulateCompletion("all"),
				Window:     "payload.window_id",
				OnTimeout: &runtimecontracts.HandlerRuleEntry{
					ID: "timeout",
					Compute: &runtimecontracts.ComputeSpec{
						Operation: runtimecontracts.ComputeOpCount,
						StoreAs:   "computed.timed_out_count",
					},
				},
			},
		},
		State: repo.snapshot,
	})
	if err != nil {
		t.Fatalf("Execute timeout accumulate: %v", err)
	}
	if got := result.Computed["timed_out_count"]; got != 2 {
		t.Fatalf("timed_out_count = %#v, want 2", got)
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

func TestExecutor_AccumulationDeduplicatesRepeatedEvent(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
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
	req := ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event: eventtest.RootIngress("evt-1",
			"task.completed", "", "", json.RawMessage(`{"item_id":"item-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate: &runtimecontracts.AccumulateSpec{
				Completion: runtimecontracts.ParseAccumulateCompletion("all"),
			},
		},
	}
	if _, err := exec.Execute(context.Background(), req); err != nil {
		t.Fatalf("first Execute error: %v", err)
	}
	if _, err := exec.Execute(context.Background(), req); err != nil {
		t.Fatalf("second Execute error: %v", err)
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
