package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	runtimeregistry "empireai/internal/runtime/core/registry"
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
		Metadata:     cloneStringAnyMap(r.snapshot.Metadata),
		Gates:        cloneBoolMap(r.snapshot.Gates),
		StateBuckets: cloneStringAnyMap(r.snapshot.StateBuckets),
		TimerState:   append([]TimerState(nil), r.snapshot.TimerState...),
	}, true, nil
}

func (r *persistentStateRepo) SaveState(_ context.Context, entityID identity.EntityID, mutation StateMutation) error {
	if !r.found {
		r.found = true
		r.snapshot = StateSnapshot{
			EntityID:     entityID,
			Metadata:     map[string]any{},
			Gates:        map[string]bool{},
			StateBuckets: map[string]any{},
		}
	}
	if !entityID.IsZero() {
		r.snapshot.EntityID = entityID
	}
	if next := mutation.NextState; next != "" {
		r.snapshot.CurrentState = next
	}
	if mutation.Metadata != nil {
		r.snapshot.Metadata = cloneStringAnyMap(mutation.Metadata)
	}
	if mutation.StateBuckets != nil {
		r.snapshot.StateBuckets = cloneStringAnyMap(mutation.StateBuckets)
	}
	if mutation.Gates != nil {
		r.snapshot.Gates = cloneBoolMap(mutation.Gates)
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
			Metadata:     map[string]any{},
			StateBuckets: map[string]any{},
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
		Event:    events.Event{ID: "evt-1", Type: "task.completed", CreatedAt: time.Now().UTC()},
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
	if result.FailureClass != FailureLogic {
		t.Fatalf("FailureClass = %q, want %q", result.FailureClass, FailureLogic)
	}
}

func TestExecutor_GuardBlocksTransitionForTerminalState(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			CurrentState: "done",
			Metadata:     map[string]any{},
			StateBuckets: map[string]any{},
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
		Event:    events.Event{ID: "evt-1", Type: "task.completed", CreatedAt: time.Now().UTC()},
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
			StateRepo:    &persistentStateRepo{found: true, snapshot: StateSnapshot{Metadata: map[string]any{"score": score}, StateBuckets: map[string]any{}}},
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

	blocked, err := newExecutor(50, false).Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", CreatedAt: time.Now().UTC()},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "entity.score >= 75",
				OnFail: "blocked",
			},
			AdvancesTo: "approved",
		},
	})
	if err != nil {
		t.Fatalf("blocked Execute error: %v", err)
	}
	if blocked.Status != OutcomeBlocked {
		t.Fatalf("blocked Status = %q, want %q", blocked.Status, OutcomeBlocked)
	}

	passed, err := newExecutor(80, true).Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-2", Type: "task.completed", CreatedAt: time.Now().UTC()},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "entity.score >= 75",
				OnFail: "blocked",
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

func TestExecutor_AccumulationDeduplicatesRepeatedEvent(t *testing.T) {
	repo := &persistentStateRepo{
		found: true,
		snapshot: StateSnapshot{
			Metadata:     map[string]any{},
			StateBuckets: map[string]any{},
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
		Event: events.Event{
			ID:        "evt-1",
			Type:      "task.completed",
			Payload:   json.RawMessage(`{"item_id":"item-1"}`),
			CreatedAt: time.Now().UTC(),
		},
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
