package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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

func (r *persistentStateRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	if !r.found {
		return StateSnapshot{}, false, nil
	}
	return StateSnapshot{
		EntityID:     r.snapshot.EntityID,
		CurrentState: r.snapshot.CurrentState,
		StateCarrier: NewStateCarrierWithOwners(
			r.snapshot.StateCarrier.Fields,
			r.snapshot.StateCarrier.Bookkeeping,
			r.snapshot.StateCarrier.Control,
			r.snapshot.StateCarrier.Gates,
			r.snapshot.StateCarrier.StateBuckets,
		),
	}, true, nil
}

func (r *persistentStateRepo) SaveState(_ context.Context, address StateAddress, mutation StateMutation) error {
	entityID := address.EntityID
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
	if mutation.StateCarrier.Fields != nil {
		r.snapshot.StateCarrier.Fields = cloneStringAnyMap(mutation.StateCarrier.Fields)
	}
	r.snapshot.StateCarrier.Bookkeeping = cloneStringAnyMap(mutation.StateCarrier.Bookkeeping)
	r.snapshot.StateCarrier.Control = mutation.StateCarrier.Control
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

func TestExecutorPersistCommitsCompleteAuthoritativeStateCarrier(t *testing.T) {
	entityID := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	cases := []struct {
		name            string
		mutation        StateMutation
		prepare         func(*StateSnapshot)
		wantAuthored    any
		wantApproved    bool
		wantCurrent     string
		wantBucketCount any
		wantActionFact  any
	}{
		{
			name: "authored_only", mutation: StateMutation{StateCarrier: NewStateCarrier(map[string]any{"authored": "changed"}, nil, nil)},
			prepare: func(state *StateSnapshot) { state.StateCarrier.Fields["authored"] = "changed" }, wantAuthored: "changed",
		},
		{
			name: "gate_only", mutation: StateMutation{StateCarrier: NewStateCarrier(nil, map[string]bool{"approved": true}, nil)},
			prepare: func(state *StateSnapshot) { state.StateCarrier.Gates["approved"] = true }, wantAuthored: "value", wantApproved: true,
		},
		{
			name: "lifecycle_only", mutation: StateMutation{NextState: "done"},
			prepare: func(state *StateSnapshot) { state.CurrentState = "done" }, wantAuthored: "value", wantCurrent: "done",
		},
		{
			name: "clear", mutation: StateMutation{StateCarrier: NewStateCarrier(map[string]any{}, nil, nil)},
			prepare: func(state *StateSnapshot) { state.StateCarrier.Fields = map[string]any{} },
		},
		{
			name: "fan_out_or_accumulation", mutation: StateMutation{StateCarrier: NewStateCarrier(nil, nil, map[string]map[string]any{"join": {"count": 3}})},
			prepare: func(state *StateSnapshot) { state.StateCarrier.StateBuckets["join"]["count"] = 3 }, wantAuthored: "value", wantBucketCount: 3,
		},
		{
			name: "action",
			mutation: StateMutation{StateCarrier: NewStateCarrierWithOwners(
				map[string]any{"action": "result"}, map[string]any{"action": "recorded"}, StateControl{}, nil, nil,
			)},
			prepare: func(state *StateSnapshot) {
				state.StateCarrier.Fields["action"] = "result"
				state.StateCarrier.Bookkeeping["action"] = "recorded"
			},
			wantAuthored: "value", wantActionFact: "recorded",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &persistentStateRepo{}
			exec := &Executor{deps: RuntimeDependencies{MutationOwner: stubMutationOwner{state: repo}}}
			state := StateSnapshot{
				EntityID: entityID, CurrentState: "ready",
				StateCarrier: NewStateCarrierWithOwners(
					map[string]any{"authored": "value"},
					map[string]any{"platform": "fact"},
					StateControl{FlowPath: "root"},
					map[string]bool{"ready": true},
					map[string]map[string]any{"join": {"count": 2}},
				),
			}
			tc.prepare(&state)
			repo.snapshot = state
			repo.found = true
			frame := executionFrame{
				ctx: context.Background(),
				req: ExecutionRequest{
					EntityID: entityID,
					Event: eventtest.RunCreatingRootIngress(
						"11111111-1111-1111-1111-111111111112", "state.changed", "", "", nil, 0,
						"11111111-1111-1111-1111-111111111113", "", events.EventEnvelope{}, time.Now().UTC(),
					),
				},
				state:  ExecutionState{State: state},
				result: ExecutionResult{StateMutation: tc.mutation},
			}
			if _, err := exec.persist(context.Background(), frame); err != nil {
				t.Fatalf("persist: %v", err)
			}
			loaded, found, err := repo.LoadState(context.Background(), StateAddress{EntityID: entityID})
			if err != nil || !found {
				t.Fatalf("LoadState found=%v err=%v", found, err)
			}
			if got := loaded.StateCarrier.Bookkeeping["platform"]; got != "fact" {
				t.Fatalf("bookkeeping = %#v, want platform fact", loaded.StateCarrier.Bookkeeping)
			}
			if got := loaded.StateCarrier.Fields["authored"]; !reflect.DeepEqual(got, tc.wantAuthored) {
				t.Fatalf("authored field = %#v, want %#v", got, tc.wantAuthored)
			}
			if loaded.StateCarrier.Control.FlowPath != "root" || !loaded.StateCarrier.Gates["ready"] {
				t.Fatalf("committed control/gates = %#v", loaded.StateCarrier)
			}
			if loaded.StateCarrier.Gates["approved"] != tc.wantApproved {
				t.Fatalf("approved gate = %v, want %v", loaded.StateCarrier.Gates["approved"], tc.wantApproved)
			}
			wantCurrent := tc.wantCurrent
			if wantCurrent == "" {
				wantCurrent = "ready"
			}
			if loaded.CurrentState != wantCurrent {
				t.Fatalf("current state = %q, want %q", loaded.CurrentState, wantCurrent)
			}
			wantBucketCount := tc.wantBucketCount
			if wantBucketCount == nil {
				wantBucketCount = 2
			}
			if !reflect.DeepEqual(loaded.StateCarrier.StateBuckets["join"]["count"], wantBucketCount) {
				t.Fatalf("join count = %#v, want %#v", loaded.StateCarrier.StateBuckets["join"]["count"], wantBucketCount)
			}
			if !reflect.DeepEqual(loaded.StateCarrier.Bookkeeping["action"], tc.wantActionFact) {
				t.Fatalf("action bookkeeping = %#v, want %#v", loaded.StateCarrier.Bookkeeping["action"], tc.wantActionFact)
			}
			if tc.name == "action" && loaded.StateCarrier.Fields["action"] != "result" {
				t.Fatalf("committed carrier = %#v", loaded.StateCarrier)
			}
		})
	}
}

func TestExecutorRejectsAccumulateWithHandlerOnCompleteWithoutBootverify(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source: stubSource(), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RunCreatingRootIngress("evt-1", "item.arrived", "", "", json.RawMessage(`{"item_id":"a"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
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
		MutationOwner:       stubMutationOwner{state: repo},
		Locker:              stubLocker{},
		Dispatcher:          stubDispatcher{},
		TransitionValidator: rejectingTransitionValidator{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
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
		Source:        stubSource(),
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
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
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
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
			Source:        stubSource(),
			StateRepo:     &persistentStateRepo{found: true, snapshot: StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{"score": score}, nil, map[string]map[string]any{})}},
			MutationOwner: stubMutationOwner{},
			Locker:        stubLocker{},
			Dispatcher:    stubDispatcher{},
		}, stubEvaluator{bools: map[string]bool{
			"entity.score >= 75": allowed,
		}})
		if err != nil {
			t.Fatalf("NewExecutor error: %v", err)
		}
		return exec
	}

	rejected, err := newExecutor(50, false).ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
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

	passed, err := newExecutor(80, true).ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RunCreatingRootIngress("evt-2", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
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
		Source:        stubSource(),
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score >= 70": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "ent-1",
		NodeID:   "node-1",
		Event: eventtest.RunCreatingRootIngress("evt-1",
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
	state, ok, err := repo.LoadState(context.Background(), StateAddress{EntityID: "ent-1"})
	if err != nil || !ok {
		t.Fatalf("LoadState = %v, ok=%v", err, ok)
	}
	got, ok := state.StateCarrier.Fields["composite"].(float64)
	if !ok {
		t.Fatalf("composite type = %T, want float64", state.StateCarrier.Fields["composite"])
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
		Source:        stubSource(),
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
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
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"task.completed", "", "", json.RawMessage(`{"item_id":"item-1","marker":"first"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: handler,
	}
	firstResult, err := exec.ExecuteSemanticFixture(context.Background(), first)
	if err != nil {
		t.Fatalf("first Execute error: %v", err)
	}
	if len(firstResult.EmitIntents) != 1 {
		t.Fatalf("first emit intents = %d, want 1", len(firstResult.EmitIntents))
	}
	duplicate := first
	duplicate.Event = eventtest.RunCreatingRootIngress("evt-2",
		"task.completed", "", "", json.RawMessage(`{"item_id":"item-1","marker":"duplicate"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	duplicateResult, err := exec.ExecuteSemanticFixture(context.Background(), duplicate)
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
	if got := repo.snapshot.StateCarrier.Fields["marker"]; got != "first" {
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
		Source:        source,
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
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
		_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
			EntityID:        templatefanin.ReceiverFlowInstance,
			NodeID:          templatefanin.ReceiverNodeID,
			FlowID:          templatefanin.ReceiverFlowID,
			HandlerEventKey: templatefanin.ReceiverEvent,
			Handler:         handler,
			Event: eventtest.RunCreatingRootIngress(
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
