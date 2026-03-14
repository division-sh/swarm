package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	runtimeregistry "empireai/internal/runtime/core/registry"
	"empireai/internal/runtime/semanticview"
)

func stubSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
}

type stubStateRepo struct{}
type stubRunner struct{}
type stubLocker struct{}
type stubOutbox struct{}
type stubTimerApplier struct{}
type stubDispatcher struct{}
type stubActionRegistry struct {
	entries map[identity.ActionKey]runtimeregistry.ActionInstruction
}
type stubActionRunner struct {
	called []string
}
type stubEvaluator struct {
	bools map[string]bool
	errs  map[string]error
}
type stubGuardRegistry struct {
	entries map[identity.GuardKey]runtimeregistry.GuardInstruction
}
type stubPayloadShaper struct{}

func (stubStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, nil
}
func (stubStateRepo) SaveState(context.Context, identity.EntityID, StateMutation) error { return nil }
func (stubRunner) Run(ctx context.Context, fn func(Tx) error) error                     { return fn(stubTx{ctx: ctx}) }
func (stubLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}
func (stubOutbox) WriteOutbox(context.Context, []EmitIntent) error { return nil }
func (stubTimerApplier) ApplyTimerIntents(context.Context, identity.EntityID, []TimerIntent) error {
	return nil
}
func (stubDispatcher) DispatchPostCommit(context.Context, []EmitIntent) error { return nil }
func (s stubEvaluator) EvalBool(expression string, _ BaseContext) (bool, error) {
	if err := s.errs[expression]; err != nil {
		return false, err
	}
	return s.bools[expression], nil
}
func (s stubEvaluator) EvalValue(string, BaseContext) (any, error) { return nil, ErrNotImplemented }
func (r stubGuardRegistry) HasGuard(id identity.GuardKey) bool     { _, ok := r.entries[id]; return ok }
func (r stubGuardRegistry) IsExecutable(id identity.GuardKey) bool { _, ok := r.entries[id]; return ok }
func (r stubGuardRegistry) Guard(id identity.GuardKey) (runtimeregistry.GuardInstruction, bool) {
	entry, ok := r.entries[id]
	return entry, ok
}
func (r stubActionRegistry) HasAction(id identity.ActionKey) bool { _, ok := r.entries[id]; return ok }
func (r stubActionRegistry) IsExecutable(id identity.ActionKey) bool {
	_, ok := r.entries[id]
	return ok
}
func (r stubActionRegistry) Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool) {
	entry, ok := r.entries[id]
	return entry, ok
}
func (r *stubActionRunner) ExecuteAction(_ context.Context, action runtimecontracts.ActionSpec, _ runtimeregistry.ActionInstruction, _ ExecutionContext) (bool, error) {
	r.called = append(r.called, action.ID)
	return true, nil
}
func (stubPayloadShaper) ShapeEmitPayload(_ context.Context, _ ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	out := cloneStringAnyMap(payload)
	out["shaped_for"] = eventType
	return out, nil
}

type stubTx struct{ ctx context.Context }

func (s stubTx) Context() context.Context { return s.ctx }

func TestNewExecutor_DefaultsMaxChainDepth(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       stubSource(),
		StateRepo:    stubStateRepo{},
		TxRunner:     stubRunner{},
		Locker:       stubLocker{},
		Outbox:       stubOutbox{},
		TimerApplier: stubTimerApplier{},
		Dispatcher:   stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	if got := exec.MaxChainDepth(); got != DefaultMaxChainDepth {
		t.Fatalf("MaxChainDepth = %d, want %d", got, DefaultMaxChainDepth)
	}
}

func TestExecutor_ValidateRequestChecksChainDepth(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		TimerApplier:  stubTimerApplier{},
		Dispatcher:    stubDispatcher{},
		MaxChainDepth: 2,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	if err := exec.ValidateRequest(ExecutionRequest{ChainDepth: 3}); err != ErrChainDepthExceeded {
		t.Fatalf("ValidateRequest error = %v, want %v", err, ErrChainDepthExceeded)
	}
}

func TestExecutor_StepOrderIsStable(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       stubSource(),
		StateRepo:    stubStateRepo{},
		TxRunner:     stubRunner{},
		Locker:       stubLocker{},
		Outbox:       stubOutbox{},
		TimerApplier: stubTimerApplier{},
		Dispatcher:   stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	steps := exec.Steps()
	if len(steps) != 18 {
		t.Fatalf("step count = %d, want 18", len(steps))
	}
	if steps[0] != StepQuery || steps[len(steps)-1] != StepClear {
		t.Fatalf("unexpected step order: %v", steps)
	}
}

type orderedStateRepo struct {
	order    *[]string
	mutation StateMutation
}

func (r *orderedStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return StateSnapshot{CurrentState: "pending", Metadata: map[string]any{}, StateBuckets: map[string]any{}}, true, nil
}

func (r *orderedStateRepo) SaveState(_ context.Context, _ identity.EntityID, mutation StateMutation) error {
	*r.order = append(*r.order, "save")
	r.mutation = mutation
	return nil
}

type orderedRunner struct{ order *[]string }

func (r orderedRunner) Run(ctx context.Context, fn func(Tx) error) error {
	*r.order = append(*r.order, "tx")
	return fn(stubTx{ctx: ctx})
}

type orderedLocker struct{ order *[]string }

func (l orderedLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	*l.order = append(*l.order, "lock")
	return fn(ctx)
}

type orderedOutbox struct{ order *[]string }
type orderedTimerApplier struct{ order *[]string }

func (o orderedOutbox) WriteOutbox(context.Context, []EmitIntent) error {
	*o.order = append(*o.order, "outbox")
	return nil
}
func (a orderedTimerApplier) ApplyTimerIntents(context.Context, identity.EntityID, []TimerIntent) error {
	*a.order = append(*a.order, "timers")
	return nil
}

type orderedDispatcher struct{ order *[]string }

func (d orderedDispatcher) DispatchPostCommit(context.Context, []EmitIntent) error {
	*d.order = append(*d.order, "dispatch")
	return nil
}

func TestExecutor_ExecuteUsesAtomicEnvelopeAndOrderedSteps(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:       stubSource(),
		StateRepo:    repo,
		TxRunner:     orderedRunner{order: &order},
		Locker:       orderedLocker{order: &order},
		Outbox:       orderedOutbox{order: &order},
		TimerApplier: orderedTimerApplier{order: &order},
		Dispatcher:   orderedDispatcher{order: &order},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "done",
			ClearGates: []string{"gate_a"},
			Emits:      runtimecontracts.EventEmission{Single: "task.recorded"},
			Action:     runtimecontracts.ActionSpec{ID: "record"},
		},
		State: StateSnapshot{CurrentState: "pending", Metadata: map[string]any{}, StateBuckets: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"lock", "tx", "save", "timers", "outbox", "dispatch"}) {
		t.Fatalf("unexpected envelope order: %v", order)
	}
	if len(result.ExecutedSteps) != len(OrderedSteps) {
		t.Fatalf("executed step count = %d, want %d", len(result.ExecutedSteps), len(OrderedSteps))
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if result.ChainDepth != 1 || len(result.EmitIntents) != 1 {
		t.Fatalf("emit chain depth wrong: depth=%d intents=%d", result.ChainDepth, len(result.EmitIntents))
	}
	if !reflect.DeepEqual(repo.mutation.ClearGates, []string{"gate_a"}) {
		t.Fatalf("clear gates mutation = %#v", repo.mutation.ClearGates)
	}
	if got := result.ActionsExecuted; !reflect.DeepEqual(got, []string{
		"record_state_change",
		"update_stage",
		"cancel_stage_timers",
		"start_stage_timers",
		"record",
	}) {
		t.Fatalf("actions executed = %#v", got)
	}
}

func TestExecutor_ListPrimitivesMutateState(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
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
	initial := StateSnapshot{
		CurrentState: "pending",
		Metadata: map[string]any{
			"dedup_key": "dup-1",
		},
		StateBuckets: map[string]any{},
	}
	storeAccumulator(&initial, "node-1", "items.submitted", &Accumulator{
		StartedAt:     "2026-03-14T00:00:00Z",
		LastEventType: "items.submitted",
	})

	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "items.submitted", Payload: json.RawMessage(`{"items":[{"score":60,"active":true},{"score":40,"active":true},{"score":60,"active":false}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{
				Source:  "payload.items",
				StoreAs: "entity.query_rows",
			},
			Filter: &runtimecontracts.FilterSpec{
				ItemsFrom: "entity.query_rows",
				Condition: "item.score > 50",
				StoreAs:   "entity.filtered",
			},
			Reduce: &runtimecontracts.ReduceSpec{
				ItemsFrom: "entity.filtered",
				Operation: "sum",
				StoreAs:   "entity.total",
			},
			Count: &runtimecontracts.CountSpec{
				ItemsFrom: "entity.filtered",
				Condition: "item.active == true",
				StoreAs:   "entity.active_count",
			},
			Clear: &runtimecontracts.ClearSpec{
				Targets: []string{"pending_dedup", "accumulator_state"},
			},
			AdvancesTo: "done",
		},
		State: initial,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	filtered, ok := repo.mutation.Metadata["filtered"].([]any)
	if !ok || len(filtered) != 2 {
		t.Fatalf("filtered = %#v", repo.mutation.Metadata["filtered"])
	}
	if got := repo.mutation.Metadata["total"]; got != 120 {
		t.Fatalf("total = %#v, want 120", got)
	}
	if got := repo.mutation.Metadata["active_count"]; got != 1 {
		t.Fatalf("active_count = %#v, want 1", got)
	}
	if _, ok := repo.mutation.Metadata["dedup_key"]; ok {
		t.Fatalf("expected dedup_key to be cleared, metadata=%#v", repo.mutation.Metadata)
	}
	if nodeBucket, ok := repo.mutation.StateBuckets["node-1"].(map[string]any); ok {
		if _, ok := nodeBucket[handlerAccumulatorBucketKey]; ok {
			t.Fatalf("expected accumulator state to be cleared, state_buckets=%#v", repo.mutation.StateBuckets)
		}
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q, want done", result.NextState)
	}
}

func TestExecutor_QueryGroupByStoresCounts(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
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
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-2", Type: "digest.requested", Payload: json.RawMessage(`{"items":[{"status":"queued"},{"status":"queued"},{"status":"done"}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{
				Source:  "payload.items",
				GroupBy: "item.status",
				Count:   true,
				StoreAs: "entity.grouped",
			},
		},
		State: StateSnapshot{CurrentState: "pending", Metadata: map[string]any{}, StateBuckets: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	grouped, ok := repo.mutation.Metadata["grouped"].(map[string]any)
	if !ok {
		t.Fatalf("grouped = %#v", repo.mutation.Metadata["grouped"])
	}
	if grouped["queued"] != 2 || grouped["done"] != 1 {
		t.Fatalf("grouped counts = %#v", grouped)
	}
}

func TestExecutor_GuardRecursesAndUsesRegistryCheck(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
		GuardRegistry: stubGuardRegistry{entries: map[identity.GuardKey]runtimeregistry.GuardInstruction{
			identity.NormalizeGuardKey("registry_guard"): {
				Key:   identity.NormalizeGuardKey("registry_guard"),
				Check: "metadata.allowed == true",
			},
		}},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5":             true,
		"vars.metadata.allowed == true": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Checks: []runtimecontracts.GuardCheck{
					{ID: "payload_score", Check: "payload.score > 5"},
					{ID: "registry_guard"},
				},
			},
		},
		State: StateSnapshot{
			Metadata:     map[string]any{"allowed": true},
			StateBuckets: map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.GuardsEvaluated; !reflect.DeepEqual(got, []string{"payload_score", "registry_guard"}) {
		t.Fatalf("GuardsEvaluated = %#v", got)
	}
}

func TestExecutor_RulesUseFirstMatchAndSkipLaterEntries(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "default",
			Rules: []runtimecontracts.HandlerRuleEntry{
				{ID: "rule-1", Condition: "payload.score > 5", AdvancesTo: "approved"},
				{ID: "rule-2", Condition: "else", AdvancesTo: "rejected"},
			},
		},
		State: StateSnapshot{CurrentState: "pending", Metadata: map[string]any{}, StateBuckets: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.RuleID != "rule-1" {
		t.Fatalf("RuleID = %q", result.RuleID)
	}
	if result.NextState != "approved" {
		t.Fatalf("NextState = %q", result.NextState)
	}
}

func TestExecutor_FanOutCreatesShapedEmitIntentsAndStopsLoop(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"items":["a","b"]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom:   "payload.items",
				EmitPerItem: "item.process",
				Target:      "agent-x",
			},
			Action: runtimecontracts.ActionSpec{ID: "should_not_run"},
		},
		State: StateSnapshot{CurrentState: "pending", Metadata: map[string]any{}, StateBuckets: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeFannedOut {
		t.Fatalf("Status = %q", result.Status)
	}
	if result.FanOutCount != 2 || len(result.EmitIntents) != 2 {
		t.Fatalf("fan_out results wrong: count=%d intents=%d", result.FanOutCount, len(result.EmitIntents))
	}
	if result.ChainDepth != 2 {
		t.Fatalf("ChainDepth = %d", result.ChainDepth)
	}
	if got := result.ActionsExecuted; len(got) != 0 {
		t.Fatalf("ActionsExecuted should be empty after fan_out stop: %#v", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["shaped_for"] != "item.process" {
		t.Fatalf("shaped payload missing marker: %#v", payload)
	}
}

func TestExecutor_ClearGatesWildcardUsesNodeGateSchema(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
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
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed"},
		Handler: runtimecontracts.SystemNodeEventHandler{
			ClearGates: []string{"*"},
		},
		State: StateSnapshot{
			Metadata: map[string]any{"note": "keep"},
			Gates:    map[string]bool{"gate_a": true, "gate_b": true},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.ClearGates; !reflect.DeepEqual(got, []string{"gate_a", "gate_b"}) {
		t.Fatalf("ClearGates = %#v", got)
	}
	if result.StateMutation.Gates["gate_a"] != false || result.StateMutation.Gates["gate_b"] != false {
		t.Fatalf("typed gates not cleared: %#v", result.StateMutation.Gates)
	}
	gates, _ := result.StateMutation.Metadata["gates"].(map[string]any)
	if gates["gate_a"] != false || gates["gate_b"] != false {
		t.Fatalf("gate metadata not cleared: %#v", result.StateMutation.Metadata)
	}
	if result.StateMutation.Metadata["note"] != "keep" {
		t.Fatalf("non-gate metadata changed: %#v", result.StateMutation.Metadata)
	}
}

func TestExecutor_ClearGatesRunsBeforeGuardEvaluation(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"vars.metadata.gates.review == false": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed"},
		Handler: runtimecontracts.SystemNodeEventHandler{
			ClearGates: []string{"review"},
			Guard: &runtimecontracts.GuardSpec{
				Check: "metadata.gates.review == false",
			},
		},
		State: StateSnapshot{
			Gates: map[string]bool{"review": true},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeCompleted {
		t.Fatalf("Status = %q", result.Status)
	}
}

func TestExecutor_ActionRegistryEmitsAndRunsActionRunner(t *testing.T) {
	runner := &stubActionRunner{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("notify"): {
				Key:   identity.NormalizeActionKey("notify"),
				Emits: "action.emitted",
			},
		}},
		ActionRunner:  runner,
		PayloadShaper: stubPayloadShaper{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Action: runtimecontracts.ActionSpec{ID: "notify"},
		},
		State: StateSnapshot{Metadata: map[string]any{}, StateBuckets: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := runner.called; !reflect.DeepEqual(got, []string{"notify"}) {
		t.Fatalf("action runner calls = %#v", got)
	}
	if got := result.ActionsExecuted; !reflect.DeepEqual(got, []string{"notify"}) {
		t.Fatalf("ActionsExecuted = %#v", got)
	}
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type) != "action.emitted" {
		t.Fatalf("unexpected action emit intents: %#v", result.EmitIntents)
	}
}

func TestExecutor_GuardOnFailEscalateCreatesEmitIntent(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{
		"payload.ok == true": false,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"ok":false}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "payload.ok == true",
				OnFail: "escalate:guard.failed",
			},
		},
		State: StateSnapshot{Metadata: map[string]any{}, StateBuckets: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeEscalated {
		t.Fatalf("Status = %q", result.Status)
	}
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type) != "guard.failed" {
		t.Fatalf("unexpected escalation intents: %#v", result.EmitIntents)
	}
	if result.ChainDepth != 2 {
		t.Fatalf("ChainDepth = %d", result.ChainDepth)
	}
}
