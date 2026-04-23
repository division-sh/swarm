package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	runtimeregistry "swarm/internal/runtime/core/registry"
	"swarm/internal/runtime/semanticview"
)

func stubSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
}

func sourceWithPolicy(values map[string]any) semanticview.Source {
	policy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{}}
	for key, value := range values {
		policy.Values[key] = runtimecontracts.PolicyValue{Value: value}
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Policy: policy})
}

func stubSourceWithRootEntityContract() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Analysis": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"summary":      {Type: "text"},
						"report_count": {Type: "integer"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"analysis": {Type: "Analysis"},
				},
			},
		},
	})
}

func sourceWithKilledState() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Stages: []runtimecontracts.WorkflowStageContract{
				{ID: "pending"},
				{ID: "killed"},
			},
			TerminalStages: []string{"killed"},
		},
	})
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
type lockOrderStateRepo struct {
	order *[]string
}
type lockOrderLocker struct {
	order *[]string
}
type stubEvaluator struct {
	bools map[string]bool
	errs  map[string]error
}
type contextualBoolEvaluator struct {
	bools map[string]func(BaseContext) (bool, error)
}
type stubGuardRegistry struct {
	entries map[identity.GuardKey]runtimeregistry.GuardInstruction
}
type stubPayloadShaper struct{}
type recordingPayloadShaper struct {
	lastReq     ExecutionRequest
	lastPayload map[string]any
	lastSurface EmitSurface
	err         error
}

func (stubStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, nil
}
func (stubStateRepo) SaveState(context.Context, identity.EntityID, StateMutation) error { return nil }
func (stubRunner) Run(ctx context.Context, fn func(Tx) error) error                     { return fn(stubTx{ctx: ctx}) }
func (stubLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}
func (r lockOrderStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	if r.order != nil {
		*r.order = append(*r.order, "load")
	}
	return testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}), true, nil
}
func (lockOrderStateRepo) SaveState(context.Context, identity.EntityID, StateMutation) error {
	return nil
}
func (l lockOrderLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	if l.order != nil {
		*l.order = append(*l.order, "lock")
	}
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
func (s contextualBoolEvaluator) EvalBool(expression string, base BaseContext) (bool, error) {
	if fn, ok := s.bools[expression]; ok {
		return fn(base)
	}
	return false, nil
}
func (s contextualBoolEvaluator) EvalValue(string, BaseContext) (any, error) {
	return nil, ErrNotImplemented
}
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
func (s *recordingPayloadShaper) ShapeEmitPayload(ctx context.Context, req ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	s.lastReq = req
	s.lastPayload = cloneStringAnyMap(payload)
	s.lastSurface = EmitSurfaceFromContext(ctx)
	if s.err != nil {
		return nil, s.err
	}
	out := cloneStringAnyMap(payload)
	out["shaped_for"] = eventType
	return out, nil
}

type stubTx struct{ ctx context.Context }

func (s stubTx) Context() context.Context { return s.ctx }

func testStateSnapshot(currentState string, metadata map[string]any, gates map[string]bool, buckets map[string]map[string]any) StateSnapshot {
	return StateSnapshot{
		CurrentState: currentState,
		StateCarrier: NewStateCarrier(metadata, gates, buckets),
	}
}

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

func TestExecutor_ValidateRequestAllowsDeepInboundChainDepth(t *testing.T) {
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
	if err := exec.ValidateRequest(ExecutionRequest{ChainDepth: 3}); err != nil {
		t.Fatalf("ValidateRequest error = %v, want nil", err)
	}
}

func TestExecutor_ValidateRequestRejectsConflictingCompletionDialect(t *testing.T) {
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
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnComplete: []runtimecontracts.HandlerRuleEntry{{Condition: "true"}},
			Rules:      []runtimecontracts.HandlerRuleEntry{{Condition: "else"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both on_complete and rules") {
		t.Fatalf("ValidateRequest error = %v, want conflicting completion error", err)
	}
}

func TestExecutionScopeResolveOperand_AllowsEventRoot(t *testing.T) {
	scope := newExecutionScope(
		nil,
		map[string]any{"entity_id": "payload-entity"},
		map[string]any{"entity_id": "event-entity"},
		nil,
		nil,
	)

	got, err := scope.resolveOperand("event.entity_id", executionOperandDefaultNone)
	if err != nil {
		t.Fatalf("resolveOperand(event.entity_id) error: %v", err)
	}
	if got != "event-entity" {
		t.Fatalf("resolveOperand(event.entity_id) = %#v, want event-entity", got)
	}
}

func TestCompiledExecutionCondition_AllowsEventRoot(t *testing.T) {
	compiled, err := compileExecutionCondition(`event.entity_id == "event-entity"`)
	if err != nil {
		t.Fatalf("compileExecutionCondition error: %v", err)
	}

	scope := newExecutionScope(
		nil,
		map[string]any{"entity_id": "payload-entity"},
		map[string]any{"entity_id": "event-entity"},
		nil,
		nil,
	)

	ok, err := compiled.Eval(scope)
	if err != nil {
		t.Fatalf("compiled condition Eval error: %v", err)
	}
	if !ok {
		t.Fatal("compiled condition evaluated false, want true")
	}
}

func TestExecutor_ValidateRequestRejectsCreateEntityWithAccumulate(t *testing.T) {
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
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
			Accumulate:   &runtimecontracts.AccumulateSpec{},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("ValidateRequest error = %v, want create_entity/accumulate error", err)
	}
}

func TestExecutor_ValidateRequestRejectsTieredWeightedAverageWithoutDimensionKey(t *testing.T) {
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
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpWeightedAverage,
				Keys: runtimecontracts.ComputeKeyConfig{
					ScoreKeys: []string{"score"},
				},
				Tiers: []runtimecontracts.ComputeTier{{Dimensions: []string{"build_complexity"}, Weight: 1}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "keys.dimension_key") {
		t.Fatalf("ValidateRequest error = %v, want keys.dimension_key error", err)
	}
}

func TestExecutor_ValidateRequestRejectsTieredWeightedAverageWithoutScoreKeys(t *testing.T) {
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
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpWeightedAverage,
				Keys: runtimecontracts.ComputeKeyConfig{
					DimensionKey: "dimension",
				},
				Tiers: []runtimecontracts.ComputeTier{{Dimensions: []string{"build_complexity"}, Weight: 1}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "keys.score_keys") {
		t.Fatalf("ValidateRequest error = %v, want keys.score_keys error", err)
	}
}

func TestExecutor_LoadsStateInsideEntityLock(t *testing.T) {
	order := []string{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  lockOrderStateRepo{order: &order},
		TxRunner:   stubRunner{},
		Locker:     lockOrderLocker{order: &order},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		NodeID:   identity.NodeID("node-1"),
		FlowID:   identity.FlowID("flow-1"),
		Event: events.Event{
			ID:        "evt-1",
			Type:      events.EventType("test.event"),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("11111111-1111-1111-1111-111111111111"),
		State: StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{})},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got, want := order, []string{"lock", "load"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
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
	if len(steps) != 19 {
		t.Fatalf("step count = %d, want 19", len(steps))
	}
	if steps[0] != StepQuery || steps[len(steps)-1] != StepClear {
		t.Fatalf("unexpected step order: %v", steps)
	}
	if steps[5] != StepGroupBy {
		t.Fatalf("expected group_by at index 5, got order %v", steps)
	}
}

func TestExecutor_ShapeEmitPayloadUsesUpdatedState(t *testing.T) {
	shaper := &recordingPayloadShaper{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		TimerApplier:  stubTimerApplier{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	req := ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		NodeID:   identity.NodeID("scoring-node"),
		FlowID:   identity.FlowID("scoring"),
		Event: events.Event{
			ID:        "evt-1",
			Type:      events.EventType("scoring/score.dimension_complete"),
			Payload:   []byte(`{"dimension":"build_complexity","score":80}`),
			CreatedAt: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		}.WithEntityID("11111111-1111-1111-1111-111111111111"),
		State: StateSnapshot{
			EntityID:     identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
			CurrentState: "discovered",
			StateCarrier: NewStateCarrier(map[string]any{
				"composite_score": 0,
			}, nil, map[string]map[string]any{}),
		},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpWeightedAverage,
				StoreAs:   "entity.composite_score",
				Tiers: []runtimecontracts.ComputeTier{
					{Dimensions: []string{"build_complexity"}, Weight: 1},
				},
				Keys: runtimecontracts.ComputeKeyConfig{
					DimensionKey: "dimension",
					ScoreKeys:    []string{"score"},
				},
			},
			Accumulate: &runtimecontracts.AccumulateSpec{
				ExpectedFrom: "entity.dimensions_requested",
				Completion:   runtimecontracts.ParseAccumulateCompletion("all"),
				DedupBy:      "payload.dimension",
			},
			OnComplete: []runtimecontracts.HandlerRuleEntry{
				{Condition: "else", Emit: runtimecontracts.EmitSpec{Event: "vertical.rejected"}},
			},
		},
	}
	req.State.SetMetadata("dimensions_requested", []any{"build_complexity"})
	result, err := exec.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("emit intents = %d, want 1", len(result.EmitIntents))
	}
	if got := shaper.lastReq.State.StateCarrier.Metadata["composite_score"]; got != 80.0 && got != 80 {
		t.Fatalf("payload shaper saw composite_score = %#v, want 80", got)
	}
}

type orderedStateRepo struct {
	order    *[]string
	mutation StateMutation
}

func (r *orderedStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}), true, nil
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
			Emit:       runtimecontracts.EmitSpec{Event: "task.recorded"},
			Action:     runtimecontracts.ActionSpec{ID: "record"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
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
		StateCarrier: NewStateCarrier(map[string]any{
			"dedup_key": "dup-1",
		}, nil, map[string]map[string]any{}),
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
	if nodeBucket, ok := repo.mutation.StateBuckets["node-1"]; ok {
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
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
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

func TestExecutor_QueryFilterUsesExplicitCollidingScopes(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     sourceWithPolicy(map[string]any{"score": 6}),
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
		Event:    events.Event{ID: "evt-2", Type: "digest.requested", Payload: json.RawMessage(`{"score":5,"items":[{"score":7},{"score":5}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{
				Source:  "payload.items",
				Filter:  "item.score > payload.score && item.score > entity.score && item.score > policy.score",
				StoreAs: "entity.query_rows",
			},
		},
		State: testStateSnapshot("pending", map[string]any{"score": 4}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rows, ok := result.StateMutation.Metadata["query_rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("query_rows = %#v", result.StateMutation.Metadata["query_rows"])
	}
	item, _ := rows[0].(map[string]any)
	if item["score"] != 7.0 {
		t.Fatalf("query_rows[0] = %#v", item)
	}
}

func TestExecutor_FilterRejectsUnqualifiedConditionField(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     sourceWithPolicy(map[string]any{"score": 1}),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "items.submitted", Payload: json.RawMessage(`{"score":5,"items":[{"score":7}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Filter: &runtimecontracts.FilterSpec{
				ItemsFrom: "payload.items",
				Condition: "score > 5",
				StoreAs:   "entity.filtered",
			},
		},
		State: testStateSnapshot("pending", map[string]any{"score": 4}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "undeclared reference") {
		t.Fatalf("Execute error = %v, want undeclared reference", err)
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
				Check: "entity.allowed == true",
			},
		}},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5":      true,
		"entity.allowed == true": true,
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
			StateCarrier: NewStateCarrier(map[string]any{"allowed": true}, nil, map[string]map[string]any{}),
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
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
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

func TestExecutor_RejectsAmbiguousHandlerTopLevelEmitWithRules(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "handler.emitted"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "rule.emitted"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatalf("expected ambiguous handler-level emit config to be rejected, got %+v", result)
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutor_RejectsAmbiguousHandlerTopLevelEmitWithRulesWithoutRuleEmit(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "handler.emitted"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:         "rule-1",
				Condition:  "payload.score > 5",
				AdvancesTo: "approved",
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatalf("expected ambiguous handler-level emit config to be rejected, got %+v", result)
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutor_RuleDataAccumulationRunsBeforeTopLevelWrites(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"score":9}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "metadata.final_source",
					Value:       runtimecontracts.LiteralExpression("handler"),
				}},
			},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				Condition: "payload.score > 5",
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
					Writes: []runtimecontracts.WorkflowDataWrite{{
						TargetField: "metadata.final_source",
						Value:       runtimecontracts.LiteralExpression("rule"),
					}, {
						TargetField: "metadata.rule_only",
						Value:       runtimecontracts.LiteralExpression("applied"),
					}},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.StateMutation.Metadata["final_source"]; got != "handler" {
		t.Fatalf("final_source = %#v, want handler", got)
	}
	if got := result.StateMutation.Metadata["rule_only"]; got != "applied" {
		t.Fatalf("rule_only = %#v, want applied", got)
	}
}

func TestExecutor_RulesDoNotSeeCurrentHandlerTopLevelWritesBeforeSelection(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`entity.branch_target == "handler"`: func(base BaseContext) (bool, error) {
			return base.Entity.Raw()["branch_target"] == "handler", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "branch_target",
					Value:       runtimecontracts.LiteralExpression("handler"),
				}},
			},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "too-early",
				Condition: `entity.branch_target == "handler"`,
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
					Writes: []runtimecontracts.WorkflowDataWrite{{
						TargetField: "rule_selected",
						Value:       runtimecontracts.LiteralExpression(true),
					}},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := strings.TrimSpace(result.RuleID); got != "" {
		t.Fatalf("rule_id = %q, want empty when branch selection cannot see top-level writes", got)
	}
	if _, exists := result.StateMutation.Metadata["rule_selected"]; exists {
		t.Fatalf("rule_selected unexpectedly present after rules evaluated before top-level writes: %#v", result.StateMutation.Metadata["rule_selected"])
	}
	if got := result.StateMutation.Metadata["branch_target"]; got != "handler" {
		t.Fatalf("branch_target = %#v, want handler after data_accumulation step", got)
	}
}

func TestExecutor_OnCompleteDoesNotSeeCurrentHandlerTopLevelWritesBeforeSelection(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`entity.branch_target == "handler"`: func(base BaseContext) (bool, error) {
			return base.Entity.Raw()["branch_target"] == "handler", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "branch_target",
					Value:       runtimecontracts.LiteralExpression("handler"),
				}},
			},
			OnComplete: []runtimecontracts.HandlerRuleEntry{{
				ID:        "too-early",
				Condition: `entity.branch_target == "handler"`,
				Emit:      runtimecontracts.EmitSpec{Event: "branch.selected"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := strings.TrimSpace(result.RuleID); got != "" {
		t.Fatalf("rule_id = %q, want empty when on_complete selection cannot see top-level writes", got)
	}
	if got := len(result.EmitIntents); got != 0 {
		t.Fatalf("emit intents = %d, want 0 when on_complete branch is not selected early", got)
	}
	if got := result.StateMutation.Metadata["branch_target"]; got != "handler" {
		t.Fatalf("branch_target = %#v, want handler after data_accumulation step", got)
	}
}

func TestExecutor_ChainDepthOverflowInterceptsEmitsButSucceeds(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		MaxChainDepth: 1,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "done",
			Emit:       runtimecontracts.EmitSpec{Event: "task.followup"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.Status; got != OutcomeCompleted {
		t.Fatalf("Status = %q, want completed", got)
	}
	if got := result.NextState; got != "done" {
		t.Fatalf("NextState = %q, want done", got)
	}
	if got := len(result.EmitIntents); got != 0 {
		t.Fatalf("EmitIntents count = %d, want 0", got)
	}
	if got := len(result.DeadLetterIntents); got != 1 {
		t.Fatalf("DeadLetterIntents count = %d, want 1", got)
	}
	if got := result.DeadLetterIntents[0].DeadLetterHint; got != "chain_depth_exceeded" {
		t.Fatalf("DeadLetterHint = %q", got)
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
				ItemsFrom: "payload.items",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
				Target:    "agent-x",
			},
			AdvancesTo: "processing",
			Action:     runtimecontracts.ActionSpec{ID: "should_not_run"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeFannedOut {
		t.Fatalf("Status = %q", result.Status)
	}
	if result.NextState != "processing" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if result.FanOutCount != 2 || len(result.EmitIntents) != 2 {
		t.Fatalf("fan_out results wrong: count=%d intents=%d", result.FanOutCount, len(result.EmitIntents))
	}
	if got := result.StateMutation.Metadata["fan_out_count"]; got != 2 {
		t.Fatalf("fan_out_count metadata = %#v", got)
	}
	if result.ChainDepth != 2 {
		t.Fatalf("ChainDepth = %d", result.ChainDepth)
	}
	if got := result.ActionsExecuted; len(got) != 4 {
		t.Fatalf("ActionsExecuted = %#v", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["shaped_for"] != "item.process" {
		t.Fatalf("shaped payload missing marker: %#v", payload)
	}
}

func TestExecutor_PayloadTransformSeesDataAccumulationWrites(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.mode == 'corpus'": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "vertical-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event: events.Event{
			ID:      "evt-1",
			Type:    "vertical.discovered",
			Payload: json.RawMessage(`{"mode":"corpus","discovery_context":{"source":"corpus"}}`),
		},
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "name", Value: runtimecontracts.LiteralExpression("Test Vertical")},
					{TargetField: "dimensions_requested", Value: runtimecontracts.LiteralExpression([]string{"a", "b"})},
				},
			},
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					Condition: "payload.mode == 'corpus'",
					DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
						Writes: []runtimecontracts.WorkflowDataWrite{
							{TargetField: "scoring_rubric", Value: runtimecontracts.LiteralExpression("corpus_rubric")},
						},
					},
					Emit: runtimecontracts.EmitSpec{
						Event: "scoring.requested",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"vertical_name":        runtimecontracts.CELExpression("entity.name"),
							"rubric":               runtimecontracts.CELExpression("entity.scoring_rubric"),
							"dimensions_requested": runtimecontracts.CELExpression("entity.dimensions_requested"),
							"discovery_context":    runtimecontracts.CELExpression("payload.discovery_context"),
						},
					},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := payload["vertical_name"]; got != "Test Vertical" {
		t.Fatalf("vertical_name = %#v", got)
	}
	if got := payload["rubric"]; got != "corpus_rubric" {
		t.Fatalf("rubric = %#v", got)
	}
	dims, ok := payload["dimensions_requested"].([]any)
	if !ok || len(dims) != 2 || dims[0] != "a" || dims[1] != "b" {
		t.Fatalf("dimensions_requested = %#v", payload["dimensions_requested"])
	}
	ctx, ok := payload["discovery_context"].(map[string]any)
	if !ok || ctx["source"] != "corpus" {
		t.Fatalf("discovery_context = %#v", payload["discovery_context"])
	}
}

func TestExecutor_DataAccumulationTargetPathWritesNestedEntityLeaf(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSourceWithRootEntityContract(),
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
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"summary":"ready"}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					SourceField:   "summary",
					TargetPathRef: "entity.analysis.summary",
				}},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"analysis": map[string]any{
				"summary":      "stale",
				"report_count": 2,
			},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	analysis, ok := result.StateMutation.Metadata["analysis"].(map[string]any)
	if !ok {
		t.Fatalf("analysis = %#v", result.StateMutation.Metadata["analysis"])
	}
	if got := analysis["summary"]; got != "ready" {
		t.Fatalf("analysis.summary = %#v, want ready", got)
	}
	if got := analysis["report_count"]; got != 2 {
		t.Fatalf("analysis.report_count = %#v, want 2", got)
	}
}

func TestExecutor_RejectsUndeclaredNestedEntityWriteBeforeExecution(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSourceWithRootEntityContract(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpCount,
				StoreAs:   "entity.analysis.missing",
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected invalid config error")
	}
	if !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("error = %v, want invalid config", err)
	}
	if !strings.Contains(err.Error(), "entity.analysis.missing") {
		t.Fatalf("error = %v, want target path context", err)
	}
}

func TestExecutor_ClearRemovesNestedEntityLeaf(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSourceWithRootEntityContract(),
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
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Target: "entity.analysis.summary"},
		},
		State: testStateSnapshot("pending", map[string]any{
			"analysis": map[string]any{
				"summary":      "stale",
				"report_count": 2,
			},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	analysis, ok := result.StateMutation.Metadata["analysis"].(map[string]any)
	if !ok {
		t.Fatalf("analysis = %#v", result.StateMutation.Metadata["analysis"])
	}
	if _, exists := analysis["summary"]; exists {
		t.Fatalf("analysis.summary unexpectedly present: %#v", analysis)
	}
	if got := analysis["report_count"]; got != 2 {
		t.Fatalf("analysis.report_count = %#v, want 2", got)
	}
}

func TestExecutor_ClearSpecialTargetsBypassContractValidation(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSourceWithRootEntityContract(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	initial := testStateSnapshot("pending", map[string]any{
		"dedup_key":         "dup-1",
		"accumulated_total": 5,
		"received_items":    []any{"a"},
	}, nil, map[string]map[string]any{
		"node-1": {
			handlerAccumulatorBucketKey: map[string]any{"items": []any{"a"}},
		},
	})
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "root",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"pending_dedup", "accumulator_state"}},
		},
		State: initial,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if _, ok := result.StateMutation.Metadata["dedup_key"]; ok {
		t.Fatalf("expected dedup_key to be cleared, metadata=%#v", result.StateMutation.Metadata)
	}
	if _, ok := result.StateMutation.Metadata["received_items"]; ok {
		t.Fatalf("expected received_items to be cleared, metadata=%#v", result.StateMutation.Metadata)
	}
	if nodeBucket, ok := result.StateMutation.StateBuckets["node-1"]; ok {
		if _, ok := nodeBucket[handlerAccumulatorBucketKey]; ok {
			t.Fatalf("expected accumulator bucket to be cleared, state_buckets=%#v", result.StateMutation.StateBuckets)
		}
	}
}

func TestExecutor_EmitFieldsCELFailureReturnsError(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, stubEvaluator{})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "vertical-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event: events.Event{
			ID:      "evt-1",
			Type:    "vertical.discovered",
			Payload: json.RawMessage(`{"mode":"corpus"}`),
		},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{
				Event: "scoring.requested",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"missing": runtimecontracts.CELExpression("payload.discovery_context.source +"),
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected emit.fields CEL failure to return an error")
	}
}

func TestExecutor_FanOutEmptyPersistsCountAndContinues(t *testing.T) {
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
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"items":[]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
			},
			AdvancesTo: "scanning",
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeCompleted {
		t.Fatalf("Status = %q", result.Status)
	}
	if result.NextState != "scanning" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if got := result.StateMutation.Metadata["fan_out_count"]; got != 0 {
		t.Fatalf("fan_out_count metadata = %#v", got)
	}
}

func TestExecutor_FanOutInternalCountBypassesEntityContractValidation(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSourceWithRootEntityContract(),
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
		FlowID:   "root",
		Event:    events.Event{ID: "evt-1", Type: "task.completed", Payload: json.RawMessage(`{"items":[]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
			},
			AdvancesTo: "scanning",
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.StateMutation.Metadata["fan_out_count"]; got != 0 {
		t.Fatalf("fan_out_count metadata = %#v", got)
	}
}

func TestExecutor_FanOutUsesExplicitEmitEvent(t *testing.T) {
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
		Event:      events.Event{ID: "evt-1", Type: "batch.submitted", Payload: json.RawMessage(`{"items":[{"kind":"a"},{"kind":"b"}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				Emit:      runtimecontracts.EmitSpec{Event: "routed.item"},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 2 {
		t.Fatalf("EmitIntents count = %d", got)
	}
	if got := string(result.EmitIntents[0].Event.Type); got != "routed.item" {
		t.Fatalf("first emit type = %q", got)
	}
	if got := string(result.EmitIntents[1].Event.Type); got != "routed.item" {
		t.Fatalf("second emit type = %q", got)
	}
	if !result.EmitIntents[1].Event.CreatedAt.After(result.EmitIntents[0].Event.CreatedAt) {
		t.Fatalf("emit CreatedAt ordering = [%s, %s]", result.EmitIntents[0].Event.CreatedAt, result.EmitIntents[1].Event.CreatedAt)
	}
}

func TestExecutor_GuardKillTransitionsToKilledStateWhenDeclared(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     sourceWithKilledState(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.score >= policy.threshold": false}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    events.Event{ID: "evt-1", Type: "check.requested", Payload: json.RawMessage(`{"score":50}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "payload.score >= policy.threshold",
				OnFail: "kill",
			},
			AdvancesTo: "done",
			Emit:       runtimecontracts.EmitSpec{Event: "check.passed"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.Status; got != OutcomeKilled {
		t.Fatalf("Status = %q", got)
	}
	if got := result.NextState; got != "killed" {
		t.Fatalf("NextState = %q", got)
	}
	if got := result.StateMutation.NextState; got != "killed" {
		t.Fatalf("StateMutation.NextState = %q", got)
	}
}

func TestExecutor_GroupByStoresGroupedItems(t *testing.T) {
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
		Event:    events.Event{ID: "evt-1", Type: "items.submitted", Payload: json.RawMessage(`{"items":[{"name":"a","category":"x"},{"name":"b","category":"y"},{"name":"c","category":"x"}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			GroupBy: &runtimecontracts.GroupBySpec{
				ItemsFrom: "payload.items",
				Key:       "category",
				StoreAs:   "entity.grouped",
			},
			AdvancesTo: "done",
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	grouped, ok := result.StateMutation.Metadata["grouped"].(map[string]any)
	if !ok {
		t.Fatalf("grouped metadata = %#v", result.StateMutation.Metadata["grouped"])
	}
	xItems, _ := grouped["x"].([]any)
	yItems, _ := grouped["y"].([]any)
	if len(xItems) != 2 || len(yItems) != 1 {
		t.Fatalf("grouped metadata = %#v", grouped)
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q", result.NextState)
	}
}

func TestExecutor_GroupByBareKeyUsesItemScopeWithoutFallbackAcrossRoots(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     sourceWithPolicy(map[string]any{"category": "policy"}),
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
		Event:    events.Event{ID: "evt-1", Type: "items.submitted", Payload: json.RawMessage(`{"category":"payload","items":[{"name":"a","category":"x"},{"name":"b","category":"y"},{"name":"c","category":"x"}]}`)},
		Handler: runtimecontracts.SystemNodeEventHandler{
			GroupBy: &runtimecontracts.GroupBySpec{
				ItemsFrom: "payload.items",
				Key:       "category",
				StoreAs:   "entity.grouped",
			},
			AdvancesTo: "done",
		},
		State: testStateSnapshot("pending", map[string]any{"category": "entity"}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	grouped, ok := result.StateMutation.Metadata["grouped"].(map[string]any)
	if !ok {
		t.Fatalf("grouped metadata = %#v", result.StateMutation.Metadata["grouped"])
	}
	xItems, _ := grouped["x"].([]any)
	yItems, _ := grouped["y"].([]any)
	if len(xItems) != 2 || len(yItems) != 1 {
		t.Fatalf("grouped metadata = %#v", grouped)
	}
	if _, ok := grouped["payload"]; ok {
		t.Fatalf("grouped metadata unexpectedly used payload scope: %#v", grouped)
	}
	if _, ok := grouped["entity"]; ok {
		t.Fatalf("grouped metadata unexpectedly used entity scope: %#v", grouped)
	}
	if _, ok := grouped["policy"]; ok {
		t.Fatalf("grouped metadata unexpectedly used policy scope: %#v", grouped)
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
			StateCarrier: NewStateCarrier(map[string]any{"note": "keep"}, map[string]bool{"gate_a": true, "gate_b": true}, nil),
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
	if result.StateMutation.Gates["gate_a"] != false || result.StateMutation.Gates["gate_b"] != false {
		t.Fatalf("typed gate state not cleared: %#v", result.StateMutation.Gates)
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
		"entity.gates.review == false": true,
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
				Check: "entity.gates.review == false",
			},
		},
		State: StateSnapshot{StateCarrier: NewStateCarrier(nil, map[string]bool{"review": true}, nil)},
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
	shaper := &recordingPayloadShaper{}
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
		PayloadShaper: shaper,
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
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
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
	if got := shaper.lastPayload["score"]; got != float64(9) {
		t.Fatalf("action emit payload score = %#v, want 9", got)
	}
	if shaper.lastSurface != EmitSurfaceAction {
		t.Fatalf("action emit surface = %q, want %q", shaper.lastSurface, EmitSurfaceAction)
	}
}

func TestExecutor_ActionRegistryEmitContractViolationRejectsHandler(t *testing.T) {
	runner := &stubActionRunner{}
	shaper := &recordingPayloadShaper{err: errors.Join(ErrEmitPayloadContractViolation, errors.New("wrapped payload contract failure"))}
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
		PayloadShaper: shaper,
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
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if !errors.Is(err, ErrEmitPayloadContractViolation) {
		t.Fatalf("Execute error = %v, want %v", err, ErrEmitPayloadContractViolation)
	}
	if result.Status != OutcomeRejected {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeRejected)
	}
	if result.FailureClass != FailureLogic {
		t.Fatalf("FailureClass = %q, want %q", result.FailureClass, FailureLogic)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("EmitIntents = %#v, want none", result.EmitIntents)
	}
	if len(result.ActionsExecuted) != 0 {
		t.Fatalf("ActionsExecuted = %#v, want none", result.ActionsExecuted)
	}
	if len(runner.called) != 0 {
		t.Fatalf("action runner calls = %#v, want none", runner.called)
	}
	if shaper.lastSurface != EmitSurfaceAction {
		t.Fatalf("action emit surface = %q, want %q", shaper.lastSurface, EmitSurfaceAction)
	}
}

func TestExecutor_GuardOnFailEscalateCreatesEmitIntent(t *testing.T) {
	shaper := &recordingPayloadShaper{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
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
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
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
	if len(shaper.lastPayload) != 0 {
		t.Fatalf("guard escalation payload = %#v, want empty explicit business payload", shaper.lastPayload)
	}
}
