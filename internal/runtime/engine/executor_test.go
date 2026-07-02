package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func stubSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
}

func sourceWithDeclarativeEmitExternalizationFlows() semanticview.Source {
	component := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "component-scaffold", Flow: "component-scaffold", Mode: "template"},
		Path:  "component-scaffold",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{Events: []string{"repo_scaffold.repo_scaffolded"}},
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"component.scaffolded"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"component.scaffolded": {},
		},
	}
	repo := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "repo-scaffold", Flow: "repo-scaffold"},
		Path:  "repo-scaffold",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"repo_scaffold.repo_scaffolded"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo_scaffold.repo_scaffolded": {},
		},
	}
	operating := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Path:  "operating",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{component, repo, operating}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"component-scaffold": &root.Children[0],
				"repo-scaffold":      &root.Children[1],
				"operating":          &root.Children[2],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"component-scaffold": &root.Children[0],
				"repo-scaffold":      &root.Children[1],
				"operating":          &root.Children[2],
			},
		},
	})
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
				"VerticalState": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"status":      {Type: "text"},
						"active_jobs": {Type: "[Job]"},
					},
				},
				"Job": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"id":    {Type: "text"},
						"title": {Type: "text"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"analysis":  {Type: "Analysis"},
					"verticals": {Type: "map[text]VerticalState"},
					"tags":      {Type: "[text]"},
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
type recordingStateRepo struct {
	saves int
}
type actionMergeStateRepo struct {
	snapshot StateSnapshot
}
type stubRunner struct{}
type stubLocker struct{}
type stubOutbox struct{}
type recordingEmitOutbox struct {
	intents []EmitIntent
	err     error
}
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
type eventErrPayloadShaper struct {
	failEvent string
	shaped    []string
}

func (stubStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, nil
}
func (stubStateRepo) SaveState(context.Context, identity.EntityID, StateMutation) error { return nil }
func (r *recordingStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, nil
}
func (r *recordingStateRepo) SaveState(context.Context, identity.EntityID, StateMutation) error {
	r.saves++
	return nil
}
func (r actionMergeStateRepo) LoadState(context.Context, identity.EntityID) (StateSnapshot, bool, error) {
	return r.snapshot, true, nil
}
func (actionMergeStateRepo) SaveState(context.Context, identity.EntityID, StateMutation) error {
	return nil
}
func (stubRunner) Run(ctx context.Context, fn func(Tx) error) error { return fn(stubTx{ctx: ctx}) }
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
func (o *recordingEmitOutbox) WriteOutbox(_ context.Context, intents []EmitIntent) error {
	if o.err != nil {
		return o.err
	}
	o.intents = append(o.intents, intents...)
	return nil
}
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
func (s *eventErrPayloadShaper) ShapeEmitPayload(_ context.Context, _ ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	s.shaped = append(s.shaped, eventType)
	if eventType == s.failEvent {
		return nil, errors.New("payload shape failed")
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

func eventPayloadMap(t *testing.T, evt events.Event) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(evt.Payload(), &out); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	return out
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

func TestExecutor_ValidateRequestRejectsCreateEntityWithSelectEntity(t *testing.T) {
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
			SelectEntity: &runtimecontracts.SelectEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "vertical_id", Ref: "payload.vertical_id"}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and select_entity") {
		t.Fatalf("ValidateRequest error = %v, want create_entity/select_entity error", err)
	}
}

func TestExecutor_ValidateRequestRejectsCreateEntityWithSelectOrCreateEntity(t *testing.T) {
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
			SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "repo_id", Ref: "payload.repo_id"}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and select_or_create_entity") {
		t.Fatalf("ValidateRequest error = %v, want create_entity/select_or_create_entity error", err)
	}
}

func TestExecutor_ValidateRequestRejectsSelectEntityWithSelectOrCreateEntity(t *testing.T) {
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
			SelectEntity: &runtimecontracts.SelectEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "repo_id", Ref: "payload.repo_id"}},
			},
			SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "repo_id", Ref: "payload.repo_id"}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both select_entity and select_or_create_entity") {
		t.Fatalf("ValidateRequest error = %v, want select_entity/select_or_create_entity error", err)
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
		Event: eventtest.RootIngress(
			"evt-1",
			events.EventType("test.event"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
			time.Now().UTC(),
		),

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
	if len(steps) != 20 {
		t.Fatalf("step count = %d, want 20", len(steps))
	}
	if steps[0] != StepQuery || steps[len(steps)-1] != StepClear {
		t.Fatalf("unexpected step order: %v", steps)
	}
	if steps[5] != StepGroupBy {
		t.Fatalf("expected group_by at index 5, got order %v", steps)
	}
	if steps[15] != StepProjection {
		t.Fatalf("expected projection after data_writes at index 15, got order %v", steps)
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
		Event: eventtest.RootIngress(
			"evt-1",
			events.EventType("scoring/score.dimension_complete"),
			"",
			"",
			[]byte(`{"dimension":"build_complexity","score":80}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),

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

func accumulatorProjectionTestSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension":  {Type: "text"},
						"tier":       {Type: "integer"},
						"score":      {Type: "integer"},
						"evidence":   {Type: "text"},
						"confidence": {Type: "text"},
					},
				},
				"DimensionSummary": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension":  {Type: "text"},
						"score":      {Type: "integer"},
						"confidence": {Type: "text"},
						"source":     {Type: "text"},
					},
				},
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"projection": {Value: map[string]any{"default_confidence": "medium"}},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"vertical": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scores": {
						Type:            "[DimensionScore]",
						Initial:         []any{},
						MaterializeFrom: "scoring-node.dimensions_received",
					},
					"summary": {
						Type:            "[DimensionSummary]",
						Initial:         []any{},
						MaterializeFrom: "scoring-node.dimensions_received",
						Project: map[string]any{
							"dimension":  "source.dimension",
							"score":      "source.score",
							"confidence": "policy.projection.default_confidence",
							"source":     "scoring-node",
						},
					},
					"unrelated_invalid_scores": {
						Type:            "[DimensionScore]",
						Initial:         []any{},
						MaterializeFrom: "other-node.missing_buffer",
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scoring-node": {
				StateSchema: runtimecontracts.NodeStateSchema{
					Fields: []runtimecontracts.NodeStateField{{Name: "dimensions_received", Type: "[DimensionScore]"}},
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "dimensions_received"},
					},
				},
			},
			"other-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.unrelated": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "missing_buffer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"vertical_id": {Type: "uuid"},
					"dimension":   {Type: "text"},
					"tier":        {Type: "integer"},
					"score":       {Type: "integer"},
					"evidence":    {Type: "text"},
					"confidence":  {Type: "text"},
				}},
			},
		},
	})
}

func TestExecutor_AccumulatorProjectionMaterializesTypedEntityFieldBeforeEmit(t *testing.T) {
	source := accumulatorProjectionTestSource()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
			OnComplete: []runtimecontracts.HandlerRuleEntry{{
				ID: "complete",
				Emit: runtimecontracts.EmitSpec{
					Event: "vertical.scored",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"scores": runtimecontracts.RefExpression("entity.scores"),
					},
				},
			}},
		},
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "scoring-node",
		Event: eventtest.RootIngress("evt-1",
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	scores, ok := result.StateMutation.Metadata["scores"].([]any)
	if !ok || len(scores) != 1 {
		t.Fatalf("projected scores = %#v", result.StateMutation.Metadata["scores"])
	}
	score, ok := scores[0].(map[string]any)
	if !ok {
		t.Fatalf("projected score item = %#v", scores[0])
	}
	if _, exists := score["event_id"]; exists {
		t.Fatalf("projected score leaked accumulator metadata: %#v", score)
	}
	if _, exists := score["vertical_id"]; exists {
		t.Fatalf("projected score leaked payload extra field: %#v", score)
	}
	if got := score["dimension"]; got != "market" {
		t.Fatalf("projected dimension = %#v", got)
	}
	summaries, ok := result.StateMutation.Metadata["summary"].([]any)
	if !ok || len(summaries) != 1 {
		t.Fatalf("projected summary = %#v", result.StateMutation.Metadata["summary"])
	}
	summary, ok := summaries[0].(map[string]any)
	if !ok {
		t.Fatalf("projected summary item = %#v", summaries[0])
	}
	if got := summary["confidence"]; got != "medium" {
		t.Fatalf("projected summary confidence = %#v", got)
	}
	if got := summary["source"]; got != "scoring-node" {
		t.Fatalf("projected summary literal source = %#v", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	emittedScores, ok := emitted["scores"].([]any)
	if !ok || len(emittedScores) != 1 {
		t.Fatalf("emit payload scores = %#v", emitted["scores"])
	}
}

func TestExecutor_AccumulatorProjectionMaterializesWithoutOnComplete(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		Emit: runtimecontracts.EmitSpec{
			Event: "vertical.scored",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"scores": runtimecontracts.RefExpression("entity.scores"),
			},
		},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	score := requireProjectedScore(t, result, "scores")
	if got := score["dimension"]; got != "market" {
		t.Fatalf("projected dimension = %#v", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	emittedScores, ok := emitted["scores"].([]any)
	if !ok || len(emittedScores) != 1 {
		t.Fatalf("emit payload scores = %#v", emitted["scores"])
	}
}

func TestExecutor_AccumulatorProjectionMaterializesWithRulesBeforeEmitFields(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "metadata.handler_marker",
				Value:       runtimecontracts.LiteralExpression("top-level"),
			}},
		},
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "matched",
			Condition: "else",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "metadata.rule_marker",
					Value:       runtimecontracts.LiteralExpression("rule"),
				}},
			},
			Emit: runtimecontracts.EmitSpec{
				Event: "vertical.scored",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"scores":         runtimecontracts.RefExpression("entity.scores"),
					"handler_marker": runtimecontracts.RefExpression("metadata.handler_marker"),
					"rule_marker":    runtimecontracts.RefExpression("metadata.rule_marker"),
				},
			},
		}},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	requireProjectedScore(t, result, "scores")
	if got := result.RuleID; got != "matched" {
		t.Fatalf("RuleID = %q, want matched", got)
	}
	if got := result.StateMutation.Metadata["handler_marker"]; got != "top-level" {
		t.Fatalf("handler_marker = %#v, want top-level", got)
	}
	if got := result.StateMutation.Metadata["rule_marker"]; got != "rule" {
		t.Fatalf("rule_marker = %#v, want rule", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	if emittedScores, ok := emitted["scores"].([]any); !ok || len(emittedScores) != 1 {
		t.Fatalf("emit payload scores = %#v", emitted["scores"])
	}
	if got := emitted["handler_marker"]; got != "top-level" {
		t.Fatalf("emit handler_marker = %#v, want top-level", got)
	}
	if got := emitted["rule_marker"]; got != "rule" {
		t.Fatalf("emit rule_marker = %#v, want rule", got)
	}
}

func TestExecutor_AccumulatorProjectionMaterializesWhenRulesDoNotMatch(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, stubEvaluator{bools: map[string]bool{
		"payload.score > 100": false,
	}})
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "too-high",
			Condition: "payload.score > 100",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "metadata.rule_marker",
					Value:       runtimecontracts.LiteralExpression("unexpected"),
				}},
			},
		}},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	requireProjectedScore(t, result, "scores")
	if got := strings.TrimSpace(result.RuleID); got != "" {
		t.Fatalf("RuleID = %q, want empty when rules do not match", got)
	}
	if _, ok := result.StateMutation.Metadata["rule_marker"]; ok {
		t.Fatalf("rule_marker unexpectedly written: %#v", result.StateMutation.Metadata)
	}
}

func TestExecutor_AccumulatorProjectionSkipsDeclaredOnCompleteNoMatch(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, stubEvaluator{bools: map[string]bool{
		"payload.score > 100": false,
	}})
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
			OnComplete: []runtimecontracts.HandlerRuleEntry{{
				ID:        "too-high",
				Condition: "payload.score > 100",
				Emit:      runtimecontracts.EmitSpec{Event: "vertical.scored"},
			}},
		},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	requireNoProjectedScores(t, result)
	if got := strings.TrimSpace(result.RuleID); got != "" {
		t.Fatalf("RuleID = %q, want empty when on_complete does not match", got)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("EmitIntents count = %d, want 0", len(result.EmitIntents))
	}
}

func TestExecutor_AccumulatorProjectionSkipsIncompleteAccumulation(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:         "dimensions_received",
			ExpectedFrom: "entity.expected_dimensions",
			ExpectedPath: runtimecontracts.RefExpression("entity.expected_dimensions").RefPath,
			Completion:   runtimecontracts.ParseAccumulateCompletion("all"),
			DedupBy:      "payload.dimension",
			DedupPath:    runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
	}
	state := testStateSnapshot("pending", map[string]any{"expected_dimensions": []any{"market", "risk"}}, nil, map[string]map[string]any{})
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, state)
	if result.Status != OutcomeWaiting {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeWaiting)
	}
	requireNoProjectedScores(t, result)
}

func TestExecutor_AccumulatorProjectionSkipsTimeoutBranch(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:       "dimensions_received",
			Completion: runtimecontracts.ParseAccumulateCompletion("all"),
			OnTimeout: &runtimecontracts.HandlerRuleEntry{
				ID: "timeout",
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
					Writes: []runtimecontracts.WorkflowDataWrite{{
						TargetField: "metadata.timeout_seen",
						Value:       runtimecontracts.LiteralExpression(true),
					}},
				},
			},
		},
	}
	state := testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{})
	storeAccumulatorForBucket(&state, accumulatorBucketRef("scoring-node", "score.dimension_complete"), &Accumulator{
		ExpectedCount: 2,
		Received:      map[string]bool{"market": true},
		Items: []map[string]any{{
			"dimension":  "market",
			"tier":       2,
			"score":      87,
			"evidence":   "strong",
			"confidence": "high",
		}},
	})
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "scoring-node",
		Event: eventtest.RootIngress("timeout-1",
			"accumulate.timeout", "", "", json.RawMessage(`{"timer_handle":{"kind":"accumulation_timeout","bucket":{"node_id":"scoring-node","event_type":"score.dimension_complete"}}}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		HandlerEventKey: "score.dimension_complete",
		Handler:         handler,
		State:           state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.StateMutation.Metadata["timeout_seen"]; got != true {
		t.Fatalf("timeout_seen = %#v, want true", got)
	}
	requireNoProjectedScores(t, result)
}

func TestExecutor_AccumulatorProjectionMaterializesBeforeTopLevelFanOutEmitFields(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "metadata.handler_marker",
				Value:       runtimecontracts.LiteralExpression("top-level"),
			}},
		},
		FanOut: &runtimecontracts.FanOutSpec{
			ItemsFrom: "payload.targets",
			Emit: runtimecontracts.EmitSpec{
				Event: "vertical.scored",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"handler_marker": runtimecontracts.RefExpression("metadata.handler_marker"),
					"scores":         runtimecontracts.RefExpression("entity.scores"),
					"target":         runtimecontracts.RefExpression("fan_out.item"),
				},
			},
		},
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "scoring-node",
		Event: eventtest.RootIngress("evt-1",
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high","targets":["agent-a"]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	requireProjectedScore(t, result, "scores")
	if result.Status != OutcomeFannedOut {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeFannedOut)
	}
	if got := result.StateMutation.Metadata["handler_marker"]; got != "top-level" {
		t.Fatalf("handler_marker state mutation = %#v, want top-level", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	emittedScores, ok := emitted["scores"].([]any)
	if !ok || len(emittedScores) != 1 {
		t.Fatalf("fan_out emit payload scores = %#v", emitted["scores"])
	}
	if got := emitted["handler_marker"]; got != "top-level" {
		t.Fatalf("fan_out emit handler_marker = %#v, want top-level", got)
	}
	if got := emitted["target"]; got != "agent-a" {
		t.Fatalf("fan_out emit target = %#v, want agent-a", got)
	}
}

func newAccumulatorProjectionTestExecutor(t *testing.T, evaluator Evaluator) *Executor {
	t.Helper()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     accumulatorProjectionTestSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, evaluator)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	return exec
}

func executeAccumulatorProjectionTestEvent(t *testing.T, exec *Executor, handler runtimecontracts.SystemNodeEventHandler, state StateSnapshot) ExecutionResult {
	t.Helper()
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "scoring-node",
		Event: eventtest.RootIngress("evt-1",
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: handler,
		State:   state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	return result
}

func requireProjectedScore(t *testing.T, result ExecutionResult, field string) map[string]any {
	t.Helper()
	scores, ok := result.StateMutation.Metadata[field].([]any)
	if !ok || len(scores) != 1 {
		t.Fatalf("projected %s = %#v", field, result.StateMutation.Metadata[field])
	}
	score, ok := scores[0].(map[string]any)
	if !ok {
		t.Fatalf("projected %s item = %#v", field, scores[0])
	}
	if _, exists := score["event_id"]; exists {
		t.Fatalf("projected score leaked accumulator metadata: %#v", score)
	}
	if _, exists := score["vertical_id"]; exists {
		t.Fatalf("projected score leaked payload extra field: %#v", score)
	}
	return score
}

func requireNoProjectedScores(t *testing.T, result ExecutionResult) {
	t.Helper()
	if _, exists := result.StateMutation.Metadata["scores"]; exists {
		t.Fatalf("projected scores unexpectedly present: %#v", result.StateMutation.Metadata["scores"])
	}
}

func TestExecutor_AccumulatorProjectionMaterializesForQualifiedRuntimeEvent(t *testing.T) {
	source := semanticview.Wrap(loadEngineProjectionFlowBundle(t))
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	handler, ok := source.NodeEventHandler("scoring-node", "scoring/score.dimension_complete")
	if !ok {
		t.Fatal("expected qualified runtime event to resolve to authored local handler")
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "scoring-node",
		FlowID:   "scoring",
		Event: eventtest.RootIngress("evt-1",
			"scoring/score.dimension_complete", "", "", json.RawMessage(`{"dimension":"market","score":87}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		HandlerEventKey: "score.dimension_complete",
		Handler:         handler,
		State:           testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if _, ok := loadAccumulatorForBucket(StateSnapshot{StateCarrier: result.StateMutation.StateCarrier}, accumulatorBucketRef("scoring-node", "score.dimension_complete")); !ok {
		t.Fatalf("logical accumulator bucket missing from state mutation: %#v", result.StateMutation.StateCarrier.StateBuckets)
	}
	if _, ok := loadAccumulatorForBucket(StateSnapshot{StateCarrier: result.StateMutation.StateCarrier}, accumulatorBucketRef("scoring-node", "scoring/score.dimension_complete")); ok {
		t.Fatalf("concrete runtime event bucket survived in state mutation: %#v", result.StateMutation.StateCarrier.StateBuckets)
	}
	scores, ok := result.StateMutation.Metadata["scores"].([]any)
	if !ok || len(scores) != 1 {
		t.Fatalf("projected scores = %#v", result.StateMutation.Metadata["scores"])
	}
	score, ok := scores[0].(map[string]any)
	if !ok {
		t.Fatalf("projected score item = %#v", scores[0])
	}
	if got := score["dimension"]; got != "market" {
		t.Fatalf("projected dimension = %#v", got)
	}
	if _, exists := score["event_type"]; exists {
		t.Fatalf("projected score leaked accumulator metadata: %#v", score)
	}
}

func TestExecutor_AccumulatorBucketUsesMatchedHandlerEventKeyForScopedConcreteEvents(t *testing.T) {
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
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			ExpectedFrom: "entity.expected_count",
			Completion:   runtimecontracts.ParseAccumulateCompletion("all"),
			DedupBy:      "payload.component_id",
			DedupPath:    runtimecontracts.RefExpression("payload.component_id").RefPath,
		},
	}
	firstState := testStateSnapshot("pending", map[string]any{"expected_count": 2}, nil, map[string]map[string]any{})
	first, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "lifecycle-orchestrator",
		FlowID:   "operating",
		Event: eventtest.RootIngress(
			"evt-a",
			"component-scaffold/a/component.scaffolded",
			"",
			"",
			json.RawMessage(`{"component_id":"a"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),

		HandlerEventKey: "component.scaffolded",
		Handler:         handler,
		State:           firstState,
	})
	if err != nil {
		t.Fatalf("first Execute error: %v", err)
	}
	if !first.AccumulatorCompletionDiagnostics.Relevant || first.AccumulatorCompletionDiagnostics.CompletionReached {
		t.Fatalf("first diagnostics = %#v, want relevant waiting accumulator", first.AccumulatorCompletionDiagnostics)
	}
	secondState := testStateSnapshot("pending", map[string]any{"expected_count": 2}, nil, first.StateMutation.StateCarrier.StateBuckets)
	second, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "lifecycle-orchestrator",
		FlowID:   "operating",
		Event: eventtest.RootIngress(
			"evt-b",
			"component-scaffold/b/component.scaffolded",
			"",
			"",
			json.RawMessage(`{"component_id":"b"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
			time.Date(2026, time.January, 1, 0, 0, 1, 0, time.UTC),
		),

		HandlerEventKey: "component.scaffolded",
		Handler:         handler,
		State:           secondState,
	})
	if err != nil {
		t.Fatalf("second Execute error: %v", err)
	}
	if !second.AccumulatorCompletionDiagnostics.CompletionReached {
		t.Fatalf("second diagnostics = %#v, want completion reached", second.AccumulatorCompletionDiagnostics)
	}
	state := StateSnapshot{StateCarrier: second.StateMutation.StateCarrier}
	acc, ok := loadAccumulatorForBucket(state, accumulatorBucketRef("lifecycle-orchestrator", "component.scaffolded"))
	if !ok {
		t.Fatalf("logical accumulator bucket missing: %#v", second.StateMutation.StateCarrier.StateBuckets)
	}
	if got := len(acc.Items); got != 2 {
		t.Fatalf("accumulator items = %d, want 2", got)
	}
	if got := acc.Items[0]["event_type"]; got != "component-scaffold/a/component.scaffolded" {
		t.Fatalf("first item event_type = %#v", got)
	}
	if got := acc.Items[1]["event_type"]; got != "component-scaffold/b/component.scaffolded" {
		t.Fatalf("second item event_type = %#v", got)
	}
	if _, ok := loadAccumulatorForBucket(state, accumulatorBucketRef("lifecycle-orchestrator", "component-scaffold/a/component.scaffolded")); ok {
		t.Fatalf("first concrete event bucket survived: %#v", second.StateMutation.StateCarrier.StateBuckets)
	}
	if _, ok := loadAccumulatorForBucket(state, accumulatorBucketRef("lifecycle-orchestrator", "component-scaffold/b/component.scaffolded")); ok {
		t.Fatalf("second concrete event bucket survived: %#v", second.StateMutation.StateCarrier.StateBuckets)
	}
}

func TestExecutor_ComputeReadsAccumulatorByMatchedHandlerEventKey(t *testing.T) {
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
	state := testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{})
	storeAccumulator(&state, "lifecycle-orchestrator", "component.scaffolded", &Accumulator{
		Items: []map[string]any{
			{"component_id": "a"},
			{"component_id": "b"},
		},
	})
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "lifecycle-orchestrator",
		FlowID:   "operating",
		Event: eventtest.RootIngress(
			"evt-b",
			"component-scaffold/b/component.scaffolded",
			"",
			"",
			json.RawMessage(`{"component_id":"b"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
			time.Time{},
		),

		HandlerEventKey: "component.scaffolded",
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpCount,
				StoreAs:   "entity.component_count",
			},
		},
		State: state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.StateMutation.Metadata["component_count"]; got != 2 {
		t.Fatalf("component_count = %#v, want 2", got)
	}
}

func TestExecutor_AccumulatorProjectionFailsClosedWhenDeclaredBindingDoesNotResolveAtRuntime(t *testing.T) {
	source := semanticview.Wrap(loadEngineProjectionFlowBundle(t))
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	handler, ok := source.NodeEventHandler("scoring-node", "scoring/score.dimension_complete")
	if !ok {
		t.Fatal("expected qualified runtime event to resolve to authored local handler")
	}
	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "scoring-node",
		FlowID:   "scoring",
		Event: eventtest.RootIngress("evt-1",
			"scoring/score.unregistered_dimension_complete", "", "", json.RawMessage(`{"dimension":"market","score":87}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "runtime_invariant_violation") {
		t.Fatalf("Execute error = %v, want runtime_invariant_violation", err)
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"items":[{"score":60,"active":true},{"score":40,"active":true},{"score":60,"active":false}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-2", "digest.requested", "", "", json.RawMessage(`{"items":[{"status":"queued"},{"status":"queued"},{"status":"done"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-2", "digest.requested", "", "", json.RawMessage(`{"score":5,"items":[{"score":7},{"score":5}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"score":5,"items":[{"score":7}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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

func TestExecutor_OnSuccessEmitWithMatchedRuleQueuesRuleThenSuccess(t *testing.T) {
	outbox := &recordingEmitOutbox{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        outbox,
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{
				Event: "handler.succeeded",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"audit": runtimecontracts.LiteralExpression("ok"),
				},
			}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit: runtimecontracts.EmitSpec{
					Event: "rule.emitted",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"score": runtimecontracts.RefExpression("payload.score"),
					},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.RuleID; got != "rule-1" {
		t.Fatalf("RuleID = %q, want rule-1", got)
	}
	if got := len(result.EmitIntents); got != 2 {
		t.Fatalf("EmitIntents len = %d, want 2", got)
	}
	if got := []string{string(result.EmitIntents[0].Event.Type()), string(result.EmitIntents[1].Event.Type())}; !reflect.DeepEqual(got, []string{"rule.emitted", "handler.succeeded"}) {
		t.Fatalf("emit order = %#v", got)
	}
	if got := len(outbox.intents); got != 2 {
		t.Fatalf("outbox intents len = %d, want 2", got)
	}
	rulePayload := eventPayloadMap(t, result.EmitIntents[0].Event)
	if got := rulePayload["score"]; got != float64(9) {
		t.Fatalf("rule payload score = %#v, want 9", got)
	}
	successPayload := eventPayloadMap(t, result.EmitIntents[1].Event)
	if got := successPayload["audit"]; got != "ok" {
		t.Fatalf("success payload audit = %#v, want ok", got)
	}
}

func TestExecutor_OnSuccessEmitFiresWhenRulesDoNotMatch(t *testing.T) {
	outbox := &recordingEmitOutbox{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        outbox,
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": false}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":3}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "rule.emitted"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.RuleID; got != "" {
		t.Fatalf("RuleID = %q, want empty on no-match success", got)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents len = %d, want 1", got)
	}
	if got := string(result.EmitIntents[0].Event.Type()); got != "handler.succeeded" {
		t.Fatalf("emit event = %q, want handler.succeeded", got)
	}
	if got := len(outbox.intents); got != 1 {
		t.Fatalf("outbox intents len = %d, want 1", got)
	}
}

func TestExecutor_OnSuccessEmitFailsClosedWhenRuleEventMatchesSuccessEvent(t *testing.T) {
	outbox := &recordingEmitOutbox{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        outbox,
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "shared.event"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "shared.event"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate declarative emit event") {
		t.Fatalf("Execute error = %v, want duplicate declarative emit event", err)
	}
	if got := len(outbox.intents); got != 0 {
		t.Fatalf("outbox intents len = %d, want 0 after duplicate failure", got)
	}
}

func TestExecutor_RejectsOnSuccessEmitWithRuleFanOut(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        stubOutbox{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "else",
				FanOut: &runtimecontracts.FanOutSpec{
					ItemsFrom: "payload.items",
					Emit:      runtimecontracts.EmitSpec{Event: "item.done"},
				},
			}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "rules[0].fan_out") {
		t.Fatalf("ValidateRequest error = %v, want rules[0].fan_out rejection", err)
	}
}

func TestExecutor_OnSuccessSecondEmitFailureDoesNotCommitFirstEmitOrState(t *testing.T) {
	stateRepo := &recordingStateRepo{}
	outbox := &recordingEmitOutbox{}
	shaper := &eventErrPayloadShaper{failEvent: "handler.succeeded"}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stateRepo,
		TxRunner:      stubRunner{},
		Locker:        stubLocker{},
		Outbox:        outbox,
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	_, err = exec.Execute(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		NodeID:     "node-1",
		FlowID:     "flow-1",
		ChainDepth: 1,
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "done",
			OnSuccess:  runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "rule.emitted"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "payload shape failed") {
		t.Fatalf("Execute error = %v, want payload shape failed", err)
	}
	if got := shaper.shaped; !reflect.DeepEqual(got, []string{"rule.emitted", "handler.succeeded"}) {
		t.Fatalf("payload shaper order = %#v", got)
	}
	if got := len(outbox.intents); got != 0 {
		t.Fatalf("outbox intents len = %d, want 0 after second emit failure", got)
	}
	if got := stateRepo.saves; got != 0 {
		t.Fatalf("state saves = %d, want 0 after second emit failure", got)
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"items":["a","b"]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
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
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &payload); err != nil {
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
		Event: eventtest.RootIngress("evt-1",
			"vertical.discovered", "", "", json.RawMessage(`{"mode":"corpus","discovery_context":{"source":"corpus"}}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

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
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &payload); err != nil {
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

func TestExecutor_EmitIntentUsesTargetStateFlowIdentityBeforeInboundSource(t *testing.T) {
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

	const (
		targetEntityID     = "validation-entity"
		targetFlowInstance = "validation/inst-1"
		sourceEntityID     = "scoring-entity"
		sourceFlowInstance = "scoring/inst-1"
	)
	manifestations := []string{
		"validation.started",
		"validation.package_ready",
		"brand.requested",
		"cto.spec_review_requested",
		"spec.revision_requested",
	}
	for _, eventType := range manifestations {
		t.Run(eventType, func(t *testing.T) {
			state := testStateSnapshot("researching", map[string]any{
				"flow_path": targetFlowInstance,
			}, nil, map[string]map[string]any{})
			state.EntityID = identity.NormalizeEntityID(targetEntityID)
			result, err := exec.Execute(context.Background(), ExecutionRequest{
				EntityID: targetEntityID,
				NodeID:   "validation-router",
				FlowID:   "validation",
				Event: eventtest.RootIngress(
					"evt-1",
					"scoring/vertical.resumed",
					"",
					"",
					json.RawMessage(`{"vertical_id":"`+sourceEntityID+`"}`),
					0,
					"",
					"",
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, sourceEntityID), sourceFlowInstance),
					time.Time{},
				),

				Handler: runtimecontracts.SystemNodeEventHandler{
					Emit: runtimecontracts.EmitSpec{
						Event: eventType,
						Fields: map[string]runtimecontracts.ExpressionValue{
							"source_entity_id":     runtimecontracts.CELExpression("event.entity_id"),
							"source_flow_instance": runtimecontracts.CELExpression("event.flow_instance"),
						},
					},
				},
				State: state,
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if got := len(result.EmitIntents); got != 1 {
				t.Fatalf("EmitIntents count = %d, want 1", got)
			}
			emitted := result.EmitIntents[0].Event
			if got := emitted.EntityID(); got != targetEntityID {
				t.Fatalf("emitted entity_id = %q, want target validation entity %q", got, targetEntityID)
			}
			if got := emitted.FlowInstance(); got != targetFlowInstance {
				t.Fatalf("emitted flow_instance = %q, want target validation flow %q", got, targetFlowInstance)
			}
			var payload map[string]any
			if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			if got := payload["source_entity_id"]; got != sourceEntityID {
				t.Fatalf("source_entity_id = %#v, want explicit source entity %q", got, sourceEntityID)
			}
			if got := payload["source_flow_instance"]; got != sourceFlowInstance {
				t.Fatalf("source_flow_instance = %#v, want explicit source flow %q", got, sourceFlowInstance)
			}
		})
	}
}

func TestExecutor_EmitIntentFallsBackToInboundFlowWhenStateFlowPathNormalizesEmpty(t *testing.T) {
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
		FlowID:   "root",
		Event: eventtest.RootIngress(
			"evt-1",
			"root.started",
			"",
			"",
			json.RawMessage(`{}`),
			0,
			"",
			"",
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), "source/inst-1"),
			time.Time{},
		),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "root.done"},
		},
		State: testStateSnapshot("pending", map[string]any{
			"flow_path": "/",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	if got := result.EmitIntents[0].Event.FlowInstance(); got != "source/inst-1" {
		t.Fatalf("emitted flow_instance = %q, want inbound fallback source/inst-1", got)
	}
}

func TestExecutor_DeclarativeEmitSurfacesUseProducerSourceRouteNamespace(t *testing.T) {
	source := sourceWithDeclarativeEmitExternalizationFlows()
	parentRoute := events.RouteIdentity{
		FlowID:       "operating",
		FlowInstance: "operating/opco-1",
		EntityID:     "opco-entity",
	}.Normalized()
	cases := []struct {
		name      string
		eventType string
		payload   json.RawMessage
		handler   runtimecontracts.SystemNodeEventHandler
	}{
		{
			name: "top-level emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				Emit: runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
			},
		},
		{
			name: "rule emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				Rules: []runtimecontracts.HandlerRuleEntry{{
					Condition: "else",
					Emit:      runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
				}},
			},
		},
		{
			name: "on-complete emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					Emit: runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
				}},
			},
		},
		{
			name: "accumulate on-complete emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					Completion: runtimecontracts.ParseAccumulateCompletion("all"),
					OnComplete: []runtimecontracts.HandlerRuleEntry{{
						Emit: runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
					}},
				},
			},
		},
		{
			name:      "accumulate on-timeout emit",
			eventType: "accumulate.timeout",
			payload: json.RawMessage(`{
				"timer_handle": {
					"kind": "accumulation_timeout",
					"bucket": {
						"node_id": "component-node",
						"event_type": "repo-scaffold/repo_scaffold.repo_scaffolded"
					}
				}
			}`),
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					Completion: runtimecontracts.ParseAccumulateCompletion("all"),
					OnTimeout: &runtimecontracts.HandlerRuleEntry{
						Emit: runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
					},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := NewExecutor(RuntimeDependencies{
				Source:     source,
				StateRepo:  stubStateRepo{},
				TxRunner:   stubRunner{},
				Locker:     stubLocker{},
				Outbox:     stubOutbox{},
				Dispatcher: stubDispatcher{},
			}, nil)
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			eventType := tc.eventType
			if eventType == "" {
				eventType = "repo-scaffold/repo_scaffold.repo_scaffolded"
			}
			payload := tc.payload
			if len(payload) == 0 {
				payload = json.RawMessage(`{}`)
			}
			result, err := exec.Execute(context.Background(), ExecutionRequest{
				EntityID: "component-entity",
				NodeID:   "component-node",
				FlowID:   "component-scaffold",
				Event:    eventtest.RootIngress("evt-1", events.EventType(eventType), "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler:  tc.handler,
				State: testStateSnapshot("ready", map[string]any{
					"flow_path":            "component-scaffold/component-1",
					"parent_flow_id":       parentRoute.FlowID,
					"parent_flow_instance": parentRoute.FlowInstance,
					"parent_entity_id":     parentRoute.EntityID,
				}, nil, map[string]map[string]any{}),
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if got := len(result.EmitIntents); got != 1 {
				t.Fatalf("EmitIntents count = %d, want 1", got)
			}
			emitted := result.EmitIntents[0].Event
			if got, want := string(emitted.Type()), "component-scaffold/component-1/component.scaffolded"; got != want {
				t.Fatalf("emitted type = %q, want %q", got, want)
			}
			if got := emitted.SourceRoute().FlowInstance; got != "component-scaffold/component-1" {
				t.Fatalf("source flow_instance = %q, want component-scaffold/component-1", got)
			}
			if got := emitted.TargetRoute().FlowInstance; got != parentRoute.FlowInstance {
				t.Fatalf("target flow_instance = %q, want %s", got, parentRoute.FlowInstance)
			}
		})
	}
}

func TestExecutor_FanOutEmitUsesProducerSourceRouteNamespace(t *testing.T) {
	source := sourceWithDeclarativeEmitExternalizationFlows()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
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
		EntityID: "component-entity",
		NodeID:   "component-node",
		FlowID:   "component-scaffold",
		Event:    eventtest.RootIngress("evt-1", "repo-scaffold/repo_scaffold.repo_scaffolded", "", "", json.RawMessage(`{"items":[{"id":"a"},{"id":"b"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				Emit: runtimecontracts.EmitSpec{
					Event: "component-scaffold/component.scaffolded",
				},
			},
		},
		State: testStateSnapshot("ready", map[string]any{
			"flow_path":            "component-scaffold/component-1",
			"parent_flow_id":       "operating",
			"parent_flow_instance": "operating/opco-1",
			"parent_entity_id":     "opco-entity",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 2 {
		t.Fatalf("EmitIntents count = %d, want 2", got)
	}
	for i, intent := range result.EmitIntents {
		if got, want := string(intent.Event.Type()), "component-scaffold/component-1/component.scaffolded"; got != want {
			t.Fatalf("emit %d type = %q, want %q", i, got, want)
		}
		if got := intent.Event.SourceRoute().FlowInstance; got != "component-scaffold/component-1" {
			t.Fatalf("emit %d source flow_instance = %q, want component-scaffold/component-1", i, got)
		}
	}
}

func TestExecutor_StaticProducerTargetRouteDoesNotOwnEventNamespace(t *testing.T) {
	source := sourceWithDeclarativeEmitExternalizationFlows()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
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
		EntityID: "repo-entity",
		NodeID:   "repo-node",
		FlowID:   "repo-scaffold",
		Event:    eventtest.RootIngress("evt-1", "repo.commit_ready", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{
				Event: "repo-scaffold/repo_scaffold.repo_scaffolded",
				Target: runtimecontracts.EmitTargetSpec{
					Kind:       runtimecontracts.EmitTargetKindInstanceID,
					Flow:       "component-scaffold",
					InstanceID: "component-1",
				},
			},
		},
		State: testStateSnapshot("ready", map[string]any{
			"flow_path": "repo-scaffold",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	emitted := result.EmitIntents[0].Event
	if got, want := string(emitted.Type()), "repo-scaffold/repo_scaffold.repo_scaffolded"; got != want {
		t.Fatalf("emitted type = %q, want %q", got, want)
	}
	if got := emitted.SourceRoute().FlowInstance; got != "repo-scaffold" {
		t.Fatalf("source flow_instance = %q, want repo-scaffold", got)
	}
	if got := emitted.TargetRoute().FlowInstance; got != "component-scaffold/component-1" {
		t.Fatalf("target flow_instance = %q, want component-scaffold/component-1", got)
	}
	if got := emitted.FlowInstance(); got != "component-scaffold/component-1" {
		t.Fatalf("legacy flow_instance projection = %q, want target component-scaffold/component-1", got)
	}
}

func TestExecutor_ChildPinOutputTargetsStoredParentRoute(t *testing.T) {
	source := sourceWithChildOutputPin()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     source,
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	parentRoute := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: "root/inst-1",
		EntityID:     "parent-ent",
	}
	state := testStateSnapshot("running", map[string]any{
		"flow_path":            "child/inst-1",
		"parent_flow_id":       parentRoute.FlowID,
		"parent_flow_instance": parentRoute.FlowInstance,
		"parent_entity_id":     parentRoute.EntityID,
	}, nil, map[string]map[string]any{})
	state.EntityID = identity.NormalizeEntityID("child-ent")

	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "child-ent",
		NodeID:   "child-node",
		FlowID:   "child",
		Event: eventtest.RootIngress(
			"evt-1",
			"child/requested",
			"",
			"",
			json.RawMessage(`{}`),
			0,
			"",
			"",
			events.EnvelopeForSourceRoute(events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "wrong-parent"), "wrong/root"), events.RouteIdentity{
				FlowID:       "wrong",
				FlowInstance: "wrong/root",
				EntityID:     "wrong-parent",
			}),
			time.Time{},
		),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "child.done"},
		},
		State: state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	if got := result.EmitIntents[0].Event.TargetRoute(); got != parentRoute {
		t.Fatalf("target route = %#v, want %#v", got, parentRoute)
	}
}

func sourceWithChildOutputPin() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "child",
			Flow: "child",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"child.done"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.done": {},
		},
		Path: "child",
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{child},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &child,
			},
		},
	})
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"summary":"ready"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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

func TestExecutor_DataAccumulationAppliesTypedContainedOperations(t *testing.T) {
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
		Event: eventtest.RootIngress(
			"evt-1",
			"job.received",
			"",
			"",
			json.RawMessage(`{"vertical_id":"north","job":{"id":"job-1","title":"Build"}}`),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{
						Operation: runtimecontracts.WorkflowDataOperationSet,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.LiteralExpression("north"),
						Value: runtimecontracts.LiteralExpression(map[string]any{
							"status":      "active",
							"active_jobs": []any{},
						}),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationMerge,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.LiteralExpression("north"),
						Value: runtimecontracts.LiteralExpression(map[string]any{
							"status": "busy",
						}),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationAppend,
						TargetRef: "entity.verticals.active_jobs",
						Key:       runtimecontracts.RefExpression("payload.vertical_id"),
						Value:     runtimecontracts.RefExpression("payload.job"),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationUpdate,
						TargetRef: "entity.tags",
						Index:     runtimecontracts.LiteralExpression(1),
						Value:     runtimecontracts.LiteralExpression("gold"),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationAppend,
						TargetRef: "entity.tags",
						Value:     runtimecontracts.LiteralExpression("vip"),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationDelete,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.LiteralExpression("obsolete"),
					},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"verticals": map[string]any{
				"obsolete": map[string]any{"status": "old", "active_jobs": []any{}},
			},
			"tags": []any{"new", "silver"},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	verticals, ok := result.StateMutation.Metadata["verticals"].(map[string]any)
	if !ok {
		t.Fatalf("verticals = %#v", result.StateMutation.Metadata["verticals"])
	}
	if _, exists := verticals["obsolete"]; exists {
		t.Fatalf("obsolete key survived delete: %#v", verticals)
	}
	north, ok := verticals["north"].(map[string]any)
	if !ok {
		t.Fatalf("verticals.north = %#v", verticals["north"])
	}
	if got := north["status"]; got != "busy" {
		t.Fatalf("verticals.north.status = %#v, want busy", got)
	}
	jobs, ok := north["active_jobs"].([]any)
	if !ok || len(jobs) != 1 {
		t.Fatalf("verticals.north.active_jobs = %#v", north["active_jobs"])
	}
	job, ok := jobs[0].(map[string]any)
	if !ok || job["id"] != "job-1" || job["title"] != "Build" {
		t.Fatalf("active job = %#v", jobs[0])
	}
	if !reflect.DeepEqual(result.StateMutation.Metadata["tags"], []any{"new", "gold", "vip"}) {
		t.Fatalf("tags = %#v", result.StateMutation.Metadata["tags"])
	}
}

func TestExecutor_SingletonCoordinatorAppliesContainedStateThroughLoadedContract(t *testing.T) {
	bundle := loadEngineSingletonCoordinatorFlowBundle(t)
	if _, err := bundle.ResolveFlowSingletonCoordinator("coordinator"); err != nil {
		t.Fatalf("ResolveFlowSingletonCoordinator: %v", err)
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     semanticview.Wrap(bundle),
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
		EntityID: "coordinator-1",
		NodeID:   "coordinator-node",
		FlowID:   "coordinator",
		Event: eventtest.RootIngress(
			"evt-1",
			"job.received",
			"",
			"",
			json.RawMessage(`{"vertical_id":"north","job":{"id":"job-1","title":"Build"}}`),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{
						Operation: runtimecontracts.WorkflowDataOperationSet,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.RefExpression("payload.vertical_id"),
						Value: runtimecontracts.LiteralExpression(map[string]any{
							"status":      "active",
							"active_jobs": []any{},
						}),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationAppend,
						TargetRef: "entity.verticals.active_jobs",
						Key:       runtimecontracts.RefExpression("payload.vertical_id"),
						Value:     runtimecontracts.RefExpression("payload.job"),
					},
				},
			},
		},
		State: testStateSnapshot("active", map[string]any{
			"verticals": map[string]any{},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	verticals, ok := result.StateMutation.Metadata["verticals"].(map[string]any)
	if !ok {
		t.Fatalf("verticals = %#v", result.StateMutation.Metadata["verticals"])
	}
	north, ok := verticals["north"].(map[string]any)
	if !ok {
		t.Fatalf("verticals.north = %#v", verticals["north"])
	}
	if got := north["status"]; got != "active" {
		t.Fatalf("verticals.north.status = %#v, want active", got)
	}
	jobs, ok := north["active_jobs"].([]any)
	if !ok || len(jobs) != 1 {
		t.Fatalf("verticals.north.active_jobs = %#v", north["active_jobs"])
	}
	job, ok := jobs[0].(map[string]any)
	if !ok || job["id"] != "job-1" || job["title"] != "Build" {
		t.Fatalf("active job = %#v", jobs[0])
	}
}

func TestExecutor_DataAccumulationContainedOperationRejectsMissingMapKey(t *testing.T) {
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
		Event:    eventtest.RootIngress("evt-1", "job.received", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					Operation: runtimecontracts.WorkflowDataOperationDelete,
					TargetRef: "entity.verticals",
					Key:       runtimecontracts.LiteralExpression("missing"),
				}},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"verticals": map[string]any{},
		}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected missing map key failure")
	}
	if !strings.Contains(err.Error(), `map key "missing" does not exist`) {
		t.Fatalf("error = %v, want missing-key context", err)
	}
}

func TestExecutor_DataAccumulationRejectsContainedSetOrMergeIndex(t *testing.T) {
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
	tests := []struct {
		name string
		op   runtimecontracts.WorkflowDataOperation
	}{
		{name: "set", op: runtimecontracts.WorkflowDataOperationSet},
		{name: "merge", op: runtimecontracts.WorkflowDataOperationMerge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), ExecutionRequest{
				EntityID: "entity-1",
				NodeID:   "node-1",
				Event:    eventtest.RootIngress("evt-1", "job.received", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler: runtimecontracts.SystemNodeEventHandler{
					DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
						Writes: []runtimecontracts.WorkflowDataWrite{{
							Operation: tc.op,
							TargetRef: "entity.verticals",
							Key:       runtimecontracts.LiteralExpression("north"),
							Index:     runtimecontracts.LiteralExpression(0),
							Value: runtimecontracts.LiteralExpression(map[string]any{
								"status": "active",
							}),
						}},
					},
				},
				State: testStateSnapshot("pending", map[string]any{
					"verticals": map[string]any{},
				}, nil, map[string]map[string]any{}),
			})
			if err == nil {
				t.Fatal("expected contained operation index rejection")
			}
			if !strings.Contains(err.Error(), "must not declare index") {
				t.Fatalf("error = %v, want index rejection", err)
			}
		})
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"entity.analysis.summary"}},
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event: eventtest.RootIngress("evt-1",
			"vertical.discovered", "", "", json.RawMessage(`{"mode":"corpus"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"items":[]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"items":[]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:      eventtest.RootIngress("evt-1", "batch.submitted", "", "", json.RawMessage(`{"items":[{"kind":"a"},{"kind":"b"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
	if got := string(result.EmitIntents[0].Event.Type()); got != "routed.item" {
		t.Fatalf("first emit type = %q", got)
	}
	if got := string(result.EmitIntents[1].Event.Type()); got != "routed.item" {
		t.Fatalf("second emit type = %q", got)
	}
	if !result.EmitIntents[1].Event.CreatedAt().After(result.EmitIntents[0].Event.CreatedAt()) {
		t.Fatalf("emit CreatedAt ordering = [%s, %s]", result.EmitIntents[0].Event.CreatedAt(), result.EmitIntents[1].Event.CreatedAt())
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
		Event:    eventtest.RootIngress("evt-1", "check.requested", "", "", json.RawMessage(`{"score":50}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"items":[{"name":"a","category":"x"},{"name":"b","category":"y"},{"name":"c","category":"x"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"category":"payload","items":[{"name":"a","category":"x"},{"name":"b","category":"y"},{"name":"c","category":"x"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "action.emitted" {
		t.Fatalf("unexpected action emit intents: %#v", result.EmitIntents)
	}
	if got := shaper.lastPayload["score"]; got != float64(9) {
		t.Fatalf("action emit payload score = %#v, want 9", got)
	}
	if shaper.lastSurface != EmitSurfaceAction {
		t.Fatalf("action emit surface = %q, want %q", shaper.lastSurface, EmitSurfaceAction)
	}
}

func TestExecutor_RuleActionRunsOnlyForSelectedRule(t *testing.T) {
	runner := &stubActionRunner{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("auto_action"): {
				Key: identity.NormalizeActionKey("auto_action"),
			},
			identity.NormalizeActionKey("human_action"): {
				Key: identity.NormalizeActionKey("human_action"),
			},
		}},
		ActionRunner: runner,
	}, stubEvaluator{bools: map[string]bool{
		"payload.amount < 100":  false,
		"payload.amount >= 100": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-1", "refund.requested", "", "", json.RawMessage(`{"amount":250}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "auto",
					Condition: "payload.amount < 100",
					Action:    runtimecontracts.ActionSpec{ID: "auto_action"},
				},
				{
					ID:        "needs-human",
					Condition: "payload.amount >= 100",
					Action:    runtimecontracts.ActionSpec{ID: "human_action"},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.RuleID; got != "needs-human" {
		t.Fatalf("RuleID = %q, want needs-human", got)
	}
	if got := runner.called; !reflect.DeepEqual(got, []string{"human_action"}) {
		t.Fatalf("action runner calls = %#v, want only selected rule action", got)
	}
	if got := result.ActionsExecuted; !reflect.DeepEqual(got, []string{"human_action"}) {
		t.Fatalf("ActionsExecuted = %#v, want only selected rule action", got)
	}
}

func TestExecutor_RejectsAmbiguousHandlerTopLevelActionWithRules(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:     stubSource(),
		StateRepo:  stubStateRepo{},
		TxRunner:   stubRunner{},
		Locker:     stubLocker{},
		Outbox:     stubOutbox{},
		Dispatcher: stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("handler_action"): {
				Key: identity.NormalizeActionKey("handler_action"),
			},
		}},
		ActionRunner: &stubActionRunner{},
	}, stubEvaluator{bools: map[string]bool{"payload.amount >= 100": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.Execute(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		NodeID:   "node-1",
		FlowID:   "flow-1",
		Event:    eventtest.RootIngress("evt-1", "refund.requested", "", "", json.RawMessage(`{"amount":250}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Action: runtimecontracts.ActionSpec{ID: "handler_action"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "needs-human",
				Condition: "payload.amount >= 100",
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatalf("expected ambiguous handler-level action config to be rejected, got %+v", result)
	}
	if !strings.Contains(err.Error(), "handler-top-level action is only allowed on handlers without rules") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutor_RejectsUnsupportedRuleActionContextsBeforeExecution(t *testing.T) {
	cases := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		want    string
	}{
		{
			name: "on_complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					ID:        "complete",
					Condition: "else",
					Action:    runtimecontracts.ActionSpec{ID: "notify"},
				}},
			},
			want: "handler.on_complete[complete] action is unsupported",
		},
		{
			name: "accumulate on_complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					Completion: runtimecontracts.ParseAccumulateCompletion("all"),
					OnComplete: []runtimecontracts.HandlerRuleEntry{{
						ID:        "complete",
						Condition: "else",
						Action:    runtimecontracts.ActionSpec{ID: "notify"},
					}},
				},
			},
			want: "handler.accumulate.on_complete[complete] action is unsupported",
		},
		{
			name: "accumulate on_timeout",
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					Completion: runtimecontracts.ParseAccumulateCompletion("all"),
					OnTimeout: &runtimecontracts.HandlerRuleEntry{
						ID:     "timeout",
						Action: runtimecontracts.ActionSpec{ID: "notify"},
					},
				},
			},
			want: "handler.accumulate.on_timeout[timeout] action is unsupported",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
						Key: identity.NormalizeActionKey("notify"),
					},
				}},
				ActionRunner: runner,
			}, stubEvaluator{bools: map[string]bool{"payload.ok": true}})
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			result, err := exec.Execute(context.Background(), ExecutionRequest{
				EntityID: "entity-1",
				NodeID:   "node-1",
				FlowID:   "flow-1",
				Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler:  tc.handler,
				State:    testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if err == nil {
				t.Fatalf("expected unsupported action context rejection, got result %+v", result)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if len(runner.called) != 0 {
				t.Fatalf("action runner calls = %#v, want none", runner.called)
			}
			if len(result.ActionsExecuted) != 0 {
				t.Fatalf("ActionsExecuted = %#v, want none", result.ActionsExecuted)
			}
		})
	}
}

func TestSelectedActionSpecConsumesRuleActionOnlyFromHandlerRules(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{Action: runtimecontracts.ActionSpec{ID: "handler_action"}}
	rule := &runtimecontracts.HandlerRuleEntry{Action: runtimecontracts.ActionSpec{ID: "rule_action"}}
	cases := []struct {
		name   string
		source handlerRuleSource
		want   string
	}{
		{name: "handler rules", source: handlerRuleSourceRules, want: "rule_action"},
		{name: "on complete", source: handlerRuleSourceOnComplete, want: "handler_action"},
		{name: "accumulate on complete", source: handlerRuleSourceAccumulateOnComplete, want: "handler_action"},
		{name: "accumulate on timeout", source: handlerRuleSourceAccumulateOnTimeout, want: "handler_action"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := selectedActionSpec(handler, rule, tc.source).ID; got != tc.want {
				t.Fatalf("selectedActionSpec = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExecutor_MergePersistedActionStatePreservesInMemoryWrites(t *testing.T) {
	entityID := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	baseline := testStateSnapshot("ready", map[string]any{
		"same":           "unchanged",
		"in_memory_only": "frame-write",
	}, map[string]bool{
		"g_frame": true,
	}, map[string]map[string]any{
		"bucket": {"in_memory_only": "frame-write"},
	})
	persisted := testStateSnapshot("ready", map[string]any{
		"same":          "unchanged",
		"action_output": "persisted-output",
	}, nil, map[string]map[string]any{
		"bucket": {"action_output": "persisted-output"},
	})
	exec := &Executor{deps: RuntimeDependencies{StateRepo: actionMergeStateRepo{snapshot: persisted}}}
	frame := &executionFrame{
		tx:  stubTx{ctx: context.Background()},
		req: ExecutionRequest{EntityID: entityID},
		state: ExecutionState{
			State: baseline,
		},
	}

	if err := exec.mergePersistedActionState(frame, baseline); err != nil {
		t.Fatalf("mergePersistedActionState: %v", err)
	}

	if got := frame.state.State.StateCarrier.Metadata["in_memory_only"]; got != "frame-write" {
		t.Fatalf("in_memory_only = %#v, want preserved frame-write", got)
	}
	if got := frame.state.State.StateCarrier.Metadata["action_output"]; got != "persisted-output" {
		t.Fatalf("action_output = %#v, want persisted-output", got)
	}
	if got := frame.state.State.StateCarrier.StateBuckets["bucket"]["in_memory_only"]; got != "frame-write" {
		t.Fatalf("bucket in_memory_only = %#v, want preserved frame-write", got)
	}
	if got := frame.state.State.StateCarrier.StateBuckets["bucket"]["action_output"]; got != "persisted-output" {
		t.Fatalf("bucket action_output = %#v, want persisted-output", got)
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
		Event:    eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"ok":false}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
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
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "guard.failed" {
		t.Fatalf("unexpected escalation intents: %#v", result.EmitIntents)
	}
	if result.ChainDepth != 2 {
		t.Fatalf("ChainDepth = %d", result.ChainDepth)
	}
	if len(shaper.lastPayload) != 0 {
		t.Fatalf("guard escalation payload = %#v, want empty explicit business payload", shaper.lastPayload)
	}
}

func TestExecutor_GuardOnFailEscalateObjectFieldsShapeExplicitPayload(t *testing.T) {
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
		Event:      eventtest.RootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"ok":false,"score":42,"legacy":"should-not-pass"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check: "payload.ok == true",
				OnFailSpec: runtimecontracts.GuardFailureSpec{
					Action: runtimecontracts.GuardFailureActionEscalate,
					Escalation: runtimecontracts.EmitSpec{
						Event: "guard.failed",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"score":  runtimecontracts.CELExpression("payload.score"),
							"reason": runtimecontracts.CELExpression(`"score_below_threshold"`),
						},
					},
				},
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
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "guard.failed" {
		t.Fatalf("unexpected escalation intents: %#v", result.EmitIntents)
	}
	if got := asInt(shaper.lastPayload["score"]); got != 42 {
		t.Fatalf("guard escalation score payload = %#v, want 42", shaper.lastPayload["score"])
	}
	if got := shaper.lastPayload["reason"]; got != "score_below_threshold" {
		t.Fatalf("guard escalation reason payload = %#v, want score_below_threshold", got)
	}
	if _, ok := shaper.lastPayload["legacy"]; ok {
		t.Fatalf("guard escalation leaked unmapped trigger payload: %#v", shaper.lastPayload)
	}
}

func loadEngineProjectionFlowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: projection-flow
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: projection-flow\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
initial_state: pending
states: [pending, scored]
terminal_states: [scored]
pins:
  inputs:
    events:
      - score.dimension_complete
  outputs:
    events:
      - vertical.scored
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "types.yaml"), `
types:
  DimensionScore:
    dimension: text
    score: integer
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), `
vertical:
  scores:
    type: "[DimensionScore]"
    initial: []
    materialize_from: scoring-node.dimensions_received
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), `
score.dimension_complete:
  dimension: text
  score: integer
vertical.scored:
  scores: "[DimensionScore]"
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), `
scoring-node:
  id: scoring-node
  execution_type: system_node
  event_handlers:
    score.dimension_complete:
      accumulate:
        into: dimensions_received
        completion: all
        dedup_by: payload.dimension
        on_complete:
          - id: complete
            emit:
              event: vertical.scored
              broadcast: true
              fields:
                scores: entity.scores
  state_schema:
    fields:
      dimensions_received: "[DimensionScore]"
`)

	repoRoot := repoRootForEngineProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadEngineSingletonCoordinatorFlowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: singleton-coordinator-runtime
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: coordinator
    flow: coordinator
    mode: singleton
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: singleton-coordinator-runtime\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "schema.yaml"), `
name: coordinator
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - job.received
  outputs:
    events: []
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "types.yaml"), `
types:
  VerticalState:
    status: text
    active_jobs: "[Job]"
  Job:
    id: text
    title: text
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "entities.yaml"), `
coordinator_state:
  verticals: map[text]VerticalState
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "events.yaml"), `
job.received:
  vertical_id: text
  job: Job
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "nodes.yaml"), `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  event_handlers: {}
`)

	repoRoot := repoRootForEngineProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writeEngineProjectionFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(content, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func repoRootForEngineProjectionTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
