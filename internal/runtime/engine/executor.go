package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/accprojection"
	runtimeaccumulator "github.com/division-sh/swarm/internal/runtime/accumulator"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type Step string

const (
	StepLoop       Step = "loop"
	StepQuery      Step = "query"
	StepClearGates Step = "clear_gates"
	StepGuard      Step = "guard"
	StepJoin       Step = "join"
	StepAccumulate Step = "accumulate"
	StepFilter     Step = "filter"
	StepGroupBy    Step = "group_by"
	StepReduce     Step = "reduce"
	StepCount      Step = "count"
	StepCompute    Step = "compute"
	StepFanOut     Step = "fan_out"
	StepOnComplete Step = "on_complete"
	StepRules      Step = "rules"
	StepAdvancesTo Step = "advances_to"
	StepSetsGate   Step = "sets_gate"
	StepDataWrites Step = "data_writes"
	StepProjection Step = "projection"
	StepTransform  Step = "transform"
	StepEmits      Step = "emits"
	StepAction     Step = "action"
	StepActivity   Step = "activity"
	StepClear      Step = "clear"
)

var OrderedSteps = []Step{
	StepLoop,
	StepQuery,
	StepClearGates,
	StepGuard,
	StepJoin,
	StepAccumulate,
	StepFilter,
	StepGroupBy,
	StepReduce,
	StepCount,
	StepCompute,
	StepFanOut,
	StepOnComplete,
	StepRules,
	StepAdvancesTo,
	StepSetsGate,
	StepDataWrites,
	StepProjection,
	StepTransform,
	StepEmits,
	StepAction,
	StepActivity,
	StepClear,
}

type Executor struct {
	deps      RuntimeDependencies
	evaluator Evaluator
}

type executionFrame struct {
	tx                        Tx
	req                       ExecutionRequest
	base                      BaseContext
	state                     ExecutionState
	result                    ExecutionResult
	rule                      *runtimecontracts.HandlerRuleEntry
	ruleSource                handlerRuleSource
	payload                   map[string]any
	accumulatorBucketRef      timeridentity.AccumulatorBucketRef
	hasAccumulatorBucketRef   bool
	topLevelDataAccumulation  runtimecontracts.WorkflowDataAccumulation
	topLevelDataWritesApplied bool
	ruleDataWritesApplied     bool
	projectionApplied         bool
	transitionApplied         bool
	lastEmitCreatedAt         time.Time
	joinResultType            runtimecontracts.CatalogTypeReference
	loopPlan                  *runtimecontracts.WorkflowLoopPlan
	loopActivation            *loopruntime.Activation
}

type handlerRuleSource string

const (
	handlerRuleSourceNone           handlerRuleSource = ""
	handlerRuleSourceRules          handlerRuleSource = "handler.rules"
	handlerRuleSourceOnComplete     handlerRuleSource = "handler.on_complete"
	handlerRuleSourceJoinOnComplete handlerRuleSource = "handler.join.on_complete"
	handlerRuleSourceJoinTimeout    handlerRuleSource = "handler.join.timeout"
)

type contextTx struct {
	Tx
	ctx context.Context
}

func (tx contextTx) Context() context.Context {
	return tx.ctx
}

func NewExecutor(deps RuntimeDependencies, evaluator Evaluator) (*Executor, error) {
	if deps.Source == nil {
		return nil, ErrMissingSemanticSource
	}
	if deps.StateRepo == nil {
		return nil, ErrMissingStateRepo
	}
	if deps.TxRunner == nil {
		return nil, ErrMissingTransaction
	}
	if deps.Locker == nil {
		return nil, ErrMissingEntityLocker
	}
	if deps.Outbox == nil {
		return nil, ErrMissingOutbox
	}
	if deps.Dispatcher == nil {
		return nil, ErrMissingDispatcher
	}
	if deps.MaxChainDepth <= 0 {
		deps.MaxChainDepth = DefaultMaxChainDepth
	}
	if evaluator == nil {
		evaluator = NoopEvaluator{}
	}
	return &Executor{deps: deps, evaluator: evaluator}, nil
}

func (e *Executor) MaxChainDepth() int {
	if e == nil || e.deps.MaxChainDepth <= 0 {
		return DefaultMaxChainDepth
	}
	return e.deps.MaxChainDepth
}

func (e *Executor) Steps() []Step {
	return append([]Step(nil), OrderedSteps...)
}

func (e *Executor) ValidateRequest(req ExecutionRequest) error {
	if handlerDeclaresConflictingCompletion(req.Handler) {
		return fmt.Errorf("%w: handler declares both on_complete and rules", ErrInvalidConfig)
	}
	if err := runtimecontracts.HandlerEmitSiteOwnershipError(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if runtimecontracts.HandlerHasAmbiguousTopLevelAction(req.Handler) {
		return fmt.Errorf("%w: handler-top-level action is only allowed on handlers without rules", ErrInvalidConfig)
	}
	if err := validateUnsupportedRuleActions(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := validateHandlerActivityRuntime(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if req.Handler.CreateEntity && req.Handler.Accumulate != nil {
		return fmt.Errorf("%w: handler declares both create_entity and accumulate", ErrInvalidConfig)
	}
	if req.Handler.Join != nil && req.Handler.Accumulate != nil {
		return fmt.Errorf("%w: handler declares both join and accumulate", ErrInvalidConfig)
	}
	if err := runtimecontracts.ValidateJoinHandlerIsolation(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := runtimecontracts.ValidateAccumulateHandlerIsolation(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := validateHandlerLoopRuntime(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if req.Handler.CreateEntity && req.Handler.SelectEntity != nil && !req.Handler.SelectEntity.Empty() {
		return fmt.Errorf("%w: handler declares both create_entity and select_entity", ErrInvalidConfig)
	}
	if req.Handler.CreateEntity && req.Handler.SelectOrCreateEntity != nil && !req.Handler.SelectOrCreateEntity.Empty() {
		return fmt.Errorf("%w: handler declares both create_entity and select_or_create_entity", ErrInvalidConfig)
	}
	if req.Handler.SelectEntity != nil && !req.Handler.SelectEntity.Empty() && req.Handler.SelectOrCreateEntity != nil && !req.Handler.SelectOrCreateEntity.Empty() {
		return fmt.Errorf("%w: handler declares both select_entity and select_or_create_entity", ErrInvalidConfig)
	}
	if err := validateHandlerComputeSpecs(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := validateHandlerEntityWriteTargets(e.deps.Source, req.FlowID.String(), req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	return nil
}

func validateUnsupportedRuleActions(handler runtimecontracts.SystemNodeEventHandler) error {
	validateRule := func(context string, rule runtimecontracts.HandlerRuleEntry) error {
		if strings.TrimSpace(rule.Action.ID) == "" {
			return nil
		}
		return fmt.Errorf("%s action is unsupported; action is only allowed in handler.rules[*]", context)
	}
	for idx, rule := range handler.OnComplete {
		if err := validateRule(handlerRuleContext("handler.on_complete", idx, rule.ID), rule); err != nil {
			return err
		}
	}
	return nil
}

func validateHandlerActivityRuntime(handler runtimecontracts.SystemNodeEventHandler) error {
	hasTopLevelActivity := !handler.Activity.Empty()
	hasRuleActivity := false
	for _, rule := range handler.Rules {
		if !rule.Activity.Empty() {
			hasRuleActivity = true
			break
		}
	}
	if hasTopLevelActivity {
		if len(handler.Rules) > 0 {
			return fmt.Errorf("handler-level activity is only allowed on handlers without rules")
		}
		if strings.TrimSpace(handler.Action.ID) != "" {
			return fmt.Errorf("activity and action are mutually exclusive")
		}
		if !handler.Emit.Empty() || !handler.OnSuccess.Empty() {
			return fmt.Errorf("activity and authored emit/on_success emit are mutually exclusive in Stage 1")
		}
	}
	if hasRuleActivity {
		if hasTopLevelActivity {
			return fmt.Errorf("handler-level activity cannot be combined with rule activities")
		}
		if !handler.Emit.Empty() || !handler.OnSuccess.Empty() {
			return fmt.Errorf("rule activity and handler emit/on_success emit are mutually exclusive in Stage 1")
		}
		for idx, rule := range handler.Rules {
			if rule.Activity.Empty() {
				continue
			}
			if strings.TrimSpace(rule.Action.ID) != "" {
				return fmt.Errorf("handler.rules[%d] activity and action are mutually exclusive", idx)
			}
			if !rule.Emit.Empty() || (rule.FanOut != nil && !rule.FanOut.Emit.Empty()) {
				return fmt.Errorf("handler.rules[%d] activity and authored emit/fan_out emit are mutually exclusive in Stage 1", idx)
			}
		}
	}
	for idx, rule := range handler.OnComplete {
		if !rule.Activity.Empty() {
			return fmt.Errorf("handler.on_complete[%d].activity is not supported in Stage 1", idx)
		}
	}
	return nil
}

func handlerRuleContext(prefix string, idx int, id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return fmt.Sprintf("%s[%s]", prefix, id)
	}
	return fmt.Sprintf("%s[%d]", prefix, idx)
}

func validateHandlerEntityWriteTargets(source semanticview.Source, flowID string, handler runtimecontracts.SystemNodeEventHandler) error {
	validateTarget := func(kind, target string) error {
		target = strings.TrimSpace(target)
		if target == "" {
			return nil
		}
		contract, resolved, entityTarget, err := resolveHandlerEntityWriteTarget(source, flowID, target)
		if err != nil {
			return fmt.Errorf("%s target %q: %w", kind, target, err)
		}
		if !entityTarget {
			return nil
		}
		if strings.Contains(resolved.Path, "[") || strings.Contains(resolved.Path, "]") {
			return fmt.Errorf("%s target %q: list index writes are not supported", kind, target)
		}
		if resolved.Nested && contract.Entity.Fields == nil {
			return fmt.Errorf("%s target %q: entity contract is unavailable", kind, target)
		}
		return nil
	}
	validateWrites := func(kind string, writes []runtimecontracts.WorkflowDataWrite) error {
		for _, write := range writes {
			if write.IsContainedOperation() {
				contract, ok := entityruntime.ResolveForFlow(source, flowID)
				if !ok {
					return fmt.Errorf("%s target %q: flow %s has no declared entity contract", kind, write.Target(), strings.TrimSpace(flowID))
				}
				if _, err := entityruntime.ResolveContainedOperationTarget(contract, write.Target(), string(write.Operation), !write.Key.IsZero(), !write.Index.IsZero()); err != nil {
					return fmt.Errorf("%s target %q: %w", kind, write.Target(), err)
				}
				continue
			}
			if err := validateTarget(kind, write.Target()); err != nil {
				return err
			}
		}
		return nil
	}
	validateRule := func(kind string, rule runtimecontracts.HandlerRuleEntry) error {
		if err := validateWrites(kind+".data_accumulation", rule.DataAccumulation.Writes); err != nil {
			return err
		}
		if rule.Compute != nil {
			if err := validateTarget(kind+".compute", rule.Compute.StoreAs); err != nil {
				return err
			}
		}
		return nil
	}
	var validateQuery func(kind string, query *runtimecontracts.QuerySpec) error
	validateQuery = func(kind string, query *runtimecontracts.QuerySpec) error {
		if query == nil {
			return nil
		}
		if err := validateTarget(kind+".query", query.StoreAs); err != nil {
			return err
		}
		for i := range query.Queries {
			if err := validateQuery(kind+".query", &query.Queries[i]); err != nil {
				return err
			}
		}
		return nil
	}

	if err := validateQuery("handler", handler.Query); err != nil {
		return err
	}
	if err := validateWrites("handler.data_accumulation", handler.DataAccumulation.Writes); err != nil {
		return err
	}
	if handler.Compute != nil {
		if err := validateTarget("handler.compute", handler.Compute.StoreAs); err != nil {
			return err
		}
	}
	if handler.Filter != nil {
		if err := validateTarget("handler.filter", handler.Filter.StoreAs); err != nil {
			return err
		}
	}
	if handler.GroupBy != nil {
		if err := validateTarget("handler.group_by", handler.GroupBy.StoreAs); err != nil {
			return err
		}
	}
	if handler.Reduce != nil {
		if err := validateTarget("handler.reduce", handler.Reduce.StoreAs); err != nil {
			return err
		}
	}
	if handler.Count != nil {
		if err := validateTarget("handler.count", handler.Count.StoreAs); err != nil {
			return err
		}
	}
	if handler.Clear != nil {
		for _, target := range handler.Clear.Targets {
			if err := validateHandlerClearTarget(source, flowID, target); err != nil {
				return err
			}
		}
	}
	for _, rule := range handler.Rules {
		if err := validateRule("handler.rule", rule); err != nil {
			return err
		}
	}
	for _, rule := range handler.OnComplete {
		if err := validateRule("handler.on_complete", rule); err != nil {
			return err
		}
	}
	return nil
}

func validateHandlerClearTarget(source semanticview.Source, flowID, target string) error {
	target = strings.TrimSpace(target)
	if target == "" || specialHandlerClearTarget(target) {
		return nil
	}
	_, _, _, err := resolveHandlerEntityWriteTarget(source, flowID, target)
	if err != nil {
		return fmt.Errorf("handler.clear target %q is invalid: %w", target, err)
	}
	return nil
}

func resolveHandlerEntityWriteTarget(source semanticview.Source, flowID, target string) (entityruntime.Contract, entityruntime.WriteTarget, bool, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return entityruntime.Contract{}, entityruntime.WriteTarget{}, false, fmt.Errorf("field is required")
	}
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return entityruntime.Contract{}, entityruntime.WriteTarget{Raw: target}, entityTarget, err
	}
	rootField, _, _ := strings.Cut(path, ".")
	unvalidated := entityruntime.WriteTarget{
		Raw:       target,
		Path:      path,
		RootField: strings.TrimSpace(rootField),
		Nested:    strings.Contains(path, "."),
	}
	// #512 is the nested-write slice. Preserve legacy top-level handler targets
	// while enforcing declared-path resolution only for dotted writes.
	if !unvalidated.Nested {
		return entityruntime.Contract{}, unvalidated, true, nil
	}
	contract, ok := entityruntime.ResolveForFlow(source, flowID)
	if !ok {
		if !strings.Contains(path, ".") {
			return entityruntime.Contract{}, unvalidated, true, nil
		}
		return entityruntime.Contract{}, unvalidated, true, fmt.Errorf("flow %s has no declared entity contract", strings.TrimSpace(flowID))
	}
	resolved, _, err := entityruntime.ResolveEntityWriteTarget(contract, target)
	return contract, resolved, true, err
}

func validateHandlerComputeSpecs(handler runtimecontracts.SystemNodeEventHandler) error {
	if err := validateComputeSpec(handler.Compute); err != nil {
		return err
	}
	for _, rule := range handler.OnComplete {
		if err := validateComputeSpec(rule.Compute); err != nil {
			return err
		}
	}
	for _, rule := range handler.Rules {
		if err := validateComputeSpec(rule.Compute); err != nil {
			return err
		}
	}
	return nil
}

func validateComputeSpec(spec *runtimecontracts.ComputeSpec) error {
	if spec == nil {
		return nil
	}
	if spec.Operation != runtimecontracts.ComputeOpWeightedAverage || len(spec.Tiers) == 0 {
		return nil
	}
	if strings.TrimSpace(spec.Keys.DimensionKey) == "" {
		return fmt.Errorf("weighted_average with tiers requires keys.dimension_key")
	}
	if len(normalizeStrings(spec.Keys.ScoreKeys)) == 0 {
		return fmt.Errorf("weighted_average with tiers requires keys.score_keys")
	}
	return nil
}

func (e *Executor) SupportsStep(step Step) bool {
	return slices.Contains(OrderedSteps, step)
}

func (e *Executor) Execute(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	if err := e.ValidateRequest(req); err != nil {
		result := ExecutionResult{Status: OutcomeRejected}
		SetExecutionFailure(&result, err, "runtime.engine", "validate_request")
		return result, err
	}
	entityID := identity.NormalizeEntityID(firstNonEmpty(
		req.EntityID.String(),
		req.State.EntityID.String(),
		req.Event.EntityID(),
	))
	if entityID.IsZero() {
		entityID = identity.NormalizeEntityID(req.Event.EntityID())
	}
	req.EntityID = entityID

	var (
		result          ExecutionResult
		intents         []EmitIntent
		activityIntents []ActivityIntent
	)
	err := e.deps.Locker.WithEntityLock(ctx, entityID, func(lockCtx context.Context) error {
		loaded, err := e.loadState(lockCtx, req)
		if err != nil {
			SetExecutionFailure(&result, err, "runtime.engine", "load_state")
			return err
		}
		req.State = loaded
		return e.deps.TxRunner.Run(lockCtx, func(tx Tx) error {
			actionIntents := []EmitIntent{}
			txCtx := WithActionEmitIntentCollector(tx.Context(), &actionIntents)
			frame := e.newExecutionFrame(contextTx{Tx: tx, ctx: txCtx}, req)
			if err := e.runSteps(&frame); err != nil {
				result = frame.result
				var moduleErr *computemodule.Error
				if errors.As(err, &moduleErr) && moduleErr.Code == computemodule.CodeReplay {
					SetExecutionFailure(&result, err, "runtime.engine", "compute_replay")
					return err
				}
				if replayErr := verifyComputeModuleReplayTraceCount(frame); replayErr != nil {
					SetExecutionFailure(&result, replayErr, "runtime.engine", "compute_replay_trace")
					return replayErr
				}
				SetExecutionFailure(&result, err, "runtime.engine", "execute_steps")
				return err
			}
			if err := verifyComputeModuleReplayTraceCount(frame); err != nil {
				result = frame.result
				SetExecutionFailure(&result, err, "runtime.engine", "compute_replay_trace")
				return err
			}
			if len(actionIntents) > 0 {
				frame.result.EmitIntents = append(frame.result.EmitIntents, actionIntents...)
			}
			result = frame.result
			if err := e.persist(frame.tx.Context(), frame); err != nil {
				return err
			}
			result = frame.result
			intents = append([]EmitIntent(nil), frame.result.EmitIntents...)
			activityIntents = append([]ActivityIntent(nil), frame.result.ActivityIntents...)
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, ErrEmitPersistencePrerequisite) || errors.Is(err, ErrEmitPayloadContractViolation) {
			result.Status = OutcomeRejected
		}
		if result.Status == OutcomeUnknown {
			result.Status = OutcomeRejected
		}
		if result.Failure == nil {
			SetExecutionFailure(&result, err, "runtime.engine", "execute")
		}
		return result, err
	}
	if len(intents) > 0 {
		if err := e.deps.Dispatcher.DispatchPostCommit(ctx, intents); err != nil {
			SetExecutionFailure(&result, err, "runtime.engine", "dispatch_post_commit")
			return result, err
		}
	}
	if len(activityIntents) > 0 {
		if e.deps.ActivityDispatcher == nil {
			err := fmt.Errorf("%w: activity dispatcher is required when handler declares activity", ErrInvalidConfig)
			SetExecutionFailure(&result, err, "runtime.engine", "dispatch_activity")
			return result, err
		}
		if err := e.deps.ActivityDispatcher.DispatchActivities(ctx, activityIntents); err != nil {
			SetExecutionFailure(&result, err, "runtime.engine", "dispatch_activity")
			return result, err
		}
	}
	return result, nil
}

func (e *Executor) loadState(ctx context.Context, req ExecutionRequest) (StateSnapshot, error) {
	state := req.State
	if state.EntityID.IsZero() {
		state.EntityID = req.EntityID
	}
	if req.EntityID.IsZero() {
		return state, nil
	}
	loaded, ok, err := e.deps.StateRepo.LoadState(ctx, req.EntityID)
	if err != nil {
		return StateSnapshot{}, err
	}
	if ok {
		return mergeStateSnapshots(state, loaded), nil
	}
	return state, nil
}

func (e *Executor) newExecutionFrame(tx Tx, req ExecutionRequest) executionFrame {
	state := req.State
	if state.StateCarrier.Metadata == nil {
		state.StateCarrier.Metadata = map[string]any{}
	}
	if state.StateCarrier.StateBuckets == nil {
		state.StateCarrier.StateBuckets = map[string]map[string]any{}
	}
	payload := decodePayload(req.Event.Payload())
	if len(payload) == 0 {
		payload = map[string]any{}
	}
	base := BuildBaseContext(ContextBuilderInput{
		Source:  e.deps.Source,
		FlowID:  req.FlowID.String(),
		State:   state,
		Event:   req.Event,
		Payload: payload,
	})
	req.State = state
	currentState := strings.TrimSpace(state.CurrentState)
	return executionFrame{
		tx:                       tx,
		req:                      req,
		base:                     base,
		payload:                  payload,
		topLevelDataAccumulation: req.Handler.DataAccumulation,
		state: ExecutionState{
			State:       state,
			Computed:    map[string]any{},
			Accumulated: map[string]any{},
			FanOut:      map[string]any{},
			Join:        map[string]any{},
			Loop:        map[string]any{},
			Transformed: map[string]any{},
		},
		result: ExecutionResult{
			Status:       OutcomeCompleted,
			CurrentState: currentState,
			NextState:    currentState,
			Computed:     map[string]any{},
		},
	}
}

func (e *Executor) runSteps(frame *executionFrame) error {
	for _, step := range OrderedSteps {
		stop, err := e.runStep(frame, step)
		if err != nil {
			return err
		}
		frame.result.ExecutedSteps = append(frame.result.ExecutedSteps, step)
		if stop {
			return nil
		}
	}
	return nil
}

func (e *Executor) runStep(frame *executionFrame, step Step) (bool, error) {
	switch step {
	case StepLoop:
		return false, e.stepLoop(frame)
	case StepQuery:
		return false, e.stepQuery(frame)
	case StepClearGates:
		return false, e.stepClearGates(frame)
	case StepGuard:
		return e.stepGuard(frame)
	case StepJoin:
		return e.stepJoin(frame)
	case StepAccumulate:
		return e.stepAccumulate(frame)
	case StepFilter:
		return false, e.stepFilter(frame)
	case StepGroupBy:
		return false, e.stepGroupBy(frame)
	case StepReduce:
		return false, e.stepReduce(frame)
	case StepCount:
		return false, e.stepCount(frame)
	case StepCompute:
		return false, e.stepCompute(frame)
	case StepFanOut:
		return e.stepFanOut(frame)
	case StepOnComplete:
		return false, e.stepOnComplete(frame)
	case StepRules:
		return false, e.stepRules(frame)
	case StepAdvancesTo:
		return false, e.stepAdvancesTo(frame)
	case StepSetsGate:
		return false, e.stepSetsGate(frame)
	case StepDataWrites:
		return false, e.stepDataWrites(frame)
	case StepProjection:
		return false, e.stepProjection(frame)
	case StepTransform:
		return false, e.stepTransform(frame)
	case StepEmits:
		return false, e.stepEmits(frame)
	case StepAction:
		return false, e.stepAction(frame)
	case StepActivity:
		return false, e.stepActivity(frame)
	case StepClear:
		return false, e.stepClear(frame)
	default:
		return false, nil
	}
}

func (e *Executor) stepJoin(frame *executionFrame) (bool, error) {
	if frame.req.Handler.Join == nil {
		return false, nil
	}
	plan, found := e.joinPlan(frame.req)
	if !found || plan.ResultType.Empty() {
		return false, fmt.Errorf("join %s has no resolved effective semantic plan", frame.req.Handler.Join.EffectiveID())
	}
	spec := &plan.Spec
	frame.joinResultType = plan.ResultType
	payload := frame.payload
	ref, timerKind, internal := timeridentity.ParseJoinRef(payload)
	if internal && (ref.NodeID != frame.req.NodeID.String() || ref.HandlerEvent != strings.TrimSpace(frame.req.HandlerEventKey) || ref.Stage != strings.TrimSpace(spec.Stage) || ref.JoinID != spec.EffectiveID()) {
		return false, failures.New(failures.ClassUnexpectedArrival, "join_timer_identity_mismatch", "runtime.engine", "join", map[string]any{
			"row_id": spec.EffectiveID(), "node_id": frame.req.NodeID.String(), "handler_event": strings.TrimSpace(frame.req.HandlerEventKey),
		})
	}
	window := ""
	generation := attemptgeneration.Generation{}
	if internal {
		window = ref.Window
		generation = ref.Generation
	} else if spec.Window != nil {
		value, ok := resolveContractPath(e.currentContext(frame), frame.state, spec.Window.ByPath, spec.Window.By)
		if !ok || strings.TrimSpace(asString(value)) == "" {
			return false, failures.New(failures.ClassUnexpectedArrival, "join_window_missing", "runtime.engine", "join", map[string]any{
				"row_id": spec.EffectiveID(), "window_by": spec.Window.By,
			})
		}
		window = strings.TrimSpace(asString(value))
	}
	if !internal && frame.loopActivation != nil {
		generation = frame.loopActivation.Generation()
	}
	key := joinruntime.ActivationKeyForGeneration(spec.Stage, spec.EffectiveID(), window, generation)
	activation, found, err := joinruntime.Load(frame.state.State.StateCarrier.StateBuckets, frame.req.NodeID.String(), key)
	if err != nil {
		return false, fmt.Errorf("load join activation %s: %w", key, err)
	}
	if !found {
		return false, e.joinArrivalFailure(frame, failures.ClassEarlyArrival, "join_not_armed", spec, window, "")
	}
	if internal && timerKind == timeridentity.TimerHandleJoinComplete {
		if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonComplete || !activation.OutcomePending || activation.OutcomeFired {
			frame.result.Status = OutcomeDiscarded
			return true, nil
		}
		activation.OutcomePending = false
		activation.OutcomeFired = true
		if err := e.storeJoinActivation(frame, activation); err != nil {
			return false, err
		}
		e.selectJoinOutcome(frame, &spec.OnComplete, handlerRuleSourceJoinOnComplete, activation)
		return false, nil
	}
	if activation.Status == joinruntime.StatusClosed {
		if internal {
			frame.result.Status = OutcomeDiscarded
			return true, nil
		}
		return false, e.joinArrivalFailure(frame, failures.ClassStaleArrival, "join_closed", spec, window, "")
	}
	if internal && timerKind == timeridentity.TimerHandleJoinTimeout {
		if !activation.Close(joinruntime.CloseReasonTimeout, false, true) {
			frame.result.Status = OutcomeDiscarded
			return true, nil
		}
		if err := e.storeJoinActivation(frame, activation); err != nil {
			return false, err
		}
		timeout := spec.Timeout.Outcome
		e.selectJoinOutcome(frame, &timeout, handlerRuleSourceJoinTimeout, activation)
		return false, nil
	}
	if internal {
		return false, e.joinArrivalFailure(frame, failures.ClassUnexpectedArrival, "join_internal_event_invalid", spec, window, "")
	}
	if strings.TrimSpace(frame.state.State.CurrentState) != strings.TrimSpace(spec.Stage) {
		return false, e.joinArrivalFailure(frame, failures.ClassStaleArrival, "join_stage_closed", spec, window, "")
	}
	memberValue, ok := resolveContractPath(e.currentContext(frame), frame.state, spec.Members.ByPath, spec.Members.By)
	member := strings.TrimSpace(asString(memberValue))
	if !ok || member == "" {
		return false, e.joinArrivalFailure(frame, failures.ClassUnexpectedArrival, "join_member_missing", spec, window, member)
	}
	output, ok := resolveContractPath(e.currentContext(frame), frame.state, spec.OutputPath, spec.Output)
	if !ok {
		return false, e.joinArrivalFailure(frame, failures.ClassUnexpectedArrival, "join_output_missing", spec, window, member)
	}
	disposition, err := activation.Add(member, output)
	if err != nil {
		return false, err
	}
	switch disposition {
	case joinruntime.AddUnexpected:
		return false, e.joinArrivalFailure(frame, failures.ClassUnexpectedArrival, "join_member_unexpected", spec, window, member)
	case joinruntime.AddConflictingDuplicate:
		return false, e.joinArrivalFailure(frame, failures.ClassConflictingDuplicate, "join_member_conflicting_duplicate", spec, window, member)
	case joinruntime.AddExactDuplicate:
		frame.state.Join = activation.Context()
		frame.result.Status = OutcomeWaiting
		return true, nil
	case joinruntime.AddAccepted:
	default:
		return false, fmt.Errorf("unsupported join add disposition %q", disposition)
	}
	frame.state.Join = activation.Context()
	complete, err := joinruntime.CompletionSatisfied(activation, spec.CompleteWhen, func(expression string, joinContext map[string]any) (bool, error) {
		frame.state.Join = joinContext
		return workflowexpr.EvalJoinBool(expression, joinContext, frame.joinResultType)
	})
	if err != nil {
		return false, fmt.Errorf("join complete_when: %w", err)
	}
	if !complete {
		if err := e.storeJoinActivation(frame, activation); err != nil {
			return false, err
		}
		frame.result.Status = OutcomeWaiting
		return true, nil
	}
	activation.Close(joinruntime.CloseReasonComplete, false, true)
	if err := e.storeJoinActivation(frame, activation); err != nil {
		return false, err
	}
	e.selectJoinOutcome(frame, &spec.OnComplete, handlerRuleSourceJoinOnComplete, activation)
	return false, nil
}

func (e *Executor) storeJoinActivation(frame *executionFrame, activation joinruntime.Activation) error {
	if frame.state.State.StateCarrier.StateBuckets == nil {
		frame.state.State.StateCarrier.StateBuckets = map[string]map[string]any{}
	}
	if err := joinruntime.Store(frame.state.State.StateCarrier.StateBuckets, activation); err != nil {
		return fmt.Errorf("store join activation: %w", err)
	}
	frame.result.StateMutation.SetStateBuckets(frame.state.State.StateCarrier.StateBuckets)
	frame.state.Join = activation.Context()
	return nil
}

func (e *Executor) selectJoinOutcome(frame *executionFrame, rule *runtimecontracts.HandlerRuleEntry, source handlerRuleSource, activation joinruntime.Activation) {
	frame.state.Join = activation.Context()
	frame.rule = rule
	frame.ruleSource = source
	e.applyRule(frame, rule)
}

func (e *Executor) joinArrivalFailure(frame *executionFrame, class failures.Class, code string, spec *runtimecontracts.JoinSpec, window, member string) error {
	attributes := map[string]any{
		"row_id":        spec.EffectiveID(),
		"stage":         strings.TrimSpace(spec.Stage),
		"node_id":       frame.req.NodeID.String(),
		"handler_event": strings.TrimSpace(frame.req.HandlerEventKey),
	}
	if strings.TrimSpace(window) != "" {
		attributes["window"] = strings.TrimSpace(window)
	}
	if strings.TrimSpace(member) != "" {
		attributes["member"] = strings.TrimSpace(member)
	}
	return failures.New(class, code, "runtime.engine", "join", attributes)
}

func (e *Executor) stepQuery(frame *executionFrame) error {
	spec := frame.req.Handler.Query
	if spec == nil {
		return nil
	}
	current := e.currentContext(frame)
	sourceValue, _ := resolveContractPath(current, frame.state, spec.SourcePath, spec.Source)
	items := executionItems(sourceValue)
	if len(items) == 0 {
		entityValue, _ := resolveContractPath(current, frame.state, spec.EntitiesPath, spec.Entities)
		items = executionItems(entityValue)
	}
	if filter := strings.TrimSpace(spec.Filter); filter != "" {
		compiled, err := compileExecutionCondition(filter)
		if err != nil {
			return err
		}
		filtered := make([]any, 0, len(items))
		for _, item := range items {
			scope := newExecutionScope(item, frame.payload, frame.base.Event.Raw(), current.Entity.Raw(), current.PlatformEntity.Raw(), current.Policy.Raw())
			passed, err := compiled.Eval(scope)
			if err != nil {
				return err
			}
			if passed {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	var value any = items
	switch {
	case strings.TrimSpace(spec.GroupBy) != "":
		grouped := map[string]any{}
		for _, item := range items {
			scope := newExecutionScope(item, frame.payload, frame.base.Event.Raw(), current.Entity.Raw(), current.PlatformEntity.Raw(), current.Policy.Raw())
			resolved, err := scope.resolveOperand(strings.TrimSpace(spec.GroupBy), executionOperandDefaultItem)
			if err != nil {
				return err
			}
			key := strings.TrimSpace(asString(resolved))
			if key == "" {
				key = "unknown"
			}
			grouped[key] = asInt(grouped[key]) + 1
		}
		value = grouped
	case spec.Count:
		value = len(items)
	case len(spec.Select) > 0:
		selected := make([]any, 0, len(items))
		for _, item := range items {
			obj, ok := asObject(item)
			if !ok {
				continue
			}
			projected := map[string]any{}
			for _, field := range spec.Select {
				field = strings.TrimSpace(field)
				if field == "" {
					continue
				}
				if fieldValue, ok := obj[field]; ok {
					projected[field] = fieldValue
				}
			}
			selected = append(selected, projected)
		}
		value = selected
	}
	if err := e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.query"), value); err != nil {
		return err
	}
	return nil
}

func (e *Executor) stepClearGates(frame *executionFrame) error {
	if len(frame.req.Handler.ClearGates) == 0 {
		return nil
	}
	frame.result.ClearGates = normalizeStrings(e.resolveClearGates(frame))
	frame.result.StateMutation.ClearGates = append([]string{}, frame.result.ClearGates...)
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	for _, gate := range frame.result.ClearGates {
		frame.state.State.SetGate(gate, false)
		frame.result.StateMutation.SetGateValue(gate, false)
	}
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	return nil
}

func (e *Executor) stepGuard(frame *executionFrame) (bool, error) {
	spec := frame.req.Handler.Guard
	if spec == nil {
		return false, nil
	}
	passed, evaluated, err := e.evaluateGuardSpec(frame, spec)
	frame.result.GuardsEvaluated = append(frame.result.GuardsEvaluated, evaluated...)
	if passed {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := e.applyGuardFailure(frame, spec); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Executor) stepAccumulate(frame *executionFrame) (bool, error) {
	spec := frame.req.Handler.Accumulate
	if spec == nil {
		return false, nil
	}
	spec, err := e.effectiveAccumulatorSpec(frame, spec)
	if err != nil {
		return false, err
	}
	current := e.currentContext(frame)
	bucketRef, matched, err := e.resolveAccumulatorBucketRef(frame, spec)
	if err != nil {
		return false, err
	}
	if !matched {
		return false, nil
	}
	acc, ok := loadAccumulatorForBucket(frame.state.State, bucketRef)
	if !ok {
		acc = &Accumulator{}
	}
	if acc.Received == nil {
		acc.Received = map[string]bool{}
	}
	arrivalID := dedupIdentifier(current, frame.state, frame.req.Event, spec)
	if arrivalID != "" && acc.Received[arrivalID] {
		frame.result.Status = OutcomeDiscarded
		return true, nil
	}
	if arrivalID != "" {
		acc.Received[arrivalID] = true
		item := cloneStringAnyMap(frame.payload)
		if item == nil {
			item = map[string]any{}
		}
		item["event_id"] = strings.TrimSpace(frame.req.Event.ID())
		item["event_type"] = strings.TrimSpace(string(frame.req.Event.Type()))
		item["source"] = strings.TrimSpace(frame.req.Event.SourceAgent())
		item["received_at"] = frame.req.Event.CreatedAt().UTC().Format(time.RFC3339Nano)
		acc.Items = append(acc.Items, item)
	}
	storeAccumulatorForBucket(&frame.state.State, bucketRef, acc)
	frame.result.StateMutation.SetStateBuckets(frame.state.State.StateCarrier.StateBuckets)
	frame.state.Accumulated = accumulatorExpressionValue(acc)
	return false, nil
}

func (e *Executor) stepFilter(frame *executionFrame) error {
	spec := frame.req.Handler.Filter
	if spec == nil {
		return nil
	}
	current := e.currentContext(frame)
	sourceValue, _ := resolveContractPath(current, frame.state, spec.ItemsPath, firstNonEmpty(spec.ItemsFrom, spec.Source))
	items := executionItems(sourceValue)
	compiled, err := compileExecutionCondition(strings.TrimSpace(spec.Condition))
	if err != nil {
		return err
	}
	filtered := make([]any, 0, len(items))
	for _, item := range items {
		scope := newExecutionScope(item, frame.payload, frame.base.Event.Raw(), current.Entity.Raw(), current.PlatformEntity.Raw(), current.Policy.Raw())
		passed, err := compiled.Eval(scope)
		if err != nil {
			return err
		}
		if passed {
			filtered = append(filtered, item)
		}
	}
	if err := e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.filter"), filtered); err != nil {
		return err
	}
	return nil
}

func (e *Executor) stepReduce(frame *executionFrame) error {
	spec := frame.req.Handler.Reduce
	if spec == nil {
		return nil
	}
	current := e.currentContext(frame)
	sourceValue, _ := resolveContractPath(current, frame.state, spec.ItemsPath, firstNonEmpty(spec.ItemsFrom, spec.Source))
	items := executionItems(sourceValue)
	value := executionReduceValue(items, strings.TrimSpace(spec.Operation))
	if err := e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.reduce"), value); err != nil {
		return err
	}
	return nil
}

func (e *Executor) stepCount(frame *executionFrame) error {
	spec := frame.req.Handler.Count
	if spec == nil {
		return nil
	}
	current := e.currentContext(frame)
	sourceValue, _ := resolveContractPath(current, frame.state, spec.ItemsPath, firstNonEmpty(spec.ItemsFrom, spec.Source))
	items := executionItems(sourceValue)
	compiled, err := compileExecutionCondition(strings.TrimSpace(spec.Condition))
	if err != nil {
		return err
	}
	count := 0
	for _, item := range items {
		if compiled == nil {
			count++
			continue
		}
		scope := newExecutionScope(item, frame.payload, frame.base.Event.Raw(), current.Entity.Raw(), current.PlatformEntity.Raw(), current.Policy.Raw())
		passed, err := compiled.Eval(scope)
		if err != nil {
			return err
		}
		if passed {
			count++
		}
	}
	if err := e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.count"), count); err != nil {
		return err
	}
	return nil
}

func (e *Executor) stepCompute(frame *executionFrame) error {
	if frame.rule != nil {
		spec := frame.rule.Compute
		if spec == nil || spec.Operation == runtimecontracts.ComputeOpLookup || spec.Operation == runtimecontracts.ComputeOpValidate || spec.Operation == runtimecontracts.ComputeOpModule {
			return nil
		}
		return e.executeComputeSpec(frame, spec)
	}
	if frame.req.Handler.Compute != nil {
		if err := e.executeComputeSpec(frame, frame.req.Handler.Compute); err != nil {
			return err
		}
	}
	for idx := range frame.req.Handler.Rules {
		rule := &frame.req.Handler.Rules[idx]
		if (rule.PolicyRow.Kind != runtimecontracts.PolicySheetRowKindLookup &&
			rule.PolicyRow.Kind != runtimecontracts.PolicySheetRowKindValidate &&
			rule.PolicyRow.Kind != runtimecontracts.PolicySheetRowKindModule) || rule.Compute == nil {
			continue
		}
		if err := e.executeComputeSpec(frame, rule.Compute); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) executeComputeSpec(frame *executionFrame, spec *runtimecontracts.ComputeSpec) error {
	if spec == nil {
		return nil
	}
	accumulateSpec, effectiveErr := e.effectiveAccumulatorSpec(frame, frame.req.Handler.Accumulate)
	if effectiveErr != nil {
		return effectiveErr
	}
	bucketRef, matched, bucketErr := e.resolveAccumulatorBucketRef(frame, accumulateSpec)
	if bucketErr != nil {
		return bucketErr
	}
	if !matched {
		return nil
	}
	acc, _ := loadAccumulatorForBucket(frame.state.State, bucketRef)
	var (
		value any
		err   error
	)
	switch spec.Operation {
	case runtimecontracts.ComputeOpLookup:
		value, err = computeLookupValue(e.currentContext(frame), spec)
	case runtimecontracts.ComputeOpValidate:
		value, err = e.computeValidationValue(frame, spec)
	case runtimecontracts.ComputeOpModule:
		value, err = e.computeModuleValue(frame, spec)
	default:
		value, err = computeValue(acc, frame.payload, spec)
	}
	if err != nil {
		return err
	}
	if spec.Operation == runtimecontracts.ComputeOpValidate {
		return e.storeComputedPathOnly(frame, spec.StoreAs, value)
	}
	if storeAs := strings.TrimSpace(spec.StoreAs); storeAs != "" {
		if handlerTargetRequiresCanonicalWrite(storeAs) {
			if err := e.writeStepValue(frame, storeAs, value); err != nil {
				return err
			}
			return nil
		}
		field := normalizeStateField(storeAs)
		if field == "" {
			field = "computed"
		}
		frame.state.SetComputed(field, value)
		frame.result.SetComputed(field, value)
		frame.state.State.SetMetadata(field, value)
		frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
		frame.result.StateMutation.SetMetadata(field, value)
		return nil
	}
	frame.state.SetComputed("computed", value)
	frame.result.SetComputed("computed", value)
	frame.state.State.SetMetadata("computed", value)
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	frame.result.StateMutation.SetMetadata("computed", value)
	return nil
}

func (e *Executor) computeModuleValue(frame *executionFrame, spec *runtimecontracts.ComputeSpec) (any, error) {
	if spec == nil || spec.Module == nil {
		return nil, ErrNotImplemented
	}
	plan := spec.Module
	rowID := strings.TrimSpace(plan.RowID)
	if rowID == "" {
		rowID = strings.TrimSpace(spec.StoreAs)
	}
	moduleID := strings.TrimSpace(plan.Module)
	bundle, ok := semanticview.Bundle(e.deps.Source)
	if !ok || bundle == nil {
		return nil, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("semantic source is not a workflow contract bundle")}
	}
	policy := bundle.ResolvedPolicyForFlow(frame.req.FlowID.String())
	module, ok := policy.Modules[moduleID]
	if !ok {
		return nil, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("unknown module %q", moduleID)}
	}
	current := e.currentContext(frame)
	inputs := map[string]any{}
	for name, rawPath := range plan.Input {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		path, ok := plan.InputPaths[name]
		if !ok {
			path = paths.Parse(rawPath)
		}
		if path.IsZero() {
			return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("input.%s has no mapped source path", name)}
		}
		value, exists := current.Lookup(path)
		if !exists {
			return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("input.%s source %s is missing", name, path.String())}
		}
		inputs[name] = value
	}
	if err := eventschema.ValidatePayloadAgainstSchema(module.InputSchema, inputs); err != nil {
		return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("input schema violation: %w", err)}
	}
	inputBytes, err := computemodule.CanonicalJSONBytes(inputs)
	if err != nil {
		return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: err}
	}
	moduleBytes, _, err := runtimecontracts.PolicyModuleBytes(bundle, module)
	if err != nil {
		return nil, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: moduleID, RowID: rowID, Err: err}
	}
	kind := strings.TrimSpace(module.Kind)
	if kind == "" {
		kind = "wasm"
	}
	abi := strings.TrimSpace(module.ABI)
	entry := strings.TrimSpace(module.Entry)
	switch kind {
	case pythonmodule.Kind:
		if abi == "" {
			abi = pythonmodule.ABI
		}
		if entry == "" {
			entry = pythonmodule.DefaultEntry
		}
	default:
		if abi == "" {
			abi = computemodule.ABI
		}
		if entry == "" {
			entry = computemodule.DefaultEntry
		}
	}
	trace := ComputeModuleTrace{
		ModuleID:   moduleID,
		RowID:      rowID,
		Kind:       kind,
		ABI:        abi,
		Entry:      entry,
		Digest:     strings.TrimSpace(module.Digest),
		SourceHash: strings.TrimSpace(module.SourceHash),
		InputHash:  computemodule.HashBytes(inputBytes),
		Outcome:    computemodule.ReplayOutcomeSuccess,
		Arch:       computemodule.CurrentArch(),
		Limits: computemodule.ReplayLimits{
			Fuel:        module.Limits.Gas,
			MemoryPages: module.Limits.MemoryPages,
			OutputBytes: module.Limits.OutputBytes,
		},
	}
	var rawOutput []byte
	switch kind {
	case "wasm":
		trace.Engine = computemodule.EngineVersion()
		result, err := computemodule.Execute(computemodule.Request{
			ModuleID:    moduleID,
			RowID:       rowID,
			Digest:      strings.TrimSpace(module.Digest),
			Entry:       entry,
			Wasm:        moduleBytes,
			Input:       inputBytes,
			Fuel:        module.Limits.Gas,
			MemoryPages: module.Limits.MemoryPages,
			OutputBytes: module.Limits.OutputBytes,
		})
		if err != nil {
			return nil, e.recordComputeModuleFailure(frame, trace, err)
		}
		rawOutput = result.Output
		trace.Engine = result.Engine
		trace.FuelConsumed = result.FuelConsumed
	case pythonmodule.Kind:
		identity := pythonmodule.RuntimeIdentity()
		trace.Engine = identity.Engine
		trace.Interpreter = identity.Interpreter
		trace.InterpreterDigest = identity.InterpreterDigest
		trace.SnapshotDigest = identity.SnapshotDigest
		trace.HarnessABI = identity.HarnessABI
		trace.SourceHash = computemodule.HashBytes(moduleBytes)
		result, err := pythonmodule.Execute(frame.tx.Context(), pythonmodule.Request{
			ModuleID:    moduleID,
			RowID:       rowID,
			Digest:      strings.TrimSpace(module.Digest),
			Entry:       entry,
			Source:      moduleBytes,
			Input:       inputBytes,
			Fuel:        module.Limits.Gas,
			MemoryPages: module.Limits.MemoryPages,
			OutputBytes: module.Limits.OutputBytes,
		})
		if err != nil {
			return nil, e.recordComputeModuleFailure(frame, trace, err)
		}
		rawOutput = result.Output
		trace.Engine = result.Engine
		trace.FuelConsumed = result.FuelConsumed
		trace.Interpreter = result.Interpreter
		trace.InterpreterDigest = result.InterpreterSHA
		trace.SnapshotDigest = result.SnapshotHash
		trace.HarnessABI = result.HarnessABI
		trace.SourceHash = result.SourceHash
	default:
		return nil, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("unsupported module kind %q", kind)}
	}
	output, err := decodeComputeModuleOutput(moduleID, rowID, rawOutput, module.OutputSchema)
	if err != nil {
		return nil, e.recordComputeModuleFailure(frame, trace, err)
	}
	outputHash, err := computemodule.CanonicalJSONHash(output)
	if err != nil {
		return nil, e.recordComputeModuleFailure(frame, trace, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: err})
	}
	trace.OutputHash = outputHash
	if err := verifyComputeModuleReplayTrace(frame, trace); err != nil {
		return nil, err
	}
	frame.result.ComputeModuleTraces = append(frame.result.ComputeModuleTraces, trace.Normalized())
	return output, nil
}

func (e *Executor) recordComputeModuleFailure(frame *executionFrame, trace ComputeModuleTrace, cause error) error {
	if frame == nil {
		return cause
	}
	trace = trace.Normalized()
	trace.Outcome = computemodule.ReplayOutcomeFailure
	trace.OutputHash = ""
	trace.ErrorCode = string(computemodule.CodeABI)
	var typed *computemodule.Error
	if errors.As(cause, &typed) {
		trace.ErrorCode = string(typed.Code)
		if strings.TrimSpace(trace.ModuleID) == "" {
			trace.ModuleID = strings.TrimSpace(typed.ModuleID)
		}
		if strings.TrimSpace(trace.RowID) == "" {
			trace.RowID = strings.TrimSpace(typed.RowID)
		}
	}
	if err := verifyComputeModuleReplayTrace(frame, trace); err != nil {
		return err
	}
	frame.result.ComputeModuleTraces = append(frame.result.ComputeModuleTraces, trace.Normalized())
	return cause
}

func decodeComputeModuleOutput(moduleID, rowID string, raw []byte, schema map[string]any) (map[string]any, error) {
	var output map[string]any
	if err := json.Unmarshal(raw, &output); err != nil {
		return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("output is not exactly one JSON object: %w", err)}
	}
	if output == nil {
		return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("output is not JSON object")}
	}
	if err := eventschema.ValidatePayloadAgainstSchema(schema, output); err != nil {
		return nil, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: moduleID, RowID: rowID, Err: fmt.Errorf("output schema violation: %w", err)}
	}
	return output, nil
}

func verifyComputeModuleReplayTrace(frame *executionFrame, actual ComputeModuleTrace) error {
	expected := frame.req.ExpectedComputeModuleTraces
	if expected == nil {
		return nil
	}
	idx := len(frame.result.ComputeModuleTraces)
	if idx >= len(expected) {
		finding := computemodule.ReplayFinding{
			Schema:      computemodule.ReplayEvidenceSchema,
			Kind:        computemodule.ReplayFindingIdentityDivergence,
			Field:       "trace_count",
			ModuleID:    actual.ModuleID,
			RowID:       actual.RowID,
			Actual:      fmt.Sprint(idx + 1),
			Expected:    fmt.Sprint(len(expected)),
			Message:     "compute_module replay unexpected trace",
			Remediation: "replay with matching persisted compute_module trace count and order",
		}.Normalized()
		return &computemodule.Error{
			Code:     computemodule.CodeReplay,
			ModuleID: actual.ModuleID,
			RowID:    actual.RowID,
			Finding:  &finding,
			Err:      fmt.Errorf("unexpected compute_module trace at replay index %d: module=%s row=%s outcome=%s output_hash=%s error_code=%s fuel=%d", idx, actual.ModuleID, actual.RowID, actual.Outcome, actual.OutputHash, actual.ErrorCode, actual.FuelConsumed),
		}
	}
	want := expected[idx]
	if finding := computemodule.CompareReplayEnvelopes(want, actual); finding != nil {
		return &computemodule.Error{
			Code:     computemodule.CodeReplay,
			ModuleID: actual.ModuleID,
			RowID:    actual.RowID,
			Finding:  finding,
			Err: fmt.Errorf("compute_module replay %s at index %d field=%s: expected module=%s row=%s kind=%s abi=%s entry=%s digest=%s input_hash=%s interpreter=%s interpreter_digest=%s snapshot_digest=%s harness_abi=%s source_hash=%s outcome=%s output_hash=%s error_code=%s fuel=%d fuel_limit=%d memory_pages=%d output_bytes=%d engine=%s arch=%s; got module=%s row=%s kind=%s abi=%s entry=%s digest=%s input_hash=%s interpreter=%s interpreter_digest=%s snapshot_digest=%s harness_abi=%s source_hash=%s outcome=%s output_hash=%s error_code=%s fuel=%d fuel_limit=%d memory_pages=%d output_bytes=%d engine=%s arch=%s",
				finding.Kind,
				idx,
				finding.Field,
				want.ModuleID, want.RowID, want.Kind, want.ABI, want.Entry, want.Digest, want.InputHash, want.Interpreter, want.InterpreterDigest, want.SnapshotDigest, want.HarnessABI, want.SourceHash, want.Outcome, want.OutputHash, want.ErrorCode, want.FuelConsumed, want.Limits.Fuel, want.Limits.MemoryPages, want.Limits.OutputBytes, want.Engine, want.Arch,
				actual.ModuleID, actual.RowID, actual.Kind, actual.ABI, actual.Entry, actual.Digest, actual.InputHash, actual.Interpreter, actual.InterpreterDigest, actual.SnapshotDigest, actual.HarnessABI, actual.SourceHash, actual.Outcome, actual.OutputHash, actual.ErrorCode, actual.FuelConsumed, actual.Limits.Fuel, actual.Limits.MemoryPages, actual.Limits.OutputBytes, actual.Engine, actual.Arch,
			),
		}
	}
	return nil
}

func verifyComputeModuleReplayTraceCount(frame executionFrame) error {
	expected := frame.req.ExpectedComputeModuleTraces
	if expected == nil || len(frame.result.ComputeModuleTraces) == len(expected) {
		return nil
	}
	finding := computemodule.ReplayFinding{
		Schema:      computemodule.ReplayEvidenceSchema,
		Kind:        computemodule.ReplayFindingIdentityDivergence,
		Field:       "trace_count",
		Expected:    fmt.Sprint(len(expected)),
		Actual:      fmt.Sprint(len(frame.result.ComputeModuleTraces)),
		Message:     "compute_module replay trace count mismatch",
		Remediation: "replay with matching persisted compute_module trace count and order",
	}.Normalized()
	return &computemodule.Error{
		Code:    computemodule.CodeReplay,
		Finding: &finding,
		Err:     fmt.Errorf("compute_module replay trace count mismatch: expected %d trace(s), got %d", len(expected), len(frame.result.ComputeModuleTraces)),
	}
}

func (e *Executor) computeValidationValue(frame *executionFrame, spec *runtimecontracts.ComputeSpec) (any, error) {
	if spec == nil || spec.Validation == nil {
		return nil, ErrNotImplemented
	}
	plan := spec.Validation
	rowID := strings.TrimSpace(plan.RowID)
	if rowID == "" {
		rowID = strings.TrimSpace(spec.StoreAs)
	}
	policy := e.deps.Source.ResolvedPolicyForFlow(frame.req.FlowID.String())
	setName := strings.TrimSpace(plan.Set)
	set, ok := policy.Validation[setName]
	if !ok {
		return nil, fmt.Errorf("validation_config_no_retry: row %s references unknown validation set %q", rowID, setName)
	}
	current := e.currentContext(frame)
	inputs := map[string]any{}
	for name, declaredType := range set.Inputs {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		path, ok := plan.InputPaths[name]
		if !ok {
			path = paths.Parse(plan.Input[name])
		}
		if path.IsZero() {
			return nil, fmt.Errorf("validation_input_no_retry: row %s input.%s has no mapped source path", rowID, name)
		}
		value, exists := current.Lookup(path)
		if !exists {
			return nil, fmt.Errorf("validation_input_no_retry: row %s input.%s source %s is missing", rowID, name, path.String())
		}
		if !validationRuntimeValueMatchesType(value, declaredType) {
			return nil, fmt.Errorf("validation_input_no_retry: row %s input.%s source %s does not match declared type %s", rowID, name, path.String(), strings.TrimSpace(declaredType))
		}
		inputs[name] = value
	}
	violations := make([]any, 0)
	for _, rule := range set.Rules {
		equal := rule.Check.Equal
		if equal == nil {
			return nil, fmt.Errorf("validation_config_no_retry: row %s rule %s has no equal check", rowID, strings.TrimSpace(rule.ID))
		}
		leftName := validationInputName(equal.Left)
		rightName := validationInputName(equal.Right)
		left, leftOK := inputs[leftName]
		right, rightOK := inputs[rightName]
		if !leftOK || !rightOK {
			return nil, fmt.Errorf("validation_input_no_retry: row %s rule %s references unmapped validation input", rowID, strings.TrimSpace(rule.ID))
		}
		if validationRuntimeValuesEqual(left, right, set.Inputs[leftName]) {
			continue
		}
		violations = append(violations, map[string]any{
			"id":          strings.TrimSpace(rule.ID),
			"class":       strings.TrimSpace(rule.Class),
			"content_ref": strings.Join([]string{strings.TrimSpace(equal.Left), strings.TrimSpace(equal.Right)}, ","),
		})
	}
	return map[string]any{
		"valid":      len(violations) == 0,
		"violations": violations,
	}, nil
}

func validationInputName(ref string) string {
	name, ok := strings.CutPrefix(strings.TrimSpace(ref), "input.")
	if !ok {
		return ""
	}
	return strings.TrimSpace(name)
}

func validationRuntimeValueMatchesType(value any, declared string) bool {
	kind, _, _, ok := runtimecontracts.CanonicalizeComputeLookupValue(value)
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(declared)) {
	case "string", "text", "uuid", "timestamp", "datetime", "date":
		return kind == "string"
	case "bool", "boolean":
		return kind == "bool"
	case "int", "integer":
		if kind == "int" {
			return true
		}
		if _, ok := integralLookupFloatSummary(value); ok {
			return true
		}
		return false
	case "number", "numeric", "float", "double":
		return kind == "number" || kind == "int"
	default:
		return false
	}
}

func validationRuntimeValuesEqual(left, right any, declared string) bool {
	switch strings.ToLower(strings.TrimSpace(declared)) {
	case "int", "integer":
		leftCanonical, leftOK := validationRuntimeIntegralCanonical(left)
		rightCanonical, rightOK := validationRuntimeIntegralCanonical(right)
		return leftOK && rightOK && leftCanonical == rightCanonical
	case "number", "numeric", "float", "double":
		leftCanonical, leftOK := validationRuntimeNumericCanonical(left)
		rightCanonical, rightOK := validationRuntimeNumericCanonical(right)
		return leftOK && rightOK && leftCanonical == rightCanonical
	default:
		return reflect.DeepEqual(left, right)
	}
}

func validationRuntimeIntegralCanonical(value any) (string, bool) {
	kind, summary, _, ok := runtimecontracts.CanonicalizeComputeLookupValue(value)
	if !ok {
		return "", false
	}
	if kind == "int" {
		return summary, true
	}
	if integral, ok := integralLookupFloatSummary(value); ok {
		return integral, true
	}
	return "", false
}

func validationRuntimeNumericCanonical(value any) (string, bool) {
	kind, summary, _, ok := runtimecontracts.CanonicalizeComputeLookupValue(value)
	if !ok {
		return "", false
	}
	switch kind {
	case "int", "number":
		return summary, true
	default:
		return "", false
	}
}

func (e *Executor) storeComputedPathOnly(frame *executionFrame, storeAs string, value any) error {
	parsed := paths.Parse(storeAs)
	if parsed.Root != paths.RootComputed || len(parsed.Segments) < 2 || parsed.Segments[0] != "validation" {
		return fmt.Errorf("validation_config_no_retry: validate.into %q must target computed.validation.*", strings.TrimSpace(storeAs))
	}
	if frame.state.Computed == nil {
		frame.state.Computed = map[string]any{}
	}
	if frame.result.Computed == nil {
		frame.result.Computed = map[string]any{}
	}
	values.Wrap(frame.state.Computed).SetPath(parsed, value)
	values.Wrap(frame.result.Computed).SetPath(parsed, value)
	return nil
}

func handlerTargetRequiresCanonicalWrite(target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	parsed := paths.Parse(target)
	if parsed.HasExplicitRoot() {
		return true
	}
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return false
	}
	return strings.Contains(path, ".")
}

func specialHandlerClearTarget(target string) bool {
	switch strings.TrimSpace(target) {
	case "accumulator_state", "pending_dedup":
		return true
	default:
		return false
	}
}

func (e *Executor) stepFanOut(frame *executionFrame) (bool, error) {
	active := e.selectedFanOut(frame)
	if active.Spec == nil {
		return false, nil
	}
	spec := active.Spec
	if e.deps.Source == nil {
		return false, fmt.Errorf("%w: semantic source is required for fan_out", ErrInvalidConfig)
	}
	handlerEvent := strings.TrimSpace(frame.req.HandlerEventKey)
	if handlerEvent == "" {
		handlerEvent = strings.TrimSpace(string(frame.req.Event.Type()))
	}
	effective, err := e.deps.Source.ResolveFanOutEffectiveSemantics(strings.TrimSpace(string(frame.req.FlowID)), handlerEvent, *spec)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	itemsValue, _ := resolveContractPath(frame.base, frame.state, effective.ItemsPath, effective.ItemsFrom)
	items := sliceFromAny(itemsValue)
	frame.result.FanOutCount = len(items)
	frame.state.FanOut = map[string]any{}
	frame.state.SetFanOut("count", len(items))
	// fan_out_count is platform-populated runtime bookkeeping, not an authored
	// entity write target.
	frame.state.State.SetMetadata("fan_out_count", len(items))
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	limit := effective.MaxItems
	if len(items) > limit {
		return false, failures.Wrap(
			failures.ClassFanOutBoundExceeded,
			"fan_out_bound",
			"runtime.engine",
			string(StepFanOut),
			map[string]any{
				"source":          strings.TrimSpace(string(active.Source)),
				"items_from":      effective.ItemsFrom,
				"actual":          len(items),
				"authored_limit":  effective.AuthoredMaxItems,
				"effective_limit": limit,
				"remediation":     "raise max_items or split the batch",
			},
			ErrFanOutBoundExceeded,
		)
	}
	if err := e.stepDataWrites(frame); err != nil {
		return false, err
	}
	if err := e.stepProjection(frame); err != nil {
		return false, err
	}
	for index, item := range items {
		eventType := fanOutEventType(spec)
		if eventType == "" {
			continue
		}
		eventType = e.resolveDeclarativeEmitEventType(frame, eventType)
		emitSpec, err := e.lowerEmitSpecForFrame(frame, string(active.EmitSource()), spec.Emit)
		if err != nil {
			return false, err
		}
		emitSpec.Event = eventType
		frame.state.SetFanOut("item", item)
		frame.state.SetFanOut("index", index)
		payload := map[string]any{}
		transformed, err := emitFieldsPayload(e.currentContext(frame), frame.state, emitSpec, workflowexpr.ValueExpressionOptions{ItemAlias: effective.ItemAlias})
		if err != nil {
			return false, err
		}
		if len(transformed) > 0 {
			payload = transformed
		}
		shaped, err := e.shapeEmitPayload(frame, eventType, payload)
		if err != nil {
			return false, err
		}
		if _, err := e.queueEmitIntentForSpec(frame, emitSpec, eventType, shaped); err != nil {
			return false, err
		}
	}
	if len(frame.result.EmitIntents) == 0 && len(frame.result.DeadLetterIntents) == 0 {
		if err := e.stepAdvancesTo(frame); err != nil {
			return false, err
		}
		frame.result.Status = OutcomeFannedOut
		return true, nil
	}
	if err := e.stepAdvancesTo(frame); err != nil {
		return false, err
	}
	frame.result.Status = OutcomeFannedOut
	return true, nil
}

func (e *Executor) stepGroupBy(frame *executionFrame) error {
	spec := frame.req.Handler.GroupBy
	if spec == nil {
		return nil
	}
	itemsValue, _ := resolveContractPath(frame.base, frame.state, spec.ItemsPath, spec.ItemsFrom)
	items := sliceFromAny(itemsValue)
	current := e.currentContext(frame)
	grouped := make(map[string]any)
	for _, item := range items {
		scope := newExecutionScope(item, frame.payload, frame.base.Event.Raw(), current.Entity.Raw(), current.PlatformEntity.Raw(), current.Policy.Raw())
		resolved, err := scope.resolveOperand(strings.TrimSpace(spec.Key), executionOperandDefaultItem)
		if err != nil {
			return err
		}
		key := strings.TrimSpace(asString(resolved))
		if key == "" {
			key = "unknown"
		}
		existing, _ := grouped[key].([]any)
		grouped[key] = append(existing, item)
	}
	if err := e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.group_by"), grouped); err != nil {
		return err
	}
	return nil
}

func (e *Executor) stepOnComplete(frame *executionFrame) error {
	if frame.ruleSource == handlerRuleSourceJoinOnComplete || frame.ruleSource == handlerRuleSourceJoinTimeout {
		return nil
	}
	topLevelRules := frame.req.Handler.OnComplete
	ruleSource := handlerRuleSourceOnComplete
	rule, err := e.selectRule(frame, topLevelRules)
	if err != nil {
		return err
	}
	if rule != nil {
		frame.rule = rule
		frame.ruleSource = ruleSource
		e.applyRule(frame, rule)
		if rule.FanOut != nil {
			if _, err := e.stepFanOut(frame); err != nil {
				return err
			}
		}
		if rule.Compute != nil {
			return e.stepCompute(frame)
		}
	}
	return nil
}

func (e *Executor) stepRules(frame *executionFrame) error {
	if frame.rule != nil {
		return nil
	}
	rule, err := e.selectRule(frame, frame.req.Handler.Rules)
	if err != nil {
		return err
	}
	if rule != nil {
		frame.rule = rule
		frame.ruleSource = handlerRuleSourceRules
		e.applyRule(frame, rule)
		if rule.FanOut != nil {
			if _, err := e.stepFanOut(frame); err != nil {
				return err
			}
		}
		if rule.Compute != nil {
			return e.stepCompute(frame)
		}
	}
	return nil
}

func (e *Executor) stepAdvancesTo(frame *executionFrame) error {
	if frame.transitionApplied {
		return nil
	}
	next := strings.TrimSpace(frame.req.Handler.AdvancesTo)
	if frame.rule != nil && strings.TrimSpace(frame.rule.AdvancesTo) != "" {
		next = strings.TrimSpace(frame.rule.AdvancesTo)
	}
	if next == "" || next == frame.result.CurrentState {
		return nil
	}
	if e.deps.TransitionValidator != nil {
		if err := e.deps.TransitionValidator.ValidateTransition(frame.result.CurrentState, next); err != nil {
			frame.result.Status = OutcomeRejected
			return err
		}
	}
	if err := e.advanceAdmittedLoop(frame, next); err != nil {
		return err
	}
	frame.result.NextState = next
	frame.state.State.CurrentState = next
	frame.result.StateMutation.NextState = next
	frame.transitionApplied = true
	frame.result.ActionsExecuted = append(frame.result.ActionsExecuted,
		identity.ActionRecordStateChange.String(),
		identity.ActionUpdateState.String(),
		identity.ActionCancelStateTimers.String(),
		identity.ActionStartStateTimers.String(),
	)
	frame.result.TimerIntents = append(frame.result.TimerIntents, e.buildTimerIntents(frame)...)
	return nil
}

func (e *Executor) stepSetsGate(frame *executionFrame) error {
	if frame.req.Handler.SetsGate == nil {
		if frame.rule == nil || strings.TrimSpace(frame.rule.AdvancesTo) == "" {
			return nil
		}
	}
	spec := frame.req.Handler.SetsGate
	if frame.rule != nil && strings.TrimSpace(frame.rule.AdvancesTo) == "" {
		// rule-level gates are not modeled yet
	}
	if spec == nil {
		return nil
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return nil
	}
	frame.result.SetsGate = name
	frame.result.StateMutation.SetGate = name
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	frame.state.State.SetGate(name, true)
	frame.result.StateMutation.SetGateValue(name, true)
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	return nil
}

func (e *Executor) stepDataWrites(frame *executionFrame) error {
	ruleHasWrites := frame.rule != nil && (frame.rule.DataAccumulation.HasWrites() || strings.TrimSpace(frame.rule.DataAccumulation.SourceEvent) != "")
	if ruleHasWrites && !frame.ruleDataWritesApplied {
		if err := e.applyDataAccumulation(frame, frame.rule.DataAccumulation); err != nil {
			return err
		}
		frame.ruleDataWritesApplied = true
	}
	if (frame.topLevelDataAccumulation.HasWrites() || strings.TrimSpace(frame.topLevelDataAccumulation.SourceEvent) != "") && !frame.topLevelDataWritesApplied {
		if err := e.applyDataAccumulation(frame, frame.topLevelDataAccumulation); err != nil {
			return err
		}
		frame.topLevelDataWritesApplied = true
	}
	return nil
}

func joinExpressionOptions(frame *executionFrame) workflowexpr.ValueExpressionOptions {
	allowJoin := frame != nil && (frame.ruleSource == handlerRuleSourceJoinOnComplete || frame.ruleSource == handlerRuleSourceJoinTimeout)
	if !allowJoin {
		return workflowexpr.ValueExpressionOptions{}
	}
	return workflowexpr.ValueExpressionOptions{AllowJoin: true, JoinResultType: frame.joinResultType}
}

func (e *Executor) joinPlan(req ExecutionRequest) (runtimecontracts.WorkflowJoinPlan, bool) {
	if e == nil || e.deps.Source == nil {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	requestFlowID := strings.TrimSpace(req.FlowID.String())
	if plan, ok := semanticview.WorkflowJoinPlanForHandler(e.deps.Source, requestFlowID, req.NodeID.String(), req.HandlerEventKey); ok {
		return plan, true
	}
	if requestFlowID == "" {
		return semanticview.WorkflowJoinPlanForHandler(e.deps.Source, strings.TrimSpace(e.deps.Source.WorkflowName()), req.NodeID.String(), req.HandlerEventKey)
	}
	return runtimecontracts.WorkflowJoinPlan{}, false
}

func (e *Executor) stepProjection(frame *executionFrame) error {
	if frame.projectionApplied {
		return nil
	}
	if !accumulatorProjectionEligible(frame) {
		return nil
	}
	handlerEventType := handlerAccumulatorEventType(frame.req)
	result := accprojection.ForHandlerWithAccumulator(e.deps.Source, frame.req.FlowID.String(), frame.req.NodeID.String(), string(handlerEventType), activeAccumulatorName(frame.req.Handler))
	if len(result.Issues) > 0 {
		return fmt.Errorf("accumulator projection declarations are invalid: %s", result.Issues[0].Message)
	}
	if len(result.Bindings) == 0 {
		if result.ExpectedBindingCount > 0 {
			return fmt.Errorf("runtime_invariant_violation: materialize_from binding declared for node %s accumulator %s but no accumulator buffer resolved at runtime for event %s; likely event identity drift between verify-time declaration and execution-time lookup", frame.req.NodeID.String(), result.ActiveAccumulatorName, string(frame.req.Event.Type()))
		}
		return nil
	}
	accumulateSpec, effectiveErr := e.effectiveAccumulatorSpec(frame, frame.req.Handler.Accumulate)
	if effectiveErr != nil {
		return effectiveErr
	}
	bucketRef, matched, bucketErr := e.resolveAccumulatorBucketRef(frame, accumulateSpec)
	if bucketErr != nil {
		return bucketErr
	}
	if !matched {
		return nil
	}
	acc, ok := loadAccumulatorForBucket(frame.state.State, bucketRef)
	if !ok {
		return fmt.Errorf("accumulator projection source missing for node %s event %s", frame.req.NodeID.String(), string(handlerEventType))
	}
	for _, binding := range result.Bindings {
		projected, err := e.projectAccumulatorItems(frame, binding, acc.Items)
		if err != nil {
			return err
		}
		if err := e.writeStepValue(frame, "entity."+binding.TargetField, projected); err != nil {
			return fmt.Errorf("materialize_from %s.%s: %w", binding.SourceNodeID, binding.AccumulatorName, err)
		}
	}
	frame.projectionApplied = true
	return nil
}

func activeAccumulatorName(handler runtimecontracts.SystemNodeEventHandler) string {
	if handler.Accumulate == nil {
		return ""
	}
	return strings.TrimSpace(handler.Accumulate.Into)
}

func accumulatorProjectionEligible(frame *executionFrame) bool {
	return frame != nil && frame.req.Handler.Accumulate != nil
}

func (e *Executor) resolveAccumulatorBucketRef(frame *executionFrame, spec *runtimecontracts.AccumulateSpec) (timeridentity.AccumulatorBucketRef, bool, error) {
	if frame == nil {
		return timeridentity.AccumulatorBucketRef{}, false, nil
	}
	if frame.hasAccumulatorBucketRef {
		return frame.accumulatorBucketRef, true, nil
	}
	bucketRef, err := handlerAccumulatorBucketRefForSpec(frame.req, e.currentContext(frame), frame.state, spec)
	if err != nil {
		return timeridentity.AccumulatorBucketRef{}, false, err
	}
	if frame.loopActivation != nil {
		bucketRef.Generation = frame.loopActivation.Generation()
		bucketRef = bucketRef.Normalize()
	}
	frame.accumulatorBucketRef = bucketRef
	frame.hasAccumulatorBucketRef = true
	return bucketRef, true, nil
}

func (e *Executor) effectiveAccumulatorSpec(frame *executionFrame, spec *runtimecontracts.AccumulateSpec) (*runtimecontracts.AccumulateSpec, error) {
	if spec == nil {
		return nil, nil
	}
	if e == nil || e.deps.Source == nil || frame == nil {
		return spec, nil
	}
	return runtimeaccumulator.EffectiveSpecForHandler(
		e.deps.Source,
		frame.req.FlowID.String(),
		frame.req.NodeID.String(),
		string(handlerAccumulatorEventType(frame.req)),
		spec,
	)
}

func (e *Executor) projectAccumulatorItems(frame *executionFrame, binding accprojection.Binding, items []map[string]any) ([]any, error) {
	out := make([]any, 0, len(items))
	for idx, item := range items {
		typedView, err := accumulatorTypedView(binding, item)
		if err != nil {
			return nil, fmt.Errorf("materialize_from %s.%s item %d: %w", binding.SourceNodeID, binding.AccumulatorName, idx, err)
		}
		if len(binding.Project) == 0 {
			out = append(out, typedView)
			continue
		}
		projected, err := e.projectAccumulatorItem(frame, binding, typedView)
		if err != nil {
			return nil, fmt.Errorf("materialize_from %s.%s item %d: %w", binding.SourceNodeID, binding.AccumulatorName, idx, err)
		}
		out = append(out, projected)
	}
	return out, nil
}

func accumulatorTypedView(binding accprojection.Binding, item map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(binding.SourceNamedType.Fields))
	for fieldName := range binding.SourceNamedType.Fields {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			continue
		}
		value, ok := item[fieldName]
		if !ok {
			return nil, fmt.Errorf("source item missing required typed-view field %q", fieldName)
		}
		out[fieldName] = cloneProjectionValue(value)
	}
	return out, nil
}

func (e *Executor) projectAccumulatorItem(frame *executionFrame, binding accprojection.Binding, source map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(binding.TargetNamedType.Fields))
	for fieldName := range binding.TargetNamedType.Fields {
		rawExpr, ok := binding.Project[fieldName]
		if !ok {
			return nil, fmt.Errorf("project missing target field %q", fieldName)
		}
		value, err := e.evaluateProjectionExpression(frame, rawExpr, source)
		if err != nil {
			return nil, fmt.Errorf("project.%s: %w", fieldName, err)
		}
		out[fieldName] = value
	}
	return out, nil
}

func (e *Executor) evaluateProjectionExpression(frame *executionFrame, raw any, source map[string]any) (any, error) {
	expr, ok := raw.(string)
	if !ok {
		return cloneProjectionValue(raw), nil
	}
	expr = strings.TrimSpace(expr)
	if fieldName, ok := strings.CutPrefix(expr, "source."); ok {
		fieldName = strings.TrimSpace(fieldName)
		if _, reserved := accprojection.ReservedAccumulatorMetadata[fieldName]; reserved {
			return nil, fmt.Errorf("reserved accumulator metadata %q is not addressable through source.*", fieldName)
		}
		value, ok := source[fieldName]
		if !ok {
			return nil, fmt.Errorf("unknown source field %q", fieldName)
		}
		return cloneProjectionValue(value), nil
	}
	if policyPath, ok := strings.CutPrefix(expr, "policy."); ok {
		value, ok := lookupProjectionPath(e.currentContext(frame).Policy.Raw(), policyPath)
		if !ok {
			return nil, fmt.Errorf("unknown policy field %q", policyPath)
		}
		return cloneProjectionValue(value), nil
	}
	for _, forbidden := range []string{"entity.", "payload.", "event.", "fan_out.", "accumulated."} {
		if strings.HasPrefix(expr, forbidden) {
			return nil, fmt.Errorf("forbidden projection binding %q", expr)
		}
	}
	return expr, nil
}

func cloneProjectionValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneStringAnyMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneProjectionValue(item)
		}
		return out
	default:
		return value
	}
}

func lookupProjectionPath(raw map[string]any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false
	}
	current := any(raw)
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func (e *Executor) stepTransform(frame *executionFrame) error {
	activeEmits, err := e.selectedDeclarativeEmitSpecs(frame)
	if err != nil {
		return err
	}
	if len(activeEmits) != 1 {
		return nil
	}
	emitSpec := activeEmits[0].Spec
	if emitSpec.Empty() || !emitSpec.HasFields() {
		return nil
	}
	// Resolve emit.fields against the current execution context so
	// data_accumulation and rule-selected writes are visible to emitted payloads.
	transformed, err := emitFieldsPayload(e.currentContext(frame), frame.state, emitSpec, joinExpressionOptions(frame))
	if err != nil {
		return err
	}
	if len(transformed) == 0 {
		return nil
	}
	frame.state.Transformed = transformed
	return nil
}

func (e *Executor) stepEmits(frame *executionFrame) error {
	activeEmits, err := e.selectedDeclarativeEmitSpecs(frame)
	if err != nil {
		return err
	}
	if len(activeEmits) == 0 {
		return nil
	}
	seen := map[string]string{}
	for _, activeEmit := range activeEmits {
		emitSpec := activeEmit.Spec
		eventType := emitSpec.EventType()
		if eventType == "" {
			continue
		}
		eventType = e.resolveDeclarativeEmitEventType(frame, eventType)
		if previousSource := seen[eventType]; previousSource != "" {
			return fmt.Errorf("duplicate declarative emit event %q from %s and %s; additive on_success emits must be distinct from the selected rule emit", eventType, previousSource, activeEmit.Source)
		}
		seen[eventType] = activeEmit.Source
		emitSpec.Event = eventType
		payload := map[string]any{}
		if emitSpec.HasFields() && len(activeEmits) == 1 && len(frame.state.Transformed) > 0 {
			payload = frame.state.Transformed
		} else if emitSpec.HasFields() {
			transformed, err := emitFieldsPayload(e.currentContext(frame), frame.state, emitSpec, joinExpressionOptions(frame))
			if err != nil {
				return err
			}
			payload = transformed
		} else if len(activeEmits) == 1 && len(frame.state.Transformed) > 0 {
			payload = frame.state.Transformed
		}
		shaped, err := e.shapeEmitPayload(frame, eventType, payload)
		if err != nil {
			return err
		}
		if _, err = e.queueEmitIntentForSpec(frame, emitSpec, eventType, shaped); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) stepAction(frame *executionFrame) error {
	actionSpec := selectedActionSpec(frame.req.Handler, frame.rule, frame.ruleSource)
	actionKey := identity.NormalizeActionKey(actionSpec.ID)
	if actionKey.IsZero() {
		return nil
	}
	if e.deps.ActionRegistry != nil {
		entry, ok := e.deps.ActionRegistry.Action(actionKey)
		if !ok || !e.deps.ActionRegistry.IsExecutable(actionKey) {
			return fmt.Errorf("action %q is not executable", actionKey.String())
		}
		if strings.TrimSpace(entry.Emits) != "" {
			actionCtx := WithEmitSurface(frame.tx.Context(), EmitSurfaceAction)
			shaped, err := e.shapeEmitPayloadWithContext(actionCtx, frame, entry.Emits, frame.payload)
			if err != nil {
				return err
			}
			if _, err := e.queueEmitIntent(frame, entry.Emits, shaped); err != nil {
				return err
			}
		}
		if e.deps.ActionRunner != nil {
			execCtx := e.executionContext(frame, StepAction)
			handled, err := e.deps.ActionRunner.ExecuteAction(frame.tx.Context(), actionSpec, entry, execCtx)
			if err != nil {
				return err
			}
			if !handled && strings.TrimSpace(entry.Emits) == "" {
				return fmt.Errorf("action %q is not executable", actionKey.String())
			}
			if handled {
				if err := e.mergePersistedActionState(frame, execCtx.Request.State); err != nil {
					return err
				}
			}
		}
	}
	frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, actionKey.String())
	return nil
}

func (e *Executor) stepActivity(frame *executionFrame) error {
	activitySpec := selectedActivitySpec(frame.req.Handler, frame.rule, frame.ruleSource)
	if activitySpec.Empty() {
		return nil
	}
	if e.deps.Source == nil {
		return fmt.Errorf("%w: activity semantic source is required", ErrInvalidConfig)
	}
	toolID := strings.TrimSpace(activitySpec.Tool)
	tool, ok := e.deps.Source.ToolEntries()[toolID]
	if !ok {
		return fmt.Errorf("%w: activity tool %q is not declared", ErrInvalidConfig, toolID)
	}
	effectClass := runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)
	if !runtimecontracts.SupportedActivityEffectClass(effectClass) {
		return fmt.Errorf("%w: activity tool %q effect_class %q is not executable in Stage 1", ErrInvalidConfig, toolID, tool.EffectClass)
	}
	input := make(map[string]any, len(activitySpec.Input))
	base := e.currentContext(frame)
	for field, expr := range activitySpec.Input {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		value, ok, err := evalExpressionValue(base, frame.state, expr, workflowexpr.ValueExpressionOptions{})
		if err != nil {
			return fmt.Errorf("activity.input.%s: %w", field, err)
		}
		if !ok {
			continue
		}
		input[field] = value
	}
	semanticInput, err := canonicaljson.FromGo(input)
	if err != nil {
		return fmt.Errorf("admit activity input: %w", err)
	}
	if semanticInput.Kind() != semanticvalue.KindObject {
		return fmt.Errorf("activity input must be a semantic object")
	}
	ruleID := ""
	ruleIndex := -1
	if frame.rule != nil {
		ruleID = strings.TrimSpace(frame.rule.ID)
		ruleIndex = selectedRuleIndex(frame.req.Handler, frame.rule)
	}
	site := runtimecontracts.ActivitySite{
		FlowID:          frame.req.FlowID.String(),
		NodeID:          frame.req.NodeID.String(),
		HandlerEventKey: frame.req.HandlerEventKey,
		RuleID:          ruleID,
		RuleIndex:       ruleIndex,
		Spec:            activitySpec,
	}
	resultEvents := runtimecontracts.ActivityResultEventsForSite(site)
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(effectClass)
	sourceRoute := emitSourceRoute(frame)
	intent := ActivityIntent{
		ActivityID:       resultEvents.ActivityID,
		Tool:             toolID,
		Input:            semanticInput,
		EffectClass:      effectClass,
		SuccessEvent:     resultEvents.SuccessEvent,
		FailureEvent:     resultEvents.FailureEvent,
		RevisionEvent:    resultEvents.RevisionRequested,
		RejectedEvent:    resultEvents.Rejected,
		RetryMaxAttempts: defaults.MaxAttempts,
		RetryBackoff:     defaults.Backoff,
		ForkPolicy:       runtimecontracts.ActivityForkPolicyForEffectClass(effectClass),
		EntityID:         frame.req.EntityID,
		NodeID:           frame.req.NodeID,
		FlowID:           frame.req.FlowID,
		FlowInstance:     sourceRoute.FlowInstance,
		HandlerEventKey:  frame.req.HandlerEventKey,
		SourceEventID:    frame.req.Event.ID(),
		SourceRunID:      frame.req.Event.RunID(),
		SourceTaskID:     frame.req.Event.TaskID(),
		ParentEventID:    frame.req.Event.ParentEventID(),
		ChainDepth:       frame.req.ChainDepth,
		Attempt:          1,
		ExecutionMode:    frame.req.Event.ExecutionMode(),
	}.Normalized()
	if activitySpec.Approval != nil {
		intent.ApprovalDecision = strings.TrimSpace(activitySpec.Approval.Decision)
	}
	if frame.loopActivation != nil {
		intent.Generation = frame.loopActivation.Generation()
		intent.LoopStage = frame.loopActivation.CurrentStage
	}
	frame.result.ActivityIntents = append(frame.result.ActivityIntents, intent)
	return nil
}

func (e *Executor) mergePersistedActionState(frame *executionFrame, baseline StateSnapshot) error {
	if e == nil || e.deps.StateRepo == nil || frame == nil || frame.req.EntityID.IsZero() {
		return nil
	}
	persisted, ok, err := e.deps.StateRepo.LoadState(frame.tx.Context(), frame.req.EntityID)
	if err != nil || !ok {
		return err
	}
	metadata := cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	for key, value := range persisted.StateCarrier.Metadata {
		if baselineValue, ok := baseline.StateCarrier.Metadata[key]; !ok || !reflect.DeepEqual(baselineValue, value) {
			metadata[key] = value
		}
	}
	gates := mapsClone(frame.state.State.StateCarrier.Gates)
	for key, value := range persisted.StateCarrier.Gates {
		if baselineValue, ok := baseline.StateCarrier.Gates[key]; !ok || baselineValue != value {
			gates[key] = value
		}
	}
	buckets := cloneStateBucketSet(frame.state.State.StateCarrier.StateBuckets)
	for key, bucket := range persisted.StateCarrier.StateBuckets {
		currentBucket := cloneStringAnyMap(buckets[key])
		baselineBucket := baseline.StateCarrier.StateBuckets[key]
		for bucketKey, value := range bucket {
			if baselineValue, ok := baselineBucket[bucketKey]; !ok || !reflect.DeepEqual(baselineValue, value) {
				currentBucket[bucketKey] = value
			}
		}
		if len(currentBucket) > 0 {
			buckets[key] = currentBucket
		}
	}
	frame.state.State.StateCarrier.Metadata = metadata
	frame.state.State.StateCarrier.Gates = gates
	frame.state.State.StateCarrier.StateBuckets = buckets
	if strings.TrimSpace(frame.state.State.CurrentState) == "" {
		frame.state.State.CurrentState = strings.TrimSpace(persisted.CurrentState)
	}
	if strings.TrimSpace(frame.state.State.WorkflowName) == "" {
		frame.state.State.WorkflowName = strings.TrimSpace(persisted.WorkflowName)
	}
	if strings.TrimSpace(frame.state.State.WorkflowVersion) == "" {
		frame.state.State.WorkflowVersion = strings.TrimSpace(persisted.WorkflowVersion)
	}
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(metadata)
	frame.result.StateMutation.StateCarrier.Gates = gates
	frame.result.StateMutation.SetStateBuckets(buckets)
	return nil
}

func (e *Executor) stepClear(frame *executionFrame) error {
	spec := frame.req.Handler.Clear
	if spec == nil {
		return nil
	}
	for _, target := range spec.Targets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		switch target {
		case "accumulator_state":
			if !frame.req.NodeID.IsZero() {
				if bucket, ok := frame.state.State.StateBucket(frame.req.NodeID.String()); ok {
					delete(bucket.Raw(), handlerAccumulatorBucketKey)
				}
			}
			delete(frame.state.State.StateCarrier.Metadata, "accumulated_count")
			delete(frame.state.State.StateCarrier.Metadata, "accumulated_total")
			delete(frame.state.State.StateCarrier.Metadata, "received_items")
		case "pending_dedup":
			delete(frame.state.State.StateCarrier.Metadata, "dedup_key")
		default:
			if err := e.clearStepValue(frame, target); err != nil {
				return err
			}
		}
	}
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	frame.result.StateMutation.SetStateBuckets(frame.state.State.StateCarrier.StateBuckets)
	return nil
}

func (e *Executor) buildTimerIntents(frame *executionFrame) []TimerIntent {
	if strings.TrimSpace(frame.result.CurrentState) == "" || strings.TrimSpace(frame.result.NextState) == "" {
		return nil
	}
	return []TimerIntent{
		{
			Operation:    TimerCancel,
			Owner:        frame.req.NodeID,
			FromState:    frame.result.CurrentState,
			ToState:      frame.result.NextState,
			TriggerEvent: strings.TrimSpace(string(frame.req.Event.Type())),
		},
		{
			Operation:    TimerStart,
			Owner:        frame.req.NodeID,
			FromState:    frame.result.CurrentState,
			ToState:      frame.result.NextState,
			TriggerEvent: strings.TrimSpace(string(frame.req.Event.Type())),
		},
	}
}

func (e *Executor) persist(ctx context.Context, frame executionFrame) error {
	deliveryContext := events.DeliveryContextFromContext(ctx)
	if !deliveryContext.Empty() {
		for i := range frame.result.EmitIntents {
			if frame.result.EmitIntents[i].Context.Empty() {
				frame.result.EmitIntents[i].Context = deliveryContext
			}
		}
		for i := range frame.result.TimerIntents {
			if frame.result.TimerIntents[i].Context.Empty() {
				frame.result.TimerIntents[i].Context = deliveryContext
			}
		}
		for i := range frame.result.ActivityIntents {
			if frame.result.ActivityIntents[i].Context.Empty() {
				frame.result.ActivityIntents[i].Context = deliveryContext
			}
		}
	}
	if frame.result.StateMutation.StateCarrier.Metadata == nil {
		frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	}
	if frame.result.StateMutation.StateCarrier.StateBuckets == nil {
		frame.result.StateMutation.SetStateBuckets(frame.state.State.StateCarrier.StateBuckets)
	}
	if err := e.deps.StateRepo.SaveState(ctx, frame.req.EntityID, frame.result.StateMutation); err != nil {
		return err
	}
	if e.deps.TimerApplier != nil && len(frame.result.TimerIntents) > 0 {
		if err := e.deps.TimerApplier.ApplyTimerIntents(ctx, frame.req.EntityID, frame.result.TimerIntents); err != nil {
			return err
		}
	}
	if len(frame.result.ActivityIntents) > 0 {
		if e.deps.ActivityIntents == nil {
			return fmt.Errorf("%w: activity intent writer is required when handler declares activity", ErrInvalidConfig)
		}
		if err := e.deps.ActivityIntents.WriteActivityIntents(ctx, frame.result.ActivityIntents); err != nil {
			return err
		}
	}
	if len(frame.result.EmitIntents) == 0 {
		return nil
	}
	if err := e.verifyEmitPersistencePrerequisites(ctx, frame); err != nil {
		return err
	}
	return e.deps.Outbox.WriteOutbox(ctx, frame.result.EmitIntents)
}

func (e *Executor) verifyEmitPersistencePrerequisites(ctx context.Context, frame executionFrame) error {
	if e == nil || e.deps.EmitVerifier == nil || len(frame.result.EmitIntents) == 0 {
		return nil
	}
	prerequisites := e.emitPersistencePrerequisites(frame)
	if len(prerequisites.Fields) == 0 {
		return nil
	}
	return e.deps.EmitVerifier.VerifyEmitPersistence(ctx, frame.req.EntityID, prerequisites)
}

func (e *Executor) emitPersistencePrerequisites(frame executionFrame) EmitPersistencePrerequisites {
	seen := map[string]int{}
	fields := make([]EmitPersistenceFieldPrerequisite, 0, 4)
	appendField := func(target string) {
		field, path, ok := emitPersistenceFieldTarget(target)
		if !ok {
			return
		}
		prerequisite := EmitPersistenceFieldPrerequisite{Field: field}
		if expected, ok := lookupParsedPath(frame.state.State.StateCarrier.Metadata, path); ok {
			prerequisite.Expected = expected
			prerequisite.HasExpected = true
		}
		if idx, ok := seen[field]; ok {
			fields[idx] = prerequisite
			return
		}
		seen[field] = len(fields)
		fields = append(fields, prerequisite)
	}
	appendWrite := func(write runtimecontracts.WorkflowDataWrite) {
		if write.IsContainedOperation() {
			contract, ok := entityruntime.ResolveForFlow(e.deps.Source, frame.req.FlowID.String())
			if !ok {
				return
			}
			target, err := entityruntime.ResolveContainedOperationTarget(contract, write.Target(), string(write.Operation), !write.Key.IsZero(), !write.Index.IsZero())
			if err != nil {
				return
			}
			appendField("entity." + target.RootField)
			return
		}
		appendField(write.Target())
	}
	if frame.topLevelDataWritesApplied {
		for _, write := range frame.topLevelDataAccumulation.Writes {
			appendWrite(write)
		}
	}
	if frame.rule != nil {
		for _, write := range frame.rule.DataAccumulation.Writes {
			appendWrite(write)
		}
	}
	if spec := e.selectedCompute(&frame); spec != nil {
		appendField(spec.StoreAs)
	}
	return EmitPersistencePrerequisites{Fields: fields}
}

func emitPersistenceFieldTarget(target string) (string, paths.Path, bool) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", paths.Path{}, false
	}
	parsed := paths.Parse(target)
	if parsed.HasExplicitRoot() {
		switch parsed.Root {
		case paths.RootEntity, paths.RootMetadata:
			parsed = paths.Path{Segments: parsed.Segments}
		default:
			return "", paths.Path{}, false
		}
	}
	if len(parsed.Segments) == 0 {
		return "", paths.Path{}, false
	}
	field := strings.Join(parsed.Segments, ".")
	if strings.TrimSpace(field) == "" {
		return "", paths.Path{}, false
	}
	return field, parsed, true
}

func decodePayload(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return map[string]any{}
	}
	return payload
}

func encodePayload(payload map[string]any) (json.RawMessage, error) {
	if len(payload) == 0 {
		return json.RawMessage(`{}`), nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(encoded), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func handlerDeclaresConflictingCompletion(handler runtimecontracts.SystemNodeEventHandler) bool {
	if len(handler.Rules) == 0 {
		return false
	}
	return len(handler.OnComplete) > 0
}

func mergeStateSnapshots(base, loaded StateSnapshot) StateSnapshot {
	out := loaded
	if out.EntityID.IsZero() {
		out.EntityID = base.EntityID
	}
	if strings.TrimSpace(out.WorkflowName) == "" {
		out.WorkflowName = strings.TrimSpace(base.WorkflowName)
	}
	if strings.TrimSpace(out.WorkflowVersion) == "" {
		out.WorkflowVersion = strings.TrimSpace(base.WorkflowVersion)
	}
	if strings.TrimSpace(out.CurrentState) == "" {
		out.CurrentState = strings.TrimSpace(base.CurrentState)
	}
	if out.EnteredStateAt.IsZero() {
		out.EnteredStateAt = base.EnteredStateAt
	}
	if len(out.StateCarrier.Metadata) == 0 {
		out.StateCarrier.Metadata = cloneStringAnyMap(base.StateCarrier.Metadata)
	}
	if len(out.StateCarrier.Gates) == 0 && len(base.StateCarrier.Gates) > 0 {
		out.StateCarrier.Gates = mapsClone(base.StateCarrier.Gates)
	}
	if len(out.StateCarrier.StateBuckets) == 0 {
		out.StateCarrier.StateBuckets = cloneStateBucketSet(base.StateCarrier.StateBuckets)
	}
	if len(out.TimerState) == 0 && len(base.TimerState) > 0 {
		out.TimerState = append([]TimerState(nil), base.TimerState...)
	}
	return out
}

func (e *Executor) currentContext(frame *executionFrame) BaseContext {
	ctx := WithPayload(frame.base, frame.payload)
	ctx = WithEvent(ctx, frame.req.Event.ContextMap(frame.state.State.CurrentState))
	ctx = WithAccumulated(ctx, frame.state.Accumulated)
	ctx = WithFanOutItem(ctx, frame.state.FanOut)
	ctx = WithJoin(ctx, frame.state.Join)
	ctx = WithLoop(ctx, frame.state.Loop)
	ctx.Metadata = values.Wrap(cloneStringAnyMap(frame.state.State.StateCarrier.Metadata))
	ctx.Gates = values.Wrap(boolMapToAnyMap(frame.state.State.StateCarrier.Gates))
	ctx.Entity = values.Wrap(frame.state.State.EntityContext())
	ctx.PlatformEntity = values.Wrap(frame.state.State.PlatformEntityContext(contextFlowInstance(frame.state.State, frame.req.Event, frame.req.FlowID.String())))
	ctx.FlowID = firstNonEmpty(strings.TrimSpace(frame.state.State.WorkflowName), strings.TrimSpace(frame.req.FlowID.String()))
	ctx.Computed = values.Wrap(cloneStringAnyMap(frame.state.Computed))
	return ctx
}

func (e *Executor) writeStepValue(frame *executionFrame, target string, value any) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil
	}
	parsed := paths.Parse(target)
	switch parsed.Root {
	case paths.RootComputed:
		frame.state.SetComputed(strings.Join(parsed.Segments, "."), value)
		frame.result.SetComputed(strings.Join(parsed.Segments, "."), value)
		return nil
	case paths.RootAccumulated:
		frame.state.SetAccumulated(strings.Join(parsed.Segments, "."), value)
		return nil
	case paths.RootFanOut:
		frame.state.SetFanOut(strings.Join(parsed.Segments, "."), value)
		return nil
	case paths.RootJoin:
		return fmt.Errorf("join context is read-only")
	}
	if frame.state.State.StateCarrier.Metadata == nil {
		frame.state.State.StateCarrier.Metadata = map[string]any{}
	}
	storagePath := parsed
	if _, resolved, entityTarget, err := resolveHandlerEntityWriteTarget(e.deps.Source, frame.req.FlowID.String(), target); err != nil {
		return err
	} else if entityTarget {
		storagePath = paths.Parse(resolved.Path)
		if value != nil && len(resolved.Field.Path) != 0 {
			contract, ok := entityruntime.ResolveForFlow(e.deps.Source, frame.req.FlowID.String())
			if !ok {
				return fmt.Errorf("flow %s has no declared entity contract", strings.TrimSpace(frame.req.FlowID.String()))
			}
			normalized, normalizeErr := entityruntime.NormalizeFieldValue(contract, resolved.Path, value)
			if normalizeErr != nil {
				return normalizeErr
			}
			value = normalized
		}
	} else if parsed.HasExplicitRoot() {
		storagePath = paths.Path{Segments: parsed.Segments}
	}
	setParsedValuePath(frame.state.State.StateCarrier.Metadata, storagePath, value)
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	return nil
}

func (e *Executor) clearStepValue(frame *executionFrame, target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil
	}
	parsed := paths.Parse(target)
	if !parsed.HasExplicitRoot() {
		if _, resolved, entityTarget, err := resolveHandlerEntityWriteTarget(e.deps.Source, frame.req.FlowID.String(), target); err != nil {
			return err
		} else if entityTarget {
			executionDeletePath(frame.state.State.StateCarrier.Metadata, strings.Split(resolved.Path, "."))
			return nil
		}
		executionDeletePath(frame.state.State.StateCarrier.Metadata, strings.Split(target, "."))
		return nil
	}
	switch parsed.Root {
	case paths.RootComputed:
		delete(frame.state.Computed, strings.Join(parsed.Segments, "."))
		delete(frame.result.Computed, strings.Join(parsed.Segments, "."))
	case paths.RootAccumulated:
		delete(frame.state.Accumulated, strings.Join(parsed.Segments, "."))
	case paths.RootFanOut:
		delete(frame.state.FanOut, strings.Join(parsed.Segments, "."))
	case paths.RootJoin:
		return fmt.Errorf("join context is read-only")
	case paths.RootEntity, paths.RootMetadata:
		if parsed.Root == paths.RootEntity {
			if _, resolved, _, err := resolveHandlerEntityWriteTarget(e.deps.Source, frame.req.FlowID.String(), target); err != nil {
				return err
			} else {
				executionDeletePath(frame.state.State.StateCarrier.Metadata, strings.Split(resolved.Path, "."))
			}
			return nil
		}
		executionDeletePath(frame.state.State.StateCarrier.Metadata, parsed.Segments)
	default:
		delete(frame.state.State.StateCarrier.Metadata, target)
	}
	return nil
}

func (e *Executor) executionContext(frame *executionFrame, step Step) ExecutionContext {
	req := frame.req
	req.State = frame.state.State
	return ExecutionContext{
		Request:   req,
		Base:      e.currentContext(frame),
		Step:      step,
		Completed: append([]Step(nil), frame.result.ExecutedSteps...),
	}
}

func (e *Executor) evaluateGuardSpec(frame *executionFrame, spec *runtimecontracts.GuardSpec) (bool, []string, error) {
	if spec == nil {
		return true, nil, nil
	}
	if len(spec.Checks) > 0 {
		evaluated := make([]string, 0, len(spec.Checks))
		for _, check := range spec.Checks {
			passed, ids, err := e.evaluateGuardCheck(frame, check.ID, check.Check, spec.PolicyRef)
			evaluated = append(evaluated, ids...)
			if err != nil {
				return false, evaluated, err
			}
			if !passed {
				return false, evaluated, nil
			}
		}
		return true, evaluated, nil
	}
	return e.evaluateGuardCheck(frame, spec.ID, spec.Check, spec.PolicyRef)
}

func (e *Executor) evaluateGuardCheck(frame *executionFrame, id, check, policyRef string) (bool, []string, error) {
	id = strings.TrimSpace(id)
	check = strings.TrimSpace(check)
	if check != "" {
		passed, err := e.evaluator.EvalBool(check, e.currentContext(frame))
		if err == nil {
			evaluated := []string{check}
			if id != "" {
				evaluated = []string{id}
			}
			return passed, evaluated, nil
		}
		if err != ErrNotImplemented || id == "" {
			return false, []string{firstNonEmpty(id, check)}, err
		}
	}
	if id == "" {
		return true, nil, nil
	}
	guardKey := identity.NormalizeGuardKey(id)
	if e.deps.GuardRegistry == nil {
		return false, []string{id}, fmt.Errorf("guard %q requires runtime registry", id)
	}
	entry, ok := e.deps.GuardRegistry.Guard(guardKey)
	if !ok || !e.deps.GuardRegistry.IsExecutable(guardKey) {
		return false, []string{id}, fmt.Errorf("guard %q is not executable", id)
	}
	if strings.TrimSpace(entry.Check) != "" {
		passed, err := e.evaluator.EvalBool(entry.Check, e.currentContext(frame))
		if err == nil {
			return passed, []string{id}, nil
		}
		if err != ErrNotImplemented {
			return false, []string{id}, err
		}
	}
	if e.deps.GuardRunner != nil {
		execCtx := e.executionContext(frame, StepGuard)
		passed, handled, err := e.deps.GuardRunner.EvaluateGuard(frame.tx.Context(), guardKey, entry, execCtx)
		if handled || err != nil {
			return passed, []string{id}, err
		}
	}
	return false, []string{id}, fmt.Errorf("guard %q is not executable", id)
}

func (e *Executor) selectRule(frame *executionFrame, rules []runtimecontracts.HandlerRuleEntry) (*runtimecontracts.HandlerRuleEntry, error) {
	for idx := range rules {
		rule := &rules[idx]
		if rule.PolicyRow.Kind == runtimecontracts.PolicySheetRowKindLookup ||
			rule.PolicyRow.Kind == runtimecontracts.PolicySheetRowKindValidate ||
			rule.PolicyRow.Kind == runtimecontracts.PolicySheetRowKindModule {
			continue
		}
		condition := strings.TrimSpace(rule.Condition)
		if condition == "" || strings.EqualFold(condition, "else") {
			return rule, nil
		}
		passed, err := e.evaluator.EvalBool(condition, e.currentContext(frame))
		if err == ErrNotImplemented {
			continue
		}
		if err != nil {
			return nil, err
		}
		if passed {
			return rule, nil
		}
	}
	return nil, nil
}

func (e *Executor) applyRule(frame *executionFrame, rule *runtimecontracts.HandlerRuleEntry) {
	if rule == nil {
		return
	}
	if id := strings.TrimSpace(rule.ID); id != "" {
		frame.result.RuleID = id
	}
}

func (e *Executor) applyDataAccumulation(frame *executionFrame, spec runtimecontracts.WorkflowDataAccumulation) error {
	current := e.currentContext(frame)
	for _, write := range spec.Writes {
		if write.IsContainedOperation() {
			if err := e.applyContainedDataOperation(frame, current, write); err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", strings.TrimSpace(write.Target()), err)
			}
			continue
		}
		target := strings.TrimSpace(write.Target())
		if target == "" {
			continue
		}
		switch parsed := paths.Parse(target); parsed.Root {
		case paths.RootComputed, paths.RootAccumulated, paths.RootFanOut, paths.RootJoin, paths.RootGates, paths.RootEvent, paths.RootPayload, paths.RootPolicy:
			return fmt.Errorf("data_accumulation target %s: unsupported target scope", target)
		}
		if write.Value.HasLiteralValue() {
			if err := e.writeStepValue(frame, target, write.Value.Literal); err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", target, err)
			}
			continue
		}
		if write.Value.HasCELValue() {
			value, err := evalWorkflowValueExpression(current, frame.state, write.Value.CEL, joinExpressionOptions(frame))
			if err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", target, err)
			}
			if err := e.writeStepValue(frame, target, value); err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", target, err)
			}
			continue
		}
		source := strings.TrimSpace(write.Source())
		if source == "" {
			continue
		}
		if value, ok := lookupPath(cloneStringAnyMap(current.Payload.Raw()), source); ok {
			if err := e.writeStepValue(frame, target, value); err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", target, err)
			}
		}
	}
	frame.state.State.SetMetadata("last_data_accumulation_event", strings.TrimSpace(string(frame.req.Event.Type())))
	frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	frame.result.StateMutation.DataAccumulation = spec
	return nil
}

func (e *Executor) selectedCompute(frame *executionFrame) *runtimecontracts.ComputeSpec {
	if frame.rule != nil && frame.rule.Compute != nil {
		return frame.rule.Compute
	}
	return frame.req.Handler.Compute
}

type activeFanOutSpec struct {
	Spec   *runtimecontracts.FanOutSpec
	Source handlerRuleSource
}

func (a activeFanOutSpec) EmitSource() string {
	switch a.Source {
	case handlerRuleSourceRules:
		return "handler.rules.fan_out.emit"
	case handlerRuleSourceOnComplete:
		return "handler.on_complete.fan_out.emit"
	default:
		return "handler.fan_out.emit"
	}
}

func (e *Executor) selectedFanOut(frame *executionFrame) activeFanOutSpec {
	if frame.rule != nil && frame.rule.FanOut != nil {
		return activeFanOutSpec{Spec: frame.rule.FanOut, Source: frame.ruleSource}
	}
	return activeFanOutSpec{Spec: frame.req.Handler.FanOut}
}

type activeDeclarativeEmitSpec struct {
	Source string
	Spec   runtimecontracts.EmitSpec
}

func (e *Executor) selectedDeclarativeEmitSpecs(frame *executionFrame) ([]activeDeclarativeEmitSpec, error) {
	active := selectedDeclarativeEmitSpecs(frame.req.Handler, frame.rule)
	for i := range active {
		lowered, err := e.lowerEmitSpecForFrame(frame, active[i].Source, active[i].Spec)
		if err != nil {
			return nil, err
		}
		active[i].Spec = lowered
	}
	return active, nil
}

func (e *Executor) lowerEmitSpecForFrame(frame *executionFrame, site string, spec runtimecontracts.EmitSpec) (runtimecontracts.EmitSpec, error) {
	if !runtimecontracts.EmitSpecNeedsFieldLowering(spec) {
		return spec, nil
	}
	if e == nil || e.deps.Source == nil || frame == nil {
		return runtimecontracts.EmitSpec{}, fmt.Errorf("%s uses emit field lowering sugar but runtime has no contract source for canonical lowering", strings.TrimSpace(site))
	}
	bundle, ok := semanticview.Bundle(e.deps.Source)
	if !ok || bundle == nil {
		return runtimecontracts.EmitSpec{}, fmt.Errorf("%s uses emit field lowering sugar but runtime source is not a workflow contract bundle", strings.TrimSpace(site))
	}
	triggerEvent := strings.TrimSpace(frame.req.HandlerEventKey)
	if triggerEvent == "" {
		triggerEvent = strings.TrimSpace(string(frame.req.Event.Type()))
	}
	return bundle.LowerEmitSpecFields(runtimecontracts.EmitFieldLoweringContext{
		NodeID:           frame.req.NodeID.String(),
		FlowID:           frame.req.FlowID.String(),
		TriggerEventType: triggerEvent,
		Site:             site,
	}, spec)
}

func selectedDeclarativeEmitSpecs(handler runtimecontracts.SystemNodeEventHandler, rule *runtimecontracts.HandlerRuleEntry) []activeDeclarativeEmitSpec {
	out := make([]activeDeclarativeEmitSpec, 0, 2)
	if rule != nil {
		if spec, ok := runtimecontracts.EffectiveRuleEmitTemplateSpec(handler, *rule); ok {
			out = append(out, activeDeclarativeEmitSpec{
				Source: "handler.rules.emit_template",
				Spec:   spec,
			})
		} else if !rule.Emit.Empty() {
			out = append(out, activeDeclarativeEmitSpec{
				Source: "handler.rules.emit",
				Spec:   rule.Emit,
			})
		}
	}
	if !handler.OnSuccess.Empty() {
		out = append(out, activeDeclarativeEmitSpec{
			Source: "handler.on_success.emit",
			Spec:   handler.OnSuccess.Emit,
		})
		return out
	}
	if rule != nil {
		return out
	}
	if !handler.Emit.Empty() {
		out = append(out, activeDeclarativeEmitSpec{
			Source: "handler.emit",
			Spec:   handler.Emit,
		})
	}
	return out
}

func selectedActionSpec(handler runtimecontracts.SystemNodeEventHandler, rule *runtimecontracts.HandlerRuleEntry, source handlerRuleSource) runtimecontracts.ActionSpec {
	if source == handlerRuleSourceRules && rule != nil && strings.TrimSpace(rule.Action.ID) != "" {
		return rule.Action
	}
	return handler.Action
}

func selectedActivitySpec(handler runtimecontracts.SystemNodeEventHandler, rule *runtimecontracts.HandlerRuleEntry, source handlerRuleSource) runtimecontracts.ActivitySpec {
	if source == handlerRuleSourceRules && rule != nil && !rule.Activity.Empty() {
		return rule.Activity
	}
	return handler.Activity
}

func selectedRuleIndex(handler runtimecontracts.SystemNodeEventHandler, selected *runtimecontracts.HandlerRuleEntry) int {
	if selected == nil {
		return -1
	}
	selectedID := strings.TrimSpace(selected.ID)
	for idx, rule := range handler.Rules {
		if selectedID != "" && strings.TrimSpace(rule.ID) == selectedID {
			return idx
		}
		if reflect.DeepEqual(rule, *selected) {
			return idx
		}
	}
	return -1
}

func (e *Executor) shapeEmitPayload(frame *executionFrame, eventType string, payload map[string]any) (map[string]any, error) {
	return e.shapeEmitPayloadWithContext(frame.tx.Context(), frame, eventType, payload)
}

func (e *Executor) shapeEmitPayloadWithContext(ctx context.Context, frame *executionFrame, eventType string, payload map[string]any) (map[string]any, error) {
	cloned := cloneStringAnyMap(payload)
	if e.deps.PayloadShaper == nil {
		return cloned, nil
	}
	req := frame.req
	req.State = frame.state.State
	return e.deps.PayloadShaper.ShapeEmitPayload(ctx, req, strings.TrimSpace(eventType), cloned)
}

func (e *Executor) resolveDeclarativeEmitEventType(frame *executionFrame, eventType string) string {
	eventType = runtimeeventidentity.Normalize(eventType)
	if eventType == "" || e == nil || e.deps.Source == nil || frame == nil {
		return eventType
	}
	flowID := strings.TrimSpace(frame.req.FlowID.String())
	if flowID == "" {
		return eventType
	}
	scope, ok := semanticview.FlowScopeByID(e.deps.Source, flowID)
	if !ok {
		return eventType
	}
	sourceRoute := emitSourceRoute(frame)
	namespacePath := emitNamespaceSourcePath(scope, sourceRoute.FlowInstance)
	localEvent := emitScopeLocalEventName(scope, namespacePath, eventType)
	if localEvent == "" {
		return eventType
	}
	if namespacePath == "" {
		return localEvent
	}
	return namespacePath + "/" + localEvent
}

func emitNamespaceSourcePath(scope semanticview.FlowScope, sourcePath string) string {
	scopePath := runtimeeventidentity.Normalize(scope.Path)
	sourcePath = runtimeeventidentity.Normalize(sourcePath)
	if scopePath == "" {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(scope.Mode), "template") || sourcePath == "" {
		return scopePath
	}
	if sourcePath == scopePath || strings.HasPrefix(sourcePath, scopePath+"/") {
		return sourcePath
	}
	return scopePath
}

func emitScopeLocalEventName(scope semanticview.FlowScope, sourcePath, eventType string) string {
	eventType = runtimeeventidentity.Normalize(eventType)
	if eventType == "" {
		return ""
	}
	localEvents := emitScopeLocalEvents(scope)
	if _, ok := localEvents[eventType]; ok {
		return eventType
	}
	for _, prefix := range []string{sourcePath, scope.Path} {
		prefix = runtimeeventidentity.Normalize(prefix)
		if prefix == "" || !strings.HasPrefix(eventType, prefix+"/") {
			continue
		}
		local := strings.TrimPrefix(eventType, prefix+"/")
		if _, ok := localEvents[local]; ok {
			return local
		}
	}
	return ""
}

func emitScopeLocalEvents(scope semanticview.FlowScope) map[string]struct{} {
	out := map[string]struct{}{}
	for _, eventType := range scope.OutputEvents {
		eventType = runtimeeventidentity.Normalize(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	return out
}

func (e *Executor) newEmitIntent(frame *executionFrame, spec runtimecontracts.EmitSpec, eventType string, payload map[string]any, chainDepth int) (EmitIntent, error) {
	encoded, err := encodePayload(payload)
	if err != nil {
		return EmitIntent{}, err
	}
	createdAt := nextPersistenceSafeEmitTime(e.emitNow(), frame.lastEmitCreatedAt)
	sourceRoute := emitSourceRoute(frame)
	entityID := sourceRoute.EntityID
	flowInstance := sourceRoute.FlowInstance
	envelope := events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: flowInstance,
	}
	if !sourceRoute.Empty() {
		envelope = events.EnvelopeForSourceRoute(envelope, sourceRoute)
	}
	resolution, err := e.resolveEmitRoute(frame, spec, eventType, sourceRoute, envelope)
	if err != nil {
		return EmitIntent{}, err
	}
	evt := events.NewChildEvent(
		"",
		events.EventType(strings.TrimSpace(eventType)),
		events.NodeProducer(frame.req.NodeID.String()),
		"",
		encoded,
		chainDepth,
		frame.req.Event,
		resolution.Envelope,
		createdAt,
	)
	frame.lastEmitCreatedAt = createdAt
	return EmitIntent{
		Event:         evt,
		ChainDepth:    chainDepth,
		ParentEventID: strings.TrimSpace(frame.req.Event.ID()),
	}, nil
}

const persistedEmitTimeResolution = time.Microsecond

func (e *Executor) emitNow() time.Time {
	if e != nil && e.deps.EmitNow != nil {
		if now := e.deps.EmitNow(); !now.IsZero() {
			return now.UTC()
		}
	}
	return time.Now().UTC()
}

func nextPersistenceSafeEmitTime(now, previous time.Time) time.Time {
	now = now.UTC().Truncate(persistedEmitTimeResolution)
	if previous.IsZero() {
		return now
	}
	previous = previous.UTC().Truncate(persistedEmitTimeResolution)
	if !now.After(previous) {
		return previous.Add(persistedEmitTimeResolution)
	}
	return now
}

func (e *Executor) resolveEmitRoute(frame *executionFrame, spec runtimecontracts.EmitSpec, eventType string, sourceRoute events.RouteIdentity, envelope events.EventEnvelope) (runtimepinrouting.Resolution, error) {
	if spec.EventType() == "" {
		spec.Event = strings.TrimSpace(eventType)
	}
	input := runtimepinrouting.ResolutionInput{
		Source:      e.deps.Source,
		FlowID:      strings.TrimSpace(frame.req.FlowID.String()),
		EventType:   strings.TrimSpace(eventType),
		Emit:        spec,
		SourceRoute: sourceRoute,
		Inbound:     frame.req.Event,
		ParentRoute: parentRouteFromState(frame.state.State.StateCarrier.Metadata),
	}
	if spec.Target.Kind == runtimecontracts.EmitTargetKindFlowMatch && len(spec.Target.Match) > 0 {
		values, err := e.resolveEmitTargetMatchValues(frame, spec.Target.Match)
		if err != nil {
			return runtimepinrouting.Resolution{}, err
		}
		input.MatchValues = values
	}
	if e.deps.TargetDescriptors != nil {
		descriptors, err := e.deps.TargetDescriptors(frame.tx.Context())
		if err != nil {
			return runtimepinrouting.Resolution{}, err
		}
		input.Descriptors = descriptors
	}
	resolution := runtimepinrouting.ResolveEnvelope(input, envelope)
	if err := runtimepinrouting.FailureError(resolution.Failure); err != nil {
		return runtimepinrouting.Resolution{}, err
	}
	return resolution, nil
}

func (e *Executor) resolveEmitTargetMatchValues(frame *executionFrame, match map[string]runtimecontracts.ExpressionValue) (map[string]string, error) {
	if len(match) == 0 {
		return nil, nil
	}
	values := make(map[string]string, len(match))
	base := e.currentContext(frame)
	for key, expr := range match {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value, ok, err := evalExpressionValue(base, frame.state, expr, workflowexpr.ValueExpressionOptions{})
		if err != nil {
			return nil, fmt.Errorf("emit target.match.%s: %w", key, err)
		}
		if !ok {
			continue
		}
		values[key] = strings.TrimSpace(asString(value))
		if values[key] == "" {
			values[key] = strings.TrimSpace(fmt.Sprint(value))
		}
	}
	return values, nil
}

func parentRouteFromState(metadata map[string]any) events.RouteIdentity {
	route := runtimeflowidentity.ParentRouteFromMetadata(metadata).Normalized()
	return events.RouteIdentity{
		FlowID:       route.FlowID,
		FlowInstance: route.FlowInstance,
		EntityID:     route.EntityID,
	}.Normalized()
}

func emitSourceRoute(frame *executionFrame) events.RouteIdentity {
	if frame == nil {
		return events.RouteIdentity{}
	}
	flowInstance := firstNonEmpty(
		normalizedFlowInstanceCandidate(asString(frame.req.State.StateCarrier.Metadata["flow_path"])),
		normalizedFlowInstanceCandidate(frame.req.Event.FlowInstance()),
	)
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(frame.req.FlowID.String()),
		FlowInstance: flowInstance,
		EntityID:     strings.TrimSpace(frame.req.EntityID.String()),
	}.Normalized()
}

func normalizedFlowInstanceCandidate(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}

func (e *Executor) queueEmitIntent(frame *executionFrame, eventType string, payload map[string]any) (bool, error) {
	return e.queueEmitIntentForSpec(frame, runtimecontracts.EmitSpec{Event: strings.TrimSpace(eventType)}, eventType, payload)
}

func (e *Executor) queueEmitIntentForSpec(frame *executionFrame, spec runtimecontracts.EmitSpec, eventType string, payload map[string]any) (bool, error) {
	nextDepth, err := nextChainDepth(frame.req.ChainDepth, e.MaxChainDepth())
	if err != nil {
		if err != ErrChainDepthExceeded {
			return false, err
		}
		intent, intentErr := e.newEmitIntent(frame, spec, eventType, payload, nextDepth)
		if intentErr != nil {
			return false, intentErr
		}
		intent.DeadLetterHint = "chain_depth_exceeded"
		frame.result.DeadLetterIntents = append(frame.result.DeadLetterIntents, intent)
		return false, nil
	}
	intent, err := e.newEmitIntent(frame, spec, eventType, payload, nextDepth)
	if err != nil {
		return false, err
	}
	frame.result.EmitIntents = append(frame.result.EmitIntents, intent)
	frame.result.ChainDepth = nextDepth
	return true, nil
}

func fanOutEventType(spec *runtimecontracts.FanOutSpec) string {
	if spec == nil {
		return ""
	}
	return spec.Emit.EventType()
}

func (e *Executor) resolveClearGates(frame *executionFrame) []string {
	if !slices.Contains(frame.req.Handler.ClearGates, "*") {
		return frame.req.Handler.ClearGates
	}
	if len(frame.state.State.StateCarrier.Gates) == 0 {
		return nil
	}
	return normalizeStrings(stringSliceFromAny(mapsKeys(boolMapToAnyMap(frame.state.State.StateCarrier.Gates))))
}

func (e *Executor) applyGuardFailure(frame *executionFrame, spec *runtimecontracts.GuardSpec) error {
	failureSpec, err := spec.FailureSpec()
	if err != nil {
		return err
	}
	parsed, err := GuardFailureFromSpec(failureSpec)
	if err != nil {
		return err
	}
	frame.req.Handler.AdvancesTo = ""
	frame.req.Handler.SetsGate = nil
	frame.req.Handler.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{}
	frame.topLevelDataAccumulation = runtimecontracts.WorkflowDataAccumulation{}
	frame.req.Handler.Emit = runtimecontracts.EmitSpec{}
	switch parsed.Action {
	case GuardFailureReject:
		frame.result.Status = OutcomeRejected
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "reject")
		return nil
	case GuardFailureDiscard:
		frame.result.Status = OutcomeDiscarded
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "discard")
		return nil
	case GuardFailureKill:
		frame.result.Status = OutcomeKilled
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "kill")
		if killedState := e.killStateTarget(frame.req.FlowID.String()); killedState != "" {
			frame.result.NextState = killedState
			frame.state.State.CurrentState = killedState
			frame.result.StateMutation.NextState = killedState
		}
		return nil
	case GuardFailureEscalate:
		eventType := parsed.EventType
		frame.result.Status = OutcomeEscalated
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "escalate:"+eventType)
		emitSpec, err := e.lowerEmitSpecForFrame(frame, "guard.on_fail.escalate", failureSpec.EscalationEmitSpec())
		if err != nil {
			return err
		}
		emitSpec.Event = eventType
		payload := map[string]any{}
		transformed, err := emitFieldsPayload(e.currentContext(frame), frame.state, emitSpec, workflowexpr.ValueExpressionOptions{})
		if err != nil {
			return err
		}
		if len(transformed) > 0 {
			payload = transformed
		}
		shaped, err := e.shapeEmitPayload(frame, eventType, payload)
		if err != nil {
			return err
		}
		if _, err := e.queueEmitIntent(frame, eventType, shaped); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported guard on_fail action %q", failureSpec.Action)
	}
}

func (e *Executor) killStateTarget(flowID string) string {
	if e == nil || e.deps.Source == nil {
		return ""
	}
	flowID = strings.TrimSpace(flowID)
	terminals := e.deps.Source.FlowTerminalStages(flowID)
	states := e.deps.Source.FlowStates(flowID)
	if flowID != "" && len(terminals) == 0 && len(states) == 0 {
		terminals = e.deps.Source.FlowTerminalStages("")
		states = e.deps.Source.FlowStates("")
	}
	for _, stage := range terminals {
		if strings.EqualFold(strings.TrimSpace(stage), "killed") {
			return strings.TrimSpace(stage)
		}
	}
	for _, stage := range states {
		if strings.EqualFold(strings.TrimSpace(stage), "killed") {
			return strings.TrimSpace(stage)
		}
	}
	return ""
}

func mapsKeys(values map[string]any) []any {
	if len(values) == 0 {
		return nil
	}
	keys := make([]any, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
