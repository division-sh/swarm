package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/core/timeridentity"
	"swarm/internal/runtime/core/values"
)

type Step string

const (
	StepQuery      Step = "query"
	StepClearGates Step = "clear_gates"
	StepGuard      Step = "guard"
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
	StepTransform  Step = "transform"
	StepEmits      Step = "emits"
	StepAction     Step = "action"
	StepClear      Step = "clear"
)

var OrderedSteps = []Step{
	StepQuery,
	StepClearGates,
	StepGuard,
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
	StepTransform,
	StepEmits,
	StepAction,
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
	payload                   map[string]any
	topLevelDataAccumulation  runtimecontracts.WorkflowDataAccumulation
	topLevelDataWritesApplied bool
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
	if req.Handler.CreateEntity && req.Handler.Accumulate != nil {
		return fmt.Errorf("%w: handler declares both create_entity and accumulate", ErrInvalidConfig)
	}
	if err := validateHandlerComputeSpecs(req.Handler); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	return nil
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
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if err := validateComputeSpec(rule.Compute); err != nil {
				return err
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			if err := validateComputeSpec(handler.Accumulate.OnTimeout.Compute); err != nil {
				return err
			}
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
		return ExecutionResult{FailureClass: ClassifyFailure(err)}, err
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
		result  ExecutionResult
		intents []EmitIntent
	)
	err := e.deps.Locker.WithEntityLock(ctx, entityID, func(lockCtx context.Context) error {
		loaded, err := e.loadState(lockCtx, req)
		if err != nil {
			result.FailureClass = ClassifyFailure(err)
			return err
		}
		req.State = loaded
		return e.deps.TxRunner.Run(lockCtx, func(tx Tx) error {
			frame := e.newExecutionFrame(tx, req)
			if err := e.runSteps(&frame); err != nil {
				result = frame.result
				result.FailureClass = ClassifyFailure(err)
				return err
			}
			result = frame.result
			intents = append([]EmitIntent(nil), frame.result.EmitIntents...)
			return e.persist(tx.Context(), frame)
		})
	})
	if err != nil {
		if result.AccumulatorCompletionDiagnostics.Relevant {
			result.AccumulatorCompletionDiagnostics.CommitOutcome = AccumulatorCompletionCommitRolledBack
		}
		if errors.Is(err, ErrEmitPersistencePrerequisite) {
			result.Status = OutcomeRejected
		}
		if result.Status == OutcomeUnknown {
			result.Status = OutcomeRejected
		}
		return result, err
	}
	if result.AccumulatorCompletionDiagnostics.Relevant {
		result.AccumulatorCompletionDiagnostics.CommitOutcome = AccumulatorCompletionCommitCommitted
	}
	if len(intents) > 0 {
		if err := e.deps.Dispatcher.DispatchPostCommit(ctx, intents); err != nil {
			result.FailureClass = ClassifyFailure(err)
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
	payload := decodePayload(req.Event.Payload)
	if len(payload) == 0 {
		payload = map[string]any{}
	}
	base := BuildBaseContext(ContextBuilderInput{
		Source:  e.deps.Source,
		FlowID:  req.FlowID.String(),
		State:   state,
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
	case StepQuery:
		return false, e.stepQuery(frame)
	case StepClearGates:
		return false, e.stepClearGates(frame)
	case StepGuard:
		return e.stepGuard(frame)
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
	case StepTransform:
		return false, e.stepTransform(frame)
	case StepEmits:
		return false, e.stepEmits(frame)
	case StepAction:
		return false, e.stepAction(frame)
	case StepClear:
		return false, e.stepClear(frame)
	default:
		return false, nil
	}
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
			scope := newExecutionScope(item, frame.payload, frame.state.State.EntityContext(), current.Policy.Raw())
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
			scope := newExecutionScope(item, frame.payload, frame.state.State.EntityContext(), current.Policy.Raw())
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
	e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.query"), value)
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
	if err := e.applyGuardFailure(frame, spec.OnFail); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Executor) stepAccumulate(frame *executionFrame) (bool, error) {
	spec := frame.req.Handler.Accumulate
	if spec == nil {
		return false, nil
	}
	frame.result.AccumulatorCompletionDiagnostics.Relevant = true
	frame.result.AccumulatorCompletionDiagnostics.CompletionMode = spec.Completion.String()
	frame.result.AccumulatorCompletionDiagnostics.OnCompleteDeclared = len(frame.req.Handler.OnComplete) > 0 || len(spec.OnComplete) > 0
	frame.result.AccumulatorCompletionDiagnostics.EvaluationOutcome = AccumulatorCompletionEvaluationNotAttempted
	bucketRef := timeridentity.NewAccumulatorBucketRef(frame.req.NodeID.String(), string(frame.req.Event.Type))
	if isAccumulationTimeoutEvent(frame.req.Event.Type) {
		parsed, ok := accumulationTimeoutBucketRefFromPayload(frame.payload)
		if !ok || parsed.NodeID != frame.req.NodeID.String() {
			return false, nil
		}
		bucketRef = parsed
	}
	acc, ok := loadAccumulatorForBucket(frame.state.State, bucketRef)
	if !ok {
		acc = &Accumulator{}
	}
	if strings.TrimSpace(acc.StartedAt) == "" {
		acc.StartedAt = frame.req.Event.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	current := e.currentContext(frame)
	expectedIDs, expectedCount := expectedAccumulatorTargets(current, frame.state, spec.ExpectedPath, spec.ExpectedFrom)
	if len(expectedIDs) > 0 {
		acc.Expected = append([]string{}, expectedIDs...)
		acc.ExpectedCount = len(expectedIDs)
	} else if expectedCount > 0 {
		acc.ExpectedCount = expectedCount
	}
	if acc.Received == nil {
		acc.Received = map[string]bool{}
	}
	if !isAccumulationTimeoutEvent(frame.req.Event.Type) {
		arrivalID := dedupIdentifier(current, frame.state, frame.req.Event, spec)
		if arrivalID != "" && !acc.Received[arrivalID] {
			acc.Received[arrivalID] = true
			item := cloneStringAnyMap(frame.payload)
			if item == nil {
				item = map[string]any{}
			}
			item["event_id"] = strings.TrimSpace(frame.req.Event.ID)
			item["event_type"] = strings.TrimSpace(string(frame.req.Event.Type))
			item["source"] = strings.TrimSpace(frame.req.Event.SourceAgent)
			item["received_at"] = frame.req.Event.CreatedAt.UTC().Format(time.RFC3339Nano)
			acc.Items = append(acc.Items, item)
		}
	}
	acc.LastEventID = strings.TrimSpace(frame.req.Event.ID)
	acc.LastEventType = strings.TrimSpace(string(frame.req.Event.Type))
	acc.LastSource = strings.TrimSpace(frame.req.Event.SourceAgent)
	acc.LastReceivedAt = frame.req.Event.CreatedAt.UTC().Format(time.RFC3339Nano)
	frame.result.AccumulatorCompletionDiagnostics.ReceivedCount = len(acc.Received)
	frame.result.AccumulatorCompletionDiagnostics.ExpectedCount = acc.ExpectedCount
	if len(acc.Expected) > 0 {
		frame.result.AccumulatorCompletionDiagnostics.ExpectedCount = len(acc.Expected)
	}
	storeAccumulatorForBucket(&frame.state.State, bucketRef, acc)
	frame.result.StateMutation.SetStateBuckets(frame.state.State.StateCarrier.StateBuckets)
	frame.state.Accumulated = accumulatorExpressionValue(acc)
	if isAccumulationTimeoutEvent(frame.req.Event.Type) && spec.OnTimeout != nil {
		frame.rule = spec.OnTimeout
		e.applyRule(frame, spec.OnTimeout)
		return false, nil
	}
	complete, err := accumulatorComplete(acc, spec, func(expression string, extraVars map[string]any) (bool, error) {
		ctx := e.currentContext(frame)
		if accumulation, ok := extraVars["accumulation"].(map[string]any); ok {
			ctx = WithAccumulated(ctx, accumulation)
		}
		return e.evaluator.EvalBool(expression, ctx)
	})
	if err == ErrNotImplemented {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !complete {
		frame.result.Status = OutcomeWaiting
		return true, nil
	}
	frame.result.AccumulatorCompletionDiagnostics.CompletionReached = true
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
		scope := newExecutionScope(item, frame.payload, frame.state.State.EntityContext(), current.Policy.Raw())
		passed, err := compiled.Eval(scope)
		if err != nil {
			return err
		}
		if passed {
			filtered = append(filtered, item)
		}
	}
	e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.filter"), filtered)
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
	e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.reduce"), value)
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
		scope := newExecutionScope(item, frame.payload, frame.state.State.EntityContext(), current.Policy.Raw())
		passed, err := compiled.Eval(scope)
		if err != nil {
			return err
		}
		if passed {
			count++
		}
	}
	e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.count"), count)
	return nil
}

func (e *Executor) stepCompute(frame *executionFrame) error {
	spec := e.selectedCompute(frame)
	if spec == nil {
		return nil
	}
	acc, _ := loadAccumulator(frame.state.State, frame.req.NodeID, frame.req.Event.Type)
	value, err := computeValue(acc, frame.payload, spec)
	if err != nil {
		return err
	}
	field := normalizeStateField(spec.StoreAs)
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

func (e *Executor) stepFanOut(frame *executionFrame) (bool, error) {
	spec := e.selectedFanOut(frame)
	if spec == nil {
		return false, nil
	}
	itemsValue, _ := resolveContractPath(frame.base, frame.state, spec.ItemsPath, spec.ItemsFrom)
	items := sliceFromAny(itemsValue)
	frame.result.FanOutCount = len(items)
	frame.state.FanOut = map[string]any{}
	frame.state.SetFanOut("target", spec.Target)
	frame.state.SetFanOut("count", len(items))
	e.writeStepValue(frame, "entity.fan_out_count", len(items))
	if len(items) == 0 {
		return false, nil
	}
	for _, item := range items {
		eventType := fanOutEventType(spec, item)
		if eventType == "" {
			continue
		}
		payload := cloneStringAnyMap(frame.payload)
		if payload == nil {
			payload = map[string]any{}
		}
		payload["item"] = item
		if target := strings.TrimSpace(spec.Target); target != "" {
			payload["target"] = target
		}
		shaped, err := e.shapeEmitPayload(frame, eventType, payload)
		if err != nil {
			return false, err
		}
		if _, err := e.queueEmitIntent(frame, eventType, shaped); err != nil {
			return false, err
		}
	}
	if len(frame.result.EmitIntents) == 0 && len(frame.result.DeadLetterIntents) == 0 {
		return false, nil
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
		scope := newExecutionScope(item, frame.payload, frame.state.State.EntityContext(), current.Policy.Raw())
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
	e.writeStepValue(frame, firstNonEmpty(strings.TrimSpace(spec.StoreAs), "computed.group_by"), grouped)
	return nil
}

func (e *Executor) stepOnComplete(frame *executionFrame) error {
	rules := frame.req.Handler.OnComplete
	if len(rules) == 0 && frame.req.Handler.Accumulate != nil {
		rules = frame.req.Handler.Accumulate.OnComplete
	}
	if frame.result.AccumulatorCompletionDiagnostics.Relevant && len(rules) > 0 {
		frame.result.AccumulatorCompletionDiagnostics.OnCompleteDeclared = true
	}
	rule, err := e.selectRule(frame, rules)
	if err != nil {
		if frame.result.AccumulatorCompletionDiagnostics.Relevant {
			frame.result.AccumulatorCompletionDiagnostics.EvaluationOutcome = AccumulatorCompletionEvaluationFailed
		}
		return err
	}
	if frame.result.AccumulatorCompletionDiagnostics.Relevant && len(rules) > 0 {
		frame.result.AccumulatorCompletionDiagnostics.EvaluationOutcome = AccumulatorCompletionEvaluationSucceeded
	}
	if rule != nil {
		frame.rule = rule
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
	frame.result.NextState = next
	frame.state.State.CurrentState = next
	frame.result.StateMutation.NextState = next
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
	if ruleHasWrites {
		if err := e.applyDataAccumulation(frame, frame.rule.DataAccumulation); err != nil {
			return err
		}
	}
	if (frame.topLevelDataAccumulation.HasWrites() || strings.TrimSpace(frame.topLevelDataAccumulation.SourceEvent) != "") && (!frame.topLevelDataWritesApplied || ruleHasWrites) {
		if err := e.applyDataAccumulation(frame, frame.topLevelDataAccumulation); err != nil {
			return err
		}
		frame.topLevelDataWritesApplied = true
	}
	return nil
}

func (e *Executor) stepTransform(frame *executionFrame) error {
	// Resolve payload transforms against the current execution context so
	// data_accumulation and rule-selected writes are visible to emitted payloads.
	transformed, err := payloadTransform(e.currentContext(frame), frame.state, frame.req.Handler.PayloadTransform)
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
	eventTypes := append([]string{}, frame.req.Handler.Emits.Values()...)
	if frame.rule != nil && !frame.rule.Emits.Empty() {
		eventTypes = append(eventTypes, frame.rule.Emits.Values()...)
	}
	eventTypes = uniqueOrderedStrings(eventTypes)
	if len(eventTypes) == 0 {
		return nil
	}
	payload := frame.payload
	if len(frame.state.Transformed) > 0 {
		payload = frame.state.Transformed
	}
	for _, eventType := range eventTypes {
		shaped, err := e.shapeEmitPayload(frame, eventType, payload)
		if err != nil {
			return err
		}
		if _, err := e.queueEmitIntent(frame, eventType, shaped); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) stepAction(frame *executionFrame) error {
	actionKey := identity.NormalizeActionKey(frame.req.Handler.Action.ID)
	if actionKey.IsZero() {
		return nil
	}
	if e.deps.ActionRegistry != nil {
		entry, ok := e.deps.ActionRegistry.Action(actionKey)
		if !ok || !e.deps.ActionRegistry.IsExecutable(actionKey) {
			return fmt.Errorf("action %q is not executable", actionKey.String())
		}
		if strings.TrimSpace(entry.Emits) != "" {
			shaped, err := e.shapeEmitPayload(frame, entry.Emits, frame.payload)
			if err != nil {
				return err
			}
			if _, err := e.queueEmitIntent(frame, entry.Emits, shaped); err != nil {
				return err
			}
		}
		if e.deps.ActionRunner != nil {
			execCtx := e.executionContext(frame, StepAction)
			handled, err := e.deps.ActionRunner.ExecuteAction(frame.tx.Context(), frame.req.Handler.Action, entry, execCtx)
			if err != nil {
				return err
			}
			if !handled && strings.TrimSpace(entry.Emits) == "" {
				return fmt.Errorf("action %q is not executable", actionKey.String())
			}
		}
	}
	frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, actionKey.String())
	return nil
}

func (e *Executor) stepClear(frame *executionFrame) error {
	spec := frame.req.Handler.Clear
	if spec == nil {
		return nil
	}
	targets := append([]string{}, spec.Targets...)
	if target := strings.TrimSpace(spec.Target); target != "" {
		targets = append(targets, target)
	}
	for _, target := range targets {
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
		case "cycle_counters":
			delete(frame.state.State.StateCarrier.Metadata, "cycle_index")
		case "pending_dedup":
			delete(frame.state.State.StateCarrier.Metadata, "dedup_key")
		default:
			e.clearStepValue(frame, target)
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
			TriggerEvent: strings.TrimSpace(string(frame.req.Event.Type)),
		},
		{
			Operation:    TimerStart,
			Owner:        frame.req.NodeID,
			FromState:    frame.result.CurrentState,
			ToState:      frame.result.NextState,
			TriggerEvent: strings.TrimSpace(string(frame.req.Event.Type)),
		},
	}
}

func (e *Executor) persist(ctx context.Context, frame executionFrame) error {
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
	if frame.topLevelDataWritesApplied {
		for _, write := range frame.topLevelDataAccumulation.Writes {
			appendField(write.Target())
		}
	}
	if frame.rule != nil {
		for _, write := range frame.rule.DataAccumulation.Writes {
			appendField(write.Target())
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
	if len(handler.OnComplete) > 0 {
		return true
	}
	return handler.Accumulate != nil && len(handler.Accumulate.OnComplete) > 0
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
	ctx = WithAccumulated(ctx, frame.state.Accumulated)
	ctx = WithFanOutItem(ctx, frame.state.FanOut)
	ctx.Metadata = values.Wrap(cloneStringAnyMap(frame.state.State.StateCarrier.Metadata))
	ctx.Gates = values.Wrap(boolMapToAnyMap(frame.state.State.StateCarrier.Gates))
	ctx.Entity = values.Wrap(frame.state.State.EntityContext())
	ctx.Computed = values.Wrap(cloneStringAnyMap(frame.state.Computed))
	return ctx
}

func (e *Executor) writeStepValue(frame *executionFrame, target string, value any) {
	target = strings.TrimSpace(target)
	if target == "" {
		return
	}
	parsed := paths.Parse(target)
	switch parsed.Root {
	case paths.RootComputed:
		frame.state.SetComputed(strings.Join(parsed.Segments, "."), value)
		frame.result.SetComputed(strings.Join(parsed.Segments, "."), value)
	case paths.RootAccumulated:
		frame.state.SetAccumulated(strings.Join(parsed.Segments, "."), value)
	case paths.RootFanOut:
		frame.state.SetFanOut(strings.Join(parsed.Segments, "."), value)
	default:
		if frame.state.State.StateCarrier.Metadata == nil {
			frame.state.State.StateCarrier.Metadata = map[string]any{}
		}
		switch {
		case parsed.Root == paths.RootEntity || parsed.Root == paths.RootMetadata:
			setParsedValuePath(frame.state.State.StateCarrier.Metadata, paths.Path{Segments: parsed.Segments}, value)
		case parsed.HasExplicitRoot():
			setParsedValuePath(frame.state.State.StateCarrier.Metadata, paths.Path{Segments: parsed.Segments}, value)
		default:
			setParsedValuePath(frame.state.State.StateCarrier.Metadata, parsed, value)
		}
		frame.result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(frame.state.State.StateCarrier.Metadata)
	}
}

func (e *Executor) clearStepValue(frame *executionFrame, target string) {
	target = strings.TrimSpace(target)
	if target == "" {
		return
	}
	parsed := paths.Parse(target)
	if !parsed.HasExplicitRoot() {
		executionDeletePath(frame.state.State.StateCarrier.Metadata, strings.Split(target, "."))
		return
	}
	switch parsed.Root {
	case paths.RootComputed:
		delete(frame.state.Computed, strings.Join(parsed.Segments, "."))
		delete(frame.result.Computed, strings.Join(parsed.Segments, "."))
	case paths.RootAccumulated:
		delete(frame.state.Accumulated, strings.Join(parsed.Segments, "."))
	case paths.RootFanOut:
		delete(frame.state.FanOut, strings.Join(parsed.Segments, "."))
	case paths.RootEntity, paths.RootMetadata:
		executionDeletePath(frame.state.State.StateCarrier.Metadata, parsed.Segments)
	default:
		delete(frame.state.State.StateCarrier.Metadata, target)
	}
}

func (e *Executor) executionContext(frame *executionFrame, step Step) ExecutionContext {
	return ExecutionContext{
		Request:   frame.req,
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
		if frame.result.AccumulatorCompletionDiagnostics.Relevant {
			frame.result.AccumulatorCompletionDiagnostics.SelectedRuleID = id
		}
	}
}

func (e *Executor) applyDataAccumulation(frame *executionFrame, spec runtimecontracts.WorkflowDataAccumulation) error {
	if err := applyDataAccumulationToState(e.currentContext(frame), frame.state, &frame.state.State, spec); err != nil {
		return err
	}
	frame.state.State.SetMetadata("last_data_accumulation_event", strings.TrimSpace(string(frame.req.Event.Type)))
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

func (e *Executor) selectedFanOut(frame *executionFrame) *runtimecontracts.FanOutSpec {
	if frame.rule != nil && frame.rule.FanOut != nil {
		return frame.rule.FanOut
	}
	return frame.req.Handler.FanOut
}

func (e *Executor) shapeEmitPayload(frame *executionFrame, eventType string, payload map[string]any) (map[string]any, error) {
	cloned := cloneStringAnyMap(payload)
	if e.deps.PayloadShaper == nil {
		return cloned, nil
	}
	req := frame.req
	req.State = frame.state.State
	return e.deps.PayloadShaper.ShapeEmitPayload(frame.tx.Context(), req, strings.TrimSpace(eventType), cloned)
}

func (e *Executor) newEmitIntent(frame *executionFrame, eventType string, payload map[string]any, chainDepth int) (EmitIntent, error) {
	encoded, err := encodePayload(payload)
	if err != nil {
		return EmitIntent{}, err
	}
	createdAt := time.Now().UTC()
	if n := len(frame.result.EmitIntents); n > 0 {
		last := frame.result.EmitIntents[n-1].Event.CreatedAt
		if !last.IsZero() && !createdAt.After(last) {
			createdAt = last.Add(time.Nanosecond)
		}
	}
	entityID := strings.TrimSpace(firstNonEmpty(
		asString(payload["entity_id"]),
		frame.req.EntityID.String(),
	))
	evt := events.Event{
		Type:       events.EventType(strings.TrimSpace(eventType)),
		Payload:    encoded,
		ChainDepth: chainDepth,
		CreatedAt:  createdAt,
	}
	if entityID != "" {
		evt = evt.WithEntityID(entityID)
	}
	return EmitIntent{
		Event:         evt,
		ChainDepth:    chainDepth,
		ParentEventID: strings.TrimSpace(frame.req.Event.ID),
	}, nil
}

func (e *Executor) queueEmitIntent(frame *executionFrame, eventType string, payload map[string]any) (bool, error) {
	nextDepth, err := nextChainDepth(frame.req.ChainDepth, e.MaxChainDepth())
	if err != nil {
		if err != ErrChainDepthExceeded {
			return false, err
		}
		intent, intentErr := e.newEmitIntent(frame, eventType, payload, nextDepth)
		if intentErr != nil {
			return false, intentErr
		}
		intent.DeadLetterHint = "chain_depth_exceeded"
		frame.result.DeadLetterIntents = append(frame.result.DeadLetterIntents, intent)
		return false, nil
	}
	intent, err := e.newEmitIntent(frame, eventType, payload, nextDepth)
	if err != nil {
		return false, err
	}
	frame.result.EmitIntents = append(frame.result.EmitIntents, intent)
	frame.result.ChainDepth = nextDepth
	return true, nil
}

func fanOutEventType(spec *runtimecontracts.FanOutSpec, item any) string {
	if spec == nil {
		return ""
	}
	if len(spec.EmitMapping) > 0 {
		if keyField := strings.TrimSpace(spec.EmitMappingKey); keyField != "" {
			if value, ok := fanOutMappingValue(item, keyField); ok {
				if eventType, ok := spec.EmitMapping[strings.TrimSpace(asString(value))]; ok {
					return strings.TrimSpace(eventType)
				}
			}
		}
		for key, eventType := range spec.EmitMapping {
			if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(asString(item))) {
				return strings.TrimSpace(eventType)
			}
		}
		if obj, ok := asObject(item); ok {
			for _, raw := range obj {
				if eventType, ok := spec.EmitMapping[strings.TrimSpace(asString(raw))]; ok {
					return strings.TrimSpace(eventType)
				}
			}
		}
	}
	return strings.TrimSpace(spec.EmitPerItem)
}

func fanOutMappingValue(item any, keyField string) (any, bool) {
	keyField = strings.TrimSpace(strings.TrimPrefix(keyField, "item."))
	if keyField == "" {
		return nil, false
	}
	obj, ok := asObject(item)
	if !ok {
		return nil, false
	}
	current := any(obj)
	for _, segment := range strings.Split(keyField, ".") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, false
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := m[segment]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
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

func (e *Executor) applyGuardFailure(frame *executionFrame, action string) error {
	parsed, err := ParseGuardFailure(action)
	if err != nil {
		return err
	}
	frame.req.Handler.AdvancesTo = ""
	frame.req.Handler.SetsGate = nil
	frame.req.Handler.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{}
	frame.topLevelDataAccumulation = runtimecontracts.WorkflowDataAccumulation{}
	frame.req.Handler.Emits = runtimecontracts.EventEmission{}
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
		if killedState := e.killStateTarget(); killedState != "" {
			frame.result.NextState = killedState
			frame.state.State.CurrentState = killedState
			frame.result.StateMutation.NextState = killedState
		}
		return nil
	case GuardFailureEscalate:
		eventType := parsed.EventType
		frame.result.Status = OutcomeEscalated
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "escalate:"+eventType)
		shaped, err := e.shapeEmitPayload(frame, eventType, frame.payload)
		if err != nil {
			return err
		}
		if _, err := e.queueEmitIntent(frame, eventType, shaped); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported guard on_fail action %q", action)
	}
}

func (e *Executor) killStateTarget() string {
	if e == nil || e.deps.Source == nil {
		return ""
	}
	for _, stage := range e.deps.Source.WorkflowTerminalStages() {
		if strings.EqualFold(strings.TrimSpace(stage), "killed") {
			return strings.TrimSpace(stage)
		}
	}
	for _, stage := range e.deps.Source.WorkflowStages() {
		if strings.EqualFold(strings.TrimSpace(stage.ID), "killed") {
			return strings.TrimSpace(stage.ID)
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
