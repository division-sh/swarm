package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	"empireai/internal/runtime/core/values"
)

type Step string

const (
	StepClearGates Step = "clear_gates"
	StepGuard      Step = "guard"
	StepAccumulate Step = "accumulate"
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
)

var OrderedSteps = []Step{
	StepClearGates,
	StepGuard,
	StepAccumulate,
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
}

type Executor struct {
	deps      RuntimeDependencies
	evaluator Evaluator
}

type executionFrame struct {
	tx      Tx
	req     ExecutionRequest
	base    BaseContext
	state   ExecutionState
	result  ExecutionResult
	rule    *runtimecontracts.HandlerRuleEntry
	payload map[string]any
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
	if req.ChainDepth > e.MaxChainDepth() {
		return ErrChainDepthExceeded
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
	loaded, err := e.loadState(ctx, req)
	if err != nil {
		return ExecutionResult{FailureClass: ClassifyFailure(err)}, err
	}
	req.State = loaded

	var (
		result  ExecutionResult
		intents []EmitIntent
	)
	err = e.deps.Locker.WithEntityLock(ctx, entityID, func(lockCtx context.Context) error {
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
		if result.Status == OutcomeUnknown {
			result.Status = OutcomeRejected
		}
		return result, err
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
	normalizeSnapshotGates(&state)
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
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	normalizeSnapshotGates(&state)
	if state.StateBuckets == nil {
		state.StateBuckets = map[string]any{}
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
		tx:      tx,
		req:     req,
		base:    base,
		payload: payload,
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
	case StepClearGates:
		return false, e.stepClearGates(frame)
	case StepGuard:
		return e.stepGuard(frame)
	case StepAccumulate:
		return e.stepAccumulate(frame)
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
	default:
		return false, nil
	}
}

func (e *Executor) stepClearGates(frame *executionFrame) error {
	if len(frame.req.Handler.ClearGates) == 0 {
		return nil
	}
	frame.result.ClearGates = normalizeStrings(e.resolveClearGates(frame))
	frame.result.StateMutation.ClearGates = append([]string{}, frame.result.ClearGates...)
	frame.result.StateMutation.Metadata = cloneStringAnyMap(frame.state.State.Metadata)
	for _, gate := range frame.result.ClearGates {
		frame.state.State.SetGate(gate, false)
		frame.result.StateMutation.SetGateValue(gate, false)
	}
	frame.result.StateMutation.Metadata = cloneStringAnyMap(frame.state.State.Metadata)
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
	acc, ok := loadAccumulator(frame.state.State, frame.req.NodeID, frame.req.Event.Type)
	if !ok {
		acc = &Accumulator{}
	}
	expectedIDs, expectedCount := expectedAccumulatorTargets(frame.base, frame.state, spec.ExpectedPath, spec.ExpectedFrom)
	if len(expectedIDs) > 0 {
		acc.Expected = append([]string{}, expectedIDs...)
		acc.ExpectedCount = len(expectedIDs)
	} else if expectedCount > 0 {
		acc.ExpectedCount = expectedCount
	}
	if acc.Received == nil {
		acc.Received = map[string]bool{}
	}
	arrivalID := dedupIdentifier(frame.base, frame.state, frame.req.Event, spec)
	if arrivalID != "" && !acc.Received[arrivalID] {
		acc.Received[arrivalID] = true
		acc.Items = append(acc.Items, map[string]any{
			"event_id":    strings.TrimSpace(frame.req.Event.ID),
			"event_type":  strings.TrimSpace(string(frame.req.Event.Type)),
			"source":      strings.TrimSpace(frame.req.Event.SourceAgent),
			"payload":     cloneStringAnyMap(frame.payload),
			"received_at": frame.req.Event.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	acc.LastEventID = strings.TrimSpace(frame.req.Event.ID)
	acc.LastEventType = strings.TrimSpace(string(frame.req.Event.Type))
	acc.LastSource = strings.TrimSpace(frame.req.Event.SourceAgent)
	acc.LastReceivedAt = frame.req.Event.CreatedAt.UTC().Format(time.RFC3339Nano)
	storeAccumulator(&frame.state.State, frame.req.NodeID, frame.req.Event.Type, acc)
	frame.result.StateMutation.SetStateBuckets(frame.state.State.StateBuckets)
	frame.state.SetAccumulated(frame.req.NodeID.String(), accumulatorExpressionValue(acc))
	complete, err := accumulatorComplete(acc, spec, func(expression string, extraVars map[string]any) (bool, error) {
		ctx := e.currentContext(frame)
		if accumulation, ok := extraVars["accumulation"].(map[string]any); ok {
			ctx = WithAccumulated(ctx, accumulation)
		}
		return e.evaluator.EvalBool(rewriteExpression(expression), ctx)
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
	return false, nil
}

func (e *Executor) stepCompute(frame *executionFrame) error {
	spec := frame.req.Handler.Compute
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
	frame.result.StateMutation.Metadata = cloneStringAnyMap(frame.state.State.Metadata)
	frame.result.StateMutation.SetMetadata(field, value)
	return nil
}

func (e *Executor) stepFanOut(frame *executionFrame) (bool, error) {
	spec := frame.req.Handler.FanOut
	if spec == nil {
		return false, nil
	}
	itemsValue, _ := resolveContractPath(frame.base, frame.state, spec.ItemsPath, spec.ItemsFrom)
	items := sliceFromAny(itemsValue)
	if len(items) == 0 {
		return false, nil
	}
	frame.result.FanOutCount = len(items)
	frame.state.FanOut = map[string]any{}
	frame.state.SetFanOut("target", spec.Target)
	frame.state.SetFanOut("count", len(items))
	nextDepth, err := nextChainDepth(frame.req.ChainDepth, e.MaxChainDepth())
	if err != nil {
		frame.result.FailureClass = FailureDeadLetter
		return false, err
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
		intent, err := e.newEmitIntent(frame, eventType, shaped, nextDepth)
		if err != nil {
			return false, err
		}
		frame.result.EmitIntents = append(frame.result.EmitIntents, intent)
	}
	if len(frame.result.EmitIntents) == 0 {
		return false, nil
	}
	frame.result.ChainDepth = nextDepth
	frame.result.Status = OutcomeFannedOut
	return true, nil
}

func (e *Executor) stepOnComplete(frame *executionFrame) error {
	rules := frame.req.Handler.OnComplete
	if len(rules) == 0 && frame.req.Handler.Accumulate != nil {
		rules = frame.req.Handler.Accumulate.OnComplete
	}
	rule, err := e.selectRule(frame, rules)
	if err != nil {
		return err
	}
	if rule != nil {
		frame.rule = rule
		e.applyRule(frame, rule)
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
	frame.result.StateMutation.Metadata = cloneStringAnyMap(frame.state.State.Metadata)
	frame.state.State.SetGate(name, true)
	frame.result.StateMutation.SetGateValue(name, true)
	frame.result.StateMutation.Metadata = cloneStringAnyMap(frame.state.State.Metadata)
	return nil
}

func (e *Executor) stepDataWrites(frame *executionFrame) error {
	spec := frame.req.Handler.DataAccumulation
	if !spec.HasWrites() && strings.TrimSpace(spec.SourceEvent) == "" {
		return nil
	}
	applyDataAccumulationToState(&frame.state.State, frame.payload, spec)
	frame.state.State.SetMetadata("last_data_accumulation_event", strings.TrimSpace(string(frame.req.Event.Type)))
	frame.result.StateMutation.Metadata = cloneStringAnyMap(frame.state.State.Metadata)
	frame.result.StateMutation.DataAccumulation = spec
	return nil
}

func (e *Executor) stepTransform(frame *executionFrame) error {
	transformed := payloadTransform(frame.base, frame.state, frame.req.Handler.PayloadTransform)
	if len(transformed) == 0 {
		return nil
	}
	frame.state.Transformed = transformed
	return nil
}

func (e *Executor) stepEmits(frame *executionFrame) error {
	eventTypes := frame.req.Handler.Emits.Values()
	if frame.rule != nil {
		eventTypes = frame.rule.Emits.Values()
	}
	if len(eventTypes) == 0 {
		return nil
	}
	nextDepth, err := nextChainDepth(frame.req.ChainDepth, e.MaxChainDepth())
	if err != nil {
		frame.result.FailureClass = FailureDeadLetter
		return err
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
		intent, err := e.newEmitIntent(frame, eventType, shaped, nextDepth)
		if err != nil {
			return err
		}
		frame.result.EmitIntents = append(frame.result.EmitIntents, intent)
	}
	frame.result.ChainDepth = nextDepth
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
			nextDepth, err := nextChainDepth(frame.req.ChainDepth, e.MaxChainDepth())
			if err != nil {
				frame.result.FailureClass = FailureDeadLetter
				return err
			}
			intent, err := e.newEmitIntent(frame, entry.Emits, shaped, nextDepth)
			if err != nil {
				return err
			}
			frame.result.EmitIntents = append(frame.result.EmitIntents, intent)
			frame.result.ChainDepth = nextDepth
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
	return e.deps.Outbox.WriteOutbox(ctx, frame.result.EmitIntents)
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
	if len(out.Metadata) == 0 {
		out.Metadata = cloneStringAnyMap(base.Metadata)
	}
	if len(out.Gates) == 0 && len(base.Gates) > 0 {
		out.Gates = mapsClone(base.Gates)
	}
	if len(out.StateBuckets) == 0 {
		out.StateBuckets = cloneStringAnyMap(base.StateBuckets)
	}
	if len(out.TimerState) == 0 && len(base.TimerState) > 0 {
		out.TimerState = append([]TimerState(nil), base.TimerState...)
	}
	normalizeSnapshotGates(&out)
	return out
}

func (e *Executor) currentContext(frame *executionFrame) BaseContext {
	ctx := WithPayload(frame.base, frame.payload)
	ctx = WithAccumulated(ctx, frame.state.Accumulated)
	ctx = WithFanOutItem(ctx, frame.state.FanOut)
	ctx.Metadata = values.Wrap(cloneStringAnyMap(frame.state.State.Metadata))
	ctx.Gates = values.Wrap(boolMapToAnyMap(frame.state.State.Gates))
	ctx.Entity = values.Wrap(frame.state.State.EntityContext())
	return ctx
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
		passed, err := e.evaluator.EvalBool(rewriteExpression(check), e.currentContext(frame))
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
		passed, err := e.evaluator.EvalBool(rewriteExpression(entry.Check), e.currentContext(frame))
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
		passed, err := e.evaluator.EvalBool(rewriteExpression(condition), e.currentContext(frame))
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
	if next := strings.TrimSpace(rule.AdvancesTo); next != "" {
		frame.req.Handler.AdvancesTo = next
	}
	if !rule.Emits.Empty() {
		frame.req.Handler.Emits = rule.Emits
	}
	if rule.DataAccumulation.HasWrites() || strings.TrimSpace(rule.DataAccumulation.SourceEvent) != "" {
		frame.req.Handler.DataAccumulation = rule.DataAccumulation
	}
}

func (e *Executor) shapeEmitPayload(frame *executionFrame, eventType string, payload map[string]any) (map[string]any, error) {
	cloned := cloneStringAnyMap(payload)
	if e.deps.PayloadShaper == nil {
		return cloned, nil
	}
	return e.deps.PayloadShaper.ShapeEmitPayload(frame.tx.Context(), frame.req, strings.TrimSpace(eventType), cloned)
}

func (e *Executor) newEmitIntent(frame *executionFrame, eventType string, payload map[string]any, chainDepth int) (EmitIntent, error) {
	encoded, err := encodePayload(payload)
	if err != nil {
		return EmitIntent{}, err
	}
	return EmitIntent{
		Event: events.Event{
			Type:    events.EventType(strings.TrimSpace(eventType)),
			Payload: encoded,
		}.WithEntityID(frame.req.EntityID.String()),
		ChainDepth:    chainDepth,
		ParentEventID: strings.TrimSpace(frame.req.Event.ID),
	}, nil
}

func fanOutEventType(spec *runtimecontracts.FanOutSpec, item any) string {
	if spec == nil {
		return ""
	}
	if len(spec.EmitMapping) > 0 {
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

func (e *Executor) resolveClearGates(frame *executionFrame) []string {
	if !slices.Contains(frame.req.Handler.ClearGates, "*") {
		return frame.req.Handler.ClearGates
	}
	if len(frame.state.State.Gates) == 0 {
		return nil
	}
	return normalizeStrings(stringSliceFromAny(mapsKeys(boolMapToAnyMap(frame.state.State.Gates))))
}

func (e *Executor) applyGuardFailure(frame *executionFrame, action string) error {
	parsed, err := ParseGuardFailure(action)
	if err != nil {
		return err
	}
	frame.req.Handler.AdvancesTo = ""
	frame.req.Handler.SetsGate = nil
	frame.req.Handler.DataAccumulation = runtimecontracts.WorkflowDataAccumulation{}
	frame.req.Handler.Emits = runtimecontracts.EventEmission{}
	switch parsed.Action {
	case GuardFailureReject:
		frame.result.Status = OutcomeRejected
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "reject")
		return nil
	case GuardFailureBlocked:
		frame.result.Status = OutcomeBlocked
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "blocked")
		return nil
	case GuardFailureDiscard:
		frame.result.Status = OutcomeDiscarded
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "discard")
		return nil
	case GuardFailureKill:
		frame.result.Status = OutcomeKilled
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "kill")
		return nil
	case GuardFailureEscalate:
		eventType := parsed.EventType
		nextDepth, err := nextChainDepth(frame.req.ChainDepth, e.MaxChainDepth())
		if err != nil {
			frame.result.FailureClass = FailureDeadLetter
			return err
		}
		frame.result.Status = OutcomeEscalated
		frame.result.ActionsExecuted = append(frame.result.ActionsExecuted, "escalate:"+eventType)
		shaped, err := e.shapeEmitPayload(frame, eventType, frame.payload)
		if err != nil {
			return err
		}
		intent, err := e.newEmitIntent(frame, eventType, shaped, nextDepth)
		if err != nil {
			return err
		}
		frame.result.EmitIntents = append(frame.result.EmitIntents, intent)
		frame.result.ChainDepth = nextDepth
		return nil
	default:
		return fmt.Errorf("unsupported guard on_fail action %q", action)
	}
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
