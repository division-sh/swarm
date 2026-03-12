package pipeline

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"

	"github.com/google/uuid"
)

type HandlerOutcomeStatus string

const (
	HandlerOutcomeCompleted HandlerOutcomeStatus = "completed"
	HandlerOutcomeBlocked   HandlerOutcomeStatus = "blocked"
	HandlerOutcomeWaiting   HandlerOutcomeStatus = "waiting"
	HandlerOutcomeFannedOut HandlerOutcomeStatus = "fanned_out"
)

type handlerExecutionOutcome struct {
	Status           HandlerOutcomeStatus
	GuardsEvaluated  []string
	ActionsExecuted  []string
	AdvancesTo       string
	SetsGate         string
	ClearGates       []string
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
	Emits            []string
	RuleID           string
	FanOutCount      int
	Computed         map[string]any
}

type handlerEngineContextKey struct{}

type handlerEngineContext struct {
	coordinator *FactoryPipelineCoordinator
	nodeID      string
	preview     bool
}

type handlerEngineAccumulator struct {
	Expected       []string         `json:"expected,omitempty"`
	ExpectedCount  int              `json:"expected_count,omitempty"`
	Received       map[string]bool  `json:"received,omitempty"`
	Items          []map[string]any `json:"items,omitempty"`
	LastEventID    string           `json:"last_event_id,omitempty"`
	LastEventType  string           `json:"last_event_type,omitempty"`
	LastSource     string           `json:"last_source,omitempty"`
	LastReceivedAt string           `json:"last_received_at,omitempty"`
}

type contractHandlerExecutionResult struct {
	Transition      WorkflowTransition
	Plan            handlerExecutionPlan
	Outcome         *handlerExecutionOutcome
	GuardsEvaluated []string
	Handled         bool
}

type handlerEngineExecution struct {
	ctx         context.Context
	scope       *handlerEngineContext
	state       *WorkflowState
	handler     runtimecontracts.SystemNodeEventHandler
	event       events.Event
	payload     map[string]any
	entityID    string
	policy      map[string]any
	accumulated map[string]any
	fanOut      map[string]any
	outcome     handlerExecutionOutcome
	ruleApplied bool
}

func withHandlerEngineContext(ctx context.Context, pc *FactoryPipelineCoordinator, nodeID string, preview bool) context.Context {
	return context.WithValue(ctx, handlerEngineContextKey{}, &handlerEngineContext{
		coordinator: pc,
		nodeID:      strings.TrimSpace(nodeID),
		preview:     preview,
	})
}

func ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event, state *WorkflowState) (*HandlerOutcome, error) {
	outcome, err := executeHandlerStepsDetailed(ctx, handler, evt, state)
	if err != nil {
		return nil, err
	}
	return &HandlerOutcome{
		Handled:         true,
		ActionsExecuted: append([]string{}, outcome.ActionsExecuted...),
	}, nil
}

func executeHandlerStepsDetailed(ctx context.Context, handler SystemNodeEventHandler, evt Event, state *WorkflowState) (*handlerExecutionOutcome, error) {
	if state == nil {
		return nil, fmt.Errorf("handler execution requires workflow state")
	}
	exec := &handlerEngineExecution{
		ctx:         ctx,
		scope:       handlerEngineContextFromContext(ctx),
		state:       state,
		handler:     handler,
		event:       evt,
		payload:     parsePayloadMap(evt.Payload),
		accumulated: map[string]any{},
		fanOut:      map[string]any{},
		outcome: handlerExecutionOutcome{
			Status:           HandlerOutcomeCompleted,
			AdvancesTo:       strings.TrimSpace(handler.AdvancesTo),
			DataAccumulation: handler.DataAccumulation,
			Emits:            handler.Emits.Values(),
			ClearGates:       append([]string{}, handler.ClearGates...),
			Computed:         map[string]any{},
		},
	}
	exec.entityID = exec.resolveEntityID()
	exec.outcome.SetsGate = gateSpecString(handler.SetsGate)
	exec.policy = exec.policyValues()
	if exec.state.Metadata == nil {
		exec.state.Metadata = map[string]any{}
	}

	passed, evaluated, err := exec.evaluateGuard()
	exec.outcome.GuardsEvaluated = append(exec.outcome.GuardsEvaluated, evaluated...)
	if err != nil {
		return nil, err
	}
	if !passed {
		exec.outcome.Status = HandlerOutcomeBlocked
		return &exec.outcome, nil
	}

	if waiting, err := exec.accumulate(); err != nil {
		return nil, err
	} else if waiting {
		exec.outcome.Status = HandlerOutcomeWaiting
		return &exec.outcome, nil
	}

	if err := exec.compute(); err != nil {
		return nil, err
	}
	if fannedOut, err := exec.fanOutItems(); err != nil {
		return nil, err
	} else if fannedOut {
		exec.outcome.Status = HandlerOutcomeFannedOut
		return &exec.outcome, nil
	}

	if err := exec.onComplete(); err != nil {
		return nil, err
	}
	if err := exec.advanceState(); err != nil {
		return nil, err
	}
	if err := exec.setGate(); err != nil {
		return nil, err
	}
	if err := exec.accumulateData(exec.outcome.DataAccumulation); err != nil {
		return nil, err
	}
	if err := exec.emitEvents(exec.outcome.Emits); err != nil {
		return nil, err
	}
	if err := exec.applyRules(); err != nil {
		return nil, err
	}

	return &exec.outcome, nil
}

func (pc *FactoryPipelineCoordinator) executeDerivedContractHandler(ctx context.Context, triggerCtx workflowTriggerContext, preview bool) (contractHandlerExecutionResult, error) {
	if pc == nil || pc.ContractBundle() == nil {
		return contractHandlerExecutionResult{}, nil
	}
	trigger := strings.TrimSpace(string(triggerCtx.Event.Type))
	if trigger == "" {
		return contractHandlerExecutionResult{}, nil
	}
	bundle := pc.ContractBundle()
	owners := bundle.RuntimeEventOwners(trigger)
	if len(owners) == 0 {
		return contractHandlerExecutionResult{}, nil
	}
	var (
		nodeID  string
		handler runtimecontracts.SystemNodeEventHandler
		matched bool
	)
	for _, owner := range owners {
		candidate, ok := bundle.NodeEventHandler(owner, trigger)
		if !ok {
			continue
		}
		if matched {
			return contractHandlerExecutionResult{}, nil
		}
		nodeID = strings.TrimSpace(owner)
		handler = candidate
		matched = true
	}
	if !matched {
		return contractHandlerExecutionResult{}, nil
	}
	return pc.executeNodeContractHandler(ctx, nodeID, handler, triggerCtx, preview)
}

func (pc *FactoryPipelineCoordinator) executeNodeContractHandler(
	ctx context.Context,
	nodeID string,
	handler runtimecontracts.SystemNodeEventHandler,
	triggerCtx workflowTriggerContext,
	preview bool,
) (contractHandlerExecutionResult, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return contractHandlerExecutionResult{}, nil
	}
	state := triggerCtx.State
	handlerCtx := withHandlerEngineContext(ctx, pc, nodeID, preview)
	outcome, err := executeHandlerStepsDetailed(handlerCtx, handler, triggerCtx.Event, &state)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	plan := handlerExecutionPlanFromNodeHandler(nodeID, strings.TrimSpace(string(triggerCtx.Event.Type)), handler)
	plan.AdvancesTo = firstNonEmptyString(outcome.AdvancesTo, plan.AdvancesTo)
	if len(outcome.Emits) > 0 {
		plan.EmitEvents = append([]string{}, outcome.Emits...)
		plan.Emits = strings.TrimSpace(outcome.Emits[0])
	}
	if outcome.SetsGate != "" {
		plan.SetsGate = outcome.SetsGate
	}
	plan.DataAccumulation = outcome.DataAccumulation
	if !preview && outcome.Status == HandlerOutcomeCompleted {
		actions := pc.executeHandlerPlanActions(ctx, workflowTriggerContext{
			Event:           triggerCtx.Event,
			State:           state,
			ValidationState: triggerCtx.ValidationState,
		}, plan)
		outcome.ActionsExecuted = append(outcome.ActionsExecuted, actions...)
	}
	return contractHandlerExecutionResult{
		Transition:      workflowTransitionFromHandlerOutcome(triggerCtx.State, nodeID, strings.TrimSpace(string(triggerCtx.Event.Type)), outcome),
		Plan:            plan,
		Outcome:         outcome,
		GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
		Handled:         true,
	}, nil
}

func handlerEngineContextFromContext(ctx context.Context) *handlerEngineContext {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(handlerEngineContextKey{}).(*handlerEngineContext)
	return scope
}

func workflowTransitionFromHandlerOutcome(state WorkflowState, nodeID, eventType string, outcome *handlerExecutionOutcome) WorkflowTransition {
	target := strings.TrimSpace(string(state.Stage))
	if outcome != nil && strings.TrimSpace(outcome.AdvancesTo) != "" {
		target = strings.TrimSpace(outcome.AdvancesTo)
	}
	transition := WorkflowTransition{
		Name:    strings.TrimSpace(nodeID) + ":" + strings.TrimSpace(eventType),
		From:    []PipelineStage{NormalizePipelineStage(string(state.Stage))},
		To:      NormalizePipelineStage(target),
		Trigger: strings.TrimSpace(eventType),
		Node:    strings.TrimSpace(nodeID),
	}
	if outcome != nil {
		transition.DataAccumulation = outcome.DataAccumulation
	}
	return transition
}

func (e *handlerEngineExecution) coordinator() *FactoryPipelineCoordinator {
	if e == nil || e.scope == nil {
		return nil
	}
	return e.scope.coordinator
}

func (e *handlerEngineExecution) preview() bool {
	return e == nil || e.scope == nil || e.scope.preview
}

func (e *handlerEngineExecution) nodeID() string {
	if e == nil || e.scope == nil {
		return ""
	}
	return strings.TrimSpace(e.scope.nodeID)
}

func (e *handlerEngineExecution) resolveEntityID() string {
	if e == nil {
		return ""
	}
	candidates := []string{
		strings.TrimSpace(asString(e.payload["entity_id"])),
		strings.TrimSpace(asString(e.payload["vertical_id"])),
		strings.TrimSpace(e.event.EntityID()),
		strings.TrimSpace(e.state.VerticalID),
	}
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func (e *handlerEngineExecution) policyValues() map[string]any {
	pc := e.coordinator()
	if pc == nil || pc.ContractBundle() == nil {
		return nil
	}
	policy := policyDocumentToMap(pc.ContractBundle().MergedPolicy)
	if len(policy) == 0 {
		policy = policyDocumentToMap(pc.ContractBundle().Policy)
	}
	return policy
}

func (e *handlerEngineExecution) evaluateGuard() (bool, []string, error) {
	return e.evaluateGuardSpec(e.handler.Guard)
}

func (e *handlerEngineExecution) evaluateGuardSpec(spec *runtimecontracts.GuardSpec) (bool, []string, error) {
	if spec == nil {
		return true, nil, nil
	}
	if len(spec.Checks) > 0 {
		evaluated := make([]string, 0, len(spec.Checks))
		for _, check := range spec.Checks {
			passed, ids, err := e.evaluateGuardCheck(check.ID, check.Check, spec.PolicyRef)
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
	return e.evaluateGuardCheck(spec.ID, spec.Check, spec.PolicyRef)
}

func (e *handlerEngineExecution) evaluateGuardCheck(id, check, policyRef string) (bool, []string, error) {
	id = strings.TrimSpace(id)
	check = strings.TrimSpace(check)
	if check != "" {
		passed, err := e.evalBool(check, map[string]any{"guard": map[string]any{"policy_ref": strings.TrimSpace(policyRef)}})
		if err == nil {
			evaluated := []string{check}
			if id != "" {
				evaluated = []string{id}
			}
			return passed, evaluated, nil
		}
		if id == "" {
			return false, []string{check}, err
		}
	}
	if id == "" {
		return true, nil, nil
	}
	if passed, handled, err := e.evaluateBuiltinGuard(id, policyRef); handled || err != nil {
		return passed, []string{id}, err
	}
	pc := e.coordinator()
	if pc == nil {
		return false, []string{id}, fmt.Errorf("guard %q requires runtime registry", id)
	}
	entry, ok := pc.resolveWorkflowGuard(id)
	if !ok {
		passed, err := e.evalBool(id, nil)
		if err != nil {
			return false, []string{id}, err
		}
		return passed, []string{id}, nil
	}
	if strings.TrimSpace(entry.Check) != "" {
		passed, err := e.evalBool(entry.Check, map[string]any{"guard": map[string]any{"policy_ref": entry.PolicyRef}})
		if err == nil {
			return passed, []string{id}, nil
		}
		if passed, handled, builtinErr := e.evaluateBuiltinGuard(firstNonEmptyString(entry.PlatformBuiltin, entry.ID), entry.PolicyRef); handled || builtinErr != nil {
			return passed, []string{id}, builtinErr
		}
		return false, []string{id}, err
	}
	if passed, handled, err := e.evaluateBuiltinGuard(firstNonEmptyString(entry.PlatformBuiltin, entry.ID), entry.PolicyRef); handled || err != nil {
		return passed, []string{id}, err
	}
	return false, []string{id}, fmt.Errorf("guard %q is not executable", id)
}

func (e *handlerEngineExecution) accumulate() (bool, error) {
	if e.handler.Accumulate == nil {
		return false, nil
	}
	acc, err := e.currentAccumulator()
	if err != nil {
		return false, err
	}
	expectedIDs, expectedCount := e.expectedAccumulatorTargets(e.handler.Accumulate.ExpectedFrom)
	if len(expectedIDs) > 0 {
		acc.Expected = append([]string{}, expectedIDs...)
		acc.ExpectedCount = len(expectedIDs)
	} else if expectedCount > 0 {
		acc.ExpectedCount = expectedCount
	}
	if acc.Received == nil {
		acc.Received = map[string]bool{}
	}
	arrivalID := e.arrivalIdentifier()
	if arrivalID != "" && !acc.Received[arrivalID] {
		acc.Received[arrivalID] = true
		acc.Items = append(acc.Items, map[string]any{
			"event_id":    strings.TrimSpace(e.event.ID),
			"event_type":  strings.TrimSpace(string(e.event.Type)),
			"source":      strings.TrimSpace(e.event.SourceAgent),
			"payload":     cloneStringAnyMap(e.payload),
			"received_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
	acc.LastEventID = strings.TrimSpace(e.event.ID)
	acc.LastEventType = strings.TrimSpace(string(e.event.Type))
	acc.LastSource = strings.TrimSpace(e.event.SourceAgent)
	acc.LastReceivedAt = time.Now().UTC().Format(time.RFC3339Nano)
	e.storeAccumulator(acc)
	e.accumulated[e.nodeID()] = accumulatorExpressionValue(acc)

	complete, err := e.accumulatorComplete(acc)
	if err != nil {
		return false, err
	}
	return !complete, nil
}

func (e *handlerEngineExecution) compute() error {
	if e.handler.Compute == nil {
		return nil
	}
	acc, err := e.currentAccumulator()
	if err != nil {
		return err
	}
	result, err := e.computeValue(acc)
	if err != nil {
		return err
	}
	field := normalizeHandlerStateField(e.handler.Compute.StoreAs)
	if field == "" {
		return nil
	}
	e.outcome.Computed[field] = result
	e.state.Metadata[field] = result
	if !e.preview() {
		if err := e.persistStateValue(field, result); err != nil {
			return err
		}
	}
	return nil
}

func (e *handlerEngineExecution) fanOutItems() (bool, error) {
	if e.handler.FanOut == nil {
		return false, nil
	}
	items := sliceFromAny(e.resolveRef(e.handler.FanOut.ItemsFrom))
	if len(items) == 0 {
		return false, nil
	}
	e.fanOut = map[string]any{"count": len(items)}
	e.outcome.FanOutCount = len(items)
	if !e.preview() {
		for _, item := range items {
			eventType := e.fanOutEventType(item)
			if eventType == "" {
				continue
			}
			payload := cloneStringAnyMap(e.payload)
			if payload == nil {
				payload = map[string]any{}
			}
			payload["item"] = item
			if target := strings.TrimSpace(e.handler.FanOut.Target); target != "" {
				payload["target"] = target
			}
			e.coordinator().publish(e.ctx, eventType, e.entityID, payload)
		}
	}
	return true, nil
}

func (e *handlerEngineExecution) onComplete() error {
	rule := e.handler.OnComplete
	if rule == nil && e.handler.Accumulate != nil {
		rule = e.handler.Accumulate.OnComplete
	}
	if rule == nil {
		return nil
	}
	condition := strings.TrimSpace(rule.Condition)
	if condition == "" {
		return e.applyRuleEntry(*rule, true)
	}
	passed, err := e.evalBool(condition, nil)
	if err != nil {
		return err
	}
	if !passed {
		return nil
	}
	return e.applyRuleEntry(*rule, true)
}

func (e *handlerEngineExecution) advanceState() error {
	target := strings.TrimSpace(e.outcome.AdvancesTo)
	if target == "" {
		return nil
	}
	current := strings.TrimSpace(string(e.state.Stage))
	target = strings.TrimSpace(string(NormalizePipelineStage(target)))
	if target == "" {
		return nil
	}
	e.state.Stage = NormalizePipelineStage(target)
	if e.preview() {
		e.outcome.ActionsExecuted = append(e.outcome.ActionsExecuted,
			"record_state_change",
			"update_stage",
			"cancel_stage_timers",
			"start_stage_timers",
		)
		return nil
	}
	pc := e.coordinator()
	if pc == nil {
		return fmt.Errorf("advances_to requires runtime coordinator")
	}
	pc.updateVerticalStage(e.ctx, e.entityID, target, strings.TrimSpace(string(e.event.Type)))
	if current != target {
		e.outcome.ActionsExecuted = append(e.outcome.ActionsExecuted,
			"record_state_change",
			"update_stage",
			"cancel_stage_timers",
			"start_stage_timers",
		)
	}
	return nil
}

func (e *handlerEngineExecution) setGate() error {
	setGate := strings.TrimSpace(e.outcome.SetsGate)
	clearGates := len(e.outcome.ClearGates) > 0
	if setGate == "" && !clearGates {
		return nil
	}
	if clearGates {
		for _, gate := range []string{"g1_research", "g2_spec", "g3_cto", "g4_brand"} {
			e.state.Metadata[gate] = false
		}
	}
	if setGate != "" {
		e.state.Metadata[setGate] = true
	}
	if e.preview() {
		return nil
	}
	pc := e.coordinator()
	if pc == nil {
		return fmt.Errorf("sets_gate requires runtime coordinator")
	}
	pc.applyWorkflowGateMutation(e.ctx, e.entityID, e.nodeID(), setGate, clearGates)
	return nil
}

func (e *handlerEngineExecution) accumulateData(spec runtimecontracts.WorkflowDataAccumulation) error {
	if !spec.HasWrites() && strings.TrimSpace(spec.SourceEvent) == "" {
		return nil
	}
	applyWorkflowDataAccumulationToState(e.state, e.payload, spec)
	if e.preview() {
		return nil
	}
	pc := e.coordinator()
	if pc == nil {
		return fmt.Errorf("data_accumulation requires runtime coordinator")
	}
	pc.applyWorkflowDataAccumulation(e.ctx, e.entityID, WorkflowTransition{
		Node:             e.nodeID(),
		Trigger:          strings.TrimSpace(string(e.event.Type)),
		DataAccumulation: spec,
	}, e.event)
	return nil
}

func (e *handlerEngineExecution) emitEvents(eventsToEmit []string) error {
	if len(eventsToEmit) == 0 || e.preview() {
		return nil
	}
	pc := e.coordinator()
	if pc == nil {
		return fmt.Errorf("emits requires runtime coordinator")
	}
	for _, emitEvent := range eventsToEmit {
		emitEvent = strings.TrimSpace(emitEvent)
		if emitEvent == "" {
			continue
		}
		pc.publish(e.ctx, emitEvent, e.entityID, pc.handlerEmitPayload(e.ctx, workflowTriggerContext{
			Event: e.event,
			State: *e.state,
		}, emitEvent))
	}
	return nil
}

func (e *handlerEngineExecution) applyRules() error {
	if len(e.handler.Rules) == 0 {
		return nil
	}
	for _, rule := range e.handler.Rules {
		condition := strings.TrimSpace(rule.Condition)
		passed := condition == ""
		if !passed {
			result, err := e.evalBool(condition, nil)
			if err != nil {
				return err
			}
			passed = result
		}
		if !passed {
			continue
		}
		return e.applyRuleEntry(rule, false)
	}
	return nil
}

func (e *handlerEngineExecution) applyRuleEntry(rule runtimecontracts.HandlerRuleEntry, override bool) error {
	if rule.ID != "" {
		e.outcome.RuleID = strings.TrimSpace(rule.ID)
	}
	if override && strings.TrimSpace(rule.AdvancesTo) != "" {
		e.outcome.AdvancesTo = strings.TrimSpace(rule.AdvancesTo)
	}
	if override && !rule.Emits.Empty() {
		e.outcome.Emits = rule.Emits.Values()
	}
	if override && (rule.DataAccumulation.HasWrites() || strings.TrimSpace(rule.DataAccumulation.SourceEvent) != "") {
		e.outcome.DataAccumulation = rule.DataAccumulation
	}
	if !override {
		if strings.TrimSpace(rule.AdvancesTo) != "" {
			e.outcome.AdvancesTo = strings.TrimSpace(rule.AdvancesTo)
			if err := e.advanceState(); err != nil {
				return err
			}
		}
		if rule.DataAccumulation.HasWrites() || strings.TrimSpace(rule.DataAccumulation.SourceEvent) != "" {
			if err := e.accumulateData(rule.DataAccumulation); err != nil {
				return err
			}
		}
		if !rule.Emits.Empty() {
			if err := e.emitEvents(rule.Emits.Values()); err != nil {
				return err
			}
		}
	}
	e.ruleApplied = true
	return nil
}

func (e *handlerEngineExecution) evalBool(expression string, extraVars map[string]any) (bool, error) {
	pc := e.coordinator()
	if pc == nil || pc.expressionEval == nil {
		return false, fmt.Errorf("workflow expression evaluator unavailable")
	}
	entity := cloneStringAnyMap(e.state.Metadata)
	if entity == nil {
		entity = map[string]any{}
	}
	entity["current_state"] = strings.TrimSpace(string(e.state.Stage))
	entity["state"] = strings.TrimSpace(string(e.state.Stage))
	entity["status"] = strings.TrimSpace(e.state.Status)
	gates := map[string]any{}
	for key, value := range entity {
		if strings.HasPrefix(strings.TrimSpace(key), "g") {
			gates[key] = value
		}
	}
	entity["gates"] = gates
	vars := cloneStringAnyMap(extraVars)
	if vars == nil {
		vars = map[string]any{}
	}
	vars["metadata"] = map[string]any{
		"revision_count": asInt(entity["revision_count"]),
	}
	if len(e.accumulated) > 0 {
		vars["accumulated"] = cloneStringAnyMap(e.accumulated)
	}
	if len(e.fanOut) > 0 {
		vars["fan_out"] = cloneStringAnyMap(e.fanOut)
	}
	rewritten := rewriteHandlerExpression(expression)
	return pc.expressionEval.EvalBool(rewritten, workflowExpressionContext{
		Entity:  entity,
		Payload: cloneStringAnyMap(e.payload),
		Policy:  cloneStringAnyMap(e.policy),
		Vars:    vars,
	})
}

func (e *handlerEngineExecution) evaluateBuiltinGuard(id, policyRef string) (bool, bool, error) {
	handler, ok := lookupWorkflowBuiltinGuard(id)
	if !ok {
		return false, false, nil
	}
	return handler(e, strings.TrimSpace(policyRef))
}

func (e *handlerEngineExecution) currentAccumulator() (*handlerEngineAccumulator, error) {
	acc := &handlerEngineAccumulator{}
	if e.preview() {
		if existing, ok := e.loadAccumulator(); ok {
			return existing, nil
		}
		return acc, nil
	}
	if existing, ok := e.loadAccumulator(); ok {
		return existing, nil
	}
	return acc, nil
}

func (e *handlerEngineExecution) loadAccumulator() (*handlerEngineAccumulator, bool) {
	pc := e.coordinator()
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || e.entityID == "" || e.nodeID() == "" {
		return nil, false
	}
	instance, ok, err := pc.workflowStore.Load(e.ctx, e.entityID)
	if err != nil || !ok {
		return nil, false
	}
	bucket, ok := workflowStateBucketObject(instance, e.nodeID())
	if !ok {
		return nil, false
	}
	rawAccumulators, ok := asObject(bucket["handler_accumulators"])
	if !ok {
		return nil, false
	}
	raw, ok := asObject(rawAccumulators[e.handlerKey()])
	if !ok {
		return nil, false
	}
	acc := &handlerEngineAccumulator{
		Expected:       normalizeStrings(handlerStringSliceFromAny(raw["expected"])),
		ExpectedCount:  asInt(raw["expected_count"]),
		Received:       map[string]bool{},
		Items:          sliceOfMapsFromAny(raw["items"]),
		LastEventID:    strings.TrimSpace(asString(raw["last_event_id"])),
		LastEventType:  strings.TrimSpace(asString(raw["last_event_type"])),
		LastSource:     strings.TrimSpace(asString(raw["last_source"])),
		LastReceivedAt: strings.TrimSpace(asString(raw["last_received_at"])),
	}
	if received, ok := asObject(raw["received"]); ok {
		for key, value := range received {
			acc.Received[strings.TrimSpace(key)] = truthyMetadataFlag(value)
		}
	}
	e.accumulated[e.nodeID()] = accumulatorExpressionValue(acc)
	return acc, true
}

func (e *handlerEngineExecution) storeAccumulator(acc *handlerEngineAccumulator) {
	if acc == nil || e.preview() {
		return
	}
	pc := e.coordinator()
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || e.entityID == "" || e.nodeID() == "" {
		return
	}
	_ = pc.workflowStore.Mutate(e.ctx, e.entityID, func(instance *WorkflowInstance) {
		bucket := workflowMutableStateBucket(instance, e.nodeID())
		accumulators, _ := asObject(bucket["handler_accumulators"])
		if accumulators == nil {
			accumulators = map[string]any{}
		}
		received := map[string]any{}
		keys := make([]string, 0, len(acc.Received))
		for key := range acc.Received {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			received[key] = acc.Received[key]
		}
		accumulators[e.handlerKey()] = map[string]any{
			"expected":         append([]string{}, acc.Expected...),
			"expected_count":   acc.ExpectedCount,
			"received":         received,
			"items":            append([]map[string]any{}, acc.Items...),
			"last_event_id":    acc.LastEventID,
			"last_event_type":  acc.LastEventType,
			"last_source":      acc.LastSource,
			"last_received_at": acc.LastReceivedAt,
		}
		bucket["handler_accumulators"] = accumulators
		workflowSetStateBucket(instance, e.nodeID(), bucket)
	})
}

func (e *handlerEngineExecution) handlerKey() string {
	nodeID := e.nodeID()
	eventType := strings.TrimSpace(string(e.event.Type))
	if nodeID == "" {
		return eventType
	}
	return nodeID + ":" + eventType
}

func (e *handlerEngineExecution) expectedAccumulatorTargets(ref string) ([]string, int) {
	value := e.resolveRef(ref)
	switch typed := value.(type) {
	case []string:
		return normalizeStrings(typed), len(typed)
	case []any:
		targets := handlerStringSliceFromAny(typed)
		if len(targets) > 0 {
			return normalizeStrings(targets), len(targets)
		}
		return nil, len(typed)
	case int:
		return nil, typed
	case int64:
		return nil, int(typed)
	case float64:
		return nil, int(typed)
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return nil, 0
		}
		if n, err := strconv.Atoi(text); err == nil {
			return nil, n
		}
		return []string{text}, 1
	default:
		return nil, asInt(value)
	}
}

func (e *handlerEngineExecution) accumulatorComplete(acc *handlerEngineAccumulator) (bool, error) {
	if acc == nil {
		return true, nil
	}
	completion := ""
	if e.handler.Accumulate != nil {
		completion = strings.TrimSpace(e.handler.Accumulate.Completion)
	}
	receivedCount := len(acc.Received)
	if completion == "" || strings.EqualFold(completion, "all") || strings.EqualFold(completion, "threshold") {
		switch {
		case len(acc.Expected) > 0:
			for _, expected := range acc.Expected {
				if !acc.Received[strings.TrimSpace(expected)] {
					return false, nil
				}
			}
			return true, nil
		case acc.ExpectedCount > 0:
			return receivedCount >= acc.ExpectedCount, nil
		default:
			return receivedCount > 0, nil
		}
	}
	if strings.EqualFold(completion, "timeout") {
		return false, nil
	}
	return e.evalBool(completion, map[string]any{
		"accumulation": map[string]any{
			"expected_count": acc.ExpectedCount,
			"received_count": receivedCount,
		},
	})
}

func (e *handlerEngineExecution) computeValue(acc *handlerEngineAccumulator) (any, error) {
	op := strings.TrimSpace(e.handler.Compute.Operation)
	switch op {
	case "weighted_average":
		return computeWeightedAverage(acc, e.handler.Compute.Tiers), nil
	case "weighted_sum":
		return computeWeightedPayload(e.payload, e.handler.Compute.Tiers), nil
	case "sum":
		return aggregateAccumulatorNumbers(acc, func(current, next float64, idx int) float64 {
			return current + next
		}), nil
	case "min":
		return aggregateAccumulatorNumbers(acc, func(current, next float64, idx int) float64 {
			if idx == 0 || next < current {
				return next
			}
			return current
		}), nil
	case "max":
		return aggregateAccumulatorNumbers(acc, func(current, next float64, idx int) float64 {
			if idx == 0 || next > current {
				return next
			}
			return current
		}), nil
	case "count":
		return len(acc.Items), nil
	default:
		return nil, fmt.Errorf("unsupported compute operation %q", op)
	}
}

func (e *handlerEngineExecution) resolveRef(ref string) any {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	switch {
	case strings.HasPrefix(ref, "entity."):
		value, _ := workflowExpressionLookupPath(e.state.Metadata, strings.TrimPrefix(ref, "entity."))
		return value
	case strings.HasPrefix(ref, "payload."):
		value, _ := workflowExpressionLookupPath(e.payload, strings.TrimPrefix(ref, "payload."))
		return value
	case strings.HasPrefix(ref, "policy."):
		value, _ := workflowExpressionLookupPath(e.policy, strings.TrimPrefix(ref, "policy."))
		return value
	case strings.HasPrefix(ref, "metadata."):
		value, _ := workflowExpressionLookupPath(e.state.Metadata, strings.TrimPrefix(ref, "metadata."))
		return value
	case strings.HasPrefix(ref, "accumulated."):
		value, _ := workflowExpressionLookupPath(e.accumulated, strings.TrimPrefix(ref, "accumulated."))
		return value
	default:
		if value, ok := workflowExpressionLookupPath(e.payload, ref); ok {
			return value
		}
		if value, ok := workflowExpressionLookupPath(e.state.Metadata, ref); ok {
			return value
		}
		if value, ok := workflowExpressionLookupPath(e.accumulated, ref); ok {
			return value
		}
		return nil
	}
}

func (e *handlerEngineExecution) arrivalIdentifier() string {
	candidates := []string{
		strings.TrimSpace(e.event.SourceAgent),
		strings.TrimSpace(asString(e.payload["source"])),
		strings.TrimSpace(asString(e.payload["from"])),
		strings.TrimSpace(asString(e.payload["agent_id"])),
		strings.TrimSpace(asString(e.payload["node_id"])),
		strings.TrimSpace(asString(e.payload["dimension"])),
		strings.TrimSpace(e.event.ID),
	}
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func (e *handlerEngineExecution) persistStateValue(field string, value any) error {
	pc := e.coordinator()
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || e.entityID == "" {
		return nil
	}
	field = normalizeHandlerStateField(field)
	if field == "" {
		return nil
	}
	allowedFields := workflowEntitySchemaFields(pc.ContractBundle())
	_, entityField := allowedFields[field]
	return pc.workflowStore.Mutate(e.ctx, e.entityID, func(instance *WorkflowInstance) {
		metadata := workflowMutableMetadata(instance)
		metadata[field] = value
		if entityField {
			entityProjection := workflowMutableStateBucket(instance, workflowStateBucketEntityProjection)
			entityProjection[field] = value
			workflowSetStateBucket(instance, workflowStateBucketEntityProjection, entityProjection)
		}
	})
}

func (e *handlerEngineExecution) fanOutEventType(item any) string {
	if len(e.handler.FanOut.EmitMapping) > 0 {
		for key, eventType := range e.handler.FanOut.EmitMapping {
			if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(asString(item))) {
				return strings.TrimSpace(eventType)
			}
		}
		if obj, ok := asObject(item); ok {
			for field, raw := range obj {
				if eventType, ok := e.handler.FanOut.EmitMapping[strings.TrimSpace(asString(raw))]; ok {
					_ = field
					return strings.TrimSpace(eventType)
				}
			}
		}
	}
	return strings.TrimSpace(e.handler.FanOut.EmitPerItem)
}

func rewriteHandlerExpression(expression string) string {
	replacer := strings.NewReplacer(
		"metadata.", "vars.metadata.",
		"accumulated.", "vars.accumulated.",
		"fan_out.", "vars.fan_out.",
	)
	return replacer.Replace(strings.TrimSpace(expression))
}

func applyWorkflowDataAccumulationToState(state *WorkflowState, payload map[string]any, spec runtimecontracts.WorkflowDataAccumulation) {
	if state == nil || len(spec.Writes) == 0 {
		return
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	for _, write := range spec.Writes {
		target := normalizeHandlerStateField(write.Target())
		if target == "" {
			continue
		}
		if write.HasLiteralValue() {
			state.Metadata[target] = write.Value.Literal
			continue
		}
		source := strings.TrimSpace(write.Source())
		if source == "" {
			continue
		}
		if value, ok := workflowExpressionLookupPath(payload, source); ok {
			state.Metadata[target] = value
		} else if value, ok := payload[source]; ok {
			state.Metadata[target] = value
		}
	}
	if sourceEvent := strings.TrimSpace(spec.SourceEvent); sourceEvent != "" {
		state.Metadata["last_data_accumulation_source"] = sourceEvent
	}
}

func normalizeHandlerStateField(field string) string {
	field = strings.TrimSpace(field)
	switch {
	case strings.HasPrefix(field, "entity."):
		return strings.TrimSpace(strings.TrimPrefix(field, "entity."))
	case strings.HasPrefix(field, "metadata."):
		return strings.TrimSpace(strings.TrimPrefix(field, "metadata."))
	default:
		return field
	}
}

func accumulatorExpressionValue(acc *handlerEngineAccumulator) map[string]any {
	if acc == nil {
		return map[string]any{}
	}
	items := make([]any, 0, len(acc.Items))
	for _, item := range acc.Items {
		items = append(items, cloneStringAnyMap(item))
	}
	expected := make([]any, 0, len(acc.Expected))
	for _, item := range acc.Expected {
		expected = append(expected, item)
	}
	return map[string]any{
		"items":          items,
		"expected":       expected,
		"expected_count": acc.ExpectedCount,
		"received_count": len(acc.Received),
	}
}

func computeWeightedAverage(acc *handlerEngineAccumulator, tiers []runtimecontracts.ComputeTier) float64 {
	if acc == nil || len(acc.Items) == 0 || len(tiers) == 0 {
		return 0
	}
	dimensionScores := map[string]float64{}
	for _, item := range acc.Items {
		payload, _ := asObject(item["payload"])
		dimension := strings.TrimSpace(asString(payload["dimension"]))
		score := firstNumeric(
			payload["score"],
			payload["value"],
			payload["dimension_score"],
		)
		if dimension == "" || math.IsNaN(score) {
			continue
		}
		dimensionScores[dimension] = score
	}
	totalWeight := 0.0
	total := 0.0
	for _, tier := range tiers {
		sum := 0.0
		count := 0
		for _, dimension := range tier.Dimensions {
			score, ok := dimensionScores[strings.TrimSpace(dimension)]
			if !ok {
				continue
			}
			sum += score
			count++
		}
		if count == 0 {
			continue
		}
		weight := tier.Weight
		if weight <= 0 {
			weight = 1
		}
		total += (sum / float64(count)) * weight
		totalWeight += weight
	}
	if totalWeight == 0 {
		return 0
	}
	return total / totalWeight
}

func computeWeightedPayload(payload map[string]any, tiers []runtimecontracts.ComputeTier) float64 {
	if len(payload) == 0 || len(tiers) == 0 {
		return 0
	}
	total := 0.0
	for _, tier := range tiers {
		sum := 0.0
		count := 0
		for _, dimension := range tier.Dimensions {
			var value any
			if resolved, ok := workflowExpressionLookupPath(payload, strings.TrimPrefix(strings.TrimSpace(dimension), "payload.")); ok {
				value = resolved
			}
			score := firstNumeric(value)
			if math.IsNaN(score) {
				continue
			}
			sum += score
			count++
		}
		if count == 0 {
			continue
		}
		weight := tier.Weight
		if weight <= 0 {
			weight = 1
		}
		total += (sum / float64(count)) * weight
	}
	return total
}

func aggregateAccumulatorNumbers(acc *handlerEngineAccumulator, combine func(current, next float64, idx int) float64) float64 {
	if acc == nil {
		return 0
	}
	current := 0.0
	idx := 0
	for _, item := range acc.Items {
		payload, _ := asObject(item["payload"])
		value := firstNumeric(payload["score"], payload["value"], payload["count"])
		if math.IsNaN(value) {
			continue
		}
		current = combine(current, value, idx)
		idx++
	}
	if idx == 0 {
		return 0
	}
	return current
}

func firstNumeric(values ...any) float64 {
	for _, value := range values {
		switch typed := value.(type) {
		case int:
			return float64(typed)
		case int64:
			return float64(typed)
		case float64:
			return typed
		case float32:
			return float64(typed)
		case string:
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
				return parsed
			}
		}
	}
	return math.NaN()
}

func sliceFromAny(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return nil
	}
}

func handlerStringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(asString(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func hasValidUUID(text string) bool {
	_, err := uuid.Parse(strings.TrimSpace(text))
	return err == nil
}
