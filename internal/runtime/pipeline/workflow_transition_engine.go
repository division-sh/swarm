package pipeline

import (
	"context"
	"encoding/json"
	"strings"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type workflowTransitionOutcome struct {
	Transition       WorkflowTransition
	PreviousState    WorkflowState
	CurrentState     WorkflowState
	GuardsEvaluated  []string
	ActionsExecuted  []string
	TriggerEventID   string
	TriggerEventType string
}

type workflowTriggerContext struct {
	Event           events.Event
	State           WorkflowState
	ValidationState *validationPipelineState
}

func workflowHookContextFromTrigger(triggerCtx workflowTriggerContext) WorkflowHookContext {
	verticalID := strings.TrimSpace(triggerCtx.Event.VerticalID)
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	if verticalID == "" {
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	return WorkflowHookContext{
		Event:      triggerCtx.Event,
		VerticalID: verticalID,
		Payload:    payload,
		State:      triggerCtx.State,
	}
}

func (pc *FactoryPipelineCoordinator) applyWorkflowEventTransition(ctx context.Context, evt events.Event) (workflowTransitionOutcome, bool) {
	if pc == nil {
		return workflowTransitionOutcome{}, false
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		payload := parsePayloadMap(evt.Payload)
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	if verticalID == "" {
		return workflowTransitionOutcome{}, false
	}

	previousState := pc.currentWorkflowState(ctx, verticalID)
	triggerCtx := workflowTriggerContext{
		Event:           evt,
		State:           previousState,
		ValidationState: pc.validationStateSnapshot(verticalID),
	}
	transition, guardsEvaluated, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		return workflowTransitionOutcome{}, false
	}

	actionsExecuted := pc.executeWorkflowTransitionActions(ctx, triggerCtx, transition, true)
	pc.updateVerticalStage(ctx, verticalID, string(transition.To), string(evt.Type))
	nextState := pc.currentWorkflowState(ctx, verticalID)
	actionsExecuted = append(actionsExecuted, pc.executeWorkflowTransitionActions(ctx, workflowTriggerContext{
		Event:           evt,
		State:           nextState,
		ValidationState: pc.validationStateSnapshot(verticalID),
	}, transition, false)...)

	return workflowTransitionOutcome{
		Transition:       transition,
		PreviousState:    previousState,
		CurrentState:     nextState,
		GuardsEvaluated:  guardsEvaluated,
		ActionsExecuted:  actionsExecuted,
		TriggerEventID:   strings.TrimSpace(evt.ID),
		TriggerEventType: strings.TrimSpace(string(evt.Type)),
	}, true
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowTransitionByEvent(
	triggerCtx workflowTriggerContext,
) (WorkflowTransition, []string, bool) {
	trigger := strings.TrimSpace(string(triggerCtx.Event.Type))
	if trigger == "" {
		return WorkflowTransition{}, nil, false
	}
	var guardsEvaluated []string
	transition, ok := DefaultPipelineWorkflow().TransitionByTrigger(triggerCtx.State, trigger, func(candidate WorkflowTransition) bool {
		passed, evaluated := pc.evaluateWorkflowTransitionGuards(triggerCtx, candidate)
		if passed {
			guardsEvaluated = evaluated
		}
		return passed
	})
	if !ok {
		return WorkflowTransition{}, nil, false
	}
	return transition, guardsEvaluated, true
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowTransitionGuards(
	triggerCtx workflowTriggerContext,
	transition WorkflowTransition,
) (bool, []string) {
	evaluated := make([]string, 0, len(transition.GuardIDs))
	for _, guardID := range transition.GuardIDs {
		guardID = strings.TrimSpace(guardID)
		if guardID == "" {
			continue
		}
		evaluated = append(evaluated, guardID)
		if !pc.evaluateWorkflowGuard(triggerCtx, guardID) {
			return false, evaluated
		}
	}
	return true, evaluated
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowGuard(triggerCtx workflowTriggerContext, guardID string) bool {
	guardID = strings.TrimSpace(guardID)
	if guardID == "" {
		return true
	}
	entry, ok := pc.resolveWorkflowGuard(guardID)
	if !ok {
		return false
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	switch workflowGuardExecutionKey(entry) {
	case "has_vertical_id", "has_entity_id":
		if strings.TrimSpace(hookCtx.VerticalID) != "" {
			return true
		}
		return strings.TrimSpace(asString(hookCtx.Payload["vertical_id"])) != ""
	case "has_human_decision":
		source := strings.TrimSpace(triggerCtx.Event.SourceAgent)
		if strings.EqualFold(source, "human") || strings.EqualFold(source, "mailbox") {
			return true
		}
		return strings.TrimSpace(asString(hookCtx.Payload["mailbox_decision_id"])) != ""
	case "inner_revision_count_below_limit", "revision_count_below_limit":
		return asInt(triggerCtx.State.Metadata["revision_count"]) < maxRevisionCycles
	case "not_in_operating_phase":
		stageDef, ok := DefaultPipelineWorkflow().Stage(triggerCtx.State.Stage)
		return !ok || !strings.EqualFold(strings.TrimSpace(stageDef.Phase), "operating")
	case "stage_in_phase":
		stageDef, ok := DefaultPipelineWorkflow().Stage(triggerCtx.State.Stage)
		return ok && strings.TrimSpace(stageDef.Phase) != ""
	default:
		if pc == nil || pc.module == nil || pc.module.WorkflowHooks() == nil {
			return false
		}
		passed, handled := pc.module.WorkflowHooks().EvaluateWorkflowGuard(context.Background(), pc, hookCtx, entry)
		return handled && passed
	}
}

func (pc *FactoryPipelineCoordinator) executeWorkflowTransitionActions(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	transition WorkflowTransition,
	preStageUpdate bool,
) []string {
	if pc == nil {
		return nil
	}
	executed := make([]string, 0, len(transition.Actions))
	for _, action := range transition.Actions {
		actionID := strings.TrimSpace(action.Name)
		if actionID == "" {
			continue
		}
		if workflowActionRunsPreStageUpdate(actionID) != preStageUpdate {
			continue
		}
		if pc.executeWorkflowAction(ctx, triggerCtx, actionID) {
			executed = append(executed, actionID)
		}
	}
	return executed
}

func workflowActionRunsPreStageUpdate(actionID string) bool {
	switch strings.TrimSpace(actionID) {
	case "increment_revision_count",
		"emit_vertical_shortlisted",
		"emit_vertical_marginal",
		"emit_vertical_rejected":
		return true
	default:
		return false
	}
}

func (pc *FactoryPipelineCoordinator) executeWorkflowAction(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	actionID string,
) bool {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return false
	}
	entry, ok := pc.resolveWorkflowAction(actionID)
	if !ok {
		return false
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	switch workflowActionExecutionKey(entry) {
	case "increment_revision_count":
		pc.mu.Lock()
		if st := pc.validationGate.states[hookCtx.VerticalID]; st != nil {
			st.RevisionCount++
		}
		pc.mu.Unlock()
		pc.PersistWorkflowMetadata(ctx, hookCtx.VerticalID, func(metadata map[string]any) {
			metadata["revision_count"] = asInt(metadata["revision_count"]) + 1
		})
		return true
	case "spinup_opco_org":
		pc.PersistWorkflowMetadata(ctx, hookCtx.VerticalID, func(metadata map[string]any) {
			metadata["opco_spinup_requested"] = true
		})
		return true
	case "begin_teardown":
		pc.PersistWorkflowMetadata(ctx, hookCtx.VerticalID, func(metadata map[string]any) {
			metadata["teardown_requested"] = true
		})
		return true
	default:
		if pc == nil || pc.module == nil || pc.module.WorkflowHooks() == nil {
			return false
		}
		executed, handled := pc.module.WorkflowHooks().ExecuteWorkflowAction(ctx, pc, hookCtx, entry)
		return handled && executed
	}
}

func (pc *FactoryPipelineCoordinator) currentWorkflowState(ctx context.Context, verticalID string) WorkflowState {
	verticalID = strings.TrimSpace(verticalID)
	state := workflowStateForVertical(verticalID, "", pc.validationStateSnapshot(verticalID))
	if verticalID == "" {
		return state
	}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
		if err == nil && ok {
			state = workflowStateFromInstance(instance, state)
			return state
		}
	}
	if pc.db != nil {
		var stage string
		if err := dbQueryRowContext(ctx, pc.db, `
			SELECT COALESCE(stage, '')
			FROM verticals
			WHERE id = $1::uuid
		`, verticalID).Scan(&stage); err == nil {
			state.Stage = NormalizePipelineStage(stage)
		}
	}
	return state
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowGuard(guardID string) (runtimecontracts.GuardActionEntry, bool) {
	if pc == nil || pc.GuardRegistry() == nil {
		return runtimecontracts.GuardActionEntry{}, false
	}
	entry, ok := pc.GuardRegistry().Guard(guardID)
	if !ok || !pc.GuardRegistry().IsExecutable(guardID) {
		return runtimecontracts.GuardActionEntry{}, false
	}
	return entry, true
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowAction(actionID string) (runtimecontracts.GuardActionEntry, bool) {
	if pc == nil || pc.ActionRegistry() == nil {
		return runtimecontracts.GuardActionEntry{}, false
	}
	entry, ok := pc.ActionRegistry().Action(actionID)
	if !ok || !pc.ActionRegistry().IsExecutable(actionID) {
		return runtimecontracts.GuardActionEntry{}, false
	}
	return entry, true
}

func workflowStateFromInstance(instance WorkflowInstance, fallback WorkflowState) WorkflowState {
	out := fallback
	out.Stage = NormalizePipelineStage(instance.CurrentStage)
	if metadata := cloneStringAnyMap(instance.Metadata); len(metadata) > 0 {
		out.Metadata = metadata
	}
	if pipelineState, ok := asObject(instance.AccumulatorState["pipeline-coordinator"]); ok {
		if out.Metadata == nil {
			out.Metadata = map[string]any{}
		}
		for key, value := range pipelineState {
			if _, exists := out.Metadata[key]; !exists {
				out.Metadata[key] = value
			}
		}
		if status := strings.TrimSpace(asString(pipelineState["status"])); status != "" {
			out.Status = status
		}
	}
	if status := strings.TrimSpace(asString(out.Metadata["status"])); status != "" {
		out.Status = status
	}
	if out.Metadata == nil {
		out.Metadata = map[string]any{}
	}
	return out
}

func (pc *FactoryPipelineCoordinator) PersistWorkflowMetadata(ctx context.Context, verticalID string, mutate func(metadata map[string]any)) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || mutate == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.Metadata == nil {
			instance.Metadata = map[string]any{}
		}
		mutate(instance.Metadata)
	})
}

func (pc *FactoryPipelineCoordinator) validationStateSnapshot(verticalID string) *validationPipelineState {
	if pc == nil {
		return nil
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.validationGate.states[verticalID]
	if st == nil {
		return nil
	}
	copyState := *st
	copyState.ResearchPayload = cloneRaw(st.ResearchPayload)
	copyState.SpecPayload = cloneRaw(st.SpecPayload)
	copyState.CTOPayload = cloneRaw(st.CTOPayload)
	copyState.BrandPayload = cloneRaw(st.BrandPayload)
	copyState.ScoringPayload = cloneRaw(st.ScoringPayload)
	return &copyState
}

func (pc *FactoryPipelineCoordinator) OpcoSpinupRequestedPayload(
	ctx context.Context,
	verticalID string,
	approvalPayload map[string]any,
) map[string]any {
	snap := pc.payloadFactory.ValidationContext(verticalID)
	name, geography := pc.payloadFactory.identityForPayload(ctx, verticalID)
	founderDirectives := strings.TrimSpace(asString(approvalPayload["founder_directives"]))
	brandChoice := strings.TrimSpace(asString(approvalPayload["brand_choice"]))
	brandPayload := cloneStringAnyMap(snap.Brand)
	if brandPayload == nil {
		brandPayload = map[string]any{}
	}
	if brandChoice != "" && strings.TrimSpace(asString(brandPayload["choice"])) == "" {
		brandPayload["choice"] = brandChoice
	}
	mandate := map[string]any{
		"vertical_id":        verticalID,
		"vertical_name":      name,
		"geography":          geography,
		"founder_directives": founderDirectives,
		"founder_notes":      founderDirectives,
		"business_brief":     cloneStringAnyMap(snap.Research),
		"mvp_spec":           cloneStringAnyMap(snap.Spec),
		"brand":              cloneStringAnyMap(brandPayload),
		"cto_feasibility":    cloneStringAnyMap(snap.CTONotes),
	}
	if launchTargets, ok := normalizePayloadObject(approvalPayload["launch_targets"]); ok {
		mandate["launch_targets"] = launchTargets
	}
	return map[string]any{
		"vertical_id":        verticalID,
		"mandate":            mandate,
		"brand":              cloneStringAnyMap(brandPayload),
		"founder_directives": founderDirectives,
	}
}

func normalizePayloadObject(raw any) (map[string]any, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		return cloneStringAnyMap(typed), true
	case json.RawMessage:
		var decoded map[string]any
		if err := json.Unmarshal(typed, &decoded); err == nil && len(decoded) > 0 {
			return decoded, true
		}
	case []byte:
		var decoded map[string]any
		if err := json.Unmarshal(typed, &decoded); err == nil && len(decoded) > 0 {
			return decoded, true
		}
	}
	return nil, false
}

func truthyMetadataFlag(raw any) bool {
	switch typed := raw.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func payloadSliceLen(raw any) int {
	switch typed := raw.(type) {
	case []any:
		return len(typed)
	case []string:
		return len(typed)
	default:
		return 0
	}
}

func (pc *FactoryPipelineCoordinator) ContractPolicyFloat(key string, fallback float64) float64 {
	if pc == nil || pc.ContractBundle() == nil {
		return fallback
	}
	if value, ok := asFloat64(pc.ContractBundle().Policy[strings.TrimSpace(key)]); ok {
		return value
	}
	return fallback
}

func (pc *FactoryPipelineCoordinator) ContractPolicyInt(key string, fallback int) int {
	if pc == nil || pc.ContractBundle() == nil {
		return fallback
	}
	if value, ok := pc.ContractBundle().Policy[strings.TrimSpace(key)]; ok {
		if got := asInt(value); got != 0 {
			return got
		}
	}
	return fallback
}

func scoringCompositeFromPayload(payload map[string]any) scoringComposite {
	out := scoringComposite{
		Result:         strings.TrimSpace(asString(payload["result"])),
		Reason:         strings.TrimSpace(asString(payload["reason"])),
		CompositeScore: asFloat(payload["composite_score"]),
		ViabilityScore: asFloat(payload["viability_score"]),
		MarketScore:    asFloat(payload["market_score"]),
		Rubric:         strings.TrimSpace(asString(payload["rubric"])),
		Partial:        truthyMetadataFlag(payload["partial"]),
		Dimensions:     map[string]scoreDimensionResult{},
	}
	rawDimensions, ok := asObject(payload["dimensions"])
	if !ok {
		return out
	}
	for dim, rawResult := range rawDimensions {
		resultMap, ok := asObject(rawResult)
		if !ok {
			continue
		}
		out.Dimensions[dim] = scoreDimensionResult{
			Score:      asInt(resultMap["score"]),
			Evidence:   strings.TrimSpace(asString(resultMap["evidence"])),
			Confidence: strings.TrimSpace(asString(resultMap["confidence"])),
		}
	}
	return out
}

func (pc *FactoryPipelineCoordinator) PipelineHasCapacity(ctx context.Context, limit int) bool {
	if pc == nil || pc.db == nil || limit <= 0 {
		return true
	}
	var count int
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM verticals
		WHERE stage IN ('researching', 'mvp_speccing', 'cto_spec_review', 'branding', 'ready_for_review')
	`).Scan(&count); err != nil {
		return true
	}
	return count < limit
}
