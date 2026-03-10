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

type workflowTransitionShadowComparison struct {
	FlatTransition    WorkflowTransition
	DerivedTransition WorkflowTransition
	Matched           bool
	Reason            string
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

	pc.applyWorkflowDataAccumulation(ctx, verticalID, transition, evt)
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

func (pc *FactoryPipelineCoordinator) applyWorkflowDataAccumulation(
	ctx context.Context,
	verticalID string,
	transition WorkflowTransition,
	evt events.Event,
) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	writes := transition.DataAccumulation.Writes
	if len(writes) == 0 {
		return
	}
	allowedFields := workflowEntitySchemaFields(pc.ContractBundle())
	if len(allowedFields) == 0 {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if len(payload) == 0 {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		if instance.AccumulatorState == nil {
			instance.AccumulatorState = map[string]any{}
		}
		entityProjection, _ := asObject(instance.AccumulatorState["entity_projection"])
		entityProjection = cloneStringAnyMap(entityProjection)
		if entityProjection == nil {
			entityProjection = map[string]any{}
		}
		for _, field := range writes {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, ok := allowedFields[field]; !ok {
				continue
			}
			if value, ok := payload[field]; ok {
				entityProjection[field] = value
			}
		}
		if len(entityProjection) > 0 {
			instance.AccumulatorState["entity_projection"] = entityProjection
		}
		if instance.Metadata == nil {
			instance.Metadata = map[string]any{}
		}
		instance.Metadata["last_data_accumulation_event"] = strings.TrimSpace(string(evt.Type))
		if source := strings.TrimSpace(transition.DataAccumulation.SourceEvent); source != "" {
			instance.Metadata["last_data_accumulation_source"] = source
		}
	})
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowTransitionByEvent(
	triggerCtx workflowTriggerContext,
) (WorkflowTransition, []string, bool) {
	trigger := strings.TrimSpace(string(triggerCtx.Event.Type))
	if trigger == "" {
		return WorkflowTransition{}, nil, false
	}
	var guardsEvaluated []string
	workflow := pc.WorkflowDefinition()
	if workflow == nil {
		return WorkflowTransition{}, nil, false
	}
	if promoted, evaluated, ok := pc.resolvePromotedDerivedWorkflowTransition(triggerCtx, workflow, trigger); ok {
		return promoted, evaluated, true
	}
	transition, ok := workflow.TransitionByTrigger(triggerCtx.State, trigger, func(candidate WorkflowTransition) bool {
		passed, evaluated := pc.evaluateWorkflowTransitionGuards(triggerCtx, candidate)
		if passed {
			guardsEvaluated = evaluated
		}
		return passed
	})
	if !ok {
		return WorkflowTransition{}, nil, false
	}
	_ = pc.shadowCompareDerivedWorkflowTransition(triggerCtx, transition)
	return transition, guardsEvaluated, true
}

func (pc *FactoryPipelineCoordinator) resolvePromotedDerivedWorkflowTransition(
	triggerCtx workflowTriggerContext,
	workflow *WorkflowDefinition,
	trigger string,
) (WorkflowTransition, []string, bool) {
	if !handlerFirstCandidateEnabled(trigger) {
		return WorkflowTransition{}, nil, false
	}
	derived, ok := pc.resolveDerivedWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		return WorkflowTransition{}, nil, false
	}
	stateStage := workflow.NormalizeStage(string(triggerCtx.State.Stage))
	var matched WorkflowTransition
	var matchedGuards []string
	var found bool
	for _, candidate := range workflow.transitions {
		if strings.TrimSpace(candidate.Trigger) != trigger {
			continue
		}
		if !containsPipelineStage(candidate.From, stateStage) {
			continue
		}
		if matched, _ := workflowTransitionSemanticParity(candidate, derived); !matched {
			continue
		}
		passed, evaluated := pc.evaluateWorkflowTransitionGuards(triggerCtx, candidate)
		if !passed {
			continue
		}
		if found {
			return WorkflowTransition{}, nil, false
		}
		matched = candidate
		matchedGuards = evaluated
		found = true
	}
	if !found {
		return WorkflowTransition{}, nil, false
	}
	return matched, matchedGuards, true
}

func handlerFirstCandidateEnabled(trigger string) bool {
	switch strings.TrimSpace(trigger) {
	case "vertical.shortlisted", "research.completed", "cto.spec_approved",
		"opco.steady_state_reached", "opco.growth_triggered", "opco.growth_stabilized",
		"build_complete", "launch_ready", "opco.teardown_requested",
		"vertical.ready_for_review", "research.vertical_rejected",
		"spec.validation_failed", "cto.spec_revision_needed":
		return true
	default:
		return false
	}
}

func (pc *FactoryPipelineCoordinator) shadowCompareDerivedWorkflowTransition(
	triggerCtx workflowTriggerContext,
	flat WorkflowTransition,
) workflowTransitionShadowComparison {
	derived, ok := pc.resolveDerivedWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		return workflowTransitionShadowComparison{FlatTransition: flat, Reason: "no_derived_candidate"}
	}
	matched, reason := workflowTransitionSemanticParity(flat, derived)
	return workflowTransitionShadowComparison{
		FlatTransition:    flat,
		DerivedTransition: derived,
		Matched:           matched,
		Reason:            reason,
	}
}

func (pc *FactoryPipelineCoordinator) resolveDerivedWorkflowTransitionByEvent(
	triggerCtx workflowTriggerContext,
) (WorkflowTransition, bool) {
	if pc == nil || pc.ContractBundle() == nil {
		return WorkflowTransition{}, false
	}
	trigger := strings.TrimSpace(string(triggerCtx.Event.Type))
	if trigger == "" {
		return WorkflowTransition{}, false
	}
	bundle := pc.ContractBundle()
	owners := bundle.RuntimeEventOwners(trigger)
	if len(owners) == 0 {
		return WorkflowTransition{}, false
	}
	var match runtimecontracts.HandlerTransitionSemantic
	var matched bool
	for _, owner := range owners {
		derived, ok := bundle.DerivedHandlerTransition(owner, trigger)
		if !ok {
			continue
		}
		if strings.TrimSpace(derived.AdvancesTo) == "" {
			continue
		}
		if matched {
			return WorkflowTransition{}, false
		}
		match = derived
		matched = true
	}
	if !matched {
		return WorkflowTransition{}, false
	}
	return workflowTransitionFromDerivedSemantic(triggerCtx.State, match), true
}

func workflowTransitionFromDerivedSemantic(
	state WorkflowState,
	derived runtimecontracts.HandlerTransitionSemantic,
) WorkflowTransition {
	transition := WorkflowTransition{
		Name:             strings.TrimSpace(derived.ID),
		From:             []PipelineStage{NormalizePipelineStage(string(state.Stage))},
		To:               NormalizePipelineStage(strings.TrimSpace(derived.AdvancesTo)),
		Trigger:          strings.TrimSpace(derived.EventType),
		Node:             strings.TrimSpace(derived.NodeID),
		DataAccumulation: derived.DataAccumulation,
	}
	if guardID := strings.TrimSpace(asString(derived.Guard)); guardID != "" {
		transition.GuardIDs = []string{guardID}
	}
	actionID := strings.TrimSpace(derived.Action)
	emitID := strings.TrimSpace(derived.Emits)
	if actionID != "" || emitID != "" {
		action := WorkflowAction{Name: actionID}
		if emitID != "" {
			action.Emits = emitID
			if action.Name == "" {
				action.Name = "emit:" + emitID
			}
		}
		transition.Actions = []WorkflowAction{action}
	}
	return transition
}

func workflowTransitionSemanticParity(flat WorkflowTransition, derived WorkflowTransition) (bool, string) {
	if NormalizePipelineStage(string(flat.To)) != NormalizePipelineStage(string(derived.To)) {
		return false, "target_mismatch"
	}
	if strings.TrimSpace(flat.Trigger) != strings.TrimSpace(derived.Trigger) {
		return false, "trigger_mismatch"
	}
	if strings.TrimSpace(flat.Node) != strings.TrimSpace(derived.Node) {
		return false, "node_mismatch"
	}
	if len(flat.GuardIDs) > 0 && len(derived.GuardIDs) > 0 && strings.TrimSpace(flat.GuardIDs[0]) != strings.TrimSpace(derived.GuardIDs[0]) {
		return false, "guard_mismatch"
	}
	if len(flat.Actions) > 0 && len(derived.Actions) > 0 {
		flatAction := strings.TrimSpace(flat.Actions[0].Name)
		derivedAction := strings.TrimSpace(derived.Actions[0].Name)
		flatEmit := strings.TrimSpace(flat.Actions[0].Emits)
		derivedEmit := strings.TrimSpace(derived.Actions[0].Emits)
		if flatEmit != "" && derivedEmit != "" && flatEmit == derivedEmit {
			return true, "emit_match"
		}
		if flatAction != "" && derivedAction != "" && !workflowTransitionActionAliasesMatch(flatAction, derivedAction) {
			return false, "action_mismatch"
		}
	}
	return true, "match"
}

func workflowTransitionActionAliasesMatch(flatAction string, derivedAction string) bool {
	flatAction = strings.TrimSpace(flatAction)
	derivedAction = strings.TrimSpace(derivedAction)
	if flatAction == derivedAction {
		return true
	}
	switch {
	case flatAction == "increment_revision_count" && derivedAction == "revision_loop":
		return true
	default:
		return false
	}
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
		workflow := pc.WorkflowDefinition()
		if workflow == nil {
			return true
		}
		stageDef, ok := workflow.Stage(triggerCtx.State.Stage)
		return !ok || !strings.EqualFold(strings.TrimSpace(stageDef.Phase), "operating")
	case "stage_in_phase":
		workflow := pc.WorkflowDefinition()
		if workflow == nil {
			return false
		}
		stageDef, ok := workflow.Stage(triggerCtx.State.Stage)
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
		pc.mutateValidationState(ctx, hookCtx.VerticalID, func(st *validationPipelineState) {
			st.RevisionCount++
		})
		if pc.workflowStore != nil && pc.workflowStore.Enabled() {
			_ = pc.workflowStore.Mutate(ctx, hookCtx.VerticalID, func(instance *WorkflowInstance) {
				if instance.Metadata == nil {
					instance.Metadata = map[string]any{}
				}
				instance.Metadata["revision_count"] = asInt(instance.Metadata["revision_count"]) + 1
				if instance.AccumulatorState == nil {
					instance.AccumulatorState = map[string]any{}
				}
				if bucket, ok := asObject(instance.AccumulatorState["validation-orchestrator"]); ok {
					bucket["revision_count"] = asInt(bucket["revision_count"]) + 1
					instance.AccumulatorState["validation-orchestrator"] = bucket
				}
			})
		}
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
	if validationState, ok := asObject(instance.AccumulatorState["validation-orchestrator"]); ok {
		if out.Metadata == nil {
			out.Metadata = map[string]any{}
		}
		if gateState, ok := asObject(validationState["gate_state"]); ok {
			for _, gate := range []string{"g1_research", "g2_spec", "g3_cto", "g4_brand"} {
				if _, exists := out.Metadata[gate]; !exists {
					out.Metadata[gate] = gateState[gate]
				}
			}
		}
		if _, exists := out.Metadata["revision_count"]; !exists {
			out.Metadata["revision_count"] = validationState["revision_count"]
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
	st := pc.validationGate.states[verticalID]
	pc.mu.Unlock()
	if st != nil {
		return cloneValidationPipelineState(st)
	}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if instance, ok, err := pc.workflowStore.Load(context.Background(), verticalID); err == nil && ok {
			if restored, ok := restoreValidationStateFromInstance(instance); ok {
				return restored
			}
		}
	}
	return nil
}

func (pc *FactoryPipelineCoordinator) mutateValidationState(
	ctx context.Context,
	verticalID string,
	mutate func(*validationPipelineState),
) *validationPipelineState {
	if pc == nil || mutate == nil {
		return nil
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return nil
	}
	pc.mu.Lock()
	if st := pc.validationGate.states[verticalID]; st != nil {
		mutate(st)
		snapshot := cloneValidationPipelineState(st)
		pc.mu.Unlock()
		return snapshot
	}
	pc.mu.Unlock()

	var restored *validationPipelineState
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if instance, ok, err := pc.workflowStore.Load(ctx, verticalID); err == nil && ok {
			if st, ok := restoreValidationStateFromInstance(instance); ok {
				restored = st
			}
		}
	}
	if restored == nil {
		return nil
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if st := pc.validationGate.states[verticalID]; st != nil {
		mutate(st)
		return cloneValidationPipelineState(st)
	}
	mutate(restored)
	pc.validationGate.states[verticalID] = restored
	return cloneValidationPipelineState(restored)
}

func cloneValidationPipelineState(st *validationPipelineState) *validationPipelineState {
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
