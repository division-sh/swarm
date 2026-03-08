package pipeline

import (
	"context"
	"encoding/json"
	"strings"

	"empireai/internal/events"
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
	transition, ok := EmpirePipelineWorkflow().TransitionByTrigger(triggerCtx.State, trigger, func(candidate WorkflowTransition) bool {
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
	switch guardID {
	case "signal_above_threshold":
		return asFloat(parsePayloadMap(triggerCtx.Event.Payload)["signal_strength"]) >= pc.contractPolicyFloat("signal_threshold", 55)
	case "composite_above_shortlist":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		if result := strings.TrimSpace(asString(payload["result"])); result != "" {
			return strings.EqualFold(result, "shortlisted")
		}
		return asFloat(payload["composite_score"]) >= pc.contractPolicyFloat("composite_shortlist", 75)
	case "composite_in_marginal_range":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		if result := strings.TrimSpace(asString(payload["result"])); result != "" {
			return strings.EqualFold(result, "marginal")
		}
		composite := asFloat(payload["composite_score"])
		low := pc.contractPolicyFloat("composite_marginal_low", 55)
		high := pc.contractPolicyFloat("composite_shortlist", 75)
		return composite >= low && composite < high && pc.evaluateWorkflowGuard(triggerCtx, "marginal_promotion_eligible")
	case "composite_below_marginal":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		if result := strings.TrimSpace(asString(payload["result"])); result != "" {
			return strings.EqualFold(result, "rejected")
		}
		composite := asFloat(payload["composite_score"])
		low := pc.contractPolicyFloat("composite_marginal_low", 55)
		high := pc.contractPolicyFloat("composite_shortlist", 75)
		if composite < low {
			return true
		}
		return composite >= low && composite < high && !pc.evaluateWorkflowGuard(triggerCtx, "marginal_promotion_eligible")
	case "both_hard_gates_pass":
		floor := pc.contractPolicyInt("hard_gate_floor", 50)
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		return scoringDimensionScore(payload, "build_complexity") >= floor &&
			scoringDimensionScore(payload, "automation_completeness") >= floor
	case "marginal_promotion_eligible":
		threshold := pc.contractPolicyInt("marginal_tier1_dimensions_above_70", 2)
		count := 0
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		for _, dim := range empireTier1Dimensions {
			if scoringDimensionScore(payload, dim) >= 70 {
				count++
			}
		}
		return count >= threshold
	case "pipeline_has_capacity":
		return pc.pipelineHasCapacity(context.Background(), pc.contractPolicyInt("pipeline_capacity_max", 3))
	case "has_vertical_id", "has_entity_id":
		if strings.TrimSpace(triggerCtx.Event.VerticalID) != "" {
			return true
		}
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		return strings.TrimSpace(asString(payload["vertical_id"])) != ""
	case "has_human_decision":
		source := strings.TrimSpace(triggerCtx.Event.SourceAgent)
		if strings.EqualFold(source, "human") || strings.EqualFold(source, "mailbox") {
			return true
		}
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		return strings.TrimSpace(asString(payload["mailbox_decision_id"])) != ""
	case "inner_revision_count_below_limit", "revision_count_below_limit":
		return asInt(triggerCtx.State.Metadata["revision_count"]) < maxRevisionCycles
	case "gate_g1_research":
		return truthyMetadataFlag(triggerCtx.State.Metadata["g1_research"])
	case "gate_g2_spec":
		return truthyMetadataFlag(triggerCtx.State.Metadata["g2_spec"])
	case "gate_g3_cto":
		return truthyMetadataFlag(triggerCtx.State.Metadata["g3_cto"])
	case "gate_g4_brand":
		return truthyMetadataFlag(triggerCtx.State.Metadata["g4_brand"])
	case "all_gates_met":
		return truthyMetadataFlag(triggerCtx.State.Metadata["g1_research"]) &&
			truthyMetadataFlag(triggerCtx.State.Metadata["g2_spec"]) &&
			truthyMetadataFlag(triggerCtx.State.Metadata["g3_cto"]) &&
			truthyMetadataFlag(triggerCtx.State.Metadata["g4_brand"])
	case "not_in_operating_phase":
		stageDef, ok := EmpirePipelineWorkflow().Stage(triggerCtx.State.Stage)
		return !ok || !strings.EqualFold(strings.TrimSpace(stageDef.Phase), "operating")
	case "stage_in_phase":
		stageDef, ok := EmpirePipelineWorkflow().Stage(triggerCtx.State.Stage)
		return ok && strings.TrimSpace(stageDef.Phase) != ""
	case "spec_validation_passed":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		return strings.EqualFold(strings.TrimSpace(asString(payload["status"])), "passed") ||
			strings.EqualFold(strings.TrimSpace(asString(payload["passed"])), "true")
	case "qa_passed":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		return strings.EqualFold(strings.TrimSpace(asString(payload["qa_passed"])), "true") ||
			strings.EqualFold(strings.TrimSpace(asString(payload["status"])), "passed")
	case "deploy_approved":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		return strings.EqualFold(strings.TrimSpace(asString(payload["decision"])), "approved") ||
			strings.EqualFold(strings.TrimSpace(asString(payload["deploy_approved"])), "true")
	case "has_retention_primitive":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		if items, ok := payload["retention_primitives"].([]any); ok {
			return len(items) > 0
		}
		if items, ok := payload["retention_primitives"].([]string); ok {
			return len(items) > 0
		}
		return false
	case "evidence_sufficient":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		competitors := payloadSliceLen(payload["competitors"])
		painSignals := payloadSliceLen(payload["pain_signals"])
		return competitors > 0 && painSignals > 0
	default:
		// Keep non-validation Empire guards permissive until their owning node is cut over.
		return true
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
	verticalID := strings.TrimSpace(triggerCtx.Event.VerticalID)
	if verticalID == "" {
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	switch actionID {
	case "increment_revision_count":
		pc.mu.Lock()
		if st := pc.validationGate.states[verticalID]; st != nil {
			st.RevisionCount++
		}
		pc.mu.Unlock()
		return true
	case "emit_validation_started":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		pc.publish(ctx, "validation.started", verticalID, payloadMap(pc.payloadFactory.BuildValidationStartedPayload(ctx, verticalID, payload, nil)))
		return true
	case "emit_vertical_shortlisted":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		pc.publish(ctx, "vertical.shortlisted", verticalID, payloadMap(pc.payloadFactory.BuildVerticalShortlistedPayload(
			verticalID,
			asFloat(payload["composite_score"]),
			asFloat(payload["viability_score"]),
			payload,
		)))
		return true
	case "emit_vertical_marginal":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		pc.publish(ctx, "vertical.marginal", verticalID, payloadMap(pc.payloadFactory.BuildVerticalMarginalPayload(
			verticalID,
			scoringCompositeFromPayload(payload),
		)))
		return true
	case "emit_vertical_rejected":
		payload := parsePayloadMap(triggerCtx.Event.Payload)
		pc.publish(ctx, "vertical.rejected", verticalID, payloadMap(pc.payloadFactory.BuildVerticalRejectedPayload(
			verticalID,
			scoringCompositeFromPayload(payload),
		)))
		return true
	case "emit_opco_spinup_requested":
		// The empire-coordinator agent still owns this emit path in the current runtime.
		return false
	case "spinup_opco_org":
		return true
	default:
		return false
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
			state.Stage = NormalizePipelineStage(instance.CurrentStage)
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

func (pc *FactoryPipelineCoordinator) opcoSpinupRequestedPayload(
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

var empireTier1Dimensions = []string{
	"icp_crispness",
	"distribution_leverage",
	"time_to_value",
	"operational_drag",
}

func (pc *FactoryPipelineCoordinator) contractPolicyFloat(key string, fallback float64) float64 {
	if pc == nil || pc.ContractBundle() == nil {
		return fallback
	}
	if value, ok := asFloat64(pc.ContractBundle().Policy[strings.TrimSpace(key)]); ok {
		return value
	}
	return fallback
}

func (pc *FactoryPipelineCoordinator) contractPolicyInt(key string, fallback int) int {
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

func scoringDimensionScore(payload map[string]any, dimension string) int {
	dimension = strings.TrimSpace(dimension)
	if dimension == "" || len(payload) == 0 {
		return 0
	}
	rawDimensions, ok := asObject(payload["dimensions"])
	if !ok {
		return 0
	}
	rawResult, ok := asObject(rawDimensions[dimension])
	if !ok {
		return 0
	}
	return asInt(rawResult["score"])
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

func (pc *FactoryPipelineCoordinator) pipelineHasCapacity(ctx context.Context, limit int) bool {
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
