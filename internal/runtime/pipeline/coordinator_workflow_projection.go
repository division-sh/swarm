package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/runtime/semanticview"
)

func (pc *FactoryPipelineCoordinator) persistWorkflowStageProjection(ctx context.Context, verticalID, currentStage, nextStage, sourceEvent string, state WorkflowState) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	currentStage = strings.TrimSpace(string(NormalizeWorkflowStateID(currentStage)))
	nextStage = strings.TrimSpace(string(NormalizeWorkflowStateID(nextStage)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || nextStage == "" {
		return
	}
	source := pc.SemanticSource()
	if err := pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		enteredStageAt := time.Now().UTC()
		if strings.TrimSpace(instance.CurrentState) == nextStage && !instance.EnteredStageAt.IsZero() {
			enteredStageAt = instance.EnteredStageAt
		}
		if strings.TrimSpace(instance.WorkflowName) == "" {
			instance.WorkflowName = source.WorkflowName()
		}
		if strings.TrimSpace(instance.WorkflowVersion) == "" {
			instance.WorkflowVersion = source.WorkflowVersion()
		}
		metadata := cloneStringAnyMap(state.Metadata)
		if strings.TrimSpace(state.Status) != "" {
			metadata["status"] = strings.TrimSpace(state.Status)
		}
		if sourceEvent != "" {
			metadata["last_source_event"] = sourceEvent
		}
		instance.CurrentState = nextStage
		instance.EnteredStageAt = enteredStageAt
		instance.Metadata = metadata
		validationStartedAt, validationCompletedAt := existingValidationProjectionTimes(instance)
		workflowSetStateBucket(instance, workflowStateBucketValidationOrchestrator, encodeValidationProjection(
			source,
			verticalID,
			metadata,
			enteredStageAt,
			nextStage,
			validationStartedAt,
			validationCompletedAt,
		))
		if currentStage != "" && currentStage != nextStage {
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(currentStage, nextStage, sourceEvent))
		} else if currentStage == "" && len(instance.TransitionHistory) == 0 {
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord("", nextStage, sourceEvent))
		}
	}); err != nil {
		runtimeWarn(runtimeWorkflowID, "workflow instance upsert failed vertical_id=%s stage=%s: %v", verticalID, nextStage, err)
	}
	pc.reconcileWorkflowStageTimers(ctx, verticalID, currentStage, nextStage, sourceEvent)
}

func existingValidationProjectionTimes(instance *WorkflowInstance) (time.Time, time.Time) {
	if instance == nil {
		return time.Time{}, time.Time{}
	}
	bucket, ok := workflowValidationProjectionBucket(*instance)
	if !ok {
		return time.Time{}, time.Time{}
	}
	return parseWorkflowTime(bucket["started_at"]), parseWorkflowTime(bucket["completed_at"])
}

func encodeValidationProjection(source semanticview.Source, verticalID string, metadata map[string]any, enteredStageAt time.Time, nextStage string, existingStartedAt, existingCompletedAt time.Time) map[string]any {
	fields := workflowSystemNodeStateSchemaFields(source, workflowStateBucketValidationOrchestrator)
	if len(fields) == 0 {
		return map[string]any{}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	out := map[string]any{}
	if _, ok := fields["vertical_id"]; ok {
		out["vertical_id"] = strings.TrimSpace(verticalID)
	}
	if _, ok := fields["gate_state"]; ok {
		out["gate_state"] = map[string]any{
			"g1_research": truthyMetadataFlag(metadata["g1_research"]),
			"g2_spec":     truthyMetadataFlag(metadata["g2_spec"]),
			"g3_cto":      truthyMetadataFlag(metadata["g3_cto"]),
			"g4_brand":    truthyMetadataFlag(metadata["g4_brand"]),
		}
	}
	if _, ok := fields["revision_count"]; ok {
		out["revision_count"] = asInt(metadata["revision_count"])
	}
	if _, ok := fields["started_at"]; ok {
		startedAt := existingStartedAt
		if startedAt.IsZero() {
			startedAt = enteredStageAt
		}
		if !startedAt.IsZero() {
			out["started_at"] = startedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	if _, ok := fields["completed_at"]; ok {
		completedAt := existingCompletedAt
		if completedAt.IsZero() && validationProjectionCompleteStage(nextStage) {
			completedAt = time.Now().UTC()
		}
		if !completedAt.IsZero() {
			out["completed_at"] = completedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	return out
}

func validationProjectionCompleteStage(stage string) bool {
	switch strings.TrimSpace(string(NormalizeWorkflowStateID(stage))) {
	case "ready_for_review", "killed":
		return true
	default:
		return false
	}
}

func workflowTransitionRecord(fromStage, toStage, sourceEvent string) WorkflowTransitionRecord {
	fromStage = strings.TrimSpace(string(NormalizeWorkflowStateID(fromStage)))
	toStage = strings.TrimSpace(string(NormalizeWorkflowStateID(toStage)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	state := WorkflowState{Stage: NormalizeWorkflowStateID(fromStage)}
	transition, ok := DefaultPipelineWorkflow().Transition(state, NormalizeWorkflowStateID(toStage))
	record := WorkflowTransitionRecord{
		From:            fromStage,
		To:              toStage,
		GuardsEvaluated: nil,
		FiredAt:         time.Now().UTC(),
	}
	if ok {
		record.TransitionID = strings.TrimSpace(transition.Name)
		record.GuardsEvaluated = append([]string{}, transition.GuardIDs...)
	} else {
		record.TransitionID = firstNonEmptyString(
			sourceEvent,
			"legacy_"+strings.ReplaceAll(fromStage, "-", "_")+"_to_"+strings.ReplaceAll(toStage, "-", "_"),
		)
	}
	return record
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
