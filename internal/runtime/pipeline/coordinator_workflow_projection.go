package pipeline

import (
	"context"
	"strings"
	"time"
)

func (pc *FactoryPipelineCoordinator) persistWorkflowStageProjection(ctx context.Context, verticalID, currentStage, nextStage, sourceEvent string, state WorkflowState) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	currentStage = strings.TrimSpace(string(NormalizePipelineStage(currentStage)))
	nextStage = strings.TrimSpace(string(NormalizePipelineStage(nextStage)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	if verticalID == "" || nextStage == "" {
		return
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		runtimeWarn("pipeline-coordinator", "workflow instance load failed vertical_id=%s: %v", verticalID, err)
		return
	}
	bundle := pc.ContractBundle()
	enteredStageAt := time.Now().UTC()
	if ok && strings.TrimSpace(instance.CurrentStage) == nextStage && !instance.EnteredStageAt.IsZero() {
		enteredStageAt = instance.EnteredStageAt
	}
	if !ok {
		instance = WorkflowInstance{
			InstanceID:      verticalID,
			WorkflowName:    strings.TrimSpace(bundle.Workflow.Workflow.Name),
			WorkflowVersion: strings.TrimSpace(bundle.Workflow.Workflow.Version),
		}
	} else {
		if strings.TrimSpace(instance.WorkflowName) == "" {
			instance.WorkflowName = strings.TrimSpace(bundle.Workflow.Workflow.Name)
		}
		if strings.TrimSpace(instance.WorkflowVersion) == "" {
			instance.WorkflowVersion = strings.TrimSpace(bundle.Workflow.Workflow.Version)
		}
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	if strings.TrimSpace(state.Status) != "" {
		state.Metadata["status"] = strings.TrimSpace(state.Status)
	}
	if sourceEvent != "" {
		state.Metadata["last_source_event"] = sourceEvent
	}
	instance.CurrentStage = nextStage
	instance.EnteredStageAt = enteredStageAt
	instance.Metadata = cloneStringAnyMap(state.Metadata)
	if instance.AccumulatorState == nil {
		instance.AccumulatorState = map[string]any{}
	}
	instance.AccumulatorState["pipeline-coordinator"] = cloneStringAnyMap(state.Metadata)
	if currentStage != "" && currentStage != nextStage {
		instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(currentStage, nextStage, sourceEvent))
	} else if currentStage == "" && len(instance.TransitionHistory) == 0 {
		instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord("", nextStage, sourceEvent))
	}
	if err := pc.workflowStore.Upsert(ctx, instance); err != nil {
		runtimeWarn("pipeline-coordinator", "workflow instance upsert failed vertical_id=%s stage=%s: %v", verticalID, nextStage, err)
	}
}

func workflowTransitionRecord(fromStage, toStage, sourceEvent string) WorkflowTransitionRecord {
	fromStage = strings.TrimSpace(string(NormalizePipelineStage(fromStage)))
	toStage = strings.TrimSpace(string(NormalizePipelineStage(toStage)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	state := WorkflowState{Stage: NormalizePipelineStage(fromStage)}
	transition, ok := EmpirePipelineWorkflow().Transition(state, NormalizePipelineStage(toStage))
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
