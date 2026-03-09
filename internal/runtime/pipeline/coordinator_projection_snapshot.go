package pipeline

import "context"

func (pc *FactoryPipelineCoordinator) fillTransitionSnapshotWorkflowCounts(out map[string]any) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	needsWorkflowCounts := asInt(out["scans_active"]) == 0 || asInt(out["scoring_active"]) == 0 || asInt(out["pending_dedup"]) == 0 || asInt(out["validations"]) == 0
	if !needsWorkflowCounts {
		return
	}
	items, err := pc.workflowStore.List(context.Background())
	if err != nil {
		return
	}
	var workflowScans, workflowScoring, workflowPending, workflowValidations int
	for _, item := range items {
		if acc, pending, ok := restoreScanStateFromInstance(item); ok && acc != nil {
			workflowScans++
			workflowPending += len(pending)
		}
		if acc, ok := restoreScoringAccumulatorFromInstance(item); ok && acc != nil {
			workflowScoring++
		}
		if st, ok := restoreValidationStateFromInstance(item); ok && st != nil {
			workflowValidations++
		}
	}
	if asInt(out["scans_active"]) == 0 && workflowScans > 0 {
		out["scans_active"] = workflowScans
	}
	if asInt(out["scoring_active"]) == 0 && workflowScoring > 0 {
		out["scoring_active"] = workflowScoring
	}
	if asInt(out["pending_dedup"]) == 0 && workflowPending > 0 {
		out["pending_dedup"] = workflowPending
	}
	if asInt(out["validations"]) == 0 && workflowValidations > 0 {
		out["validations"] = workflowValidations
	}
}
