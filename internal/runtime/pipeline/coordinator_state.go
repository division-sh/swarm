package pipeline

import (
	"context"
	"database/sql"
	"strings"
)

func (pc *FactoryPipelineCoordinator) ensureStateLoaded(ctx context.Context) {
	if pc == nil || pc.db == nil {
		return
	}
	if !pc.isStatePersistenceEnabled(ctx) {
		pc.mu.Lock()
		pc.stateLoaded = true
		pc.mu.Unlock()
		return
	}
	pc.mu.Lock()
	loaded := pc.stateLoaded
	pc.mu.Unlock()
	if loaded {
		return
	}
	workflowValidations := map[string]*validationPipelineState{}
	workflowScoring := map[string]*scoringAccumulator{}
	workflowScans := map[string]*scanAccumulator{}
	workflowPending := map[string]pendingCandidate{}
	workflowStages := map[string]string{}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if instances, err := pc.workflowStore.List(ctx); err == nil {
			for _, instance := range instances {
				if st, ok := restoreValidationStateFromInstance(instance); ok {
					workflowValidations[strings.TrimSpace(instance.InstanceID)] = st
				}
				if acc, ok := pc.loadWorkflowScoringAccumulator(ctx, strings.TrimSpace(instance.InstanceID)); ok {
					workflowScoring[strings.TrimSpace(instance.InstanceID)] = acc
				}
				if acc, pending, ok := restoreScanStateFromInstance(instance); ok {
					workflowScans[strings.TrimSpace(acc.ScanID)] = acc
					for dedupID, cand := range pending {
						workflowPending[dedupID] = cand
					}
				}
				if verticalID := strings.TrimSpace(instance.InstanceID); verticalID != "" {
					workflowStages[verticalID] = strings.TrimSpace(instance.CurrentState)
				}
			}
		}
	}
	pc.mu.Lock()
	if pc.stateLoaded {
		pc.mu.Unlock()
		return
	}
	pc.scanCoordinator.scans = mergeScanAccumulators(nil, workflowScans)
	if pc.scoringState.accumulators == nil {
		pc.scoringState.accumulators = make(map[string]*scoringAccumulator)
	}
	for verticalID, acc := range workflowScoring {
		pc.scoringState.accumulators[verticalID] = acc
	}
	pc.scanCoordinator.pendingDedup = mergePendingCandidates(nil, workflowPending)
	pc.validationGate.states = mergeValidationStates(nil, workflowValidations)
	pc.stateLoaded = true
	pc.mu.Unlock()

	// Ensure dashboard-facing stage projection is consistent with recovered validation state.
	if len(workflowStages) > 0 {
		for verticalID, stage := range workflowStages {
			if strings.TrimSpace(stage) == "" {
				continue
			}
			pc.updateVerticalStage(ctx, verticalID, stage, "")
		}
		return
	}
}

func mergeScanAccumulators(base, override map[string]*scanAccumulator) map[string]*scanAccumulator {
	if len(base) == 0 && len(override) == 0 {
		return map[string]*scanAccumulator{}
	}
	out := make(map[string]*scanAccumulator, len(base)+len(override))
	for scanID, acc := range base {
		if acc == nil {
			continue
		}
		out[scanID] = cloneScanAccumulator(acc)
	}
	for scanID, acc := range override {
		if acc == nil {
			continue
		}
		out[scanID] = cloneScanAccumulator(acc)
	}
	return out
}

func mergePendingCandidates(base, override map[string]pendingCandidate) map[string]pendingCandidate {
	if len(base) == 0 && len(override) == 0 {
		return map[string]pendingCandidate{}
	}
	out := make(map[string]pendingCandidate, len(base)+len(override))
	for dedupID, cand := range base {
		out[dedupID] = cand
	}
	for dedupID, cand := range override {
		out[dedupID] = cand
	}
	return out
}

func mergeValidationStates(base, override map[string]*validationPipelineState) map[string]*validationPipelineState {
	if len(base) == 0 && len(override) == 0 {
		return map[string]*validationPipelineState{}
	}
	out := make(map[string]*validationPipelineState, len(base)+len(override))
	for verticalID, st := range base {
		if st == nil {
			continue
		}
		copied := *st
		copied.ResearchPayload = cloneRaw(st.ResearchPayload)
		copied.SpecPayload = cloneRaw(st.SpecPayload)
		copied.CTOPayload = cloneRaw(st.CTOPayload)
		copied.BrandPayload = cloneRaw(st.BrandPayload)
		copied.ScoringPayload = cloneRaw(st.ScoringPayload)
		out[verticalID] = &copied
	}
	for verticalID, st := range override {
		if st == nil {
			continue
		}
		copied := *st
		copied.ResearchPayload = cloneRaw(st.ResearchPayload)
		copied.SpecPayload = cloneRaw(st.SpecPayload)
		copied.CTOPayload = cloneRaw(st.CTOPayload)
		copied.BrandPayload = cloneRaw(st.BrandPayload)
		copied.ScoringPayload = cloneRaw(st.ScoringPayload)
		out[verticalID] = &copied
	}
	return out
}

func (pc *FactoryPipelineCoordinator) markEventProcessed(ctx context.Context, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false
	}
	pc.mu.Lock()
	if _, ok := pc.processed[eventID]; ok {
		pc.mu.Unlock()
		return false
	}
	pc.processed[eventID] = struct{}{}
	pc.mu.Unlock()
	return true
}

func (pc *FactoryPipelineCoordinator) persistRuntimeState(ctx context.Context) {
	if pc == nil || pc.db == nil {
		return
	}
	ctx = withoutSQLTxContext(ctx)
	if !pc.isStatePersistenceEnabled(ctx) {
		return
	}
	pc.mu.Lock()
	scans := pc.scanCoordinator.scans
	pending := pc.scanCoordinator.pendingDedup
	defer pc.mu.Unlock()
	pc.persistWorkflowScanProjection(ctx, scans, pending)
}

func (pc *FactoryPipelineCoordinator) clearPersistentState(ctx context.Context) {
	_ = ctx
}

func (pc *FactoryPipelineCoordinator) isStatePersistenceEnabled(ctx context.Context) bool {
	if pc == nil || pc.db == nil {
		return false
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.statePersistenceEnabled
}

func detectStatePersistence(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	var ok bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.workflow_instances') IS NOT NULL`).Scan(&ok); err != nil {
		return false
	}
	return ok
}
