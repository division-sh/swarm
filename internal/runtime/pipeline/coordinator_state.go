package pipeline

import (
	"context"
	"database/sql"
	"strings"
)

func (pc *FactoryPipelineCoordinator) ensureStateLoaded(ctx context.Context) {
	if pc == nil || pc.db == nil || pc.stateStore == nil {
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
	workflowStages := map[string]string{}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if instances, err := pc.workflowStore.List(ctx); err == nil {
			for _, instance := range instances {
				if st, ok := restoreValidationStateFromInstance(instance); ok {
					workflowValidations[strings.TrimSpace(instance.InstanceID)] = st
				}
				if acc, ok := restoreScoringAccumulatorFromInstance(instance); ok {
					workflowScoring[strings.TrimSpace(instance.InstanceID)] = acc
				}
				if verticalID := strings.TrimSpace(instance.InstanceID); verticalID != "" {
					workflowStages[verticalID] = strings.TrimSpace(instance.CurrentStage)
				}
			}
		}
	}
	snapshot := pc.stateStore.Load(ctx)

	pc.mu.Lock()
	if pc.stateLoaded {
		pc.mu.Unlock()
		return
	}
	if len(snapshot.Scans) > 0 {
		pc.scanCoordinator.scans = snapshot.Scans
	}
	if pc.scoringState.accumulators == nil {
		pc.scoringState.accumulators = make(map[string]*scoringAccumulator)
	}
	for verticalID, acc := range workflowScoring {
		pc.scoringState.accumulators[verticalID] = acc
	}
	if len(snapshot.PendingDedup) > 0 {
		pc.scanCoordinator.pendingDedup = snapshot.PendingDedup
	}
	if len(workflowValidations) > 0 {
		pc.validationGate.states = workflowValidations
	} else if len(snapshot.Validations) > 0 {
		pc.validationGate.states = snapshot.Validations
	}
	if len(snapshot.Processed) > 0 {
		pc.processed = snapshot.Processed
	}
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
	for verticalID, st := range snapshot.Validations {
		if st == nil {
			continue
		}
		pc.updateVerticalStage(ctx, verticalID, pc.validationGate.stageForState(st), "")
	}
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
	ok := pc.stateStore.MarkProcessed(ctx, pc.processed, eventID)
	pc.mu.Unlock()
	return ok
}

func (pc *FactoryPipelineCoordinator) persistRuntimeState(ctx context.Context) {
	if pc == nil || pc.db == nil || pc.stateStore == nil {
		return
	}
	ctx = withoutSQLTxContext(ctx)
	if !pc.isStatePersistenceEnabled(ctx) {
		return
	}
	pc.mu.Lock()
	scans := pc.scanCoordinator.scans
	pending := pc.scanCoordinator.pendingDedup
	validations := pc.validationGate.states
	defer pc.mu.Unlock()
	pc.stateStore.Persist(ctx, scans, pending, validations)
}

func (pc *FactoryPipelineCoordinator) clearPersistentState(ctx context.Context) {
	if pc == nil || pc.db == nil || pc.stateStore == nil {
		return
	}
	if !pc.isStatePersistenceEnabled(ctx) {
		return
	}
	pc.stateStore.Clear(ctx, pc.isScoringDigestBufferEnabled(ctx))
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
	return detectStatePersistenceTables(ctx, db)
}
