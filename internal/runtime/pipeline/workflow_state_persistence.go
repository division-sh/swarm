package pipeline

import (
	"context"
	"strings"
	"sync"
	"time"
)

func (pc *FactoryPipelineCoordinator) currentWorkflowState(ctx context.Context, entityID string) WorkflowState {
	entityID = strings.TrimSpace(entityID)
	state := WorkflowState{
		EntityID: entityID,
		Stage:    NormalizeWorkflowStateID(""),
		Metadata: map[string]any{},
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || entityID == "" {
		return state
	}
	instance, ok, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		return state
	}
	state.Stage = NormalizeWorkflowStateID(instance.CurrentState)
	state.Metadata = cloneStringAnyMap(instance.Metadata)
	return state
}

func (pc *FactoryPipelineCoordinator) updateEntityState(ctx context.Context, entityID, nextState, sourceEvent string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	entityID = strings.TrimSpace(entityID)
	nextState = strings.TrimSpace(string(NormalizeWorkflowStateID(nextState)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	if entityID == "" || nextState == "" {
		return
	}
	current := pc.currentWorkflowState(ctx, entityID)
	currentState := strings.TrimSpace(string(current.Stage))
	source := pc.SemanticSource()
	_ = pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		enteredStateAt := time.Now().UTC()
		if strings.TrimSpace(instance.CurrentState) == nextState && !instance.EnteredStageAt.IsZero() {
			enteredStateAt = instance.EnteredStageAt
		}
		if strings.TrimSpace(instance.WorkflowName) == "" {
			instance.WorkflowName = source.WorkflowName()
		}
		if strings.TrimSpace(instance.WorkflowVersion) == "" {
			instance.WorkflowVersion = source.WorkflowVersion()
		}
		metadata := cloneStringAnyMap(current.Metadata)
		if strings.TrimSpace(current.Status) != "" {
			metadata["status"] = strings.TrimSpace(current.Status)
		}
		if sourceEvent != "" {
			metadata["last_source_event"] = sourceEvent
		}
		instance.CurrentState = nextState
		instance.EnteredStageAt = enteredStateAt
		instance.Metadata = metadata
		if currentState != "" && currentState != nextState {
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(currentState, nextState, sourceEvent))
		} else if currentState == "" && len(instance.TransitionHistory) == 0 {
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord("", nextState, sourceEvent))
		}
	})
	pc.notifyTestEntityStateUpdated(entityID, nextState)
	pc.reconcileWorkflowStageTimers(ctx, entityID, currentState, nextState, sourceEvent)
}

func (pc *FactoryPipelineCoordinator) applyWorkflowGateMutation(ctx context.Context, entityID, _sourceEvent, setGate string, clear bool) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	entityID = strings.TrimSpace(entityID)
	setGate = strings.TrimSpace(setGate)
	if entityID == "" {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		metadata := cloneStringAnyMap(instance.Metadata)
		gates := payloadMap(metadata["gates"])
		if clear {
			for key := range gates {
				delete(gates, key)
			}
		}
		if setGate != "" {
			gates[setGate] = true
		}
		if len(gates) == 0 {
			delete(metadata, "gates")
		} else {
			metadata["gates"] = gates
		}
		instance.Metadata = metadata
	})
}

func (pc *FactoryPipelineCoordinator) recordWorkflowEvidence(ctx context.Context, entityID string, nodeID string, payload map[string]any) bool {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return false
	}
	entityID = strings.TrimSpace(entityID)
	nodeID = strings.TrimSpace(nodeID)
	if entityID == "" || nodeID == "" {
		return false
	}
	_ = pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		bucket := workflowMutableStateBucket(instance, "evidence")
		bucket[nodeID] = cloneMap(payload)
		workflowSetStateBucket(instance, "evidence", bucket)
	})
	return true
}

func (pc *FactoryPipelineCoordinator) lockWorkflowEntity(entityID string) func() {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return func() {}
	}
	pc.entityLockMu.Lock()
	lock, ok := pc.entityLocks[entityID]
	if !ok {
		lock = &sync.Mutex{}
		pc.entityLocks[entityID] = lock
	}
	pc.entityLockMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func workflowTransitionRecord(fromState, toState, sourceEvent string) WorkflowTransitionRecord {
	fromState = strings.TrimSpace(string(NormalizeWorkflowStateID(fromState)))
	toState = strings.TrimSpace(string(NormalizeWorkflowStateID(toState)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	state := WorkflowState{Stage: NormalizeWorkflowStateID(fromState)}
	transition, ok := DefaultPipelineWorkflow().Transition(state, NormalizeWorkflowStateID(toState))
	record := WorkflowTransitionRecord{
		From:            fromState,
		To:              toState,
		GuardsEvaluated: nil,
		FiredAt:         time.Now().UTC(),
	}
	if ok {
		record.TransitionID = strings.TrimSpace(transition.Name)
		record.GuardsEvaluated = append([]string{}, transition.GuardIDs...)
	} else {
		record.TransitionID = firstNonEmptyString(
			sourceEvent,
			"legacy_"+strings.ReplaceAll(fromState, "-", "_")+"_to_"+strings.ReplaceAll(toState, "-", "_"),
		)
	}
	return record
}
