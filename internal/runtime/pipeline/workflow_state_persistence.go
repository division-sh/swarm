package pipeline

import (
	"context"
	"strings"
	"sync"
	"time"
)

func (pc *PipelineCoordinator) currentWorkflowState(ctx context.Context, entityID string) WorkflowState {
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
	state.Stage = NormalizeWorkflowStateID(strings.TrimSpace(instance.CurrentState))
	state.Metadata = cloneStringAnyMap(instance.Metadata)
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state
}

func (pc *PipelineCoordinator) updateEntityState(ctx context.Context, entityID, nextState, sourceEvent string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	nextState = strings.TrimSpace(string(NormalizeWorkflowStateID(nextState)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	if entityID == "" || nextState == "" {
		return nil
	}
	source := pc.SemanticSource()
	return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		currentState := ""
		if err := pc.workflowStore.Mutate(txctx, entityID, func(instance *WorkflowInstance) {
			currentState = strings.TrimSpace(instance.CurrentState)
			enteredStateAt := time.Now().UTC()
			if currentState == nextState && !instance.EnteredStageAt.IsZero() {
				enteredStateAt = instance.EnteredStageAt
			}
			if strings.TrimSpace(instance.WorkflowName) == "" {
				instance.WorkflowName = source.WorkflowName()
			}
			if strings.TrimSpace(instance.WorkflowVersion) == "" {
				instance.WorkflowVersion = source.WorkflowVersion()
			}
			metadata := cloneStringAnyMap(instance.Metadata)
			if sourceEvent != "" {
				metadata["last_source_event"] = sourceEvent
			}
			instance.Metadata = metadata
			instance.CurrentState = nextState
			instance.EnteredStageAt = enteredStateAt
			if currentState != "" && currentState != nextState {
				instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(pc.WorkflowDefinition(), currentState, nextState, sourceEvent))
			} else if currentState == "" && len(instance.TransitionHistory) == 0 {
				instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(pc.WorkflowDefinition(), "", nextState, sourceEvent))
			}
		}); err != nil {
			return err
		}
		pc.notifyTestEntityStateUpdated(entityID, nextState)
		if err := pc.reconcileWorkflowStageTimers(txctx, entityID, currentState, nextState, sourceEvent); err != nil {
			return err
		}
		return pc.applyWorkflowJoinIntents(txctx, entityID, currentState, nextState)
	})
}

func (pc *PipelineCoordinator) applyWorkflowGateMutation(ctx context.Context, entityID, _sourceEvent, setGate string, clear bool) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	setGate = strings.TrimSpace(setGate)
	if entityID == "" {
		return nil
	}
	return pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
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

func (pc *PipelineCoordinator) recordWorkflowEvidence(ctx context.Context, entityID string, flowID string, bucketID string, payload map[string]any) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	flowID = strings.TrimSpace(flowID)
	bucketID = strings.TrimSpace(bucketID)
	if entityID == "" || bucketID == "" {
		return nil
	}
	return pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
		source := pc.SemanticSource()
		if strings.TrimSpace(instance.WorkflowName) == "" {
			defaultWorkflowName := flowID
			if defaultWorkflowName == "" && source != nil {
				defaultWorkflowName = strings.TrimSpace(source.WorkflowName())
			}
			instance.WorkflowName = defaultWorkflowName
		}
		if strings.TrimSpace(instance.WorkflowVersion) == "" && source != nil {
			instance.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
		}
		if strings.TrimSpace(instance.CurrentState) == "" {
			instance.CurrentState = strings.TrimSpace(firstNonEmptyString(workflowInitialStateForFlow(source, flowID), "pending"))
		}
		if instance.EnteredStageAt.IsZero() {
			instance.EnteredStageAt = time.Now().UTC()
		}
		instance.Metadata = workflowMaterializeEntityMetadata(source, flowID, instance.Metadata)
		bucket := workflowMutableStateBucket(instance, "evidence")
		workflowAppendEvidence(bucket, bucketID, payload)
		workflowSetStateBucket(instance, "evidence", bucket)
	})
}

func workflowAppendEvidence(bucket map[string]any, bucketID string, payload map[string]any) {
	if bucket == nil {
		return
	}
	bucketID = strings.TrimSpace(bucketID)
	if bucketID == "" {
		return
	}
	entry := cloneMap(payload)
	switch typed := bucket[bucketID].(type) {
	case nil:
		bucket[bucketID] = []any{entry}
	case []any:
		next := append([]any{}, typed...)
		next = append(next, entry)
		bucket[bucketID] = next
	case map[string]any:
		bucket[bucketID] = []any{cloneMap(typed), entry}
	default:
		bucket[bucketID] = []any{typed, entry}
	}
}

func (pc *PipelineCoordinator) lockWorkflowEntity(entityID string) func() {
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

func workflowTransitionRecord(workflow *WorkflowDefinition, fromState, toState, sourceEvent string) WorkflowTransitionRecord {
	fromState = strings.TrimSpace(string(NormalizeWorkflowStateID(fromState)))
	toState = strings.TrimSpace(string(NormalizeWorkflowStateID(toState)))
	sourceEvent = strings.TrimSpace(sourceEvent)
	state := WorkflowState{Stage: NormalizeWorkflowStateID(fromState)}
	transition, ok := WorkflowStateTransition(workflow, state.Stage, NormalizeWorkflowStateID(toState))
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
