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
	current := pc.currentWorkflowState(ctx, entityID)
	currentState := strings.TrimSpace(string(current.Stage))
	source := pc.SemanticSource()
	if err := pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
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
		instance.Metadata = metadata
		instance.CurrentState = nextState
		instance.EnteredStageAt = enteredStateAt
		if currentState != "" && currentState != nextState {
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(currentState, nextState, sourceEvent))
		} else if currentState == "" && len(instance.TransitionHistory) == 0 {
			instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord("", nextState, sourceEvent))
		}
	}); err != nil {
		return err
	}
	pc.notifyTestEntityStateUpdated(entityID, nextState)
	pc.reconcileWorkflowStageTimers(ctx, entityID, currentState, nextState, sourceEvent)
	return nil
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

func (pc *PipelineCoordinator) projectWorkflowSubjectGates(ctx context.Context, entityID string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil
	}
	instance, ok, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		if err != nil {
			return err
		}
		flowID := strings.TrimSpace(pipelineFlowScope(ctx))
		if flowID == "" || pc.SemanticSource() == nil {
			return nil
		}
		flowPath := strings.Trim(strings.TrimSpace(pc.SemanticSource().FlowPath(flowID)), "/")
		if flowPath == "" {
			flowPath = flowID
		}
		if flowPath == "" {
			return nil
		}
		instance, ok, err = pc.workflowStore.Load(ctx, flowPath)
		if err != nil || !ok {
			return err
		}
	}
	subjectID := strings.TrimSpace(firstNonEmptyString(instance.SubjectID, asString(instance.Metadata["subject_id"]), entityID))
	if subjectID == "" || subjectID == entityID {
		return nil
	}
	flowPath := strings.Trim(strings.TrimSpace(asString(instance.Metadata["flow_path"])), "/")
	scopeKey := ""
	if workflowName := strings.TrimSpace(instance.WorkflowName); workflowName != "" {
		scopeKey = strings.TrimSpace(workflowScopeKey(pc.SemanticSource(), workflowName))
	}
	if scopeKey == "" {
		scopeKey = flowPath
		if idx := strings.Index(scopeKey, "/"); idx > 0 {
			scopeKey = strings.TrimSpace(scopeKey[:idx])
		}
	}
	if flowPath == "" {
		flowID := strings.TrimSpace(pipelineFlowScope(ctx))
		if flowID != "" {
			if pc.SemanticSource() != nil {
				flowPath = strings.Trim(strings.TrimSpace(pc.SemanticSource().FlowPath(flowID)), "/")
			}
			if flowPath == "" {
				flowPath = flowID
			}
		}
	}
	if scopeKey == "" {
		scopeKey = flowPath
	}
	if scopeKey == "" {
		return nil
	}
	prefix := strings.Trim(scopeKey, "/") + "/"
	scoped := map[string]bool{}
	for key, value := range workflowStateGatesAsBools(instance.Metadata) {
		if strings.HasPrefix(strings.TrimSpace(key), prefix) {
			scoped[strings.TrimSpace(key)] = value
		}
	}
	return pc.workflowStore.Mutate(ctx, subjectID, func(subject *WorkflowInstance) {
		metadata := cloneStringAnyMap(subject.Metadata)
		if metadata == nil {
			metadata = map[string]any{}
		}
		gates := workflowStateGatesAsBools(metadata)
		for key := range gates {
			if strings.HasPrefix(strings.TrimSpace(key), prefix) {
				delete(gates, key)
			}
		}
		for key, value := range scoped {
			gates[key] = value
		}
		if len(gates) == 0 {
			delete(metadata, "gates")
		} else {
			metadata["gates"] = workflowBoolGatesAsMap(gates)
		}
		subject.Metadata = metadata
	})
}

func (pc *PipelineCoordinator) recordWorkflowEvidence(ctx context.Context, entityID string, bucketID string, payload map[string]any) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	bucketID = strings.TrimSpace(bucketID)
	if entityID == "" || bucketID == "" {
		return nil
	}
	return pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) {
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
