package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func (pc *PipelineCoordinator) currentWorkflowState(ctx context.Context, entityID string) WorkflowState {
	entityID = strings.TrimSpace(entityID)
	state := WorkflowState{
		EntityID: entityID,
		Stage:    NormalizeWorkflowStateID(""),
		Metadata: map[string]any{},
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() || entityID == "" {
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

func (pc *PipelineCoordinator) recordWorkflowEvidence(ctx context.Context, entityID string, flowID string, bucketID string, payload map[string]any) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	flowID = strings.TrimSpace(flowID)
	bucketID = strings.TrimSpace(bucketID)
	if entityID == "" || bucketID == "" {
		return nil
	}
	_, found, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil {
		return err
	}
	if !found {
		inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
		if !ok || inbound.ID() == "" || inbound.CreatedAt().IsZero() {
			return fmt.Errorf("record_evidence initial materialization requires exact accepted event identity")
		}
		source := pc.SemanticSource()
		workflowName := flowID
		workflowVersion := ""
		if source != nil {
			workflowName = firstNonEmptyString(workflowName, source.WorkflowName())
			workflowVersion = source.WorkflowVersion()
		}
		initialState := strings.TrimSpace(firstNonEmptyString(workflowInitialStateForFlow(source, flowID), "pending"))
		materialization, err := pc.workflowStore.MaterializeInitialEntry(ctx, WorkflowInstance{
			InstanceID:      entityID,
			WorkflowName:    workflowName,
			WorkflowVersion: workflowVersion,
			CurrentState:    initialState,
			Metadata:        workflowMaterializeEntityMetadata(source, flowID, nil),
		}, inbound.CreatedAt())
		if err != nil {
			return err
		}
		if materialization != WorkflowInitialMaterializationCreated && materialization != WorkflowInitialMaterializationAlreadyExists {
			return fmt.Errorf("workflow initial materialization returned unknown result %d", materialization)
		}
		if err := pc.workflowStore.ArmInitialEntryTimers(ctx, entityID); err != nil {
			return err
		}
	}
	return pc.workflowStore.mutate(ctx, entityID, func(instance *WorkflowInstance) {
		instance.Metadata = workflowMaterializeEntityMetadata(pc.SemanticSource(), flowID, instance.Metadata)
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

func workflowTransitionRecord(workflow *WorkflowDefinition, fromState, toState, sourceEventID, sourceEventType string, firedAt time.Time) WorkflowTransitionRecord {
	fromState = strings.TrimSpace(string(NormalizeWorkflowStateID(fromState)))
	toState = strings.TrimSpace(string(NormalizeWorkflowStateID(toState)))
	sourceEventID = strings.TrimSpace(sourceEventID)
	sourceEventType = strings.TrimSpace(sourceEventType)
	state := WorkflowState{Stage: NormalizeWorkflowStateID(fromState)}
	transition, ok := WorkflowStateTransition(workflow, state.Stage, NormalizeWorkflowStateID(toState))
	record := WorkflowTransitionRecord{
		From:            fromState,
		To:              toState,
		TriggerEventID:  sourceEventID,
		GuardsEvaluated: nil,
		FiredAt:         firedAt.UTC(),
	}
	if ok {
		record.TransitionID = strings.TrimSpace(transition.Name)
		record.GuardsEvaluated = append([]string{}, transition.GuardIDs...)
	} else {
		record.TransitionID = firstNonEmptyString(
			sourceEventType,
			"legacy_"+strings.ReplaceAll(fromState, "-", "_")+"_to_"+strings.ReplaceAll(toState, "-", "_"),
		)
	}
	return record
}

func workflowTransitionIdentity(workflow *WorkflowDefinition, fromState, toState, sourceEventType string) string {
	return workflowTransitionRecord(workflow, fromState, toState, "", sourceEventType, time.Unix(0, 0)).TransitionID
}
