package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func (pc *PipelineCoordinator) currentWorkflowState(ctx context.Context, route runtimeflowidentity.Route, entityID identity.EntityID) (WorkflowState, error) {
	entityID = identity.NormalizeEntityID(entityID.String())
	state := WorkflowState{
		EntityID: entityID.String(),
		Stage:    NormalizeWorkflowStateID(""),
		Metadata: map[string]any{},
	}
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return state, nil
	}
	if entityID.IsZero() {
		return WorkflowState{}, fmt.Errorf("load current workflow state requires an exact entity identity")
	}
	if !route.Valid() {
		return WorkflowState{}, fmt.Errorf("load current workflow state requires an exact workflow instance route")
	}
	instance, ok, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return WorkflowState{}, err
	}
	if !ok {
		return state, nil
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, instance); err != nil {
		return WorkflowState{}, fmt.Errorf("validate loaded workflow state identity: %w", err)
	}
	state.Stage = NormalizeWorkflowStateID(strings.TrimSpace(instance.CurrentState))
	state.Metadata = cloneStringAnyMap(instance.Fields)
	state.Control = workflowInstanceStateControl(instance)
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state, nil
}

func (pc *PipelineCoordinator) projectWorkflowEvidence(execCtx runtimeengine.ExecutionContext, bucketID string, payload map[string]any) (*runtimeengine.StateMutation, error) {
	if pc == nil {
		return nil, fmt.Errorf("record_evidence requires pipeline coordinator")
	}
	route := execCtx.Request.StateAddress().Route
	entityID := strings.TrimSpace(execCtx.Request.EntityID.String())
	flowID := strings.TrimSpace(execCtx.Request.FlowID.String())
	bucketID = strings.TrimSpace(bucketID)
	if entityID == "" || bucketID == "" {
		return nil, fmt.Errorf("record_evidence requires exact entity and evidence target")
	}
	if !route.Valid() {
		return nil, fmt.Errorf("record workflow evidence requires an exact workflow instance route")
	}
	event := execCtx.Request.Event
	if strings.TrimSpace(event.ID()) == "" || event.CreatedAt().IsZero() {
		return nil, fmt.Errorf("record_evidence requires exact accepted event identity")
	}
	metadata := workflowMaterializeEntityFields(pc.SemanticSource(), flowID, execCtx.Request.State.StateCarrier.Fields)
	buckets := make(map[string]map[string]any, len(execCtx.Request.State.StateCarrier.StateBuckets)+1)
	for key, bucket := range execCtx.Request.State.StateCarrier.StateBuckets {
		buckets[key] = cloneStringAnyMap(bucket)
	}
	evidence := cloneStringAnyMap(buckets["evidence"])
	if evidence == nil {
		evidence = map[string]any{}
	}
	workflowAppendEvidence(evidence, bucketID, payload)
	buckets["evidence"] = evidence
	mutation := &runtimeengine.StateMutation{
		StateCarrier: runtimeengine.NewStateCarrierWithOwners(
			metadata,
			execCtx.Request.State.StateCarrier.Bookkeeping,
			execCtx.Request.State.StateCarrier.Control,
			execCtx.Request.State.StateCarrier.Gates,
			buckets,
		),
		TriggerEventID:   strings.TrimSpace(event.ID()),
		TriggerEventType: strings.TrimSpace(string(event.Type())),
		TriggeredAt:      event.CreatedAt(),
	}
	return mutation, nil
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
