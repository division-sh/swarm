package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func (pc *PipelineCoordinator) currentWorkflowState(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance, entityID identity.EntityID) (WorkflowState, error) {
	owner = owner.Normalize()
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
	if err := owner.Validate(); err != nil {
		return WorkflowState{}, err
	}
	instance, ok, err := pc.workflowStore.Load(ctx, owner)
	if err != nil {
		return WorkflowState{}, err
	}
	if !ok {
		return state, nil
	}
	if _, err := requireWorkflowInstanceIdentity(owner.Route, entityID, instance); err != nil {
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
