package pipeline

import (
	"context"
	"fmt"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"strings"
	"time"
)

func (s *workflowInstanceStore) mutate(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, fn func(*WorkflowInstance)) error {
	if fn == nil {
		return nil
	}
	return s.mutateE(ctx, identity, func(instance *WorkflowInstance) error {
		fn(instance)
		return nil
	})
}

func (s *workflowInstanceStore) mutateE(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance, fn func(*WorkflowInstance) error) error {
	identity = identity.Normalize()
	requestedKey := strings.TrimSpace(identity.Route.InstancePath)
	if err := identity.Validate(); err != nil {
		return &WorkflowInstanceLookupMiss{RequestedKey: requestedKey}
	}
	if s == nil || !s.enabled() || fn == nil {
		return nil
	}
	if s.engineMutations == nil {
		return fmt.Errorf("workflow instance mutation requires the selected workflow engine mutation owner")
	}
	route, runID := identity.Route, identity.RunID
	instance, ok, err := s.Load(ctx, identity)
	if err != nil {
		return err
	}
	if !ok {
		return &WorkflowInstanceLookupMiss{RequestedKey: requestedKey}
	}
	expectedState := strings.TrimSpace(instance.CurrentState)
	expectedRevision := instance.Revision
	if err := fn(&instance); err != nil {
		return err
	}
	record, err := workflowEngineStateRecord(runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: route}, instance, expectedState, expectedRevision, WorkflowEngineStateTransitionUpdateStateAndCompanion, time.Now().UTC())
	if err != nil {
		return err
	}
	_, err = s.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: record})
	return err
}
