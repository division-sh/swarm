package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// The external native proof reaches the existing exact wakeup owner, not a
// fixture mutation or reconstructed transaction coordinator.
func FireWorkflowTimerOccurrenceForTest(ctx context.Context, pc *PipelineCoordinator, activation WorkflowTimerActivation) (WorkflowTimerFireOutcome, error) {
	return fireWorkflowTimerTestWakeup(ctx, pc, activation)
}

func loadSelectedWorkflowTimerActivationForTest(t *testing.T, store WorkflowTimerActivationPersistence, ctx context.Context, id string) WorkflowTimerActivation {
	t.Helper()
	activation, found, err := store.LoadWorkflowTimerActivation(ctx, id)
	if err != nil || !found {
		t.Fatalf("selected workflow timer %s readback = %v, %v", id, found, err)
	}
	return activation
}

func listTimerCauseReplayActivationsForTest(t *testing.T, store WorkflowTimerActivationPersistence, ctx context.Context, entityID string) []WorkflowTimerActivation {
	t.Helper()
	activations, err := store.ListWorkflowTimerActivations(ctx, correlation.RunIDFromContext(ctx), entityID, false)
	if err != nil {
		t.Fatal(err)
	}
	return activations
}

func cancelSelectedWorkflowTimerForTest(ctx context.Context, pc *PipelineCoordinator, activation WorkflowTimerActivation) error {
	committed, err := pc.workflowStore.timerActivations.CommitWorkflowTimerReconciliation(ctx, WorkflowTimerReconciliationCommand{
		RunID: activation.RunID, Route: activation.Route, EntityID: activation.EntityID,
		Plan: WorkflowLifecycleMutationPlan{Timers: []WorkflowTimerMutation{{Kind: WorkflowTimerMutationCancel, Activation: activation}}},
	})
	if err != nil {
		return err
	}
	if !committed.Committed || len(committed.Cancellations) != 1 || committed.Cancellations[0] != activation.Ref {
		return fmt.Errorf("selected cancellation did not acknowledge the exact activation: %+v", committed)
	}
	return pc.workflowTimers.queueCancellation(ctx, activation)
}
