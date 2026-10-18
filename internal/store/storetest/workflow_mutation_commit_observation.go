package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type WorkflowMutationCommitObservation = private.WorkflowMutationCommitObservationForTest

func HoldWorkflowMutationCommit(ctx context.Context, original any, claim deliverylifecycle.Claim) (*WorkflowMutationCommitObservation, error) {
	return private.HoldWorkflowMutationCommitForTest(ctx, original, claim)
}
