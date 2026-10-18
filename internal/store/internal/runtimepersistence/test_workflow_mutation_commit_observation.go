package runtimepersistence

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type WorkflowMutationCommitObservationForTest = pipelinepersistence.WorkflowMutationCommitObservationForTest

func HoldWorkflowMutationCommitForTest(ctx context.Context, original any, claim deliverylifecycle.Claim) (*WorkflowMutationCommitObservationForTest, error) {
	switch original := original.(type) {
	case *PostgresStore:
		return original.pipelinePostgresOwner.HoldWorkflowMutationCommitForTest(ctx, claim)
	case *SQLiteRuntimeStore:
		return original.pipelineSQLiteOwner.HoldWorkflowMutationCommitForTest(ctx, claim)
	default:
		return nil, errors.New("workflow commit observation requires its original selected owner")
	}
}
