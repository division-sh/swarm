package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

// Exact live competing-writer cuts, not normal engine mutation or revision evidence.
func AdvanceSelectionRevisionPairForTest(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.AdvanceSelectionRevisionPairTx(ctx, tx, state)
	})
}

func AdvanceProspectiveRevisionPairForTest(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.AdvanceProspectiveRevisionPairTx(ctx, tx, state)
	})
}
