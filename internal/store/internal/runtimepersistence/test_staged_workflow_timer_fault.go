package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type StagedWorkflowTimerFault = pipelinepersistence.StagedWorkflowTimerFault

const (
	StagedWorkflowTimerMissing       = pipelinepersistence.StagedWorkflowTimerMissing
	StagedWorkflowTimerProgressed    = pipelinepersistence.StagedWorkflowTimerProgressed
	StagedWorkflowTimerForeignOrigin = pipelinepersistence.StagedWorkflowTimerForeignOrigin
)

func CorruptStagedWorkflowTimerForTest(ctx context.Context, selected any, runID, activationID string, fault StagedWorkflowTimerFault) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.CorruptStagedWorkflowTimerTxForTest(ctx, tx, runID, activationID, fault)
	})
}
