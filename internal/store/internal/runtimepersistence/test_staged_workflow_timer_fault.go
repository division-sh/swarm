package runtimepersistence

import (
	"context"
	"fmt"

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
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.CorruptStagedWorkflowTimerForTest(ctx, runID, activationID, fault)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.CorruptStagedWorkflowTimerForTest(ctx, runID, activationID, fault)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
