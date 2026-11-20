package runtimepersistence

import (
	"context"
	"fmt"
)

// Preserve the complete table and dependent keys while making the exact native
// writer's named journal unavailable. Restoration never fabricates history.
func HideWorkflowMutationTableForTest(ctx context.Context, selected any, hidden bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.HideWorkflowMutationTableForTest(ctx, hidden)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.HideWorkflowMutationTableForTest(ctx, hidden)
	default:
		return fmt.Errorf("workflow projection fault requires the original native owner")
	}
}
