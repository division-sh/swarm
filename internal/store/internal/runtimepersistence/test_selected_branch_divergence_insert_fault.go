package runtimepersistence

import (
	"context"
	"fmt"
)

// This fixed native fault aborts only the original divergence INSERT. It neither
// supplies selected-execution authority nor accepts SQL or target selectors.
func SetSelectedBranchDivergenceInsertFaultForTest(ctx context.Context, selected any, enabled bool) error {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.SetSelectedBranchDivergenceInsertFaultForTest(ctx, enabled)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.SetSelectedBranchDivergenceInsertFaultForTest(ctx, enabled)
	default:
		return fmt.Errorf("workflow projection fault requires the original native owner")
	}
}
