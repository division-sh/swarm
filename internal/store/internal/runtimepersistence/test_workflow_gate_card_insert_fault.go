package runtimepersistence

import (
	"context"
	"fmt"
)

// Abort the exact card insert inside the original selected mutation transaction.
// The fixed trigger is removed through that same writer before fixture close.
func SetWorkflowGateCardInsertFaultForTest(ctx context.Context, selected any, run string, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := validateSelectedForkStorageIdentity(run); err != nil {
		return err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.SetWorkflowGateCardInsertFaultForTest(ctx, run, enabled)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.SetWorkflowGateCardInsertFaultForTest(ctx, run, enabled)
	default:
		return fmt.Errorf("workflow projection fault requires the original native owner")
	}
}
