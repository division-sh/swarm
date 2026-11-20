package runtimepersistence

import (
	"context"
	"fmt"
)

func SetBarrierCompletionPublicationFaultForTest(ctx context.Context, selected any, run, entity string, revision, mutations int, enabled bool) error {
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.SetBarrierCompletionPublicationFaultForTest(ctx, run, entity, revision, mutations, enabled)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.SetBarrierCompletionPublicationFaultForTest(ctx, run, entity, revision, mutations, enabled)
	default:
		return fmt.Errorf("workflow projection fault requires the original native owner")
	}
}
