package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// Hold an original-coordinator writer during the unstamped routing refusal.
// Neither its transaction nor its context crosses this named temporal cut.
func HoldUnstampedPipelineAdmissionTransactionForTest(ctx context.Context, selected any, run string) (func() error, error) {
	if ctx == nil || correlation.RunIDFromContext(ctx) != run {
		return nil, fmt.Errorf("pipeline transaction cut requires its exact run context")
	}
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return nil, err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.HoldUnstampedPipelineAdmissionTransactionForTest(ctx)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.HoldUnstampedPipelineAdmissionTransactionForTest(ctx)
	default:
		return nil, fmt.Errorf("workflow projection fault requires the original native owner")
	}
}
