package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
)

func ProbeWorkflowMutationMissingDomainPathForTest(ctx context.Context, selected any, run, entity string) error {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return err
	}
	if err := validateWorkflowProjectionFaultRun(entity); err != nil {
		return err
	}
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return mutationlog.ProbeMissingDomainPathForTest(ctx, tx, postgres, run, entity)
	})
}
