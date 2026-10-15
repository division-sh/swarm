package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"

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

func ProbeWorkflowGateEmptyPathForTest(ctx context.Context, selected any, run, entity string) error {
	if err := validateWorkflowProjectionFaultKey(run, entity, true); err != nil {
		return err
	}
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	accepted := errors.New("roll back accepted empty-gate-path schema probe")
	err = runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := mutationlog.ProbeEmptyGatePathForTest(ctx, tx, postgres, run, entity); err != nil {
			return err
		}
		return accepted
	})
	// Exact equality is intentional: a joined cleanup failure is not successful
	// rollback, and must not be confused with either acceptance or a constraint.
	if err == accepted {
		return nil
	}
	return err
}
