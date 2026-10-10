package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func RemoveWorkflowTimerForTest(ctx context.Context, selected any, run, activation string) error {
	return applyWorkflowTimerFault(ctx, selected, run, activation, pipelinepersistence.RemoveWorkflowTimerForTest)
}

func SetWorkflowTimerForeignDeclarationForTest(ctx context.Context, selected any, run, activation string) error {
	return applyWorkflowTimerFault(ctx, selected, run, activation, pipelinepersistence.SetWorkflowTimerForeignDeclarationForTest)
}

func SetWorkflowTimerMalformedNameForTest(ctx context.Context, selected any, run, activation string) error {
	return applyWorkflowTimerFault(ctx, selected, run, activation, pipelinepersistence.SetWorkflowTimerMalformedNameForTest)
}

func SetWorkflowTimerForeignRouteAndMalformedNameForTest(ctx context.Context, selected any, run, activation string) error {
	return applyWorkflowTimerFault(ctx, selected, run, activation, pipelinepersistence.SetWorkflowTimerForeignRouteAndMalformedNameForTest)
}

func applyWorkflowTimerFault(ctx context.Context, selected any, run, activation string, fault func(context.Context, *sql.Tx, bool, string, string) error) error {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return err
	}
	if err := validateWorkflowProjectionFaultRun(activation); err != nil {
		return err
	}
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return fault(ctx, tx, postgres, run, activation)
	})
}
