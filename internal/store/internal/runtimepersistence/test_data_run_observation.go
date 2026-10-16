package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type DeploymentIntentStatusStorage = pipelinepersistence.DeploymentIntentStatusStorage
type DeploymentCompletionClockStorage = runlifecycle.DeploymentCompletionClockStorage

func ReadDeploymentIntentStatusStorageForTest(ctx context.Context, selected any, run string) (DeploymentIntentStatusStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, pipelinepersistence.ReadDeploymentIntentStatusStorageTx)
}

func ReadDeploymentCompletionClockStorageForTest(ctx context.Context, selected any, run string) (DeploymentCompletionClockStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, run, runlifecycle.ReadDeploymentCompletionClockStorageTx)
}

func CountRunTransportCacheStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, run, apiidempotency.CountRunTransportCacheStorageTx)
}

func ExpireRunTransportCacheForTest(ctx context.Context, selected any, run string) error {
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return apiidempotency.ExpireRunTransportCacheTx(ctx, tx, run)
	})
}
