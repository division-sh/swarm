package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func SetBarrierCompletionPublicationFaultForTest(ctx context.Context, selected any, run, entity string, revision, mutations int, enabled bool) error {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.SetBarrierCompletionPublicationFaultTx(ctx, tx, postgres, run, entity, revision, mutations, enabled)
	})
}
