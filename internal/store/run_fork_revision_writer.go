package store

import (
	"context"
	"database/sql"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func commitPostgresRunForkRevisionTx(ctx context.Context, tx *sql.Tx) error {
	if err := runtimepipeline.CapturePipelineRunForkRevisionChanges(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
