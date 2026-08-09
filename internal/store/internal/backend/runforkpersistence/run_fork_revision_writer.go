package runforkpersistence

import (
	"context"
	"database/sql"

	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func commitPostgresRunForkRevisionTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := privaterunforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RunForkPostgresOwner) CommitRunForkRevisionTx(ctx context.Context, tx *sql.Tx) error {
	return commitPostgresRunForkRevisionTx(ctx, tx)
}
