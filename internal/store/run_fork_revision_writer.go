package store

import (
	"context"
	"database/sql"

	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/runforkrevision"
)

func commitPostgresRunForkRevisionTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := privaterunforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
