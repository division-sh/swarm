package runforkpersistence

import (
	"context"
	"database/sql"

	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func finalizePostgresRunForkRevisionTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects) error {
	_, err := privaterunforkrevision.FinalizePostgres(ctx, tx, effects)
	return err
}

func (s *RunForkPostgresOwner) FinalizeRunForkRevisionTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects) error {
	return finalizePostgresRunForkRevisionTx(ctx, tx, effects)
}
