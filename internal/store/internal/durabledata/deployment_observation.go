package durabledata

import (
	"context"
	"database/sql"
)

func ReadRootReadyResourcePinStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, runID).Scan(&version)
	return version, err
}
