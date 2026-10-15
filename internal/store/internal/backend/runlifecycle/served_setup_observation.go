package runlifecycle

import (
	"context"
	"database/sql"
)

type ServedSetupRunStorage struct{ Status, OriginKind, Trigger, BundleHash string }

func ReadServedSetupRunStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedSetupRunStorage, error) {
	var out ServedSetupRunStorage
	err := tx.QueryRowContext(ctx, `SELECT status, origin_kind, COALESCE(trigger_event_type, ''), COALESCE(bundle_hash, '') FROM runs WHERE run_id = $1`, runID).Scan(&out.Status, &out.OriginKind, &out.Trigger, &out.BundleHash)
	return out, err
}
