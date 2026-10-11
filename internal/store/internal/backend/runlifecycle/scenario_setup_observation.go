package runlifecycle

import (
	"context"
	"database/sql"
)

type ScenarioSetupRunStorage struct{ Status, OriginKind, BundleHash string }

func ReadScenarioSetupRunStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ScenarioSetupRunStorage, error) {
	var out ScenarioSetupRunStorage
	err := tx.QueryRowContext(ctx, `SELECT status, origin_kind, bundle_hash FROM runs WHERE run_id=$1`, runID).
		Scan(&out.Status, &out.OriginKind, &out.BundleHash)
	if err != nil {
		return ScenarioSetupRunStorage{}, err
	}
	return out, nil
}
