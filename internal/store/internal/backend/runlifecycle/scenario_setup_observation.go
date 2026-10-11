package runlifecycle

import (
	"context"
	"database/sql"
)

type ScenarioSetupRunStorage struct{ Status, OriginKind, BundleHash string }

func CountGlobalRunStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count)
	return count, err
}

func ReadScenarioSetupRunStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ScenarioSetupRunStorage, error) {
	var out ScenarioSetupRunStorage
	err := tx.QueryRowContext(ctx, `SELECT status, origin_kind, bundle_hash FROM runs WHERE run_id=$1`, runID).
		Scan(&out.Status, &out.OriginKind, &out.BundleHash)
	if err != nil {
		return ScenarioSetupRunStorage{}, err
	}
	return out, nil
}
