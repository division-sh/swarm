package runlifecycle

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type DeploymentRunStatusStorage struct{ Status, BundleHash string }
type DeploymentRunControlStorage struct{ Status, ControlStatus string }

func ReadDeploymentRunStatusStorageTx(ctx context.Context, tx *sql.Tx, runID string) (DeploymentRunStatusStorage, error) {
	var out DeploymentRunStatusStorage
	err := tx.QueryRowContext(ctx, `SELECT status,bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&out.Status, &out.BundleHash)
	return out, err
}

func ReadDeploymentRunControlStorageTx(ctx context.Context, tx *sql.Tx, runID string) (DeploymentRunControlStorage, error) {
	var out DeploymentRunControlStorage
	err := tx.QueryRowContext(ctx, `SELECT r.status,c.control_status FROM runs r JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, runID).Scan(&out.Status, &out.ControlStatus)
	return out, err
}

func ReadDeploymentParentRunStorageTx(ctx context.Context, tx *sql.Tx, runID string) (delivery.WorkloadNullableText, error) {
	var out sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT CAST(forked_from_run_id AS TEXT) FROM runs WHERE run_id=$1`, runID).Scan(&out)
	return delivery.WorkloadNullableText(out), err
}
