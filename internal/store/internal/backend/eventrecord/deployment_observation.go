package eventrecord

import (
	"context"
	"database/sql"
)

func CountPinnedPortfolioSourceEventsStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name IN ('portfolio/account.registered','portfolio/account.notify.requested')`, run).Scan(&out)
	return out, err
}

type DeploymentRuntimeLogStorage struct{ EventID, Payload string }
type DeploymentRecentEventStorage struct {
	Name    string
	Payload []byte
}

func ReadFirstRootReadyEventStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='root.ready' ORDER BY event_id LIMIT 1`, runID).Scan(&out)
	return out, err
}

func ReadDeploymentRuntimeLogStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentRuntimeLogStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT),CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='platform.runtime_log' ORDER BY insertion_sequence DESC LIMIT 20`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentRuntimeLogStorage
	for rows.Next() {
		var row DeploymentRuntimeLogStorage
		if err := rows.Scan(&row.EventID, &row.Payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadDeploymentRecentEventStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]DeploymentRecentEventStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_name, payload FROM events WHERE run_id=$1 ORDER BY created_at LIMIT 12`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentRecentEventStorage
	for rows.Next() {
		var row DeploymentRecentEventStorage
		if err := rows.Scan(&row.Name, &row.Payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
