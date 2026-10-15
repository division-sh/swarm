package runlifecycle

import (
	"context"
	"database/sql"
)

type ServedRunControlStorage struct{ RunStatus, ControlStatus, Reason, ControlledBy string }
type ServedIngressStorage struct{ Status, TransitionEventID string }

func ReadServedRunControlStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) (ServedRunControlStorage, error) {
	query := `SELECT r.status, COALESCE(rc.control_status, ''), COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
		FROM runs r LEFT JOIN run_control_state rc ON rc.run_id = r.run_id WHERE r.run_id = ?`
	if postgres {
		query = `SELECT r.status, COALESCE(rc.control_status, ''), COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
			FROM runs r LEFT JOIN run_control_state rc ON rc.run_id = r.run_id WHERE r.run_id = $1::uuid`
	}
	var out ServedRunControlStorage
	err := tx.QueryRowContext(ctx, query, runID).Scan(&out.RunStatus, &out.ControlStatus, &out.Reason, &out.ControlledBy)
	return out, err
}

func ReadServedIngressStorageTx(ctx context.Context, tx *sql.Tx, postgres bool) (ServedIngressStorage, error) {
	query := `SELECT status, COALESCE(transition_event_id, '') FROM runtime_ingress_state WHERE id = 1`
	if postgres {
		query = `SELECT status, COALESCE(transition_event_id::text, '') FROM runtime_ingress_state WHERE id = 1`
	}
	var out ServedIngressStorage
	err := tx.QueryRowContext(ctx, query).Scan(&out.Status, &out.TransitionEventID)
	return out, err
}

func CountStandingOriginRunsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE origin_kind = 'standing_generation'`).Scan(&count)
	return count, err
}
