package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// This is a physical cleanup fixture, not selected execution admission.
// reviewer-b #2698/6210011010 requires the run-wide retention cut in this tx.
func ValidateUnretainedCleanupHistoryTargetTx(ctx context.Context, tx *sql.Tx, sqlite bool, fork, source, event string) (string, error) {
	if tx == nil || ctx == nil || fork == source {
		return "", fmt.Errorf("unretained history requires its distinct original fork and source")
	}
	for _, id := range []string{fork, source, event} {
		if _, err := uuid.Parse(id); err != nil {
			return "", err
		}
	}
	query := `SELECT status FROM runs WHERE run_id=$1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var status string
	if err := tx.QueryRowContext(ctx, query, fork).Scan(&status); err != nil {
		return "", err
	}
	if status != "paused" {
		return "", fmt.Errorf("unretained history requires the materialized pause")
	}
	var hash string
	err := tx.QueryRowContext(ctx, `SELECT r.bundle_hash FROM run_fork_selected_contract_bindings b
		JOIN runs r ON r.run_id=b.fork_run_id JOIN runs s ON s.run_id=b.source_run_id
		JOIN events e ON e.event_id=b.fork_event_id AND e.run_id=s.run_id
		WHERE b.fork_run_id=$1 AND b.source_run_id=$2 AND b.fork_event_id=$3
		AND b.fork_point_kind='event' AND b.mode='selected_contracts'
		AND b.fork_revision>0 AND r.bundle_hash=s.bundle_hash`, fork, source, event).Scan(&hash)
	if err != nil {
		return "", fmt.Errorf("unretained history exact binding: %w", err)
	}
	var executions int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, fork).Scan(&executions); err != nil {
		return "", err
	}
	if executions != 0 {
		return "", fmt.Errorf("unretained history refuses every retained selected execution")
	}
	return hash, nil
}
