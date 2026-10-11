package runforkpersistence

import (
	"context"
	"database/sql"
)

// These are physical refusal witnesses, not eligibility or current-authority
// projections. In particular, delivery replay history is counted independently.
func ReadSelectedSourceRefusalStorageTx(ctx context.Context, tx *sql.Tx, sourceRunID string) (map[string]int, error) {
	queries := map[string]string{
		"fork_runs":                            `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`,
		"selected_contract_bindings":           `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE source_run_id=$1`,
		"selected_contract_executions":         `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id=$1`,
		"selected_contract_branch_divergences": `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE source_run_id=$1`,
		"selected_contract_route_recoveries":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE source_run_id=$1`,
		"delivery_event_replays":               `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE source_run_id=$1`,
	}
	out := make(map[string]int, len(queries))
	for name, query := range queries {
		var count int
		if err := tx.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
			return nil, err
		}
		out[name] = count
	}
	return out, nil
}

func ReadSelectedForkRefusalStorageTx(ctx context.Context, tx *sql.Tx, forkRunID string) (map[string]int, error) {
	queries := map[string]string{
		"run_fork_selected_contract_executions":   `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1`,
		"run_fork_selected_contract_divergences":  `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id=$1`,
		"run_fork_selected_contract_route_rows":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id=$1`,
		"run_fork_delivery_event_replay_lineages": `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id=$1`,
	}
	out := make(map[string]int, len(queries))
	for name, query := range queries {
		var count int
		if err := tx.QueryRowContext(ctx, query, forkRunID).Scan(&count); err != nil {
			return nil, err
		}
		out[name] = count
	}
	return out, nil
}

func CountSelectedForkRunsForSourceTx(ctx context.Context, tx *sql.Tx, sourceRunID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, sourceRunID).Scan(&count)
	return count, err
}
