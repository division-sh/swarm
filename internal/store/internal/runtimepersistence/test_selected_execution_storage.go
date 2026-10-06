package runtimepersistence

import (
	"context"
	"database/sql"
)

type SelectedExecutionStorage struct {
	State, RunStatus string
	Occurrences      int
}

func ReadSelectedExecutionStorageForTest(ctx context.Context, selected any, runID string) (SelectedExecutionStorage, error) {
	var out SelectedExecutionStorage
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT e.state,r.status,(SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)
		FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id WHERE r.run_id=$1`, runID).Scan(&out.State, &out.RunStatus, &out.Occurrences)
	})
	if err != nil {
		return SelectedExecutionStorage{}, err
	}
	return out, nil
}
