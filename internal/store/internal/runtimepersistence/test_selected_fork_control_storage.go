package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SelectedForkControlStorage struct {
	AwaitingMutation bool
	BindingID        string
	AgentCount       int
}

// This fixed physical witness preserves the public control proof's readiness,
// exact binding and same-run agent predicates; it does not authorize execution.
func ReadSelectedForkControlStorageForTest(ctx context.Context, selected any, runID, loadedBundleHash, agentID string) (SelectedForkControlStorage, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return SelectedForkControlStorage{}, err
	}
	if strings.TrimSpace(loadedBundleHash) == "" || strings.TrimSpace(agentID) == "" {
		return SelectedForkControlStorage{}, fmt.Errorf("selected control observation requires a loaded bundle and exact agent")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedForkControlStorage{}, err
	}
	var out SelectedForkControlStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT status='running' AND (bundle_hash <> $2 OR completion_due_at IS NULL) FROM runs WHERE run_id=$1`, runID, loadedBundleHash).Scan(&out.AwaitingMutation); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT binding_id FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, runID).Scan(&out.BindingID); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE run_id=$1 AND agent_id=$2`, runID, agentID).Scan(&out.AgentCount)
	})
	if err != nil {
		return SelectedForkControlStorage{}, err
	}
	return out, nil
}
