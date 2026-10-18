package managedcapabilitystore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
)

func requireSurfaceLiveParentsTx(ctx context.Context, tx *sql.Tx, surface managedcapabilities.Surface, postgres bool) error {
	if surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork {
		return nil
	}
	_, execution, child := surfaceRunProjections(surface)
	query := `SELECT execution_id FROM run_fork_selected_contract_execution_live_parents p
		WHERE execution_id=$1 AND fork_run_id=$2
		AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_execution_detachments retained
		 WHERE retained.execution_id=p.execution_id)`
	if postgres {
		// Serialize with removal of the exact association, not with a retained
		// canonical row that must remain readable after reset.
		query += ` FOR UPDATE`
	}
	var id string
	if err := tx.QueryRowContext(ctx, query, execution, child).Scan(&id); err != nil {
		return fmt.Errorf("managed capability persistence requires selected execution live-parent association: %w", err)
	}
	return nil
}
