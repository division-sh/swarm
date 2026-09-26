package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// A selected delivery authority is executable only while its exact selected
// execution still owns the admitted binding and a live store lease.
func (a *Adapter) selectedExecutionCurrent(ctx context.Context, tx *sql.Tx, authority deliverylifecycle.ExecutionAuthority) (bool, error) {
	return a.selectedExecutionCurrentWithLock(ctx, tx, authority, true)
}

func (a *Adapter) selectedExecutionCurrentRead(ctx context.Context, tx *sql.Tx, authority deliverylifecycle.ExecutionAuthority) (bool, error) {
	return a.selectedExecutionCurrentWithLock(ctx, tx, authority, false)
}

func (a *Adapter) selectedExecutionCurrentWithLock(ctx context.Context, tx *sql.Tx, authority deliverylifecycle.ExecutionAuthority, lock bool) (bool, error) {
	if authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork {
		return true, nil
	}
	if tx == nil {
		return false, fmt.Errorf("selected delivery execution fence requires a transaction")
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return false, err
	}
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM run_fork_selected_contract_runtime_executions AS execution
			JOIN run_fork_selected_contract_bindings AS binding
			  ON binding.binding_id = execution.binding_id
			 AND binding.fork_run_id = execution.fork_run_id
			JOIN runs AS run ON run.run_id = execution.fork_run_id
			WHERE execution.execution_id = $1
			  AND execution.fork_run_id = $2
			  AND execution.generation = $3
			  AND run.bundle_hash = $4
			  AND (binding.mode = 'selected_contracts' OR
			       (binding.mode = 'bundle_hash' AND binding.bundle_hash = run.bundle_hash))
			  AND execution.state = 'running'
			  AND execution.lease_expires_at > $5`
	if lock && a.dialect == DialectPostgres {
		query += ` FOR SHARE OF execution, binding`
	}
	query += `)`
	var current bool
	err = tx.QueryRowContext(ctx, query, authority.ExecutionID(), authority.ForkRunID(), authority.Generation(), authority.SourceArtifact().BundleHash(), now).Scan(&current)
	if err != nil {
		return false, fmt.Errorf("verify selected delivery execution fence: %w", err)
	}
	return current, nil
}
