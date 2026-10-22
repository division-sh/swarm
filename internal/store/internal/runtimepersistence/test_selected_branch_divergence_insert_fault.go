package runtimepersistence

import (
	"context"
	"database/sql"
)

// This fixed native fault aborts only the original divergence INSERT. It neither
// supplies selected-execution authority nor accepts SQL or target selectors.
func SetSelectedBranchDivergenceInsertFaultForTest(ctx context.Context, selected any, enabled bool) error {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return err
	}
	statements := []string{`DROP TRIGGER IF EXISTS selected_branch_divergence_insert_fault`}
	if postgres {
		statements = []string{`DROP TRIGGER IF EXISTS selected_branch_divergence_insert_fault ON run_fork_selected_contract_branch_divergences`, `DROP FUNCTION IF EXISTS selected_branch_divergence_insert_fault()`}
	}
	if enabled {
		statements = []string{`CREATE TRIGGER IF NOT EXISTS selected_branch_divergence_insert_fault AFTER INSERT ON run_fork_selected_contract_branch_divergences BEGIN SELECT RAISE(ABORT,'typed_receiver_owner_fault'); END`}
		if postgres {
			statements = []string{
				`CREATE OR REPLACE FUNCTION selected_branch_divergence_insert_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'typed_receiver_owner_fault'; END $$`,
				`DROP TRIGGER IF EXISTS selected_branch_divergence_insert_fault ON run_fork_selected_contract_branch_divergences`,
				`CREATE TRIGGER selected_branch_divergence_insert_fault AFTER INSERT ON run_fork_selected_contract_branch_divergences FOR EACH ROW EXECUTE FUNCTION selected_branch_divergence_insert_fault()`,
			}
		}
	}
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}
