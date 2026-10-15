package runforkpersistence

import (
	"context"
	"database/sql"
)

func CountSelectedForkBindingStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, run).Scan(&count)
	return count, err
}

func CountSelectedForkExecutionStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1`, run).Scan(&count)
	return count, err
}

func CountSelectedForkRouteRecoveryStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id=$1`, run).Scan(&count)
	return count, err
}

func SetSelectedExecutionLineageInsertFaultTx(ctx context.Context, tx *sql.Tx, enabled bool) error {
	statement := `DROP TRIGGER IF EXISTS fail_selected_contract_execution_lineage_insert ON run_fork_selected_contract_executions;
DROP FUNCTION IF EXISTS fail_selected_contract_execution_lineage_insert();`
	if enabled {
		statement = `CREATE OR REPLACE FUNCTION fail_selected_contract_execution_lineage_insert()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'forced selected execution lineage failure';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER fail_selected_contract_execution_lineage_insert
BEFORE INSERT ON run_fork_selected_contract_executions
FOR EACH ROW EXECUTE FUNCTION fail_selected_contract_execution_lineage_insert();`
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}
