package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

// The selected fixture owner validates canonical identities before this fixed
// physical fault. It fails the real UPDATE, not the handler or its observer.
func InstallNodeRecoveryPoisonMutationTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("node recovery poison fault requires one selected entity: found=%d", count)
	}
	statement := fmt.Sprintf(`CREATE TRIGGER fail_node_recovery_poison_update
		BEFORE UPDATE ON entity_state
		WHEN OLD.run_id = '%s' AND OLD.entity_id = '%s'
		BEGIN
			SELECT RAISE(ABORT, 'injected node recovery poison mutation');
		END`, runID, entityID)
	if postgres {
		statement = fmt.Sprintf(`CREATE FUNCTION fail_node_recovery_poison_update_fn() RETURNS trigger AS $$
			BEGIN
				IF OLD.run_id = '%s'::uuid AND OLD.entity_id = '%s'::uuid THEN
					RAISE EXCEPTION 'injected node recovery poison mutation';
				END IF;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER fail_node_recovery_poison_update
			BEFORE UPDATE ON entity_state
			FOR EACH ROW EXECUTE FUNCTION fail_node_recovery_poison_update_fn()`, runID, entityID)
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}

func RemoveNodeRecoveryPoisonMutationTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	statement := `DROP TRIGGER fail_node_recovery_poison_update`
	if postgres {
		statement = `DROP TRIGGER fail_node_recovery_poison_update ON entity_state;
			DROP FUNCTION fail_node_recovery_poison_update_fn()`
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}
