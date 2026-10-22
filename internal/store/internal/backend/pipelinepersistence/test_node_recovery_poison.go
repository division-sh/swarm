package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// The selected fixture owner validates canonical identities before this fixed
// physical fault. It fails the real UPDATE, not the handler or its observer.
func InstallNodeRecoveryPoisonMutationTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID string) error {
	var count int
	var path sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),MIN(instance_path) FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&count, &path); err != nil {
		return err
	}
	if count != 1 || !path.Valid || path.String == "" {
		return fmt.Errorf("node recovery poison fault requires one selected entity: found=%d", count)
	}
	headerPath := strings.ReplaceAll(path.String, "'", "''")
	statement := fmt.Sprintf(`CREATE TRIGGER fail_node_recovery_poison_update
		BEFORE UPDATE ON flow_instances
		WHEN OLD.run_id = '%s' AND OLD.entity_id = '%s' AND OLD.instance_path = '%s'
		BEGIN
			SELECT RAISE(ABORT, 'injected node recovery poison mutation');
		END`, runID, entityID, headerPath)
	if postgres {
		statement = fmt.Sprintf(`CREATE FUNCTION fail_node_recovery_poison_update_fn() RETURNS trigger AS $$
			BEGIN
				IF OLD.run_id = '%s'::uuid AND OLD.entity_id = '%s'::uuid AND OLD.instance_path = '%s' THEN
					RAISE EXCEPTION 'injected node recovery poison mutation';
				END IF;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER fail_node_recovery_poison_update
			BEFORE UPDATE ON flow_instances
			FOR EACH ROW EXECUTE FUNCTION fail_node_recovery_poison_update_fn()`, runID, entityID, headerPath)
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}

func RemoveNodeRecoveryPoisonMutationTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	statement := `DROP TRIGGER fail_node_recovery_poison_update`
	if postgres {
		statement = `DROP TRIGGER fail_node_recovery_poison_update ON flow_instances;
			DROP FUNCTION fail_node_recovery_poison_update_fn()`
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}
