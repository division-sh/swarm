package entitystore

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
)

func InstallNodeRecoveryPoisonFaultTx(ctx context.Context, tx *sql.Tx, postgres bool, entityID string) error {
	parsed, err := uuid.Parse(entityID)
	if err != nil || parsed.String() != entityID {
		return fmt.Errorf("poison fault requires an exact canonical entity")
	}
	query := fmt.Sprintf(`CREATE TRIGGER fail_node_recovery_poison_update BEFORE UPDATE ON entity_state WHEN OLD.entity_id = '%s' BEGIN SELECT RAISE(ABORT, 'injected node recovery poison mutation'); END`, entityID)
	if postgres {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION fail_node_recovery_poison_update_fn() RETURNS trigger AS $$ BEGIN IF OLD.entity_id = '%s'::uuid THEN RAISE EXCEPTION 'injected node recovery poison mutation'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql`, entityID)); err != nil {
			return err
		}
		query = `CREATE TRIGGER fail_node_recovery_poison_update BEFORE UPDATE ON entity_state FOR EACH ROW EXECUTE FUNCTION fail_node_recovery_poison_update_fn()`
	}
	_, err = tx.ExecContext(ctx, query)
	return err
}

func RemoveNodeRecoveryPoisonFaultTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	query := `DROP TRIGGER IF EXISTS fail_node_recovery_poison_update`
	if postgres {
		query += ` ON entity_state`
	}
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return err
	}
	if postgres {
		_, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS fail_node_recovery_poison_update_fn()`)
		return err
	}
	return nil
}
