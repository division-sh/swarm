package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
)

// Abort the exact card insert inside the original selected mutation transaction.
// The fixed trigger is removed through that same writer before fixture close.
func SetWorkflowGateCardInsertFaultForTest(ctx context.Context, selected any, run string, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := validateSelectedForkStorageIdentity(run); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	literal := "'" + strings.ReplaceAll(run, "'", "''") + "'"
	statements := []string{"DROP TRIGGER workflow_gate_card_insert_cut"}
	if postgres {
		statements = []string{"DROP TRIGGER workflow_gate_card_insert_cut ON decision_cards", "DROP FUNCTION workflow_gate_card_insert_cut()"}
	}
	if enabled {
		statements = []string{"CREATE TRIGGER workflow_gate_card_insert_cut AFTER INSERT ON decision_cards WHEN NEW.run_id=" + literal + " BEGIN SELECT RAISE(ABORT,'workflow_gate_card_insert_cut'); END"}
		if postgres {
			statements = []string{"CREATE FUNCTION workflow_gate_card_insert_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id=" + literal + "::uuid THEN RAISE EXCEPTION 'workflow_gate_card_insert_cut'; END IF; RETURN NEW; END $$", "CREATE TRIGGER workflow_gate_card_insert_cut AFTER INSERT ON decision_cards FOR EACH ROW EXECUTE FUNCTION workflow_gate_card_insert_cut()"}
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
