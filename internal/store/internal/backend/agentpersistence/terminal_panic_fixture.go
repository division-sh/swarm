package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func InstallTerminalPanicMutationFailureTx(ctx context.Context, tx *sql.Tx, postgres bool, agentID string, failFinalizer bool) error {
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agents WHERE agent_id=$1)`, agentID).Scan(&present); err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("terminal panic fault requires existing agent %q", agentID)
	}
	const failure = "exact panic terminal mutation rollback"
	condition := "NEW.agent_id = '" + strings.ReplaceAll(agentID, "'", "''") + "' AND NEW.lifecycle_phase = 'failed'"
	if failFinalizer {
		condition = "NEW.agent_id = '" + strings.ReplaceAll(agentID, "'", "''") + "' AND NEW.lifecycle_phase IN ('failed', 'registered')"
	}
	ddl := "CREATE TRIGGER panic_terminal_failure BEFORE UPDATE ON agents WHEN " + condition + " BEGIN SELECT RAISE(ABORT, '" + failure + "'); END"
	if postgres {
		if _, err := tx.ExecContext(ctx, "CREATE FUNCTION panic_terminal_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION '"+failure+"'; END $$"); err != nil {
			return err
		}
		ddl = "CREATE TRIGGER panic_terminal_failure BEFORE UPDATE ON agents FOR EACH ROW WHEN (" + condition + ") EXECUTE FUNCTION panic_terminal_failure()"
	}
	_, err := tx.ExecContext(ctx, ddl)
	return err
}
