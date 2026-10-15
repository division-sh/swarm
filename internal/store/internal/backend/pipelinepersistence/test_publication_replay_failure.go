package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/google/uuid"
)

func SetPublicationReplayFailureTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID, template string, enabled bool) error {
	name := "publication_replay_" + strings.ReplaceAll(id.String(), "-", "")
	statements := []string{"DROP TRIGGER IF EXISTS " + name}
	if postgres {
		statements = []string{"DROP TRIGGER IF EXISTS " + name + " ON committed_replay_scopes", "DROP FUNCTION IF EXISTS " + name + "()"}
	}
	if enabled {
		template = strings.ReplaceAll(template, "'", "''")
		priorDelivery := delivery.ReplayScopePriorDeliveryWitnessSQL(postgres)
		statements = []string{fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON committed_replay_scopes BEGIN
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM events WHERE event_id=NEW.event_id) OR NOT (%s)
THEN RAISE(ABORT,'replay fault prerequisite: exact event/delivery absent') END;
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM flow_instances fi
JOIN entity_state es ON es.run_id=fi.run_id AND es.flow_instance=fi.instance_path
JOIN routing_rules rr ON rr.run_id=fi.run_id AND rr.flow_instance=fi.instance_path
WHERE fi.run_id=(SELECT run_id FROM events WHERE event_id=NEW.event_id) AND fi.flow_template='%s')
THEN RAISE(ABORT,'replay fault prerequisite: exact lifecycle absent') END;
SELECT RAISE(ABORT,'injected committed replay-scope persistence failure'); END`, name, priorDelivery, template)}
		if postgres {
			statements = []string{fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM events WHERE event_id=NEW.event_id) OR NOT (%s) THEN
RAISE EXCEPTION 'replay fault prerequisite: exact event/delivery absent'; END IF;
IF NOT EXISTS (SELECT 1 FROM flow_instances fi
JOIN entity_state es ON es.run_id=fi.run_id AND es.flow_instance=fi.instance_path
JOIN routing_rules rr ON rr.run_id=fi.run_id AND rr.flow_instance=fi.instance_path
WHERE fi.run_id=(SELECT run_id FROM events WHERE event_id=NEW.event_id) AND fi.flow_template='%s') THEN
RAISE EXCEPTION 'replay fault prerequisite: exact lifecycle absent'; END IF;
RAISE EXCEPTION 'injected committed replay-scope persistence failure'; END $$`, name, priorDelivery, template),
				fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON committed_replay_scopes FOR EACH ROW EXECUTE FUNCTION %s()`, name, name)}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func CountPublicationReplayFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID) (int, error) {
	name := "publication_replay_" + strings.ReplaceAll(id.String(), "-", "")
	var count int
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
	if postgres {
		query = `SELECT (SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1) + (SELECT COUNT(*) FROM pg_proc WHERE proname=$1 AND pronamespace=current_schema()::regnamespace)`
	}
	err := tx.QueryRowContext(ctx, query, name).Scan(&count)
	return count, err
}
