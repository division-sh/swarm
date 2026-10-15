package apiidempotency

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/google/uuid"
)

func SetPublicationCompletionFailureTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID, template string, enabled bool) error {
	name := "publication_completion_" + strings.ReplaceAll(id.String(), "-", "")
	statements := []string{"DROP TRIGGER IF EXISTS " + name}
	if postgres {
		statements = []string{"DROP TRIGGER IF EXISTS " + name + " ON api_idempotency", "DROP FUNCTION IF EXISTS " + name + "()"}
	}
	if enabled {
		template = strings.ReplaceAll(template, "'", "''")
		priorDelivery := delivery.APICompletionPriorDeliveryWitnessSQL(postgres)
		statements = []string{fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON api_idempotency WHEN NEW.method='event.publish' BEGIN
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM events WHERE event_id=NEW.resource_id) OR NOT (%s) OR
NOT EXISTS (SELECT 1 FROM committed_replay_scopes WHERE event_id=NEW.resource_id)
THEN RAISE(ABORT,'API completion fault prerequisite: exact publication absent') END;
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM flow_instances fi
JOIN workflow_instance_initial_materializations m ON m.run_id=fi.run_id AND m.instance_path=fi.instance_path AND m.entity_id=fi.entity_id
JOIN flow_instance_runtime_readiness r ON r.run_id=fi.run_id AND r.instance_path=fi.instance_path
WHERE fi.run_id=(SELECT run_id FROM events WHERE event_id=NEW.resource_id) AND fi.flow_template='%s'
AND json_extract(m.projection,'$.creating_input.event_id')=NEW.resource_id
AND (fi.entity_type IS NULL OR EXISTS (SELECT 1 FROM entity_state es
WHERE es.run_id=fi.run_id AND es.flow_instance=fi.instance_path AND es.entity_id=fi.entity_id AND es.entity_type=fi.entity_type)))
THEN RAISE(ABORT,'API completion fault prerequisite: exact lifecycle absent') END;
SELECT RAISE(ABORT,'injected API idempotency completion persistence failure'); END`, name, priorDelivery, template)}
		if postgres {
			statements = []string{fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM events WHERE event_id=NEW.resource_id::uuid) OR NOT (%s) OR
NOT EXISTS (SELECT 1 FROM committed_replay_scopes WHERE event_id=NEW.resource_id::uuid) THEN
RAISE EXCEPTION 'API completion fault prerequisite: exact publication absent'; END IF;
IF NOT EXISTS (SELECT 1 FROM flow_instances fi
JOIN workflow_instance_initial_materializations m ON m.run_id=fi.run_id AND m.instance_path=fi.instance_path AND m.entity_id=fi.entity_id
JOIN flow_instance_runtime_readiness r ON r.run_id=fi.run_id AND r.instance_path=fi.instance_path
WHERE fi.run_id=(SELECT run_id FROM events WHERE event_id=NEW.resource_id::uuid) AND fi.flow_template='%s'
AND m.projection->'creating_input'->>'event_id'=NEW.resource_id
AND (fi.entity_type IS NULL OR EXISTS (SELECT 1 FROM entity_state es
WHERE es.run_id=fi.run_id AND es.flow_instance=fi.instance_path AND es.entity_id=fi.entity_id AND es.entity_type=fi.entity_type))) THEN
RAISE EXCEPTION 'API completion fault prerequisite: exact lifecycle absent'; END IF;
RAISE EXCEPTION 'injected API idempotency completion persistence failure'; END $$`, name, priorDelivery, template),
				fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON api_idempotency FOR EACH ROW WHEN (NEW.method='event.publish') EXECUTE FUNCTION %s()`, name, name)}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func CountPublicationCompletionFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID) (int, error) {
	name := "publication_completion_" + strings.ReplaceAll(id.String(), "-", "")
	var count int
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
	if postgres {
		query = `SELECT (SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1) + (SELECT COUNT(*) FROM pg_proc WHERE proname=$1 AND pronamespace=current_schema()::regnamespace)`
	}
	err := tx.QueryRowContext(ctx, query, name).Scan(&count)
	return count, err
}
