package platformschema

import (
	"context"
	"database/sql"
	"fmt"
)

// The former composite FK cannot represent a run-less platform event whose
// delivery is owned by a subscriber run. Keep both cases structural instead.
func InstallPostgresDeliveryRunIdentity(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE FUNCTION swarm_check_event_delivery_run_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM events e WHERE e.event_id = NEW.event_id AND
            (e.run_id = NEW.run_id OR
             (e.run_id IS NULL AND e.event_class IN ('runtime_control', 'runtime_diagnostic')
              AND e.produced_by_type = 'platform' AND e.source_event_id IS NULL
              AND NEW.delivery_context->>'run_id' = NEW.run_id::text))
        FOR SHARE
    ) THEN
        RAISE EXCEPTION 'event delivery run identity mismatch' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$`,
		`CREATE TRIGGER swarm_event_delivery_run_identity_insert
BEFORE INSERT ON event_deliveries FOR EACH ROW
EXECUTE FUNCTION swarm_check_event_delivery_run_identity()`,
		`CREATE TRIGGER swarm_event_delivery_run_identity_update
BEFORE UPDATE OF event_id, run_id, delivery_context ON event_deliveries FOR EACH ROW
EXECUTE FUNCTION swarm_check_event_delivery_run_identity()`,
		`CREATE FUNCTION swarm_check_event_run_delivery_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM event_deliveries d WHERE d.event_id = NEW.event_id AND
            (NEW.run_id = d.run_id OR
             (NEW.run_id IS NULL AND NEW.event_class IN ('runtime_control', 'runtime_diagnostic')
              AND NEW.produced_by_type = 'platform' AND NEW.source_event_id IS NULL
              AND d.delivery_context->>'run_id' = d.run_id::text)) IS NOT TRUE
        FOR SHARE
    ) THEN
        RAISE EXCEPTION 'event delivery run identity mismatch' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$`,
		`CREATE TRIGGER swarm_event_run_delivery_identity_update
BEFORE UPDATE OF run_id, event_class, produced_by_type, source_event_id ON events FOR EACH ROW
EXECUTE FUNCTION swarm_check_event_run_delivery_identity()`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("install postgres event delivery run identity: %w", err)
		}
	}
	return nil
}

func InstallSQLiteDeliveryRunIdentity(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) error {
	for _, verb := range []string{"INSERT", "UPDATE OF event_id, run_id, delivery_context"} {
		name := "swarm_event_delivery_run_identity_insert"
		if verb != "INSERT" {
			name = "swarm_event_delivery_run_identity_update"
		}
		statement := fmt.Sprintf(`CREATE TRIGGER %s BEFORE %s ON event_deliveries FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'event delivery run identity mismatch') WHERE NOT EXISTS (
        SELECT 1 FROM events e WHERE e.event_id = NEW.event_id AND
            (e.run_id = NEW.run_id OR
             (e.run_id IS NULL AND e.event_class IN ('runtime_control', 'runtime_diagnostic')
              AND e.produced_by_type = 'platform' AND e.source_event_id IS NULL
              AND json_extract(NEW.delivery_context, '$.run_id') = NEW.run_id))
    );
END`, name, verb)
		if _, err := q.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("install sqlite event delivery run identity: %w", err)
		}
	}
	statement := `CREATE TRIGGER swarm_event_run_delivery_identity_update
BEFORE UPDATE OF run_id, event_class, produced_by_type, source_event_id ON events FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'event delivery run identity mismatch') WHERE EXISTS (
        SELECT 1 FROM event_deliveries d WHERE d.event_id = NEW.event_id AND
            COALESCE(NEW.run_id = d.run_id OR
             (NEW.run_id IS NULL AND NEW.event_class IN ('runtime_control', 'runtime_diagnostic')
              AND NEW.produced_by_type = 'platform' AND NEW.source_event_id IS NULL
              AND json_extract(d.delivery_context, '$.run_id') = d.run_id), 0) = 0
    );
END`
	if _, err := q.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("install sqlite event-side delivery run identity: %w", err)
	}
	return nil
}
