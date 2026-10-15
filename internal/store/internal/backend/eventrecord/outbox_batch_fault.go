package eventrecord

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// SetOutboxAfterFirstEventWriteFaultTx cuts the second event insert only after
// the first event is visible in that same native transaction. No fixture row is
// seeded, and all trigger installation/removal uses the selected writer.
func SetOutboxAfterFirstEventWriteFaultTx(ctx context.Context, tx *sql.Tx, postgres bool, run, first, second string, enabled bool) error {
	for _, value := range []string{run, first, second} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("outbox write fault requires exact canonical identities")
		}
	}
	if first == second {
		return fmt.Errorf("outbox write fault requires distinct event occurrences")
	}
	statements := []string{"DROP TRIGGER IF EXISTS outbox_after_first_event_write"}
	if postgres {
		statements = []string{"DROP TRIGGER IF EXISTS outbox_after_first_event_write ON events", "DROP FUNCTION IF EXISTS outbox_after_first_event_write()"}
	}
	if enabled {
		statements = []string{fmt.Sprintf(`CREATE TRIGGER outbox_after_first_event_write AFTER INSERT ON events
WHEN NEW.run_id='%s' AND NEW.event_id='%s' BEGIN
SELECT CASE WHEN EXISTS(SELECT 1 FROM events WHERE run_id='%s' AND event_id='%s')
THEN RAISE(ABORT,'outbox_after_first_event_write') ELSE RAISE(ABORT,'outbox_missing_first_event') END; END`, run, second, run, first)}
		if postgres {
			statements = []string{fmt.Sprintf(`CREATE FUNCTION outbox_after_first_event_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.run_id='%s'::uuid AND NEW.event_id='%s'::uuid THEN
IF EXISTS(SELECT 1 FROM events WHERE run_id='%s'::uuid AND event_id='%s'::uuid) THEN
RAISE EXCEPTION 'outbox_after_first_event_write'; ELSE RAISE EXCEPTION 'outbox_missing_first_event'; END IF;
END IF; RETURN NEW; END $$`, run, second, run, first), "CREATE TRIGGER outbox_after_first_event_write AFTER INSERT ON events FOR EACH ROW EXECUTE FUNCTION outbox_after_first_event_write()"}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
