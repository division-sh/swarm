package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

func SetOutboxDeadLetterInsertFaultTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string, enabled bool) error {
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return fmt.Errorf("outbox diagnostic fault requires exact event identity")
	}
	statements := []string{"DROP TRIGGER IF EXISTS outbox_dead_letter_insert_fault"}
	if postgres {
		statements = []string{"DROP TRIGGER IF EXISTS outbox_dead_letter_insert_fault ON dead_letters", "DROP FUNCTION IF EXISTS outbox_dead_letter_insert_fault()"}
	}
	if enabled {
		statements = []string{fmt.Sprintf(`CREATE TRIGGER outbox_dead_letter_insert_fault BEFORE INSERT ON dead_letters
WHEN NEW.original_event_id='%s' BEGIN SELECT RAISE(ABORT,'outbox_dead_letter_insert_fault'); END`, eventID)}
		if postgres {
			statements = []string{fmt.Sprintf(`CREATE FUNCTION outbox_dead_letter_insert_fault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.original_event_id='%s'::uuid THEN RAISE EXCEPTION 'outbox_dead_letter_insert_fault'; END IF; RETURN NEW; END $$`, eventID), "CREATE TRIGGER outbox_dead_letter_insert_fault BEFORE INSERT ON dead_letters FOR EACH ROW EXECUTE FUNCTION outbox_dead_letter_insert_fault()"}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
