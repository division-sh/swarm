package eventpersistence

import (
	"context"
	"database/sql"
)

func (s *EventPostgresOwner) SetSessionPublicationInsertFaultForTest(ctx context.Context, enabled bool) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return setSessionPublicationInsertFaultTx(ctx, tx, true, enabled)
	})
}

func (s *EventSQLiteOwner) SetSessionPublicationInsertFaultForTest(ctx context.Context, enabled bool) error {
	return s.backend.RunTransaction(ctx, "session publication insert fault fixture", func(ctx context.Context, tx *sql.Tx) error {
		return setSessionPublicationInsertFaultTx(ctx, tx, false, enabled)
	})
}

func setSessionPublicationInsertFaultTx(ctx context.Context, tx *sql.Tx, postgres, enabled bool) error {
	statements := []string{`DROP TRIGGER reject_whatsapp_publication`}
	if enabled {
		statements = []string{`CREATE TRIGGER reject_whatsapp_publication BEFORE INSERT ON inbound_publication_events BEGIN SELECT RAISE(ABORT,'publication rollback proof'); END`}
	}
	if postgres {
		statements = []string{`DROP TRIGGER reject_whatsapp_publication ON inbound_publication_events`, `DROP FUNCTION reject_whatsapp_publication()`}
		if enabled {
			statements = []string{
				`CREATE FUNCTION reject_whatsapp_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'publication rollback proof'; END $$`,
				`CREATE TRIGGER reject_whatsapp_publication BEFORE INSERT ON inbound_publication_events FOR EACH ROW EXECUTE FUNCTION reject_whatsapp_publication()`,
			}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
