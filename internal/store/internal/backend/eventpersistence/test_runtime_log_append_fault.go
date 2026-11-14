package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *EventPostgresOwner) SetRuntimeLogFixtureAppendFaultForTest(ctx context.Context, runID string, enabled bool) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("runtime log append fault requires the original postgres owner")
	}
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return setRuntimeLogFixtureAppendFaultTx(ctx, tx, runID, true, enabled)
	})
}

func (s *EventSQLiteOwner) SetRuntimeLogFixtureAppendFaultForTest(ctx context.Context, runID string, enabled bool) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("runtime log append fault requires the original sqlite owner")
	}
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return setRuntimeLogFixtureAppendFaultTx(ctx, tx, runID, false, enabled)
	})
}

func setRuntimeLogFixtureAppendFaultTx(ctx context.Context, tx *sql.Tx, runID string, postgres, enabled bool) error {
	literal := "'" + strings.ReplaceAll(runID, "'", "''") + "'"
	statements := []string{`DROP TRIGGER runtime_log_exact_append_cut`}
	if postgres {
		statements = []string{`DROP TRIGGER runtime_log_exact_append_cut ON events`, `DROP FUNCTION runtime_log_exact_append_cut()`}
	}
	if enabled {
		statements = []string{`CREATE TRIGGER runtime_log_exact_append_cut AFTER INSERT ON events WHEN NEW.event_name='platform.runtime_log' AND NEW.run_id=` + literal + ` BEGIN SELECT RAISE(ABORT,'runtime_log_exact_append_cut'); END`}
		if postgres {
			statements = []string{
				`CREATE FUNCTION runtime_log_exact_append_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_name='platform.runtime_log' AND NEW.run_id=` + literal + `::uuid THEN RAISE EXCEPTION 'runtime_log_exact_append_cut'; END IF; RETURN NEW; END $$`,
				`CREATE TRIGGER runtime_log_exact_append_cut AFTER INSERT ON events FOR EACH ROW EXECUTE FUNCTION runtime_log_exact_append_cut()`,
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
