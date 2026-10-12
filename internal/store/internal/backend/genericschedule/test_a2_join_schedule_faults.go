package genericschedule

import (
	"context"
	"database/sql"
)

func SetA2ActivationJoinScheduleFailureForTest(ctx context.Context, tx *sql.Tx, postgres, enabled bool) error {
	if !enabled {
		query := "DROP TRIGGER a2_activation_join_failure"
		if postgres {
			query += " ON timers"
		}
		_, err := tx.ExecContext(ctx, query)
		return err
	}
	query := `CREATE TRIGGER a2_activation_join_failure BEFORE INSERT ON timers BEGIN SELECT RAISE(ABORT,'a2_activation_join_sql_fault'); END`
	if postgres {
		if _, err := tx.ExecContext(ctx, `CREATE FUNCTION a2_activation_join_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'a2_activation_join_sql_fault'; END $$`); err != nil {
			return err
		}
		query = `CREATE TRIGGER a2_activation_join_failure BEFORE INSERT ON timers FOR EACH ROW EXECUTE FUNCTION a2_activation_join_fail()`
	}
	_, err := tx.ExecContext(ctx, query)
	return err
}

func SetA2JoinCloseScheduleFailureForTest(ctx context.Context, tx *sql.Tx, postgres, enabled bool) error {
	if !enabled {
		query := "DROP TRIGGER a2_join_close_failure"
		if postgres {
			query += " ON timers"
		}
		_, err := tx.ExecContext(ctx, query)
		return err
	}
	query := `CREATE TRIGGER a2_join_close_failure BEFORE INSERT ON timers WHEN NEW.fire_event='platform.join_complete' BEGIN SELECT RAISE(ABORT,'a2_join_close_sql_fault'); END`
	if postgres {
		if _, err := tx.ExecContext(ctx, `CREATE FUNCTION a2_join_close_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.fire_event='platform.join_complete' THEN RAISE EXCEPTION 'a2_join_close_sql_fault'; END IF; RETURN NEW; END $$`); err != nil {
			return err
		}
		query = `CREATE TRIGGER a2_join_close_failure BEFORE INSERT ON timers FOR EACH ROW EXECUTE FUNCTION a2_join_close_fail()`
	}
	_, err := tx.ExecContext(ctx, query)
	return err
}
