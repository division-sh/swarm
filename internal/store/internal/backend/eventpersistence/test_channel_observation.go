package eventpersistence

import (
	"context"
	"database/sql"
)

func countChannelObservedEventsForTestTx(ctx context.Context, tx *sql.Tx, run, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, run, event).Scan(&count)
	return count, err
}

func (s *EventPostgresOwner) CountChannelObservedEventsForTest(ctx context.Context, run, event string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = countChannelObservedEventsForTestTx(ctx, tx, run, event)
		return err
	})
	return count, err
}

func (s *EventSQLiteOwner) CountChannelObservedEventsForTest(ctx context.Context, run, event string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = countChannelObservedEventsForTestTx(ctx, tx, run, event)
		return err
	})
	return count, err
}
