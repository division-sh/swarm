package eventpersistence

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

func (s *EventPostgresOwner) ObserveEventCardinalityForTest(ctx context.Context, eventID string) (int, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeEventCardinality(ctx, tx, eventID, &count) })
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *EventSQLiteOwner) ObserveEventCardinalityForTest(ctx context.Context, eventID string) (int, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeEventCardinality(ctx, tx, eventID, &count) })
	if err != nil {
		return 0, err
	}
	return count, nil
}

func observeEventCardinality(ctx context.Context, tx *sql.Tx, eventID string, count *int) error {
	if _, err := uuid.Parse(eventID); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, eventID).Scan(count)
}
