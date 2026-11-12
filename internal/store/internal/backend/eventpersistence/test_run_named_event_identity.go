package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *EventPostgresOwner) ReadRunNamedEventIdentityForTest(ctx context.Context, runID, eventName string) (string, error) {
	var eventID string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		eventID, err = readRunNamedEventIdentityTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func (s *EventSQLiteOwner) ReadRunNamedEventIdentityForTest(ctx context.Context, runID, eventName string) (string, error) {
	var eventID string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		eventID, err = readRunNamedEventIdentityTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func readRunNamedEventIdentityTx(ctx context.Context, tx *sql.Tx, runID, eventName string) (string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1 AND event_name=$2 LIMIT 2`, runID, eventName)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var eventID string
	count := 0
	for rows.Next() {
		count++
		if err := rows.Scan(&eventID); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	switch count {
	case 0:
		return "", sql.ErrNoRows
	case 1:
		return eventID, nil
	default:
		return "", fmt.Errorf("ambiguous physical event identity for run %s and event %s", runID, eventName)
	}
}
