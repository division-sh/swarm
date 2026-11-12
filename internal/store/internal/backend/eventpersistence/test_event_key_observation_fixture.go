package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *EventPostgresOwner) SeedEventKeyCardinalityObservationForTest(ctx context.Context, eventIDs [3]string, key string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return seedEventKeyCardinalityObservationTx(ctx, tx, eventIDs, key)
	})
}

func (s *EventSQLiteOwner) SeedEventKeyCardinalityObservationForTest(ctx context.Context, eventIDs [3]string, key string) error {
	return s.backend.RunTransaction(ctx, "event-key cardinality observation fixture", func(ctx context.Context, tx *sql.Tx) error {
		return seedEventKeyCardinalityObservationTx(ctx, tx, eventIDs, key)
	})
}

func seedEventKeyCardinalityObservationTx(ctx context.Context, tx *sql.Tx, eventIDs [3]string, key string) error {
	for index, eventID := range eventIDs {
		value := key
		if index == 2 {
			value = " " + key + " "
		}
		result, err := tx.ExecContext(ctx, `UPDATE events SET idempotency_key=$1 WHERE event_id=$2`, value, eventID)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return fmt.Errorf("event-key fixture requires its existing exact event: rows=%d err=%v", count, err)
		}
	}
	return nil
}
