package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// The events key is a physical observation predicate, not an API receipt key.
// Seed duplicates and a whitespace-distinct key on three already admitted
// fixture events; no caller query or arbitrary field mutation is accepted.
func SeedEventKeyCardinalityObservationForTest(ctx context.Context, selected any, eventIDs [3]string, key string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("event-key fixture requires a nonempty exact key")
	}
	seen := make(map[string]bool, len(eventIDs))
	for _, eventID := range eventIDs {
		id, err := uuid.Parse(eventID)
		if err != nil || id == uuid.Nil || id.String() != eventID || seen[eventID] {
			return fmt.Errorf("event-key fixture requires three distinct exact event identities")
		}
		seen[eventID] = true
	}
	write := func(ctx context.Context, tx *sql.Tx) error {
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
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		return owner.backend.RunTransaction(ctx, "event-key cardinality observation fixture", write)
	default:
		return fmt.Errorf("event-key fixture requires an original native owner")
	}
}
