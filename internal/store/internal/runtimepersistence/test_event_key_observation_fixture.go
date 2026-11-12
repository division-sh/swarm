package runtimepersistence

import (
	"context"
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
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.SeedEventKeyCardinalityObservationForTest(ctx, eventIDs, key)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.SeedEventKeyCardinalityObservationForTest(ctx, eventIDs, key)
	default:
		return fmt.Errorf("event-key fixture requires an original native owner")
	}
}
