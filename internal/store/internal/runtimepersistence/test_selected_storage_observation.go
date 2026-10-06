package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// This private adapter is extracted from the existing served observation owner.
// Its callback never crosses a public fixture boundary.
func readServedDeliveryObservation(ctx context.Context, selected any, read func(context.Context, *sql.Tx) error) error {
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		return owner.backend.RunReadTransaction(ctx, read)
	default:
		return fmt.Errorf("storage observation requires the original selected owner, got %T", selected)
	}
}

func validateEntityToolStorageIdentity(runID, entityID string) error {
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("entity tool storage requires an exact run identity: %w", err)
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return fmt.Errorf("entity tool storage requires an exact entity identity: %w", err)
	}
	return nil
}

func validateChannelObservationOwner(selected any) error {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return fmt.Errorf("channel observation requires an initialized postgres read owner")
		}
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return fmt.Errorf("channel observation requires an initialized sqlite read owner")
		}
	default:
		return fmt.Errorf("channel observation requires an original native read owner, got %T", selected)
	}
	return nil
}

