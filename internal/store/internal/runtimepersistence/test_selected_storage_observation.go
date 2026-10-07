package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
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
