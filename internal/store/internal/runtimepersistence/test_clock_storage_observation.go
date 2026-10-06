package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type ClockStorageObservation struct {
	Rows                  int
	Status, ImmutableHash string
}

func validateClockStorageCoordinate(runID, activationID string) error {
	for _, value := range []string{runID, activationID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("clock storage observation requires exact run and activation identities")
		}
	}
	return nil
}

// Closed hostile storage fault, never an admission or historical fact writer.
func CorruptClockImmutableHashForTest(ctx context.Context, selected any, runID, activationID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := validateClockStorageCoordinate(runID, activationID); err != nil {
		return err
	}
	write := func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE timers SET immutable_hash='corrupt' WHERE run_id=$1 AND timer_id=$2 AND owner_kind='instance'`, runID, activationID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return fmt.Errorf("clock fault requires one exact existing instance clock: rows=%d error=%v", count, err)
		}
		return nil
	}
	switch native := selected.(type) {
	case *PostgresStore:
		return native.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		return native.backend.RunTransaction(ctx, "clock immutable hash fault", write)
	default:
		return fmt.Errorf("clock storage fault requires its original native owner")
	}
}

func ReadClockStorageForTest(ctx context.Context, selected any, runID, activationID string) (ClockStorageObservation, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ClockStorageObservation{}, err
	}
	if err := validateClockStorageCoordinate(runID, activationID); err != nil {
		return ClockStorageObservation{}, err
	}
	var out ClockStorageObservation
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT status,immutable_hash FROM timers WHERE run_id=$1 AND timer_id=$2`, runID, activationID).Scan(&out.Status, &out.ImmutableHash); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers`).Scan(&out.Rows)
	})
	if err != nil {
		return ClockStorageObservation{}, err
	}
	return out, nil
}
