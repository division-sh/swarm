package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ClockStorageObservation struct {
	Rows                  int
	Status, ImmutableHash string
}

// Catalog observations use PostgreSQL's transaction clock, not an application
// timestamp. SQLite keeps its separately defined application-clock boundary.
func ReadPostgresObservationTimeForTest(ctx context.Context, selected any) (time.Time, error) {
	owner, ok := selected.(*PostgresStore)
	if !ok || owner == nil {
		return time.Time{}, fmt.Errorf("postgres observation time requires its original native owner")
	}
	if err := validateChannelObservationOwner(owner); err != nil {
		return time.Time{}, err
	}
	var out time.Time
	err := readServedDeliveryObservation(ctx, owner, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT NOW()`).Scan(&out)
	})
	if err != nil {
		return time.Time{}, err
	}
	return out.UTC(), nil
}

// Counts every instance clock, including terminal rows. An empty active list
// alone cannot prove that a finite host never admitted a deployment binding.
func CountInstanceClockActivationsForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE owner_kind='instance'`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
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
