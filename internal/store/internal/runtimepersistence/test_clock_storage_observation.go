package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

type ClockStorageObservation = pipelinepersistence.ClockStorageObservation

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
	return owner.pipelinePostgresOwner.ReadPostgresObservationTimeForTest(ctx)
}

// Counts every instance clock, including terminal rows. An empty active list
// alone cannot prove that a finite host never admitted a deployment binding.
func CountInstanceClockActivationsForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch native := selected.(type) {
	case *PostgresStore:
		return native.pipelinePostgresOwner.CountInstanceClockActivationsForTest(ctx)
	case *SQLiteRuntimeStore:
		return native.pipelineSQLiteOwner.CountInstanceClockActivationsForTest(ctx)
	default:
		return 0, fmt.Errorf("clock observation requires its original native owner")
	}
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
	switch native := selected.(type) {
	case *PostgresStore:
		return native.pipelinePostgresOwner.CorruptClockImmutableHashForTest(ctx, runID, activationID)
	case *SQLiteRuntimeStore:
		return native.pipelineSQLiteOwner.CorruptClockImmutableHashForTest(ctx, runID, activationID)
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
	switch native := selected.(type) {
	case *PostgresStore:
		return native.pipelinePostgresOwner.ReadClockStorageForTest(ctx, runID, activationID)
	case *SQLiteRuntimeStore:
		return native.pipelineSQLiteOwner.ReadClockStorageForTest(ctx, runID, activationID)
	default:
		return ClockStorageObservation{}, fmt.Errorf("clock observation requires its original native owner")
	}
}
