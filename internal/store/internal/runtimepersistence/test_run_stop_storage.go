package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type RunStopStorage = runlifecycle.RunStopStorage

func RemovePausedRunControlForTest(ctx context.Context, selected any, runID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runLifecyclePostgresOwner.RemovePausedRunControlForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return store.runLifecycleSQLiteOwner.RemovePausedRunControlForTest(ctx, runID)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func ContradictPausedRunControlForTest(ctx context.Context, selected any, runID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runLifecyclePostgresOwner.ContradictPausedRunControlForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return store.runLifecycleSQLiteOwner.ContradictPausedRunControlForTest(ctx, runID)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func ReadRunStopStorageForTest(ctx context.Context, selected any, runID string) (RunStopStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return RunStopStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runLifecyclePostgresOwner.ReadRunStopStorageForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return store.runLifecycleSQLiteOwner.ReadRunStopStorageForTest(ctx, runID)
	default:
		return RunStopStorage{}, fmt.Errorf("run-stop storage requires the original selected owner")
	}
}
