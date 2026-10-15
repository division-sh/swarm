package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

func readCLIForkCount(ctx context.Context, selected any, ids []string, read func(context.Context, *sql.Tx) (int, error)) (int, error) {
	for _, id := range ids {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return 0, err
		}
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = read(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountSelectedForkExactExecutionStorageForTest(ctx context.Context, selected any, fork, source, event string) (int, error) {
	return readCLIForkCount(ctx, selected, []string{fork, source, event}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runforkpersistence.CountSelectedForkExactExecutionStorageTx(ctx, tx, fork, source, event)
	})
}

func CountSelectedForkDeliveryReplayStorageForTest(ctx context.Context, selected any, fork string) (int, error) {
	return readCLIForkCount(ctx, selected, []string{fork}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runforkpersistence.CountSelectedForkDeliveryReplayStorageTx(ctx, tx, fork)
	})
}

func CountRunDeliveryStorageForTest(ctx context.Context, selected any, run string) (int, error) {
	return readCLIForkCount(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountRunDeliveryStorageTx(ctx, tx, run)
	})
}

func ReadCLIMaterializedEntityStateForTest(ctx context.Context, selected any, run, entity string) (string, error) {
	for _, id := range []string{run, entity} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return "", err
		}
	}
	var state string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		state, err = pipelinepersistence.ReadCLIMaterializedEntityStateTx(ctx, tx, run, entity)
		return err
	})
	if err != nil {
		return "", err
	}
	return state, nil
}

func ReadSelectedForkBindingModeStorageForTest(ctx context.Context, selected any, fork, source, event string) (string, error) {
	for _, id := range []string{fork, source, event} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return "", err
		}
	}
	var mode string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		mode, err = runforkpersistence.ReadSelectedForkBindingModeStorageTx(ctx, tx, fork, source, event)
		return err
	})
	if err != nil {
		return "", err
	}
	return mode, nil
}

// Headerless state-only history, with the two original independent commits.
// No construction, executable admission or revision capture is manufactured.
func SeedPostgresCLIMaterializationHistoryForTest(ctx context.Context, selected any, run, entity, event string, at time.Time) error {
	if pg, ok := selected.(*PostgresStore); !ok || pg == nil {
		return fmt.Errorf("CLI materialization history requires the native PostgreSQL owner")
	}
	for _, id := range []string{run, entity, event} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return err
		}
	}
	if at.IsZero() {
		return fmt.Errorf("CLI materialization history requires its historical clock")
	}
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertCLIMaterializationHistoryMutationsTx(ctx, tx, run, entity, event, at)
	}); err != nil {
		return fmt.Errorf("seed mutations: %w", err)
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertCLIMaterializationHistoryStateTx(ctx, tx, run, entity, at)
	})
}
