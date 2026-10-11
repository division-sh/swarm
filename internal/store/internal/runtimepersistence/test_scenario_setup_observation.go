package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type ScenarioSetupRunStorage = runlifecycle.ScenarioSetupRunStorage
type PostgresScenarioSetupEntityStorage = entitystore.PostgresScenarioSetupEntityStorage

type NestedFanOutEntityStorage = entitystore.NestedFanOutEntityStorage

func ReadNestedRunEntityStateFieldsForTest(ctx context.Context, selected any, run string) ([]NestedFanOutEntityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]NestedFanOutEntityStorage, error) {
		return entitystore.ReadNestedRunEntityStateFieldsTx(ctx, tx, run)
	})
}

func ReadNestedRunEntityStatesForTest(ctx context.Context, selected any, run string) ([]NestedFanOutEntityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]NestedFanOutEntityStorage, error) {
		return entitystore.ReadNestedRunEntityStatesTx(ctx, tx, run)
	})
}

func ReadNestedEntityFieldsRevisionForTest(ctx context.Context, selected any, run, path string) (NestedFanOutEntityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (NestedFanOutEntityStorage, error) {
		return entitystore.ReadNestedEntityFieldsRevisionTx(ctx, tx, run, path)
	})
}

func ReadPostgresScenarioSetupRunStorageForTest(ctx context.Context, selected any, runID string) (ScenarioSetupRunStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return ScenarioSetupRunStorage{}, err
	}
	var out ScenarioSetupRunStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadScenarioSetupRunStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return ScenarioSetupRunStorage{}, err
	}
	return out, nil
}

func ReadPostgresScenarioSetupEntityStorageForTest(ctx context.Context, selected any, runID, entityID string) (PostgresScenarioSetupEntityStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return PostgresScenarioSetupEntityStorage{}, err
	}
	var out PostgresScenarioSetupEntityStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = entitystore.ReadPostgresScenarioSetupEntityStorageTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return PostgresScenarioSetupEntityStorage{}, err
	}
	return out, nil
}

func CountPostgresScenarioSetupMutationsForTest(ctx context.Context, selected any, runID, entityID string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = mutationlog.CountScenarioSetupMutationsTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadPostgresScenarioSetupFieldsStorageForTest(ctx context.Context, selected any, runID, entityID string) ([]byte, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return nil, err
	}
	var out []byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = entitystore.ReadScenarioSetupFieldsStorageTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func CountPhysicalEntitiesForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = entitystore.CountPhysicalEntitiesTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func RemoveScenarioSetupReplayReceiptsForTest(ctx context.Context, selected any) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return apiidempotency.RemoveScenarioSetupReplayReceiptsTx(ctx, tx)
		})
	})
}
func ReadFanOutPortfolioFieldsStorageForTest(ctx context.Context, selected any, run string) ([]byte, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return entitystore.ReadFanOutPortfolioFieldsStorageTx(ctx, tx, run)
	})
}

func ReadFanOutAccountIDsMutationSourceStorageForTest(ctx context.Context, selected any, run, mutation, entity string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run, mutation, entity}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return mutationlog.ReadFanOutAccountIDsMutationSourceStorageTx(ctx, tx, run, mutation, entity)
	})
}
