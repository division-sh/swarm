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

type TemplateAccountIdentityStorage = entitystore.TemplateAccountIdentityStorage

type TemplateAccountDiagnosticStorage = entitystore.TemplateAccountDiagnosticStorage

type GateDuplicateMutationDiagnosticStorage = mutationlog.GateDuplicateMutationDiagnosticStorage

func ReadGateDuplicateMutationDiagnosticsForTest(ctx context.Context, selected any, eventID string) ([]GateDuplicateMutationDiagnosticStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) ([]GateDuplicateMutationDiagnosticStorage, error) {
		return mutationlog.ReadGateDuplicateMutationDiagnosticsTx(ctx, tx, eventID)
	})
}

func ReadGateRunRootFieldsStorageForTest(ctx context.Context, selected any, runID string) ([]byte, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return entitystore.ReadScenarioSetupFieldsStorageTx(ctx, tx, runID, runID)
	})
}

func CountRunMutationStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountRunMutationStorageTx(ctx, tx, runID)
	})
}

func CountRunEventMutationStorageForTest(ctx context.Context, selected any, runID, eventID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountRunEventMutationStorageTx(ctx, tx, runID, eventID)
	})
}

func ReadTemplateAccountDiagnosticsForTest(ctx context.Context, selected any) ([]TemplateAccountDiagnosticStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, entitystore.ReadTemplateAccountDiagnosticsTx)
}

func CountTemplateAccountEntitiesForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, entitystore.CountTemplateAccountEntitiesTx)
}
func CountPostgresReviewedTemplateAccountForTest(ctx context.Context, selected any, fields string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return entitystore.CountPostgresReviewedTemplateAccountTx(ctx, tx, fields)
	})
}
func ReadLatestTemplateAccountIdentityForTest(ctx context.Context, selected any) (TemplateAccountIdentityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, entitystore.ReadLatestTemplateAccountIdentityTx)
}

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
