package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type PublicationRunStorage = runlifecycle.PublicationRunStorage
type PublicationRunTriggerStorage = runlifecycle.PublicationRunTriggerStorage

func validatePostgresPublicationIdentities(selected any, identities ...string) error {
	if _, ok := selected.(*PostgresStore); !ok {
		return fmt.Errorf("physical publication fixture requires selected PostgreSQL storage")
	}
	for _, id := range identities {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return err
		}
	}
	return nil
}

func SeedPostgresPublicationIdentityLookalikesForTest(ctx context.Context, selected any, runID, entityID string) error {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return pipelinepersistence.SeedPostgresPublicationIdentityLookalikesTx(ctx, tx, runID, entityID)
		})
	})
}

func SeedPostgresPublicationUnroutableEntityForTest(ctx context.Context, selected any, runID, entityID, flowInstance string) error {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return pipelinepersistence.SeedPostgresPublicationUnroutableEntityTx(ctx, tx, runID, entityID, flowInstance)
		})
	})
}

func ReadPublicationRunTriggerStorageForTest(ctx context.Context, selected any, runID string) (PublicationRunTriggerStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return PublicationRunTriggerStorage{}, err
	}
	var out PublicationRunTriggerStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadPublicationRunTriggerStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return PublicationRunTriggerStorage{}, err
	}
	return out, nil
}

func ReadCreatedPublicationRunStorageForTest(ctx context.Context, selected any, runID string) (PublicationRunStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return PublicationRunStorage{}, err
	}
	var out PublicationRunStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadCreatedPublicationRunStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return PublicationRunStorage{}, err
	}
	return out, nil
}

func ReadExistingPublicationRunStorageForTest(ctx context.Context, selected any, runID string) (PublicationRunStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return PublicationRunStorage{}, err
	}
	var out PublicationRunStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runlifecycle.ReadExistingPublicationRunStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return PublicationRunStorage{}, err
	}
	return out, nil
}

func ReadPublicationDeliveryTargetEncodingsForTest(ctx context.Context, selected any, eventID string) ([]string, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = delivery.ReadPublicationDeliveryTargetEncodingsTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
