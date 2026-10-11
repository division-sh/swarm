package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type PublicationRunStorage = runlifecycle.PublicationRunStorage
type PublicationRunTriggerStorage = runlifecycle.PublicationRunTriggerStorage
type PublicationEventProjection = eventrecord.PublicationEventProjection
type PublicationEventIdentityStorage = eventrecord.PublicationEventIdentityStorage
type OperatorReferenceStorage = eventrecord.OperatorReferenceStorage
type RootTemplatePublicationStorage = eventrecord.RootTemplatePublicationStorage

func ReadPostgresRootTemplatePublicationForTest(ctx context.Context, selected any, eventID string) (RootTemplatePublicationStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return RootTemplatePublicationStorage{}, err
	}
	var out RootTemplatePublicationStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPostgresRootTemplatePublicationTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return RootTemplatePublicationStorage{}, err
	}
	return out, nil
}

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
			return entitystore.SeedPostgresPublicationIdentityLookalikesTx(ctx, tx, runID, entityID)
		})
	})
}

func SeedPostgresPublicationUnroutableEntityForTest(ctx context.Context, selected any, runID, entityID, flowInstance string) error {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return err
	}
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return entitystore.SeedPostgresPublicationUnroutableEntityTx(ctx, tx, runID, entityID, flowInstance)
		})
	})
}

type RunStartStorage struct {
	Header  runlifecycle.RunStartHeaderStorage
	Event   eventrecord.PublicationEventProjection
	EventID string
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

func ReadPostgresRunStartStorageForTest(ctx context.Context, selected any, runID, eventName string) (RunStartStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return RunStartStorage{}, err
	}
	if _, ok := selected.(*PostgresStore); !ok {
		return RunStartStorage{}, fmt.Errorf("run.start physical projection requires selected PostgreSQL storage")
	}
	var out RunStartStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Header, err = runlifecycle.ReadRunStartHeaderStorageTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		out.Event, err = eventrecord.ReadRunStartEventProjectionTx(ctx, tx, runID, eventName)
		if err != nil {
			return err
		}
		out.EventID, err = eventrecord.ReadRunStartEventIdentityTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return RunStartStorage{}, err
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

func ReadPublicationEventProjectionForTest(ctx context.Context, selected any, eventID string) (PublicationEventProjection, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return PublicationEventProjection{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out PublicationEventProjection
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPublicationEventProjectionTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return PublicationEventProjection{}, err
	}
	return out, nil
}

func ReadPublicationEventIdentityStorageForTest(ctx context.Context, selected any, eventID string) (PublicationEventIdentityStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return PublicationEventIdentityStorage{}, err
	}
	var out PublicationEventIdentityStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadPublicationEventIdentityStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return PublicationEventIdentityStorage{}, err
	}
	return out, nil
}

func ReadOperatorReferenceStorageForTest(ctx context.Context, selected any, eventID string) (OperatorReferenceStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return OperatorReferenceStorage{}, err
	}
	var out OperatorReferenceStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadOperatorReferenceStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return OperatorReferenceStorage{}, err
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
