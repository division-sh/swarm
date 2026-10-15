package runtimepersistence

import (
	"context"
	"database/sql"

	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
)

func CountRunEntityStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return entitystore.CountRunEntityStorageTx(ctx, tx, runID)
	})
}

type EntityToolMutationAuthorityStorage = mutationlog.EntityToolMutationAuthorityStorage
type EntityToolStatusRevisionStorage = entitystore.EntityToolStatusRevisionStorage

func ReadEntityToolStatusForTest(ctx context.Context, selected any, runID, entityID string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return entitystore.ReadEntityToolStatusTx(ctx, tx, runID, entityID)
	})
}

func ReadEntityToolStatusRevisionForTest(ctx context.Context, selected any, runID, entityID string) (EntityToolStatusRevisionStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return EntityToolStatusRevisionStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) (EntityToolStatusRevisionStorage, error) {
		return entitystore.ReadEntityToolStatusRevisionTx(ctx, tx, runID, entityID)
	})
}

func CountEntityToolAgentStatusMutationsForTest(ctx context.Context, selected any, runID, entityID string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountEntityToolAgentStatusMutationsTx(ctx, tx, runID, entityID)
	})
}

func ReadEntityToolFieldWireForTest(ctx context.Context, selected any, runID, entityID, field string) ([]byte, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return nil, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID, entityID}, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return entitystore.ReadEntityToolFieldWireTx(ctx, tx, runID, entityID, field)
	})
}

func CountEntityToolAuthoredPathMutationsForTest(ctx context.Context, selected any, entityID, path string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, entityID); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{entityID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mutationlog.CountEntityToolAuthoredPathMutationsTx(ctx, tx, entityID, path)
	})
}

func ReadEntityToolInitialMutationAuthoritiesForTest(ctx context.Context, selected any, entityID string) ([]EntityToolMutationAuthorityStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, entityID); err != nil {
		return nil, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{entityID}, func(ctx context.Context, tx *sql.Tx) ([]EntityToolMutationAuthorityStorage, error) {
		return mutationlog.ReadEntityToolInitialMutationAuthoritiesTx(ctx, tx, entityID)
	})
}
