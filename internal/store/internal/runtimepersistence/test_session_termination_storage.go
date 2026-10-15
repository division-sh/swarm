package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
)

type PostgresSessionTerminationStorage = llmpersistence.PostgresSessionTerminationStorage

func ReadPostgresSessionTerminationStorageForTest(ctx context.Context, selected any, sessionID string) (PostgresSessionTerminationStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, sessionID); err != nil {
		return PostgresSessionTerminationStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresSessionTerminationStorage, error) {
		return llmpersistence.ReadPostgresSessionTerminationStorageTx(ctx, tx, sessionID)
	})
}
