package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresSessionTerminationStorage = private.PostgresSessionTerminationStorage

func ReadPostgresRecoveryFailedPayloadStorage(ctx context.Context, selected any) ([]byte, error) {
	return private.ReadPostgresRecoveryFailedPayloadStorageForTest(ctx, selected)
}

func ReadPostgresResetPayloadStorage(ctx context.Context, selected any) ([]byte, error) {
	return private.ReadPostgresResetPayloadStorageForTest(ctx, selected)
}

func ReadPostgresSessionTerminationStorage(ctx context.Context, selected any, sessionID string) (PostgresSessionTerminationStorage, error) {
	return private.ReadPostgresSessionTerminationStorageForTest(ctx, selected, sessionID)
}
