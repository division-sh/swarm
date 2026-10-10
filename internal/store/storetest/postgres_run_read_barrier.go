package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresRunTableReadBarrier = private.PostgresRunTableReadBarrier

func HoldPostgresRunTableReadBarrier(ctx context.Context, selected any) (*PostgresRunTableReadBarrier, error) {
	return private.HoldPostgresRunTableReadBarrierForTest(ctx, selected)
}

func ReadPostgresRunOriginLockCount(ctx context.Context, selected any) (int, error) {
	return private.ReadPostgresRunOriginLockCountForTest(ctx, selected)
}

func ReadPostgresDatabaseLockCount(ctx context.Context, selected any) (int, error) {
	return private.ReadPostgresDatabaseLockCountForTest(ctx, selected)
}
