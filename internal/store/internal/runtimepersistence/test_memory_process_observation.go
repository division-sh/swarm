package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
)

type MemoryEnabledSessionStorage = llmpersistence.MemoryEnabledSessionStorage

func ReadMemoryEnabledSessionsStorageForTest(ctx context.Context, selected any) ([]MemoryEnabledSessionStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]MemoryEnabledSessionStorage, error) {
		return llmpersistence.ReadMemoryEnabledSessionsStorageTx(ctx, tx, postgres)
	})
}

func CountSucceededTelegramModeAttemptsStorageForTest(ctx context.Context, selected any, executionMode string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return activityjournal.CountSucceededTelegramModeAttemptsStorageTx(ctx, tx, postgres, executionMode)
	})
}

func CountTelegramToolAttemptsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return activityjournal.CountTelegramToolAttemptsStorageTx(ctx, tx)
	})
}

func CountUnfinishedMemoryAgentDeliveriesStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountUnfinishedMemoryAgentDeliveriesStorageTx(ctx, tx)
	})
}
