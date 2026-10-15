package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type MemoryEnabledSessionStorage = private.MemoryEnabledSessionStorage

func ReadMemoryEnabledSessionsStorage(ctx context.Context, selected any) ([]MemoryEnabledSessionStorage, error) {
	return private.ReadMemoryEnabledSessionsStorageForTest(ctx, selected)
}

func CountSucceededTelegramModeAttemptsStorage(ctx context.Context, selected any, executionMode string) (int, error) {
	return private.CountSucceededTelegramModeAttemptsStorageForTest(ctx, selected, executionMode)
}

func CountTelegramToolAttemptsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountTelegramToolAttemptsStorageForTest(ctx, selected)
}

func CountUnfinishedMemoryAgentDeliveriesStorage(ctx context.Context, selected any) (int, error) {
	return private.CountUnfinishedMemoryAgentDeliveriesStorageForTest(ctx, selected)
}

func CountTelegramRequestedReplyStorage(ctx context.Context, selected any) (int, error) {
	return private.CountTelegramRequestedReplyStorageForTest(ctx, selected)
}
