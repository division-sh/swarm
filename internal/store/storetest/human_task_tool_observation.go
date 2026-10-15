package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountHumanTaskToolCardStorage(ctx context.Context, selected any, cardID string) (int, error) {
	return private.CountHumanTaskToolCardStorageForTest(ctx, selected, cardID)
}

func CountHumanTaskToolContinuationStorage(ctx context.Context, selected any, cardID string) (int, error) {
	return private.CountHumanTaskToolContinuationStorageForTest(ctx, selected, cardID)
}

func CountAllHumanTaskToolCardsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountAllHumanTaskToolCardsStorageForTest(ctx, selected)
}
