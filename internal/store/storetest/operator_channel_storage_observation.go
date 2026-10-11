package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountOperatorChannelActionIntentStorage(ctx context.Context, selected any) (int, error) {
	return runtimepersistence.CountOperatorChannelActionIntentStorageForTest(ctx, selected)
}

func ReadOperatorChannelTextIntentFactStorage(ctx context.Context, selected any, providerEventID string) ([]byte, error) {
	return runtimepersistence.ReadOperatorChannelTextIntentFactStorageForTest(ctx, selected, providerEventID)
}
