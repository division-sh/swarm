package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ConversationForkCompletionExpiryStorage = private.ConversationForkCompletionExpiryStorage

func CountConversationForkStorage(ctx context.Context, selected any) (int, error) {
	return private.CountConversationForkStorageForTest(ctx, selected)
}

func CountConversationForkCompletionStorage(ctx context.Context, selected any) (int, error) {
	return private.CountConversationForkCompletionStorageForTest(ctx, selected)
}

func ReadConversationForkCompletionExpiryStorage(ctx context.Context, selected any) (ConversationForkCompletionExpiryStorage, error) {
	return private.ReadConversationForkCompletionExpiryStorageForTest(ctx, selected)
}

func InstallConversationForkCompletionInsertFailure(ctx context.Context, selected any) error {
	return private.InstallConversationForkCompletionInsertFailureForTest(ctx, selected)
}

func RemoveConversationForkCompletionInsertFailure(ctx context.Context, selected any) error {
	return private.RemoveConversationForkCompletionInsertFailureForTest(ctx, selected)
}
