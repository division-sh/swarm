package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CheckConversationStorageColumns(ctx context.Context, selected any) error {
	return private.CheckConversationStorageColumnsForTest(ctx, selected)
}

func CheckRuntimeLogStorageColumns(ctx context.Context, selected any) error {
	return private.CheckRuntimeLogStorageColumnsForTest(ctx, selected)
}

func CheckMutationStorageColumns(ctx context.Context, selected any) error {
	return private.CheckMutationStorageColumnsForTest(ctx, selected)
}

func CheckDeliveryLifecycleStorageColumns(ctx context.Context, selected any) error {
	return private.CheckDeliveryLifecycleStorageColumnsForTest(ctx, selected)
}
