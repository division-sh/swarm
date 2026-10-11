package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReplyContextStorageCounts = private.ReplyContextStorageCounts
type FirstReplyContextStorage = private.FirstReplyContextStorage

func ReadReplyContextRequestIDs(ctx context.Context, selected any, run string) ([]string, error) {
	return private.ReadReplyContextRequestIDsForTest(ctx, selected, run)
}

func ReadFirstReplyContextStorage(ctx context.Context, selected any, run string) (FirstReplyContextStorage, error) {
	return private.ReadFirstReplyContextStorageForTest(ctx, selected, run)
}

func ReadReplyContextStorageCounts(ctx context.Context, selected any, run string) (ReplyContextStorageCounts, error) {
	return private.ReadReplyContextStorageCountsForTest(ctx, selected, run)
}
