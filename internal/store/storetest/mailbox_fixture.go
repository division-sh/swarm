package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CorruptHumanTaskRequester(ctx context.Context, selected any, runID, cardID, coordinate, hostile string) (func(context.Context) error, error) {
	return private.CorruptHumanTaskRequesterForTest(ctx, selected, runID, cardID, coordinate, hostile)
}

func SwapMailboxRunSource(ctx context.Context, selected any, runID, from, to string) error {
	return private.SwapMailboxRunSourceForTest(ctx, selected, runID, from, to)
}

func SetMailboxNoticeTime(ctx context.Context, selected any, itemID string, at time.Time) error {
	return private.SetMailboxNoticeTimeForTest(ctx, selected, itemID, at)
}
