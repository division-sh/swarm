package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/bus"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedReplyReturnPublicationCounts = private.SelectedReplyReturnPublicationCounts
type SelectedReplyReturnPublicationRollbackProof = private.SelectedReplyReturnPublicationRollbackProof

func ReadSelectedReplyReturnPublicationCounts(ctx context.Context, selected any, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationCounts, error) {
	return private.ReadSelectedReplyReturnPublicationCountsForTest(ctx, selected, request)
}

func ProveSelectedReplyReturnPublicationRollback(ctx context.Context, selected any, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationRollbackProof, error) {
	return private.ProveSelectedReplyReturnPublicationRollbackForTest(ctx, selected, request)
}
