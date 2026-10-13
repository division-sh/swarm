package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/bus"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedFanOutPublicationCounts = private.SelectedFanOutPublicationCounts
type SelectedFanOutPublicationRollbackProof = private.SelectedFanOutPublicationRollbackProof

func ProveSelectedFanOutPublicationRollback(ctx context.Context, selected any, request bus.CommitSelectedForkEventRequest) (SelectedFanOutPublicationRollbackProof, error) {
	return private.ProveSelectedFanOutPublicationRollbackForTest(ctx, selected, request)
}
