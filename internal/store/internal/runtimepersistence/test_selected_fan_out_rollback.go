package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type SelectedFanOutPublicationCounts = pipelinepersistence.SelectedFanOutPublicationCounts
type SelectedFanOutPublicationRollbackProof = pipelinepersistence.SelectedFanOutPublicationRollbackProof

func ProveSelectedFanOutPublicationRollbackForTest(ctx context.Context, selected any, request bus.CommitSelectedForkEventRequest) (SelectedFanOutPublicationRollbackProof, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedFanOutPublicationRollbackProof{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ProveSelectedFanOutPublicationRollbackForTest(ctx, request)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ProveSelectedFanOutPublicationRollbackForTest(ctx, request)
	default:
		return SelectedFanOutPublicationRollbackProof{}, fmt.Errorf("rollback proof requires the original native publication owner")
	}
}
