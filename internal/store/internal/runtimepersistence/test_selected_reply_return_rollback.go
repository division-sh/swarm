package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type SelectedReplyReturnPublicationCounts = pipelinepersistence.SelectedReplyReturnPublicationCounts
type SelectedReplyReturnPublicationRollbackProof = pipelinepersistence.SelectedReplyReturnPublicationRollbackProof

func ReadSelectedReplyReturnPublicationCountsForTest(ctx context.Context, selected any, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationCounts, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedReplyReturnPublicationCounts{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ReadSelectedReplyReturnPublicationCountsForTest(ctx, request)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ReadSelectedReplyReturnPublicationCountsForTest(ctx, request)
	default:
		return SelectedReplyReturnPublicationCounts{}, fmt.Errorf("accepted-return counts require the original native publication owner")
	}
}

func ProveSelectedReplyReturnPublicationRollbackForTest(ctx context.Context, selected any, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationRollbackProof, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedReplyReturnPublicationRollbackProof{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ProveSelectedReplyReturnPublicationRollbackForTest(ctx, request)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ProveSelectedReplyReturnPublicationRollbackForTest(ctx, request)
	default:
		return SelectedReplyReturnPublicationRollbackProof{}, fmt.Errorf("accepted-return rollback requires the original native publication owner")
	}
}
