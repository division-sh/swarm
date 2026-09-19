package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Legacy raw fixtures satisfy the expanded owner shape but cannot manufacture
// production group authority. Group tests bind the actual retained grant port.
func (s *PostgresStore) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	return s.pipelinePostgresOwner.BeginFanOutPublicationGroup(ctx, claim)
}

func (s *SQLiteRuntimeStore) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	return s.pipelineSQLiteOwner.BeginFanOutPublicationGroup(ctx, claim)
}
