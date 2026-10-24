package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (s *PostgresStore) ReadBundleUpgradeState(ctx context.Context, runID string, source semanticview.Source) (runbundle.UpgradeState, error) {
	return s.pipelinePostgresOwner.ReadBundleUpgradeState(ctx, runID, source)
}

func (s *SQLiteRuntimeStore) ReadBundleUpgradeState(ctx context.Context, runID string, source semanticview.Source) (runbundle.UpgradeState, error) {
	return s.pipelineSQLiteOwner.ReadBundleUpgradeState(ctx, runID, source)
}
