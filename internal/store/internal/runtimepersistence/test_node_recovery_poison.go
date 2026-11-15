package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type NodeRecoveryPoisonCleanup = pipelinepersistence.NodeRecoveryPoisonCleanup

func InstallNodeRecoveryPoisonMutationForTest(ctx context.Context, selected any, runID, entityID string) (*NodeRecoveryPoisonCleanup, error) {
	for _, id := range []string{runID, entityID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return nil, err
		}
	}
	if correlation.RunIDFromContext(ctx) != runID {
		return nil, fmt.Errorf("node recovery poison fault requires its exact run context")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.InstallNodeRecoveryPoisonMutationForTest(ctx, runID, entityID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.InstallNodeRecoveryPoisonMutationForTest(ctx, runID, entityID)
	default:
		return nil, fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
