package runtimepersistence

import (
	"context"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/correlation"
)

func InstallNodeRecoveryPoisonMutationForTest(ctx context.Context, selected any, runID, entityID string) (func(context.Context) error, error) {
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
	var install func(context.Context) error
	var remove func(context.Context) error
	switch store := selected.(type) {
	case *PostgresStore:
		install = func(ctx context.Context) error {
			return store.pipelinePostgresOwner.InstallNodeRecoveryPoisonMutationForTest(ctx, runID, entityID)
		}
		remove = store.pipelinePostgresOwner.RemoveNodeRecoveryPoisonMutationForTest
	case *SQLiteRuntimeStore:
		install = func(ctx context.Context) error {
			return store.pipelineSQLiteOwner.InstallNodeRecoveryPoisonMutationForTest(ctx, runID, entityID)
		}
		remove = store.pipelineSQLiteOwner.RemoveNodeRecoveryPoisonMutationForTest
	}
	if err := install(ctx); err != nil {
		return nil, err
	}
	var once sync.Once
	var cleanupErr error
	return func(ctx context.Context) error {
		// A repeated cleanup cannot remove a later installation in this store.
		once.Do(func() { cleanupErr = remove(ctx) })
		return cleanupErr
	}, nil
}
