package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
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
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return nil, err
	}
	postgres := dialect == authoractivityfixture.DialectPostgres
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InstallNodeRecoveryPoisonMutationTx(ctx, tx, postgres, runID, entityID)
	}); err != nil {
		return nil, err
	}
	var once sync.Once
	var cleanupErr error
	return func(ctx context.Context) error {
		// A repeated cleanup cannot remove a later installation in this store.
		once.Do(func() {
			cleanupErr = runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
				return pipelinepersistence.RemoveNodeRecoveryPoisonMutationTx(ctx, tx, postgres)
			})
		})
		return cleanupErr
	}, nil
}
