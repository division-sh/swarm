package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func SeedReplayDeliverySessionForTest(ctx context.Context, selected any, sessionID string, identity agentidentity.Identity) error {
	if _, err := identity.StorageFields(); err != nil {
		return err
	}
	if err := validateStandaloneStorageEvent(selected, sessionID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return llmpersistence.SeedReplayDeliverySessionTx(ctx, tx, postgres, sessionID, identity)
		})
	})
}
