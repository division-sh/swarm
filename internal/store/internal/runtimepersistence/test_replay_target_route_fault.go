package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func CorruptReplayEventTargetRouteForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return eventrecord.CorruptReplayEventTargetRouteTx(ctx, tx, postgres, eventID)
		})
	})
}

func RestoreReplayEventTargetRouteForTest(ctx context.Context, selected any, original events.Event) error {
	if err := validateStandaloneStorageEvent(selected, original.ID()); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return eventrecord.RestoreReplayEventTargetRouteTx(ctx, tx, postgres, original)
		})
	})
}
