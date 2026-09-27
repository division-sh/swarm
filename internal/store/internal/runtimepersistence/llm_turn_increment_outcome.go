package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func (s *PostgresStore) IncrementTurnOutcome(ctx context.Context, lease *sessions.Lease) (sessions.TurnIncrementResult, error) {
	return s.lLMPostgresOwner.IncrementTurnOutcome(ctx, lease)
}

func (s *SQLiteRuntimeStore) IncrementTurnOutcome(ctx context.Context, lease *sessions.Lease) (sessions.TurnIncrementResult, error) {
	return s.lLMSQLiteOwner.IncrementTurnOutcome(ctx, lease)
}
