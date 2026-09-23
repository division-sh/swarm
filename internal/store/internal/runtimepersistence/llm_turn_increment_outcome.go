package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func (s *PostgresStore) IncrementTurnOutcome(ctx context.Context, identity agentmemory.Identity, sessionID string) (sessions.TurnIncrementResult, error) {
	return s.lLMPostgresOwner.IncrementTurnOutcome(ctx, identity, sessionID)
}

func (s *SQLiteRuntimeStore) IncrementTurnOutcome(ctx context.Context, identity agentmemory.Identity, sessionID string) (sessions.TurnIncrementResult, error) {
	return s.lLMSQLiteOwner.IncrementTurnOutcome(ctx, identity, sessionID)
}
