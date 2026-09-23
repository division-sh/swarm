package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func (s *PostgresStore) ReleaseOutcome(ctx context.Context, lease *sessions.Lease) (sessions.ReleaseResult, error) {
	return s.lLMPostgresOwner.ReleaseOutcome(ctx, lease)
}

func (s *SQLiteRuntimeStore) ReleaseOutcome(ctx context.Context, lease *sessions.Lease) (sessions.ReleaseResult, error) {
	return s.lLMSQLiteOwner.ReleaseOutcome(ctx, lease)
}
