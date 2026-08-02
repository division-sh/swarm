package store

import (
	"context"
	"database/sql"

	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

// runEventTransaction is a test-only fixture boundary for legacy semantic
// fixtures that need to assemble several private facts atomically. Production
// code has no generic event transaction operation.
func (s *SQLiteRuntimeStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runPrivateAuthorActivityMutation(ctx, "sqlite event fixture transaction", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		eventCtx, attached := eventCommitterForPipelineContext(txctx, s, story)
		if !attached {
			return sql.ErrTxDone
		}
		return fn(eventCtx, tx)
	})
}

func (s *PostgresStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		eventCtx, attached := eventCommitterForPipelineContext(txctx, s, story)
		if !attached {
			return sql.ErrTxDone
		}
		return fn(eventCtx, tx)
	})
}
