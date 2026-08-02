package store

import (
	"context"
	"database/sql"
	"fmt"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
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

func eventFixtureStory(ctx context.Context) (runtimeauthoractivity.Mutation, error) {
	transaction, ok := runtimebus.CommitPublishTransactionFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("semantic event fixture transaction owner is required")
	}
	committer, ok := transaction.(*sqlPublishCommitter)
	if !ok || committer.story == nil {
		return nil, fmt.Errorf("semantic event fixture private story is required")
	}
	return committer.story, nil
}
