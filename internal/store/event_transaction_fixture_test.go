package store

import (
	"context"
	"database/sql"
	"fmt"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

type eventFixtureStoryKey struct{}

// runEventTransaction is a test-only fixture boundary for legacy semantic
// fixtures that need to assemble several private facts atomically. Production
// code has no generic event transaction operation.
func (s *SQLiteRuntimeStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runPrivateAuthorActivityMutation(ctx, "sqlite event fixture transaction", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		eventCtx := context.WithValue(txctx, eventFixtureStoryKey{}, runtimeauthoractivity.Mutation(story))
		return fn(eventCtx, tx)
	})
}

func (s *PostgresStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		eventCtx := context.WithValue(txctx, eventFixtureStoryKey{}, runtimeauthoractivity.Mutation(story))
		return fn(eventCtx, tx)
	})
}

func eventFixtureStory(ctx context.Context) (runtimeauthoractivity.Mutation, error) {
	story, ok := ctx.Value(eventFixtureStoryKey{}).(runtimeauthoractivity.Mutation)
	if !ok || story == nil {
		return nil, fmt.Errorf("semantic event fixture transaction owner is required")
	}
	return story, nil
}
