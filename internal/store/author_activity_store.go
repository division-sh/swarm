package store

import (
	"context"
	"database/sql"
	"fmt"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (s *PostgresStore) runAuthorActivityMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		if runtimeauthoractivity.InMutation(ctx, tx) {
			return fn(ctx, tx)
		}
		if !runtimeauthoractivity.FinalizedMutation(ctx, tx) {
			return fmt.Errorf("%s entered from a raw transaction without author activity ownership", label)
		}
		ctx = runtimepipeline.WithoutPipelineSQLTxContext(ctx)
	}
	return s.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		storyctx, err := runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := fn(storyctx, tx); err != nil {
			return err
		}
		if err := runtimepipeline.CapturePipelineRunForkRevisionChanges(storyctx, tx); err != nil {
			return err
		}
		return runtimeauthoractivity.Finalize(storyctx)
	})
}

func (s *SQLiteRuntimeStore) runAuthorActivityMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		if runtimeauthoractivity.InMutation(ctx, tx) {
			return fn(ctx, tx)
		}
		if !runtimeauthoractivity.FinalizedMutation(ctx, tx) {
			return fmt.Errorf("%s entered from a raw transaction without author activity ownership", label)
		}
		ctx = runtimepipeline.WithoutPipelineSQLTxContext(ctx)
	}
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		storyctx, err := runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := fn(storyctx, tx); err != nil {
			return err
		}
		return runtimeauthoractivity.Finalize(storyctx)
	})
}

func (s *PostgresStore) ListAuthorActivity(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	if s == nil || s.DB == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("postgres store is required")
	}
	return runtimeauthoractivity.List(ctx, s.DB, runtimeauthoractivity.DialectPostgres, opts)
}

func (s *SQLiteRuntimeStore) ListAuthorActivity(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	if s == nil || s.DB == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("sqlite runtime store is required")
	}
	return runtimeauthoractivity.List(ctx, s.DB, runtimeauthoractivity.DialectSQLite, opts)
}
