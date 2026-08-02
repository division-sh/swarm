package store

import (
	"context"
	"database/sql"
	"fmt"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/authoractivityadapter"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

func (s *PostgresStore) runPrivateAuthorActivityMutation(
	ctx context.Context,
	fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error,
) error {
	if fn == nil {
		return nil
	}
	return s.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := fn(txctx, tx, story); err != nil {
			return err
		}
		if err := runtimepipeline.CapturePipelineRunForkRevisionChanges(txctx, tx); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *SQLiteRuntimeStore) runPrivateAuthorActivityMutation(
	ctx context.Context,
	label string,
	fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error,
) error {
	if fn == nil {
		return nil
	}
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := fn(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *PostgresStore) runAuthorActivityMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		if runtimeauthoractivity.InMutation(ctx, tx) {
			eventCtx, attached := eventCommitterForPipelineContext(ctx, s, nil)
			if !attached {
				return fmt.Errorf("%s could not attach the event commit owner", label)
			}
			return fn(eventCtx, tx)
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
			eventCtx, attached := eventCommitterForPipelineContext(ctx, s, nil)
			if !attached {
				return fmt.Errorf("%s could not attach the event commit owner", label)
			}
			return fn(eventCtx, tx)
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
	if s == nil || s.backend.db == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("postgres store is required")
	}
	return authoractivityadapter.List(ctx, s.backend.db, authoractivityadapter.DialectPostgres, opts)
}

func (s *SQLiteRuntimeStore) ListAuthorActivity(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	if s == nil || s.backend.db == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("sqlite runtime store is required")
	}
	return authoractivityadapter.List(ctx, s.backend.db, authoractivityadapter.DialectSQLite, opts)
}

func (s *PostgresStore) HeadAuthorActivity(ctx context.Context) (int64, error) {
	if s == nil || s.backend.db == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	return authoractivityadapter.Head(ctx, s.backend.db)
}

func (s *SQLiteRuntimeStore) HeadAuthorActivity(ctx context.Context) (int64, error) {
	if s == nil || s.backend.db == nil {
		return 0, fmt.Errorf("sqlite runtime store is required")
	}
	return authoractivityadapter.Head(ctx, s.backend.db)
}
