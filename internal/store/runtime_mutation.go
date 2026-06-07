package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const (
	sqliteRuntimeMutationRetryBudget = 5 * time.Second
	sqliteRuntimeMutationBaseDelay   = 10 * time.Millisecond
	sqliteRuntimeMutationMaxDelay    = 100 * time.Millisecond
)

// RunRuntimeMutation is the canonical selected-store write boundary for the
// SQLite runtime backend. It owns process-local write serialization, bounded
// SQLITE_BUSY/database-locked retry, transaction context propagation, and
// post-commit action flushing for runtime mutation producers.
func (s *SQLiteRuntimeStore) RunRuntimeMutation(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runRuntimeMutation(ctx, "sqlite runtime mutation", fn)
}

func (s *SQLiteRuntimeStore) RunEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runRuntimeMutation(ctx, "sqlite event transaction", fn)
}

func (s *PostgresStore) RunEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	postCommit := make([]func(), 0, 4)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommit)
	if err := fn(txctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return nil
}

func (s *SQLiteRuntimeStore) runRuntimeMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return fn(ctx, tx)
	}
	retryDeadline := time.Now().Add(sqliteRuntimeMutationRetryBudget)
	ctxDeadline, hasCtxDeadline := ctx.Deadline()
	if hasCtxDeadline && ctxDeadline.Before(retryDeadline) {
		retryDeadline = ctxDeadline
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Until(retryDeadline) <= 0 {
			if hasCtxDeadline && !time.Now().Before(ctxDeadline) {
				return context.DeadlineExceeded
			}
			return sqliteRuntimeMutationRetryBudgetError(label, lastErr)
		}
		attemptCtx, cancel := context.WithDeadline(ctx, retryDeadline)
		err := s.runRuntimeMutationOnce(attemptCtx, fn)
		attemptErr := attemptCtx.Err()
		cancel()
		if err == nil {
			if attemptErr != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				return sqliteRuntimeMutationRetryBudgetError(label, lastErr)
			}
			return nil
		}
		if attemptErr != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			return sqliteRuntimeMutationRetryBudgetError(label, lastErr)
		}
		if !sqliteRuntimeMutationBusyError(err) {
			return err
		}
		lastErr = err
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := sqliteRuntimeMutationRetryDelay(attempt)
		if remaining := time.Until(retryDeadline); remaining <= 0 {
			if hasCtxDeadline && !time.Now().Before(ctxDeadline) {
				return context.DeadlineExceeded
			}
			return sqliteRuntimeMutationRetryBudgetError(label, lastErr)
		} else if delay > remaining {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *SQLiteRuntimeStore) runRuntimeMutationOnce(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	postCommit, err := s.runRuntimeMutationOnceLocked(ctx, fn)
	if err != nil {
		return err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return nil
}

func (s *SQLiteRuntimeStore) runRuntimeMutationOnceLocked(ctx context.Context, fn func(context.Context, *sql.Tx) error) ([]func(), error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	postCommit := make([]func(), 0, 4)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommit)
	if err := fn(txctx, tx); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return postCommit, nil
}

func sqliteRuntimeMutationRetryBudgetError(label string, lastErr error) error {
	label = strings.TrimSpace(label)
	if label == "" {
		label = "sqlite runtime mutation"
	}
	if lastErr == nil {
		return fmt.Errorf("%s retry budget %s exceeded: %w", label, sqliteRuntimeMutationRetryBudget, context.DeadlineExceeded)
	}
	return fmt.Errorf("%s retry budget %s exceeded: %w", label, sqliteRuntimeMutationRetryBudget, lastErr)
}

func sqliteRuntimeMutationRetryDelay(attempt int) time.Duration {
	delay := time.Duration(attempt+1) * sqliteRuntimeMutationBaseDelay
	if delay > sqliteRuntimeMutationMaxDelay {
		return sqliteRuntimeMutationMaxDelay
	}
	return delay
}

func sqliteRuntimeMutationBusyError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") ||
		strings.Contains(text, "sqlite_locked") ||
		strings.Contains(text, "database is locked") ||
		strings.Contains(text, "database table is locked") ||
		strings.Contains(text, "database is busy")
}
