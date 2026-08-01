package store

import (
	"context"
	"database/sql"
	"errors"
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

func (s *SQLiteRuntimeStore) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	return s.runAuthorActivityMutation(ctx, "sqlite pipeline mutation", func(txctx context.Context, _ *sql.Tx) error {
		return fn(txctx)
	})
}

func (s *PostgresStore) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	return s.runAuthorActivityMutation(ctx, "postgres pipeline mutation", func(txctx context.Context, _ *sql.Tx) error {
		return fn(txctx)
	})
}

func (s *SQLiteRuntimeStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runAuthorActivityMutation(ctx, "sqlite event transaction", fn)
}

func (s *PostgresStore) runEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.runAuthorActivityMutation(ctx, "postgres event transaction", fn)
}

func (s *PostgresStore) runPostgresRuntimeMutation(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	if fn == nil {
		return nil
	}
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session, borrowed, err := postgresSessionAuthorityFromContext(ctx)
	if err != nil {
		return err
	}
	if !borrowed {
		conn, connErr := s.DB.Conn(ctx)
		if connErr != nil {
			return connErr
		}
		session = newPostgresSessionAuthority(conn)
		defer func() {
			err = errors.Join(err, session.release())
		}()
	}
	ctx, err = session.bindContext(ctx)
	if err != nil {
		return err
	}
	tx, err := session.beginTx(ctx)
	if err != nil {
		return err
	}
	postCommit := make([]runtimepipeline.OwnerAction, 0, 4)
	rollbackActions := make([]runtimepipeline.OwnerAction, 0, 4)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if eventCtx, ok := eventCommitterForPipelineContext(txctx, s); ok {
		txctx = eventCtx
	} else {
		rollbackErr := rollbackPostgresSessionTransaction(tx, session)
		return errors.Join(
			fmt.Errorf("postgres runtime mutation could not attach the event commit owner"),
			rollbackErr,
		)
	}
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommit)
	txctx = runtimepipeline.WithPipelineRollbackActions(txctx, &rollbackActions)
	if runErr := fn(txctx, tx); runErr != nil {
		rollbackErr := rollbackPostgresSessionTransaction(tx, session)
		runtimepipeline.FlushPipelineRollbackActions(rollbackActions)
		return errors.Join(runErr, rollbackErr)
	}
	if commitErr := tx.Commit(); commitErr != nil {
		rollbackErr := rollbackPostgresSessionTransaction(tx, session)
		runtimepipeline.FlushPipelineRollbackActions(rollbackActions)
		return errors.Join(commitErr, rollbackErr)
	}
	endErr := session.endTx(tx)
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return endErr
}

func rollbackPostgresSessionTransaction(tx *sql.Tx, session *postgresSessionAuthority) error {
	if tx == nil || session == nil {
		return errors.New("PostgreSQL session transaction is missing")
	}
	rollbackErr := tx.Rollback()
	if errors.Is(rollbackErr, sql.ErrTxDone) {
		rollbackErr = nil
	}
	return errors.Join(rollbackErr, session.endTx(tx))
}

func (s *SQLiteRuntimeStore) runRuntimeMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	postCommit, err := s.runRuntimeMutationCommitted(ctx, label, fn)
	if err != nil {
		return err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return nil
}

func (s *SQLiteRuntimeStore) runRuntimeMutationCommitted(
	ctx context.Context,
	label string,
	fn func(context.Context, *sql.Tx) error,
) ([]runtimepipeline.OwnerAction, error) {
	if fn == nil {
		return nil, nil
	}
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		eventCtx, attached := eventCommitterForPipelineContext(ctx, s)
		if !attached {
			return nil, fmt.Errorf("%s could not attach the event commit owner", label)
		}
		return nil, fn(eventCtx, tx)
	}
	retryDeadline := time.Now().Add(sqliteRuntimeMutationRetryBudget)
	ctxDeadline, hasCtxDeadline := ctx.Deadline()
	if hasCtxDeadline && ctxDeadline.Before(retryDeadline) {
		retryDeadline = ctxDeadline
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if time.Until(retryDeadline) <= 0 {
			if hasCtxDeadline && !time.Now().Before(ctxDeadline) {
				return nil, context.DeadlineExceeded
			}
			return nil, sqliteRuntimeMutationRetryBudgetError(label, lastErr)
		}
		attemptCtx, cancel := context.WithDeadline(ctx, retryDeadline)
		postCommit, err := s.runRuntimeMutationOnceLocked(attemptCtx, fn)
		attemptErr := attemptCtx.Err()
		cancel()
		if err == nil {
			if attemptErr != nil {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if hasCtxDeadline && errors.Is(attemptErr, context.DeadlineExceeded) && !time.Now().Before(ctxDeadline) {
					return nil, context.DeadlineExceeded
				}
				return nil, sqliteRuntimeMutationRetryBudgetError(label, lastErr)
			}
			return postCommit, nil
		}
		if attemptErr != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if hasCtxDeadline && errors.Is(attemptErr, context.DeadlineExceeded) && !time.Now().Before(ctxDeadline) {
				return nil, context.DeadlineExceeded
			}
			return nil, sqliteRuntimeMutationRetryBudgetError(label, lastErr)
		}
		if !sqliteRuntimeMutationBusyError(err) {
			return nil, err
		}
		lastErr = err
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		delay := sqliteRuntimeMutationRetryDelay(attempt)
		if remaining := time.Until(retryDeadline); remaining <= 0 {
			if hasCtxDeadline && !time.Now().Before(ctxDeadline) {
				return nil, context.DeadlineExceeded
			}
			return nil, sqliteRuntimeMutationRetryBudgetError(label, lastErr)
		} else if delay > remaining {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *SQLiteRuntimeStore) runRuntimeMutationOnceLocked(ctx context.Context, fn func(context.Context, *sql.Tx) error) ([]runtimepipeline.OwnerAction, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	postCommit := make([]runtimepipeline.OwnerAction, 0, 4)
	rollbackActions := make([]runtimepipeline.OwnerAction, 0, 4)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if eventCtx, ok := eventCommitterForPipelineContext(txctx, s); ok {
		txctx = eventCtx
	} else {
		return nil, errors.Join(
			fmt.Errorf("sqlite runtime mutation could not attach the event commit owner"),
			rollbackSQLTransaction(tx),
		)
	}
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommit)
	txctx = runtimepipeline.WithPipelineRollbackActions(txctx, &rollbackActions)
	if err := fn(txctx, tx); err != nil {
		rollbackErr := rollbackSQLTransaction(tx)
		runtimepipeline.FlushPipelineRollbackActions(rollbackActions)
		return nil, errors.Join(err, rollbackErr)
	}
	if err := tx.Commit(); err != nil {
		rollbackErr := rollbackSQLTransaction(tx)
		runtimepipeline.FlushPipelineRollbackActions(rollbackActions)
		return nil, errors.Join(err, rollbackErr)
	}
	return postCommit, nil
}

func rollbackSQLTransaction(tx *sql.Tx) error {
	if tx == nil {
		return errors.New("SQL transaction is missing")
	}
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
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
