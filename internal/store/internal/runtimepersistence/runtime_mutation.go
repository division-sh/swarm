package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	sqliteRuntimeMutationRetryBudget = 5 * time.Second
	sqliteRuntimeMutationBaseDelay   = 10 * time.Millisecond
	sqliteRuntimeMutationMaxDelay    = 100 * time.Millisecond
)

func (s *PostgresStore) runPostgresRuntimeMutation(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	if fn == nil {
		return nil
	}
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.backend.Conn(ctx)
	if err != nil {
		return err
	}
	session := newPostgresSessionAuthority(conn)
	defer func() {
		err = errors.Join(err, session.release())
	}()
	return runPostgresAuthorityTransaction(ctx, session, fn)
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
	if fn == nil {
		return nil
	}
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
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
		err := s.runRuntimeMutationOnceLocked(attemptCtx, fn)
		attemptErr := attemptCtx.Err()
		cancel()
		if err == nil {
			if attemptErr != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				if hasCtxDeadline && errors.Is(attemptErr, context.DeadlineExceeded) && !time.Now().Before(ctxDeadline) {
					return context.DeadlineExceeded
				}
				return sqliteRuntimeMutationRetryBudgetError(label, lastErr)
			}
			return nil
		}
		if attemptErr != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if hasCtxDeadline && errors.Is(attemptErr, context.DeadlineExceeded) && !time.Now().Before(ctxDeadline) {
				return context.DeadlineExceeded
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

func (s *SQLiteRuntimeStore) runRuntimeMutationOnceLocked(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		rollbackErr := rollbackSQLTransaction(tx)
		return errors.Join(err, rollbackErr)
	}
	if err := tx.Commit(); err != nil {
		rollbackErr := rollbackSQLTransaction(tx)
		return errors.Join(err, rollbackErr)
	}
	return nil
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
