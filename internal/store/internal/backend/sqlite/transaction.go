package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	mutationRetryBudget  = 5 * time.Second
	mutationBaseDelay    = 10 * time.Millisecond
	mutationMaximumDelay = 100 * time.Millisecond
)

func (b *Backend) RunTransaction(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if !b.Valid() {
		return fmt.Errorf("sqlite backend is required")
	}
	if operation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(mutationRetryBudget)
	callerOwnsDeadline := false
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
		callerOwnsDeadline = true
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			if callerOwnsDeadline {
				return context.DeadlineExceeded
			}
			return mutationBudgetError(label, lastErr)
		}
		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		b.mutationMu.Lock()
		err := b.runTransactionOnce(attemptCtx, operation)
		b.mutationMu.Unlock()
		attemptErr := attemptCtx.Err()
		cancel()
		if attemptErr != nil {
			if callerOwnsDeadline {
				return attemptErr
			}
			return mutationBudgetError(label, lastErr)
		}
		if err == nil {
			return nil
		}
		if !mutationBusyError(err) {
			return err
		}
		lastErr = err
		delay := time.Duration(attempt+1) * mutationBaseDelay
		if delay > mutationMaximumDelay {
			delay = mutationMaximumDelay
		}
		if remaining := time.Until(deadline); delay > remaining {
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

func (b *Backend) runTransactionOnce(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if operationErr := operation(ctx, tx); operationErr != nil {
		return errors.Join(operationErr, rollback(tx))
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return errors.Join(commitErr, rollback(tx))
	}
	return nil
}

func rollback(tx *sql.Tx) error {
	if tx == nil {
		return errors.New("SQLite transaction is missing")
	}
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func mutationBudgetError(label string, lastErr error) error {
	label = strings.TrimSpace(label)
	if label == "" {
		label = "sqlite mutation"
	}
	if lastErr == nil {
		return fmt.Errorf("%s retry budget %s exceeded: %w", label, mutationRetryBudget, context.DeadlineExceeded)
	}
	return fmt.Errorf("%s retry budget %s exceeded: %w", label, mutationRetryBudget, lastErr)
}

func mutationBusyError(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") || strings.Contains(text, "sqlite_locked") ||
		strings.Contains(text, "database is locked") || strings.Contains(text, "database table is locked") ||
		strings.Contains(text, "database is busy")
}
