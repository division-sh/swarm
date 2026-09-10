package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	mutationRetryBudget  = 5 * time.Second
	mutationBaseDelay    = 10 * time.Millisecond
	mutationMaximumDelay = 100 * time.Millisecond
)

type mutationPhase string

const (
	mutationPhaseFirstAttempt mutationPhase = "first_attempt"
	mutationPhaseRetryAttempt mutationPhase = "retry_attempt"
)

type mutationOperation struct {
	label     string
	phase     mutationPhase
	attempt   int
	contended bool
}

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
	label = mutationLabel(label)
	var recoveryDeadline time.Time
	var lastBusy error
	busyAttempts := 0
	for {
		current := mutationOperation{label: label, phase: mutationPhaseFirstAttempt, attempt: busyAttempts + 1}
		if err := ctx.Err(); err != nil {
			b.observeFirstCancellation(current, "before_attempt", err)
			return err
		}

		recovering := lastBusy != nil
		if recovering && !time.Now().Before(recoveryDeadline) {
			return mutationBudgetError(label, lastBusy)
		}

		attemptCtx := ctx
		cancelAttempt := func() {}
		phase := mutationPhaseFirstAttempt
		if recovering {
			attemptCtx, cancelAttempt = context.WithDeadline(ctx, recoveryDeadline)
			phase = mutationPhaseRetryAttempt
		}
		current = mutationOperation{label: label, phase: phase, attempt: busyAttempts + 1}
		if err := b.acquireMutation(attemptCtx, current); err != nil {
			cancelAttempt()
			if callerErr := ctx.Err(); callerErr != nil {
				return callerErr
			}
			if recovering && errors.Is(err, context.DeadlineExceeded) {
				return mutationBudgetError(label, lastBusy)
			}
			return err
		}
		err := func() error {
			defer b.releaseMutation()
			return b.runTransactionOnce(attemptCtx, nil, operation)
		}()
		attemptErr := attemptCtx.Err()
		cancelAttempt()

		if callerErr := ctx.Err(); callerErr != nil {
			b.observeFirstCancellation(current, "attempt", callerErr)
			return errors.Join(callerErr, err)
		}
		if err == nil {
			return nil
		}
		if attemptErr != nil {
			if recovering && errors.Is(attemptErr, context.DeadlineExceeded) {
				return errors.Join(mutationBudgetError(label, lastBusy), err)
			}
			return errors.Join(attemptErr, err)
		}
		var terminal *transactionTerminationError
		if errors.As(err, &terminal) || !mutationBusyError(err) {
			return err
		}

		lastBusy = err
		if recoveryDeadline.IsZero() {
			recoveryDeadline = time.Now().Add(mutationRetryBudget)
			b.observeFirstBusy(current, err)
		}
		busyAttempts++
		delay := time.Duration(busyAttempts) * mutationBaseDelay
		if delay > mutationMaximumDelay {
			delay = mutationMaximumDelay
		}
		if err := waitMutationBackoff(ctx, recoveryDeadline, delay); err != nil {
			if callerErr := ctx.Err(); callerErr != nil {
				b.observeFirstCancellation(current, "backoff", callerErr)
				return callerErr
			}
			return mutationBudgetError(label, lastBusy)
		}
	}
}

// RunReadTransaction owns the lifecycle of one caller-scoped consistent read.
// Reads deliberately bypass mutation admission and busy-recovery accounting.
func (b *Backend) RunReadTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if !b.Valid() {
		return fmt.Errorf("sqlite backend is required")
	}
	if operation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := b.runTransactionOnce(ctx, &sql.TxOptions{ReadOnly: true}, operation)
	// database/sql may report ErrTxDone after its cancellation rollback wins.
	// Retain the caller's cause without dropping callback or cleanup failures.
	return errors.Join(ctx.Err(), err)
}

func (b *Backend) acquireMutation(ctx context.Context, operation mutationOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-b.mutationToken:
	case <-ctx.Done():
		return ctx.Err()
	default:
		operation = b.beginMutationWait(operation)
		select {
		case <-b.mutationToken:
		case <-ctx.Done():
			b.finishMutationWait(operation)
			return ctx.Err()
		}
	}
	// Cancellation and token availability can become ready together. The
	// caller must win before callback entry even when token selection wins.
	if err := ctx.Err(); err != nil {
		b.finishMutationWait(operation)
		b.mutationToken <- struct{}{}
		return err
	}
	b.finishMutationWait(operation)
	return nil
}

func (b *Backend) beginMutationWait(operation mutationOperation) mutationOperation {
	b.mutationState.Lock()
	b.mutationState.waiting++
	b.mutationState.Unlock()
	operation.contended = true
	return operation
}

func (b *Backend) finishMutationWait(operation mutationOperation) {
	if !operation.contended {
		return
	}
	b.mutationState.Lock()
	b.mutationState.waiting--
	b.mutationState.Unlock()
}

func (b *Backend) releaseMutation() { b.mutationToken <- struct{}{} }

func (b *Backend) observeFirstBusy(operation mutationOperation, cause error) {
	b.firstBusyObservation.Do(func() {
		slog.Info("sqlite mutation busy recovery started",
			"operation", operation.label,
			"phase", operation.phase,
			"attempt", operation.attempt,
			"cause", cause,
		)
	})
}

func (b *Backend) observeFirstCancellation(operation mutationOperation, stage string, cause error) {
	b.firstCancellationObservation.Do(func() {
		slog.Info("sqlite mutation caller cancelled",
			"operation", operation.label,
			"phase", operation.phase,
			"attempt", operation.attempt,
			"stage", stage,
			"cause", cause,
		)
	})
}

func waitMutationBackoff(ctx context.Context, deadline time.Time, delay time.Duration) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	if delay > remaining {
		delay = remaining
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
}

func (b *Backend) runTransactionOnce(ctx context.Context, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) error) (err error) {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return err
	}
	var tx *sql.Tx
	discard := false
	defer func() {
		var cleanupErr error
		if tx != nil {
			rollbackErr := tx.Rollback()
			// ErrTxDone describes Go's handle, not the physical transaction. A
			// concurrent cancellation may already be disposing this connection.
			if rollbackErr != nil {
				discard = true
				if !errors.Is(rollbackErr, sql.ErrTxDone) {
					cleanupErr = rollbackErr
				}
			}
		}
		cleanupErr = errors.Join(cleanupErr, CloseConnection(conn, discard))
		if cleanupErr != nil {
			// This also reports cleanup failure while the original callback panic
			// unwinds. No recover here: the original panic remains authoritative.
			slog.Error("sqlite transaction cleanup failed", "error", cleanupErr)
			err = &transactionTerminationError{errors.Join(err, cleanupErr)}
		}
	}()
	tx, err = conn.BeginTx(ctx, opts)
	if err != nil {
		discard = true
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = operation(ctx, tx); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		discard = true
		// Only this owner's Commit result is classified here. Automatic rollback
		// can win after the context check; callback and cleanup errors stay intact.
		if err == sql.ErrTxDone && ctx.Err() != nil {
			return ctx.Err()
		}
		var coded interface{ Code() int }
		if !errors.As(err, &coded) || (coded.Code()&255 != 5 && coded.Code()&255 != 6) {
			return &transactionTerminationError{err}
		}
		return err
	}
	tx = nil
	return nil
}

// A failed cleanup or an uncertain commit never grants busy-policy replay.
type transactionTerminationError struct{ error }

func (e *transactionTerminationError) Unwrap() error { return e.error }

// CloseConnection releases a pinned connection only after its transaction owner
// has finished the sql.Tx or raw bootstrap transaction. Discard is exact, not pool-wide.
func CloseConnection(conn *sql.Conn, discard bool) error {
	var err error
	if discard {
		err = conn.Raw(func(any) error { return driver.ErrBadConn })
		if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
			err = nil
		}
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(err, closeErr)
}

func mutationBudgetError(label string, lastErr error) error {
	label = mutationLabel(label)
	if lastErr == nil {
		return fmt.Errorf("%s busy recovery invariant violated: missing busy/locked cause", label)
	}
	return fmt.Errorf("%s retry budget %s exceeded: %w", label, mutationRetryBudget, lastErr)
}

func mutationLabel(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return "sqlite mutation"
	}
	return label
}

func mutationBusyError(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") || strings.Contains(text, "sqlite_locked") ||
		strings.Contains(text, "database is locked") || strings.Contains(text, "database table is locked") ||
		strings.Contains(text, "database is busy")
}
