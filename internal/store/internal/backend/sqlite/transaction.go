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

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
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
	_, err := b.RunTransactionOutcome(ctx, label, operation)
	return err
}

// RunAdmittedTransactionOutcome requires original runtime/process evidence;
// native construction and control retain their separate existing entrance.
func (b *Backend) RunAdmittedTransaction(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	_, err := b.RunAdmittedTransactionOutcome(ctx, label, operation)
	return err
}

func (b *Backend) RunAdmittedTransactionOutcome(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) (bool, error) {
	original, err := generationauthority.PrepareMutationAdmission(ctx)
	if err != nil {
		return false, cancellationBeforeMutationAdmission(ctx, err)
	}
	return b.RunTransactionOutcome(original, label, operation)
}

// RunTransactionOutcome keeps acknowledged COMMIT separate from later cleanup
// errors. False is not evidence that an uncertain COMMIT rolled back.
func (b *Backend) RunTransactionOutcome(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) (bool, error) {
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return false, err
	}
	if !b.Valid() {
		return false, fmt.Errorf("sqlite backend is required")
	}
	if operation == nil {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if runtimeownership.HasMutationAuthority(ctx) {
		original, err := generationauthority.PrepareMutationAdmission(ctx)
		if err != nil {
			return false, cancellationBeforeMutationAdmission(ctx, err)
		}
		ctx = original
	}
	label = mutationLabel(label)
	var recoveryDeadline time.Time
	var lastBusy error
	busyAttempts := 0
	for {
		current := mutationOperation{label: label, phase: mutationPhaseFirstAttempt, attempt: busyAttempts + 1}
		if err := ctx.Err(); err != nil {
			b.observeFirstCancellation(current, "before_attempt", err)
			return false, cancellationBeforeFirstAttempt(err, err, busyAttempts)
		}

		recovering := lastBusy != nil
		if recovering && !time.Now().Before(recoveryDeadline) {
			return false, mutationBudgetError(label, lastBusy)
		}

		attemptCtx := ctx
		cancelAttempt := func() {}
		phase := mutationPhaseFirstAttempt
		if recovering {
			attemptCtx, cancelAttempt = context.WithDeadline(ctx, recoveryDeadline)
			phase = mutationPhaseRetryAttempt
		}
		current = mutationOperation{label: label, phase: phase, attempt: busyAttempts + 1}
		permitStarted := time.Now()
		if err := b.acquireMutation(attemptCtx, current); err != nil {
			cancelAttempt()
			if callerErr := ctx.Err(); callerErr != nil {
				return false, cancellationBeforeFirstAttempt(callerErr, err, busyAttempts)
			}
			if recovering && errors.Is(err, context.DeadlineExceeded) {
				return false, mutationBudgetError(label, lastBusy)
			}
			return false, err
		}
		permitWait := time.Since(permitStarted)
		committed, err := func() (bool, error) {
			defer b.releaseMutation()
			return b.runTransactionOnceOutcome(attemptCtx, nil, permitWait, operation)
		}()
		attemptErr := attemptCtx.Err()
		cancelAttempt()
		// Cancellation after COMMIT admission cannot erase acknowledged success
		// or grant busy-policy replay because of a later cleanup failure.
		if committed {
			return true, err
		}

		if callerErr := ctx.Err(); callerErr != nil {
			b.observeFirstCancellation(current, "attempt", callerErr)
			return false, cancelledAttemptError(callerErr, err, busyAttempts)
		}
		if err == nil {
			return false, nil
		}
		if attemptErr != nil {
			if recovering && errors.Is(attemptErr, context.DeadlineExceeded) {
				return false, errors.Join(mutationBudgetError(label, lastBusy), err)
			}
			return false, errors.Join(attemptErr, err)
		}
		var terminal *transactionTerminationError
		if errors.As(err, &terminal) || !mutationBusyError(err) {
			return false, err
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
				return false, callerErr
			}
			return false, mutationBudgetError(label, lastBusy)
		}
	}
}

// RunReadTransaction owns the lifecycle of one caller-scoped consistent read.
// Reads deliberately bypass mutation admission and busy-recovery accounting.
func (b *Backend) RunReadTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if tx, err := b.inspectionTransaction(ctx); err != nil {
		return err
	} else if tx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if operation == nil {
			return nil
		}
		return errors.Join(operation(ctx, tx), ctx.Err())
	}
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
	_, err = b.runTransactionOnceOutcome(ctx, opts, 0, operation)
	return err
}

func (b *Backend) runTransactionOnceOutcome(ctx context.Context, opts *sql.TxOptions, permitWait time.Duration, operation func(context.Context, *sql.Tx) error) (committed bool, err error) {
	write := opts == nil || !opts.ReadOnly
	if err := ctx.Err(); err != nil {
		return false, cancellationBeforeConnection(ctx, err, write)
	}
	poolStarted := time.Now()
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return false, cancellationBeforeConnection(ctx, err, write)
	}
	poolWait := time.Since(poolStarted)
	var tx *sql.Tx
	beginAttempted, commitAttempted := false, false
	var stopRollback func() bool
	var rollbackDone <-chan error
	releaseOrdering := func() {}
	defer func() { releaseOrdering() }()
	discard := false
	probe := b.testTransactions.Begin(opts != nil && opts.ReadOnly, false)
	defer func() { probe.Finish(err) }()
	defer func() {
		cleanupStarted := time.Now()
		if tx != nil {
			probe.RollbackAttempted()
		}
		finalized := finalizeRollback(tx, stopRollback, rollbackDone, discard)
		if tx != nil {
			probe.OrderingRolledBack(finalized.rollbackErr)
		}
		releaseOrdering()
		finalized = finalized.closeConnection(conn, write)
		if finalized.err != nil {
			// This also reports cleanup failure while the original callback panic
			// unwinds. No recover here: the original panic remains authoritative.
			slog.Error("sqlite transaction cleanup failed", "error", finalized.err)
			err = &transactionTerminationError{errors.Join(err, finalized.err)}
		}
		err = finalized.cancellation(ctx, err, write, !beginAttempted, commitAttempted)
		probe.RecordCleanup(time.Since(cleanupStarted))
	}()
	if err = ctx.Err(); err != nil {
		return false, err
	}
	beginStarted := time.Now()
	transactionCtx := ctx
	if write {
		// Keep the original callback/admission context. Only the SQL handle's
		// finalizer is owner-held so cancellation rollback can be joined exactly.
		transactionCtx = context.WithoutCancel(ctx)
	}
	beginAttempted = true
	tx, err = conn.BeginTx(transactionCtx, opts)
	probe.RecordAdmission(permitWait, poolWait, time.Since(beginStarted))
	if err != nil {
		discard = true
		return false, err
	}
	probe.Begun()
	if write {
		rollbackTx := tx
		done := make(chan error, 1)
		rollbackDone = done
		stopRollback = context.AfterFunc(ctx, func() { done <- rollbackTx.Rollback() })
	}
	ctx, releaseOrdering = authoractivity.BindTransaction(ctx, ctx, tx, authoractivity.DialectSQLite)
	ctx = transactiontest.WithAttempt(ctx, probe)
	admittedCtx, retireAdmission, admissionErr := admitOptionalMutationTx(ctx, tx, opts)
	if admissionErr != nil {
		return false, admissionErr
	}
	ctx = admittedCtx
	defer retireAdmission()
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = operation(ctx, tx); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	probe.BeforeCommit()
	commitAttempted = true
	err = tx.Commit()
	if err == nil {
		probe.OrderingCommitted()
	}
	releaseOrdering()
	if err != nil {
		probe.CommitFailed()
		discard = true
		// Only this owner's Commit result is classified here. Automatic rollback
		// can win after the context check; callback and cleanup errors stay intact.
		if err == sql.ErrTxDone && ctx.Err() != nil {
			return false, ctx.Err()
		}
		var coded interface{ Code() int }
		if !errors.As(err, &coded) || (coded.Code()&255 != 5 && coded.Code()&255 != 6) {
			return false, &transactionTerminationError{err}
		}
		return false, err
	}
	probe.Committed()
	tx = nil
	return true, nil
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

func admitOptionalMutationTx(ctx context.Context, tx *sql.Tx, opts *sql.TxOptions) (context.Context, func(), error) {
	if (opts == nil || !opts.ReadOnly) && runtimeownership.HasMutationAuthority(ctx) {
		return generationauthority.AdmitMutationTx(ctx, tx, true)
	}
	return ctx, func() {}, nil
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
