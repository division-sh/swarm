package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type transactionFinalization struct {
	rollbackErr      error
	err              error
	rolledBack       bool
	connectionJoined bool
	discard          bool
}

func finalizeRollback(tx *sql.Tx, stopRollback func() bool, rollbackDone <-chan error, discard bool) transactionFinalization {
	rollbackErr := joinCancellationRollback(tx, stopRollback, rollbackDone)
	var cleanupErr error
	if rollbackErr != nil && (tx != nil || rollbackErr != sql.ErrTxDone) {
		discard = true
		if rollbackErr != sql.ErrTxDone {
			cleanupErr = rollbackErr
		}
	}
	return transactionFinalization{
		rollbackErr: rollbackErr, err: cleanupErr, discard: discard,
		rolledBack: tx != nil && rollbackErr == nil,
	}
}

func (f transactionFinalization) closeConnection(conn *sql.Conn, write bool) transactionFinalization {
	var closeErr error
	if write && !f.discard {
		closeErr = conn.Close()
	} else {
		closeErr = CloseConnection(conn, f.discard)
	}
	f.err = errors.Join(f.err, closeErr)
	f.connectionJoined = !f.discard && closeErr == nil
	return f
}

func joinCancellationRollback(tx *sql.Tx, stop func() bool, done <-chan error) error {
	if stop != nil && !stop() {
		return <-done
	}
	if tx != nil {
		return tx.Rollback()
	}
	return nil
}

func (f transactionFinalization) cancellation(ctx context.Context, err error, write, beforeAttempt, commitAttempted bool) error {
	if _, nested := CleanCancellationBeforeCommit(err); nested {
		return fmt.Errorf("sqlite transaction callback: %w", err)
	}
	if write && !commitAttempted && f.connectionJoined && f.err == nil &&
		(beforeAttempt || f.rolledBack) && ctx.Err() == context.Canceled && onlyCallerCancellation(err) {
		return &cleanTransactionCancellation{cause: err, beforeAttempt: beforeAttempt}
	}
	return err
}

func cancellationBeforeFirstAttempt(callerErr, admissionErr error, priorAttempts int) error {
	if callerErr == context.Canceled && priorAttempts == 0 && onlyCallerCancellation(admissionErr) {
		return &cleanTransactionCancellation{cause: admissionErr, beforeAttempt: true}
	}
	return callerErr
}

// Admission has not acquired a permit, connection or transaction. Validate the
// original independently of cancellation; this context is never used for SQL.
func cancellationBeforeMutationAdmission(ctx context.Context, err error) error {
	if err != context.Canceled || ctx == nil || ctx.Err() != context.Canceled {
		return err
	}
	if _, authorityErr := runtimeownership.CaptureMutationAuthority(context.WithoutCancel(ctx)); authorityErr != nil {
		return errors.Join(err, authorityErr)
	}
	return cancellationBeforeFirstAttempt(context.Canceled, err, 0)
}

func cancelledAttemptError(callerErr, nativeErr error, priorAttempts int) error {
	if _, clean := CleanCancellationBeforeCommit(nativeErr); clean && priorAttempts == 0 {
		return nativeErr
	}
	return errors.Join(callerErr, nativeErr)
}

func cancellationBeforeConnection(ctx context.Context, err error, write bool) error {
	if write && ctx.Err() == context.Canceled && onlyCallerCancellation(err) {
		return &cleanTransactionCancellation{cause: err, beforeAttempt: true}
	}
	return err
}

// Only the native owner constructs this after its finalizer has joined. A
// failed or merely unacknowledged COMMIT is never this outcome.
type cleanTransactionCancellation struct {
	cause         error
	beforeAttempt bool
}

func (e *cleanTransactionCancellation) Error() string { return e.cause.Error() }
func (e *cleanTransactionCancellation) Unwrap() error { return e.cause }

func CleanCancellationBeforeCommit(err error) (beforeAttempt, ok bool) {
	cancelled, ok := err.(*cleanTransactionCancellation)
	if !ok || cancelled == nil {
		return false, false
	}
	return cancelled.beforeAttempt, true
}

// Do not turn a typed domain failure, ErrTxDone, or a storage/cleanup failure
// joined with cancellation into successful cleanup evidence.
func onlyCallerCancellation(err error) bool {
	if err == context.Canceled {
		return true
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) == 0 {
		return false
	}
	for _, branch := range joined.Unwrap() {
		if !onlyCallerCancellation(branch) {
			return false
		}
	}
	return true
}
