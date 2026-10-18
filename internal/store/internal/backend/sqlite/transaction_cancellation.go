package sqlite

import "context"

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
