package postgres

import "context"

// Distinct from serialization-conflict retry evidence. This outcome conveys
// joined cancellation only and grants no replay entitlement.
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
