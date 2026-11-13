package fanoutobligation

import (
	"errors"
	"math/rand/v2"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const (
	InitialReadRetryDelay = time.Second
	MaxReadRetryDelay     = time.Minute
)

// ReadRetry separates the conservative admitted-attempt floor from an observed
// failure. It survives claim consumption; RetryWait never accompanies a claim.
type ReadRetry struct {
	NotBefore time.Time                 `json:"not_before"`
	Delay     time.Duration             `json:"delay"`
	Since     *time.Time                `json:"since,omitempty"`
	Failure   *runtimefailures.Envelope `json:"failure,omitempty"`
}

func (r ReadRetry) Validate() error {
	if r.NotBefore.IsZero() || r.Delay < InitialReadRetryDelay || r.Delay > MaxReadRetryDelay {
		return errors.New("fan-out read retry requires a bounded delay and admission floor")
	}
	if (r.Since == nil) != (r.Failure == nil) {
		return errors.New("fan-out observed read failure requires both its time and cause")
	}
	if r.Since != nil {
		if r.Since.IsZero() || r.Since.After(r.NotBefore) {
			return errors.New("fan-out observed read failure time is invalid")
		}
		return (RetryWait{ReadyAt: r.NotBefore, Failure: *r.Failure}).Validate()
	}
	return nil
}

func (r *ReadRetry) NextAttempt(now time.Time) ReadRetry {
	next := ReadRetry{Delay: InitialReadRetryDelay}
	if r != nil {
		next = *r
		next.Delay = min(2*r.Delay, MaxReadRetryDelay)
	}
	// Draw once at native claim admission and retain the result through restart.
	floor := max(InitialReadRetryDelay, next.Delay/2)
	jitter := time.Duration(rand.Int64N(int64(next.Delay-floor) + 1))
	next.NotBefore = now.UTC().Add(floor + jitter)
	return next
}

func (r *ReadRetry) ObserveFailure(now time.Time, failure runtimefailures.Envelope) error {
	if r.Since == nil {
		at := now.UTC()
		r.Since = &at
	}
	r.Failure = &failure
	if r.NotBefore.Before(now) {
		r.NotBefore = now.UTC().Add(r.Delay)
	}
	return r.Validate()
}

type ResumeResult struct {
	Admitted  bool
	Observed  int
	Reopened  int
	Remaining int
	Changed   int
}
