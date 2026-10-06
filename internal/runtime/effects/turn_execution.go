package effects

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

// AuthoredTurnCancellationError carries acknowledged intent, not a generic
// context cancellation and not an assertion that the origin is settled.
type AuthoredTurnCancellationError struct {
	Cancellation TurnCancellation
}

func (e *AuthoredTurnCancellationError) Error() string {
	return fmt.Sprintf("agent turn canceled: %s (cause %s)", e.Cancellation.Reason, e.Cancellation.CauseEvent)
}

func (c TurnCancellation) ValidateIntent() error {
	if !c.Committed || !c.Requested || c.Origin.Validate() != nil || c.RequestedAt.IsZero() {
		return fmt.Errorf("authored turn cancellation requires acknowledged exact intent")
	}
	if _, err := deliverylifecycle.ParseCancellationReason(string(c.Reason)); err != nil {
		return err
	}
	if _, err := uuid.Parse(c.CauseEvent); err != nil {
		return fmt.Errorf("authored turn cancellation requires its exact cause event: %w", err)
	}
	return nil
}

type TurnExecutionResult struct {
	Attempt      Attempt
	Cancellation TurnCancellation
}

type TurnExecution struct {
	mu      sync.Mutex
	parent  context.Context
	cancel  context.CancelCauseFunc
	timer   func(time.Time) (<-chan time.Time, func())
	stop    chan struct{}
	done    chan struct{}
	clock   *LogicalTurnClock
	result  TurnExecutionResult
	err     error
	closed  bool
	started bool
}

type turnExecutionContextKey struct{}

// WithTurnExecution owns the one launch-based bound for this logical carrier.
// The caller must Finish after provider/tool cleanup, before origin settlement.
func WithTurnExecution(ctx context.Context) (context.Context, *TurnExecution) {
	return newTurnExecution(ctx, func(deadline time.Time) (<-chan time.Time, func()) {
		timer := time.NewTimer(time.Until(deadline))
		return timer.C, func() { timer.Stop() }
	})
}

func newTurnExecution(ctx context.Context, timer func(time.Time) (<-chan time.Time, func())) (context.Context, *TurnExecution) {
	if ctx == nil {
		ctx = context.Background()
	}
	turnCtx, cancel := context.WithCancelCause(ctx)
	owner := &TurnExecution{parent: ctx, cancel: cancel, timer: timer, stop: make(chan struct{}), done: make(chan struct{})}
	return context.WithValue(turnCtx, turnExecutionContextKey{}, owner), owner
}

func observeTurnLaunch(ctx context.Context, handle *Handle, clock LogicalTurnClock) error {
	owner, _ := ctx.Value(turnExecutionContextKey{}).(*TurnExecution)
	if owner == nil {
		return nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed || owner.parent.Err() != nil {
		return fmt.Errorf("logical turn execution is no longer admitted")
	}
	if owner.clock != nil {
		previous := owner.clock
		if !previous.Origin.Same(clock.Origin) || previous.FirstAttempt != clock.FirstAttempt ||
			!previous.LaunchedAt.Equal(clock.LaunchedAt) || !previous.DeadlineAt.Equal(clock.DeadlineAt) || previous.TimeoutEvent != clock.TimeoutEvent {
			return fmt.Errorf("provider tool round changed its logical turn clock")
		}
		return nil
	}
	store, ok := handle.controller.store.(TurnLifetimeStore)
	if clock.Timeout != nil && !ok {
		return fmt.Errorf("bounded turn execution requires its selected-store lifetime owner")
	}
	ownedClock := cloneLogicalTurnClock(clock)
	owner.clock = &ownedClock
	owner.result.Attempt = handle.Attempt()
	if clock.Timeout == nil {
		return nil
	}
	wake, stopTimer := owner.timer(clock.DeadlineAt)
	owner.started = true
	go owner.awaitTimeout(store, handle.Attempt(), wake, stopTimer)
	return nil
}

func (o *TurnExecution) awaitTimeout(store TurnLifetimeStore, attempt Attempt, wake <-chan time.Time, stopTimer func()) {
	defer close(o.done)
	defer stopTimer()
	select {
	case <-o.stop:
		return
	case <-o.parent.Done():
		return
	case now := <-wake:
		if o.parent.Err() != nil {
			return
		}
		intent, err := store.RequestTurnTimeout(o.parent, attempt, now)
		if !intent.Committed && err == nil {
			err = fmt.Errorf("turn timeout has no commit acknowledgment")
		}
		if intent.Requested && intent.Committed && !intent.OriginSettled {
			if validationErr := intent.ValidateIntent(); validationErr != nil {
				err = errors.Join(err, validationErr)
			} else if !intent.Origin.Same(attempt.Origin) || intent.Reason != deliverylifecycle.CancellationTurnTimeout || intent.CauseEvent != o.clock.TimeoutEvent {
				err = errors.Join(err, fmt.Errorf("turn timeout returned foreign authored intent"))
			} else {
				o.mu.Lock()
				o.result.Cancellation = intent
				o.mu.Unlock()
				o.cancel(&AuthoredTurnCancellationError{Cancellation: intent})
			}
		}
		o.mu.Lock()
		o.err = errors.Join(o.err, err)
		o.mu.Unlock()
		if err != nil {
			o.cancel(err)
		}
	}
}

func (o *TurnExecution) Finish() (TurnExecutionResult, error) {
	if o == nil {
		return TurnExecutionResult{}, nil
	}
	o.mu.Lock()
	if !o.closed {
		o.closed = true
		close(o.stop)
	}
	started := o.started
	o.mu.Unlock()
	if started {
		<-o.done
	}
	o.mu.Lock()
	result, err := o.result, o.err
	o.mu.Unlock()
	o.cancel(nil)
	return result, err
}
