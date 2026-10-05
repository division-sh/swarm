package effects

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

// LogicalTurnClock describes the first acknowledged provider launch for a real
// delivery or directive. Primitive tool rounds never start another clock.
type LogicalTurnClock struct {
	Origin       CompletionOrigin
	FirstAttempt string
	LaunchedAt   time.Time
	Timeout      *timeridentity.TurnTimeout
	DeadlineAt   time.Time
	TimeoutEvent string
}

func (c LogicalTurnClock) Validate() error {
	if err := c.Origin.Validate(); err != nil {
		return err
	}
	if _, err := uuid.Parse(c.FirstAttempt); err != nil || c.LaunchedAt.IsZero() {
		return fmt.Errorf("logical turn clock requires exact first provider launch")
	}
	if c.Timeout == nil {
		if !c.DeadlineAt.IsZero() || c.TimeoutEvent != "" {
			return fmt.Errorf("unbounded logical turn cannot carry a deadline or timeout event")
		}
		return nil
	}
	if err := c.Timeout.Validate(); err != nil {
		return err
	}
	if _, err := uuid.Parse(c.TimeoutEvent); err != nil || !c.DeadlineAt.Equal(c.LaunchedAt.Add(c.Timeout.After)) {
		return fmt.Errorf("logical turn deadline must retain the first provider launch bound")
	}
	return nil
}

func cloneLogicalTurnClock(clock LogicalTurnClock) LogicalTurnClock {
	clock.Timeout = timeridentity.CloneTurnTimeout(clock.Timeout)
	return clock
}

type ExternalAttemptLaunch struct {
	Committed bool
	Turn      *LogicalTurnClock
}

// TurnCancellation is durable intent, not proof that provider work or the
// delivery/directive has finished. Those owners must acknowledge their joins.
type TurnCancellation struct {
	Committed   bool
	Requested   bool
	Origin      CompletionOrigin
	Reason      deliverylifecycle.CancellationReason
	CauseEvent  string
	RequestedAt time.Time
}

type TurnLifetimeStore interface {
	RequestTurnTimeout(context.Context, Attempt, time.Time) (TurnCancellation, error)
}

type turnTimeoutContextKey struct{}

// WithTurnTimeout carries admitted configuration, not launch or cancel rights.
func WithTurnTimeout(ctx context.Context, timeout *timeridentity.TurnTimeout) context.Context {
	return context.WithValue(ctx, turnTimeoutContextKey{}, timeridentity.CloneTurnTimeout(timeout))
}

func turnTimeoutFromContext(ctx context.Context) *timeridentity.TurnTimeout {
	timeout, _ := ctx.Value(turnTimeoutContextKey{}).(*timeridentity.TurnTimeout)
	return timeridentity.CloneTurnTimeout(timeout)
}
