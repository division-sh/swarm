package worklifetime

import (
	"context"
	"sync"
	"time"
)

// WithStopDeadline bounds owned work by the existing stop reservation. The
// reservation contributes a deadline and cancellation, never authority values.
func WithStopDeadline(owner, stop context.Context) (context.Context, context.CancelFunc) {
	deadline, bounded := stop.Deadline()
	if own, ok := owner.Deadline(); ok && (!bounded || own.Before(deadline)) {
		deadline, bounded = own, true
	}
	values, cause := context.WithCancelCause(context.WithoutCancel(owner))
	ctx := &stopDeadlineContext{Context: values, deadline: deadline, bounded: bounded, done: make(chan struct{})}
	var once sync.Once
	finish := func(err, reason error) {
		once.Do(func() {
			ctx.err = err
			cause(reason)
			close(ctx.done)
		})
	}
	follow := func(parent context.Context) func() bool {
		if err := parent.Err(); err != nil {
			finish(err, context.Cause(parent))
			return func() bool { return false }
		}
		return context.AfterFunc(parent, func() { finish(parent.Err(), context.Cause(parent)) })
	}
	stopOwner, stopReservation := follow(owner), follow(stop)
	return ctx, func() {
		stopOwner()
		stopReservation()
		finish(context.Canceled, context.Canceled)
	}
}

type stopDeadlineContext struct {
	context.Context
	deadline time.Time
	bounded  bool
	done     chan struct{}
	err      error
}

func (c *stopDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, c.bounded }

// The distinct Done lets derived contexts propagate the original Err, not the
// Canceled status of the private context used to retain the cause and values.
func (c *stopDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *stopDeadlineContext) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}
