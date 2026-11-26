package worklifetime

import (
	"context"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/forkcancelcapture"
)

// WithStopDeadline bounds owned work by the existing stop reservation. The
// reservation contributes a deadline and cancellation, never authority values.
func WithStopDeadline(owner, stop context.Context) (context.Context, context.CancelFunc) {
	deadline, bounded := stop.Deadline()
	if own, ok := owner.Deadline(); ok && (!bounded || own.Before(deadline)) {
		deadline, bounded = own, true
	}
	values, cause := forkcancelcapture.WithCancelCause(forkcancelcapture.WithoutCancel(owner))
	ctx := &stopDeadlineContext{Context: values, deadline: deadline, bounded: bounded, done: make(chan struct{})}
	trace := forkcancelcapture.Resolve(owner, owner)
	trace.Link("owned stop context (values)", ctx, values)
	trace.Note("owned stop context cancellation parents", owner, stop, ctx, nil, false)
	var once sync.Once
	finish := func(parent context.Context, err, reason error) {
		once.Do(func() {
			trace.Note("owned stop context cancel invoke", ctx, parent, ctx, reason, true)
			ctx.err = err
			cause(reason)
			close(ctx.done)
			trace.Note("owned stop context cancel return", ctx, parent, ctx, reason, false)
		})
	}
	follow := func(parent context.Context) func() bool {
		if err := parent.Err(); err != nil {
			finish(parent, err, context.Cause(parent))
			return func() bool { return false }
		}
		return context.AfterFunc(parent, func() { finish(parent, parent.Err(), context.Cause(parent)) })
	}
	stopOwner, stopReservation := follow(owner), follow(stop)
	return ctx, func() {
		stopOwner()
		stopReservation()
		finish(nil, context.Canceled, context.Canceled)
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
