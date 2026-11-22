package testutil

import (
	"context"
	"sync"
	"time"
)

// StopDeadline keeps a future deadline timestamp while the proof controls when
// it fires. This detects consumers that copy the timestamp instead of the signal.
func StopDeadline(deadline time.Time) (context.Context, func(error)) {
	cause, cancel := context.WithCancelCause(context.Background())
	ctx := &stopDeadline{Context: cause, deadline: deadline, done: make(chan struct{})}
	var once sync.Once
	return ctx, func(err error) {
		once.Do(func() {
			ctx.err = err
			cancel(err)
			close(ctx.done)
		})
	}
}

type stopDeadline struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	err      error
}

func (c *stopDeadline) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *stopDeadline) Done() <-chan struct{}       { return c.done }
func (c *stopDeadline) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}
