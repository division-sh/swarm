//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func (s *serveSessionBootstrap) connectSessionAttempt(ctx context.Context, attempt *serveSessionBootstrapAttempt) (err error) {
	if ctx == nil || attempt == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	select {
	case <-attempt.done:
	default:
		return channelonboarding.ErrSessionPending
	}
	if attempt.err != nil {
		return attempt.err
	}
	if attempt.connection == nil {
		return channelonboarding.ErrRevisionConflict
	}
	attempt.mu.Lock()
	if attempt.connected || attempt.connectErr != nil {
		attempt.mu.Unlock()
		_, err := attempt.current(ctx)
		return err
	}
	if attempt.connecting {
		attempt.mu.Unlock()
		return channelonboarding.ErrSessionPending
	}
	attempt.connecting, attempt.connectDone = true, make(chan struct{})
	attempt.mu.Unlock()
	defer func() {
		attempt.mu.Lock()
		attempt.connected, attempt.connectErr, attempt.connecting = err == nil, err, false
		close(attempt.connectDone)
		attempt.mu.Unlock()
	}()
	connection := attempt.connection
	if err = connection.Connect(ctx); err == nil && !attempt.bootstrap {
		err = connection.AwaitSessionAccount(ctx)
	}
	if err == nil {
		err = connection.CheckBootstrap(ctx)
	}
	if err == nil && attempt.incoming != nil {
		err = connection.ReconcileIncoming(ctx)
	}
	if err != nil {
		err = errors.Join(err, connection.Close(ctx))
	}
	return err
}

// Startup needs the completed original transport before identity/recovery work.
// Public progress surfaces never call this waiter and never start a successor.
func (s *serveSessionBootstrap) AwaitSessionBootstrap(ctx context.Context, op channelonboarding.Operation) error {
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return err
	}
	s.mu.Lock()
	attempt := s.connections[op.OperationID]
	s.mu.Unlock()
	if attempt == nil {
		return channelonboarding.ErrNotFound
	}
	select {
	case <-attempt.done:
	case <-ctx.Done():
		attempt.mu.Lock()
		if !attempt.connecting && !attempt.connected && attempt.connectErr == nil {
			attempt.connectErr = context.Cause(ctx)
		}
		attempt.mu.Unlock()
		return context.Cause(ctx)
	}
	err := s.connectSessionAttempt(ctx, attempt)
	if !errors.Is(err, channelonboarding.ErrSessionPending) {
		return err
	}
	attempt.mu.Lock()
	done := attempt.connectDone
	attempt.mu.Unlock()
	if done == nil {
		return channelonboarding.ErrRevisionConflict
	}
	select {
	case <-done:
		_, err := attempt.current(ctx)
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
