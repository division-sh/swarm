//go:build linux || darwin

package serveapp

import (
	"context"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"time"
)

func (s *serveSessionBootstrap) sessionRecovery(ctx context.Context, op channelonboarding.Operation) (time.Time, bool, error) {
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return time.Time{}, false, err
	}
	s.mu.Lock()
	attempt := s.connections[op.OperationID]
	s.mu.Unlock()
	if attempt == nil {
		return time.Time{}, false, channelonboarding.ErrNotFound
	}
	if _, err := attempt.finishedConnection(ctx); err != nil && !channelonboarding.SessionUnavailableOnly(err) {
		return time.Time{}, false, err
	}
	if channelonboarding.SessionUnavailableOnly(attempt.err) {
		return time.Time{}, false, attempt.err
	}
	return attempt.connection.SessionRecovery(ctx, op)
}
