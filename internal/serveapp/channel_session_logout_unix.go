//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func (s *serveSessionBootstrap) PrepareSessionLogout(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionLogoutTarget, error) {
	connection, err := s.currentConnection(ctx, op.OperationID)
	if err != nil {
		return channelonboarding.SessionLogoutTarget{}, err
	}
	return connection.PrepareSessionLogout(ctx, op)
}

func (s *serveSessionBootstrap) DispatchSessionLogout(ctx context.Context, op channelonboarding.TeardownOperation) error {
	if op.Logout == nil || op.Kind != channelonboarding.TeardownLogout || op.Phase != channelonboarding.TeardownAuthorityRetired {
		return channelonboarding.ErrConflict
	}
	connection, err := s.originalLogoutConnection(ctx, *op.Logout)
	if err != nil {
		return err
	}
	return connection.DispatchSessionLogout(ctx, op)
}

// Business authority is deliberately retired. This lookup retains only the
// original construction/cleanup owner, never a new connection or fresh account.
func (s *serveSessionBootstrap) originalLogoutConnection(ctx context.Context, target channelonboarding.SessionLogoutTarget) (*sessionprovider.RuntimeConnection, error) {
	if s == nil || ctx == nil || target.Validate() != nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	s.mu.Lock()
	attempt := s.connections[target.OperationID]
	s.mu.Unlock()
	if attempt == nil {
		return nil, channelonboarding.ErrNotFound
	}
	select {
	case <-attempt.done:
		if attempt.err != nil || attempt.connection == nil || attempt.connection.ConnectionID() != target.Account.ConnectionID {
			return nil, errors.Join(channelonboarding.ErrConflict, attempt.err)
		}
		return attempt.connection, ctx.Err()
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
