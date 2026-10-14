package channelonboarding

import (
	"context"
	"errors"
)

func (s *Service) prepareSessionPairing(ctx context.Context, op Operation, candidate Candidate) (Operation, bool, error) {
	if err := s.sessions.BootstrapSession(ctx, op, candidate); err != nil {
		if errors.Is(err, ErrSessionPending) {
			return op, false, nil
		}
		return Operation{}, false, err
	}
	return s.sessions.CheckpointSessionPairing(ctx, op)
}
