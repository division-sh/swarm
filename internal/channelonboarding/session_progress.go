package channelonboarding

import (
	"context"
	"errors"
	"fmt"
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

func (s *Service) restoreSessionProgress(ctx context.Context, op Operation, candidate Candidate) error {
	if err := s.resumeSession(ctx, op, candidate); err != nil {
		if !errors.Is(err, ErrSessionPending) {
			return fmt.Errorf("restore channel onboarding %s: %w", op.OperationID, err)
		}
		if err := s.sessions.AwaitSessionBootstrap(ctx, op); err != nil {
			return fmt.Errorf("await restored channel onboarding %s: %w", op.OperationID, err)
		}
	}
	return nil
}
