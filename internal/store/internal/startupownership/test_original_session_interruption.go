package startupownership

import "context"

func (s *postgresSession) PrepareOriginalSessionInterruptionForTest(ctx context.Context) (func(context.Context) error, error) {
	if _, err := s.Authority(); err != nil {
		return nil, err
	}
	return s.owner.backend.PrepareOriginalSessionInterruptionForTest(ctx, s.lease.Session())
}
