package startupownership

import (
	"context"
	"errors"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *postgresSession) InterruptAgentRecoverySeedSessionForTest(ctx context.Context, expected runtimeownership.Authority) error {
	current, err := s.Authority()
	if err := errors.Join(err, contextError(ctx)); err != nil {
		return err
	}
	if current != expected || s.lease == nil {
		return errors.New("agent recovery interruption requires its exact retained PostgreSQL process")
	}
	session := s.lease.Session()
	if session == nil {
		return errors.New("agent recovery interruption lost its original retained PostgreSQL session")
	}
	return session.ForceDiscard()
}

func (s *sqliteSession) InterruptAgentRecoverySeedSessionForTest(ctx context.Context, expected runtimeownership.Authority) error {
	current, err := s.Authority()
	if err := errors.Join(err, contextError(ctx)); err != nil {
		return err
	}
	if current != expected || s.proof == nil {
		return errors.New("agent recovery interruption requires its exact retained SQLite proof connection")
	}
	// The held operation resumes through the existing proveCurrent/terminal
	// path. Closing this proof connection does not release OS possession.
	return s.proof.Close()
}
