package startupownership

import (
	"context"
	"errors"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type agentRecoverySeedFaultSession interface {
	Authority() (runtimeownership.Authority, error)
	interruptAgentRecoverySeedSessionForTest(context.Context, runtimeownership.Authority) error
}

func InterruptPostgresAgentRecoverySeedForTest(ctx context.Context, owner *StartupPostgresOwner, process runtimeownership.ProcessCapability, expected runtimeownership.Authority) error {
	if owner == nil {
		return errors.New("agent recovery interruption requires its original PostgreSQL owner")
	}
	return owner.interruptAgentRecoverySeedForTest(ctx, process, expected)
}

func InterruptSQLiteAgentRecoverySeedForTest(ctx context.Context, owner *StartupSQLiteOwner, process runtimeownership.ProcessCapability, expected runtimeownership.Authority) error {
	if owner == nil {
		return errors.New("agent recovery interruption requires its original SQLite owner")
	}
	return owner.interruptAgentRecoverySeedForTest(ctx, process, expected)
}

func (p *processOperations) interruptAgentRecoverySeedForTest(ctx context.Context, process runtimeownership.ProcessCapability, expected runtimeownership.Authority) error {
	p.mu.Lock()
	current, session := p.current, p.recoverySession
	p.mu.Unlock()
	if process == nil || current != process || session == nil {
		return errors.New("agent recovery interruption differs from its original acquired process")
	}
	authority, err := session.Authority()
	if err != nil || authority != expected {
		return errors.Join(errors.New("agent recovery interruption differs from its retained native session"), err)
	}
	if err := runtimeownership.ConsumeAgentRecoverySeedFaultCutForTest(ctx, process, expected); err != nil {
		return err
	}
	return session.interruptAgentRecoverySeedSessionForTest(ctx, expected)
}

func (s *postgresSession) interruptAgentRecoverySeedSessionForTest(ctx context.Context, expected runtimeownership.Authority) error {
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

func (s *sqliteSession) interruptAgentRecoverySeedSessionForTest(ctx context.Context, expected runtimeownership.Authority) error {
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
