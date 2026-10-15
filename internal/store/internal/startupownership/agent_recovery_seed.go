package startupownership

import (
	"context"
	"errors"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

// Assembly binds existing readers once, before acquiring a process. Neither a
// caller's old image nor a later substitute reader can seed the retained owner.
func (p *processOperations) BindAgentRecoveryReader(reader startuprecovery.AgentRecoveryReader) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if reader == nil || p.recoveryReader != nil || p.current != nil {
		return errors.New("agent recovery readers must be bound exactly once before process acquisition")
	}
	p.recoveryReader = reader
	return nil
}

func (p *processOperations) readAgentRecoverySeed(ctx context.Context, authority runtimeownership.Authority) (runtimeownership.AgentRecoverySeedRead, error) {
	p.mu.Lock()
	reader := p.recoveryReader
	p.mu.Unlock()
	if reader == nil {
		return runtimeownership.AgentRecoverySeedRead{}, errors.New("agent recovery requires its assembled selected readers")
	}
	candidate, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(ctx, reader)
	if err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	if candidate.Image.Process.Authority == nil || *candidate.Image.Process.Authority != authority {
		return runtimeownership.AgentRecoverySeedRead{}, errors.New("agent recovery image differs from the exact retained process stamp")
	}
	return runtimeownership.AgentRecoverySeedRead{
		Process: authority, SourceHead: candidate.Image.SourceHead,
		InProgressByProfile: candidate.InProgressByProfile,
	}, nil
}

func proveAgentRecoveryGeneration(ctx context.Context, query authorityQueryer, authority runtimeownership.Authority, sqlite bool) error {
	current, err := generationauthority.ReadProcessGeneration(ctx, query, sqlite)
	if err != nil {
		return err
	}
	if current != authority.LeaseStamp() {
		return errors.New("agent recovery process generation differs from its retained native owner")
	}
	return nil
}

func (s *postgresSession) ReadAgentRecoverySeed(ctx context.Context) (runtimeownership.AgentRecoverySeedRead, error) {
	authority, err := s.Authority()
	if err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	var read runtimeownership.AgentRecoverySeedRead
	var imageErr error
	nativeErr := s.owner.backend.InspectRetainedAgentRecovery(ctx, s.lease, func(snapshot context.Context) error {
		if err := proveAgentRecoveryGeneration(snapshot, s.owner.backend, authority, false); err != nil {
			return err
		}
		runtimeownership.ObserveAgentRecoverySeedReadForTest(snapshot)
		read, imageErr = s.owner.readAgentRecoverySeed(snapshot, authority)
		// A domain refusal is returned only after native settlement. Re-proving
		// here also detects a failed SQL transaction; it is never hidden as data.
		return proveAgentRecoveryGeneration(snapshot, s.owner.backend, authority, false)
	})
	if err := errors.Join(imageErr, nativeErr, contextError(ctx)); err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	return read, nil
}

func (s *sqliteSession) ReadAgentRecoverySeed(ctx context.Context) (runtimeownership.AgentRecoverySeedRead, error) {
	if err := s.proveCurrent(ctx, true); err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	authority, err := s.Authority()
	if err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	var read runtimeownership.AgentRecoverySeedRead
	var imageErr error
	nativeErr := s.owner.backend.InspectAgentRecovery(ctx, func(snapshot context.Context) error {
		if err := proveAgentRecoveryGeneration(snapshot, s.owner.backend, authority, true); err != nil {
			return err
		}
		runtimeownership.ObserveAgentRecoverySeedReadForTest(snapshot)
		read, imageErr = s.owner.readAgentRecoverySeed(snapshot, authority)
		return proveAgentRecoveryGeneration(snapshot, s.owner.backend, authority, true)
	})
	// The retained OS/proof owner stays live across the one image transaction.
	// Its original proof runs only after that read and cleanup have joined.
	proofErr := s.proveCurrent(ctx, true)
	if err := errors.Join(imageErr, nativeErr, proofErr, contextError(ctx)); err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	return read, nil
}
