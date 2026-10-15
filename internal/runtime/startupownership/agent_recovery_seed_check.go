package startupownership

import (
	"context"
	"errors"
)

type agentRecoverySeedCheck struct {
	writes     *AgentRecoveryWriteObserver
	seed       *agentRecoverySeed
	epoch      uint64
	authority  Authority
	sourceHead string
}

// CheckAgentRecoverySeed checks a private observation at one read boundary.
// It returns no counts or authority for a later claim, admission or launch.
func CheckAgentRecoverySeed(ctx context.Context, capability ProcessCapability) error {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || ctx == nil {
		return errors.New("agent recovery seed check requires context and its original process capability")
	}
	observed, err := p.captureAgentRecoverySeedCheck(ctx)
	if err != nil {
		return err
	}
	probe := p.agentRecoverySeedProbe.Load()
	if probe != nil && probe.AfterCheckCapture != nil {
		probe.AfterCheckCapture()
	}
	// Capture releases opMu as well as the observer/process locks. The existing
	// possession operation owns its normal serialization, not a nested lock.
	if err := p.ProveCurrent(ctx); err != nil {
		return err
	}
	if probe != nil && probe.AfterCheckProof != nil {
		probe.AfterCheckProof()
	}
	current, err := observed.writes.readSeedCheck(ctx, p, observed.authority)
	if err != nil {
		return err
	}
	if current != observed {
		return errAgentRecoverySeedChanged
	}
	return nil
}

func (p *processCapability) captureAgentRecoverySeedCheck(ctx context.Context) (agentRecoverySeedCheck, error) {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
		return agentRecoverySeedCheck{}, err
	}
	authority, err := p.session.Authority()
	if err != nil {
		return agentRecoverySeedCheck{}, err
	}
	if err := authority.Validate(); err != nil {
		return agentRecoverySeedCheck{}, err
	}
	owner, ok := p.session.(agentRecoveryWriteObserverOwner)
	if !ok {
		return agentRecoverySeedCheck{}, errAgentRecoverySeedChanged
	}
	return owner.AgentRecoveryWriteObserver().readSeedCheck(ctx, p, authority)
}

func (o *AgentRecoveryWriteObserver) readSeedCheck(ctx context.Context, p *processCapability, authority Authority) (agentRecoverySeedCheck, error) {
	if o == nil {
		return agentRecoverySeedCheck{}, errAgentRecoverySeedChanged
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return agentRecoverySeedCheck{}, err
	}
	if p.terminal.Cause != "" {
		return agentRecoverySeedCheck{}, errors.New("agent recovery seed check lost its live process")
	}
	seed := p.agentRecoverySeed
	if o.invalid || o.process != p || o.active != 0 || p.agentRecoveryWrites != o ||
		seed == nil || seed.process != authority || seed.sourceHead == "" || seed.inProgressByProfile == nil {
		return agentRecoverySeedCheck{}, errAgentRecoverySeedChanged
	}
	return agentRecoverySeedCheck{writes: o, seed: seed, epoch: o.epoch, authority: seed.process, sourceHead: seed.sourceHead}, nil
}
