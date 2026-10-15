package startupownership

import (
	"context"
	"encoding/json"
	"errors"
	"maps"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

// AgentRecoverySeedRead is the fixed retained-session result. Only that session
// composes the complete canonical image/count and same-cut native proof.
// It is not accepted from callers of SeedAgentRecovery.
type AgentRecoverySeedRead struct {
	Process             Authority
	SourceHead          agenttopology.SourceSetPlan
	InProgressByProfile map[string]int
}

type agentRecoverySeedReader interface {
	ReadAgentRecoverySeed(context.Context) (AgentRecoverySeedRead, error)
}

type agentRecoverySeed struct {
	process             Authority
	sourceHead          string
	inProgressByProfile map[string]int
}

// SeedAgentRecovery installs one private observation in its original process
// owner. It neither admits work nor exposes capacity, and accepts no old image.
func SeedAgentRecovery(ctx context.Context, capability ProcessCapability) error {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || ctx == nil {
		return errors.New("agent recovery seed requires context and its original process capability")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
		return err
	}
	p.mu.Lock()
	seeded := p.agentRecoverySeed != nil
	p.mu.Unlock()
	if seeded {
		return errors.New("agent recovery seed is already installed")
	}
	reader, ok := p.session.(agentRecoverySeedReader)
	if !ok {
		return errors.New("retained process session has no agent recovery seed reader")
	}
	probe := p.agentRecoverySeedProbe.Load()
	read, err := reader.ReadAgentRecoverySeed(context.WithValue(ctx, agentRecoverySeedProbeContextKey{}, probe))
	if err != nil {
		return errors.Join(err, ctx.Err(), p.requireLive())
	}
	seed, err := p.prepareAgentRecoverySeed(read)
	if err != nil {
		return errors.Join(err, ctx.Err(), p.requireLive())
	}
	if probe != nil && probe.BeforePublication != nil {
		probe.observe(probe.BeforePublication)
	}
	if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
		return err
	}
	// Join the retained native owner's final proof before local publication.
	// This is not a substitute for the same-cut proof performed by the reader.
	if err := p.session.ProveCurrent(ctx); err != nil {
		if ctx.Err() == nil {
			p.terminalize(possessionTerminalResult(err))
		}
		return errors.Join(err, ctx.Err(), p.requireLive())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// terminalize records its result under this same mutex before closing Done.
	// Checking Done alone would allow publication in that intervening interval.
	if p.terminal.Cause != "" || ctx.Err() != nil {
		return errors.Join(errors.New("agent recovery seed lost its live process before publication"), ctx.Err())
	}
	p.agentRecoverySeed = seed
	return nil
}

func (p *processCapability) prepareAgentRecoverySeed(read AgentRecoverySeedRead) (*agentRecoverySeed, error) {
	current, err := p.session.Authority()
	if err != nil {
		return nil, err
	}
	if read.Process.Validate() != nil || read.Process != current || read.InProgressByProfile == nil {
		return nil, errors.New("agent recovery seed lacks its exact full process stamp or complete counts")
	}
	if err := read.SourceHead.Validate(); err != nil {
		return nil, err
	}
	for profile, count := range read.InProgressByProfile {
		if profile == "" || count < 1 {
			return nil, errors.New("agent recovery seed contains an invalid profile count")
		}
	}
	// The canonical reader owns interpretation. Retain immutable plan bytes and
	// a private count copy, not mutable aliases returned by its store assembly.
	head, err := json.Marshal(read.SourceHead)
	if err != nil {
		return nil, err
	}
	return &agentRecoverySeed{
		process: read.Process, sourceHead: string(head),
		inProgressByProfile: maps.Clone(read.InProgressByProfile),
	}, nil
}
