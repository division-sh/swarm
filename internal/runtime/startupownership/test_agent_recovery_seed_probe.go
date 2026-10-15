package startupownership

import (
	"context"
	"errors"
	"sync/atomic"
)

type AgentRecoverySeedFaultProbeForTest struct {
	InsideRead        func()
	BeforePublication func()
	active            atomic.Bool
}

type agentRecoverySeedProbeContextKey struct{}

func InstallAgentRecoverySeedFaultProbeForTest(capability ProcessCapability, probe *AgentRecoverySeedFaultProbeForTest) (func(), error) {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || probe == nil || p.requireLive() != nil || !p.agentRecoverySeedProbe.CompareAndSwap(nil, probe) {
		return nil, errors.New("agent recovery seed probe requires its exact live process and one installation")
	}
	return func() { p.agentRecoverySeedProbe.CompareAndSwap(probe, nil) }, nil
}

// Only SeedAgentRecovery supplies this process-owned observation hook. It
// carries no transaction, decoded image, capacity result or mutation authority.
func ObserveAgentRecoverySeedReadForTest(ctx context.Context) {
	if probe, _ := ctx.Value(agentRecoverySeedProbeContextKey{}).(*AgentRecoverySeedFaultProbeForTest); probe != nil && probe.InsideRead != nil {
		probe.observe(probe.InsideRead)
	}
}

func (p *AgentRecoverySeedFaultProbeForTest) observe(observe func()) {
	p.active.Store(true)
	defer p.active.Store(false)
	observe()
}

type agentRecoverySeedInterruption interface {
	InterruptAgentRecoverySeedSessionForTest(context.Context, Authority) error
}

// This fixed fault cannot run outside a held seed cut, on a substituted process
// or twice. It returns native errors and exposes neither the session nor SQL.
func InterruptAgentRecoverySeedSessionForTest(ctx context.Context, capability ProcessCapability, expected Authority) error {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || ctx == nil || expected.Validate() != nil {
		return errors.New("agent recovery interruption requires its original process and full stamp")
	}
	if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
		return err
	}
	probe := p.agentRecoverySeedProbe.Load()
	current, err := p.session.Authority()
	if err != nil || current != expected || probe == nil || !probe.active.Load() {
		return errors.Join(errors.New("agent recovery interruption differs from its held native process cut"), err)
	}
	session, ok := p.session.(agentRecoverySeedInterruption)
	if !ok || !p.agentRecoverySeedFaultUsed.CompareAndSwap(false, true) {
		return errors.New("agent recovery interruption requires one original native session fault")
	}
	return session.InterruptAgentRecoverySeedSessionForTest(ctx, expected)
}
