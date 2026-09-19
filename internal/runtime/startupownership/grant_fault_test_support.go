package startupownership

import "errors"

// GenerationGrantFaultProbeForTest observes/refuses calls on a native grant. It
// cannot replace evidence, a session, or any successful native transition.
type GenerationGrantFaultProbeForTest struct {
	EvidenceError        func() error
	BeforeSettlement     func() error
	BeforeRetirement     func()
	AfterRetirementError func() error
}

// InstallGenerationGrantFaultProbeForTest keeps the original concrete authority
// intact. Install before startup; callbacks own their synchronization, and an
// already-entered call retains its probe until that call returns.
func InstallGenerationGrantFaultProbeForTest(grant LiveGenerationGrant, hooks GenerationGrantFaultProbeForTest) (func(), error) {
	live, ok := grant.(*liveGenerationGrant)
	if !ok || live == nil || live.generationGrant == nil || live.owner == nil {
		return nil, errors.New("generation grant fault probe requires a native live grant")
	}
	g := live.generationGrant
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.requireLive(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.evidence.State != GrantPrepared {
		return nil, errors.New("generation grant fault probe requires a prepared grant")
	}
	probe := &hooks
	if !g.testProbe.CompareAndSwap(nil, probe) {
		return nil, errors.New("generation grant already has a fault probe")
	}
	return func() { g.testProbe.CompareAndSwap(probe, nil) }, nil
}
