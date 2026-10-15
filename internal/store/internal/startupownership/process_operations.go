package startupownership

import (
	"context"
	"sync"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
)

// Only acquisition installs this exact native capability. No copied stamp,
// inspector, caller-supplied capability or global lookup can install one.
type processOperations struct {
	mu      sync.Mutex
	current runtimeownership.ProcessCapability

	recoveryReader  startuprecovery.AgentRecoveryReader
	recoverySession agentRecoverySeedFaultSession
}

func (p *processOperations) bindProcess(capability runtimeownership.ProcessCapability, session agentRecoverySeedFaultSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = capability
	p.recoverySession = session
}

func (p *processOperations) RunPreparedProcessOperation(ctx context.Context, operation runtimeownership.PreparedProcessOperation) error {
	p.mu.Lock()
	current := p.current
	p.mu.Unlock()
	if current == nil {
		return runtimeownership.ErrProcessNotCurrent
	}
	return current.RunPreparedOperation(ctx, operation)
}
