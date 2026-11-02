package startupownership

import (
	"context"
	"errors"
	"sync"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

// Only acquisition installs this exact native capability. No copied stamp,
// inspector, caller-supplied capability or global lookup can install one.
type processOperations struct {
	mu      sync.Mutex
	current runtimeownership.ProcessCapability
}

func (p *processOperations) bindProcess(capability runtimeownership.ProcessCapability) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = capability
}

// A descriptor must match the actually installed owner; it cannot acquire the
// latest process merely because the caller omitted a process context.
func (p *processOperations) originalGrantContext(ctx context.Context, grant runtimeownership.GrantEvidence) (context.Context, error) {
	p.mu.Lock()
	current := p.current
	p.mu.Unlock()
	if current == nil {
		return nil, runtimeownership.ErrProcessNotCurrent
	}
	original, err := current.Evidence()
	if err != nil {
		return nil, err
	}
	if original.LeaseStamp().Coordinates() != grant.ProcessCoordinates() {
		return nil, errors.New("generation grant differs from its original installed process")
	}
	return generationauthority.MutationContext(ctx, original.LeaseStamp(), false)
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
