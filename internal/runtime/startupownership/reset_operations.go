package startupownership

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
)

func (p *processCapability) AdmitResetOperation(ctx context.Context, req destructivereset.Request) (out destructivereset.Operation, err error) {
	if p == nil {
		return out, errors.New("process startup/topology capability is missing")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err = p.proveCurrent(ctx); err != nil {
		return out, err
	}
	out, err = p.session.AdmitResetOperation(ctx, req)
	p.retireOnPossessionFailure(err)
	return
}

func (p *processCapability) ReadResetOperation(ctx context.Context, id string) (out destructivereset.Operation, err error) {
	if p == nil {
		return out, errors.New("process startup/topology capability is missing")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err = p.proveCurrent(ctx); err != nil {
		return out, err
	}
	out, err = p.session.ReadResetOperation(ctx, id)
	p.retireOnPossessionFailure(err)
	return
}

func (p *processCapability) PendingResetOperations(ctx context.Context) (out []destructivereset.Operation, err error) {
	if p == nil {
		return nil, errors.New("process startup/topology capability is missing")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err = p.proveCurrent(ctx); err != nil {
		return nil, err
	}
	out, err = p.session.PendingResetOperations(ctx)
	p.retireOnPossessionFailure(err)
	return
}

func (p *processCapability) AdvanceResetOperation(ctx context.Context, before, after destructivereset.Operation) error {
	if p == nil {
		return errors.New("process startup/topology capability is missing")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return err
	}
	err := p.session.AdvanceResetOperation(ctx, before, after)
	p.retireOnPossessionFailure(err)
	return err
}
