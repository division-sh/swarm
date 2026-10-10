package runforkexecution

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

var ErrManagerDeliveryClaimGateReleased = errors.New("manager delivery proof finished its real selected execution claim")

type ManagerDeliveryExecutionClaim struct {
	Issued       runfork.SelectedContractRuntimeExecution
	Authority    effects.Authority
	Declarations agenttopology.SelectedDeclarationPlan
}

type managerDeliveryClaimGate struct {
	SelectedContractRuntimeExecutionLifecycle
	ready        chan<- ManagerDeliveryExecutionClaim
	release      <-chan struct{}
	declarations agenttopology.SelectedDeclarationPlan
}

func (g *managerDeliveryClaimGate) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, request runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	issued, err := g.SelectedContractRuntimeExecutionLifecycle.IssueRunForkSelectedContractRuntimeExecution(ctx, request)
	if err == nil {
		g.declarations = request.DeclarationPlan
	}
	return issued, err
}

func (g *managerDeliveryClaimGate) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (effects.Authority, error) {
	authority, err := g.SelectedContractRuntimeExecutionLifecycle.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, owner, lease)
	if err != nil {
		return authority, err
	}
	select {
	case g.ready <- ManagerDeliveryExecutionClaim{Issued: issued, Authority: authority, Declarations: g.declarations}:
	case <-ctx.Done():
		return authority, ctx.Err()
	}
	select {
	case <-g.release:
		return authority, ErrManagerDeliveryClaimGateReleased
	case <-ctx.Done():
		return authority, ctx.Err()
	}
}

// The test cut cannot supply or replace execution authority. It observes only
// the original completed claim, before publication, then uses normal failure
// settlement to retire the held execution after the manager has joined.
func WithManagerDeliveryClaimGateForTest(owner SelectedContractExecutionOwner, ready chan<- ManagerDeliveryExecutionClaim, release <-chan struct{}) (SelectedContractExecutionOwner, error) {
	ports, err := owner.require()
	if err != nil {
		return SelectedContractExecutionOwner{}, err
	}
	if ready == nil || release == nil {
		return SelectedContractExecutionOwner{}, errors.New("manager delivery claim gate requires both channels")
	}
	copy := *ports
	copy.runtimeExecution = &managerDeliveryClaimGate{SelectedContractRuntimeExecutionLifecycle: ports.runtimeExecution, ready: ready, release: release}
	return SelectedContractExecutionOwner{ports: &copy}, nil
}
