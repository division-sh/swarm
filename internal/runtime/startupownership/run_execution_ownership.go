package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/manager"
)

func (g *generationGrant) InspectRunExecutionOwnership(ctx context.Context, runID string) (manager.RunExecutionOwnership, error) {
	if g == nil || g.owner == nil {
		return 0, errors.New("run execution admission requires a generation grant")
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return 0, err
	}
	g.mu.Lock()
	evidence := g.evidence.clone()
	g.mu.Unlock()
	if evidence.State == GrantRetired {
		return 0, errors.New("run execution admission generation is retired")
	}
	if err := g.requireExecutionAuthorityLocked(ctx, evidence); err != nil {
		return 0, err
	}
	result, err := g.owner.session.InspectRunExecutionOwnership(ctx, evidence, runID)
	if err != nil {
		err = g.owner.retireOnPossessionFailure(err)
	}
	return result, errors.Join(err, g.owner.requireLive())
}
