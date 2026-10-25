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
	// Ordinary source-set proof belongs to the inspection transaction. Selected
	// execution retains its separate proof and local-fence boundary.
	if evidence.SelectedFork != nil {
		if err := g.requireExecutionAuthorityLocked(ctx, evidence); err != nil {
			return 0, err
		}
	}
	result, err := g.owner.session.InspectRunExecutionOwnership(ctx, evidence, runID)
	if err != nil {
		err = g.owner.retireOnPossessionFailure(err)
	}
	return result, errors.Join(err, g.owner.requireLive())
}

// Startup source applicability is not executable ownership. In particular a
// prepared ordinary grant may hydrate retained facts but cannot dispatch them.
func (g *generationGrant) ObserveStartupRunSource(ctx context.Context, runID string) (bool, error) {
	if g == nil || g.owner == nil {
		return false, errors.New("startup source observation requires a generation grant")
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return false, err
	}
	g.mu.Lock()
	evidence := g.evidence.clone()
	g.mu.Unlock()
	if evidence.SelectedFork == nil && (evidence.State == GrantPrepared || evidence.State == GrantProbeSettled) {
		reader, ok := g.owner.session.(interface {
			ObserveStartupRunSource(context.Context, GrantEvidence, string) (bool, error)
		})
		if !ok {
			return false, errors.New("startup source observation requires its retained store reader")
		}
		observed, err := reader.ObserveStartupRunSource(ctx, evidence, runID)
		err = errors.Join(g.owner.retireOnPossessionFailure(err), g.owner.requireLive())
		return observed && err == nil, err
	}
	if evidence.State == GrantRetired {
		return false, errors.New("startup source observation generation is retired")
	}
	if evidence.SelectedFork != nil {
		if err := g.requireExecutionAuthorityLocked(ctx, evidence); err != nil {
			return false, err
		}
	}
	ownership, err := g.owner.session.InspectRunExecutionOwnership(ctx, evidence, runID)
	err = errors.Join(g.owner.retireOnPossessionFailure(err), g.owner.requireLive())
	return ownership == manager.RunExecutionOwned && err == nil, err
}
