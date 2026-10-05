package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

func (g *generationGrant) RebindFlowReadinessSourceSet(ctx context.Context, req manager.FlowReadinessSourceSetRebindRequest) (manager.FlowReadinessSourceSetRebindResult, error) {
	if g == nil || g.owner == nil {
		return manager.FlowReadinessSourceSetRebindResult{}, errors.New("readiness rebind requires a process-owned generation grant")
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return manager.FlowReadinessSourceSetRebindResult{}, err
	}
	g.mu.Lock()
	evidence := g.evidence
	g.mu.Unlock()
	if evidence.SelectedFork != nil || evidence.State != GrantAdmitted {
		return manager.FlowReadinessSourceSetRebindResult{}, errors.New("source-set readiness rebind requires an admitted ordinary generation")
	}
	if err := g.requireExecutionAuthorityLocked(ctx, evidence); err != nil {
		return manager.FlowReadinessSourceSetRebindResult{}, err
	}
	binding := manager.ProcessExecutionBinding{
		ProcessAuthorityID: evidence.ProcessAuthorityID, ProcessOwnerID: evidence.ProcessOwnerID,
		ProcessBootID: evidence.ProcessBootID, GenerationGrantID: evidence.GrantID,
		BundleHash: evidence.BundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID, RuntimeGeneration: evidence.RuntimeGeneration,
	}
	for _, transition := range req.Transitions {
		if !transition.ProcessBinding.IsZero() {
			return manager.FlowReadinessSourceSetRebindResult{}, errors.New("readiness rebind writer binding is grant-owned")
		}
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash))
	result, err := g.owner.session.RebindFlowReadinessSourceSet(ctx, req, binding)
	if err != nil {
		return result, g.owner.retireOnPossessionFailure(err)
	}
	return result, g.owner.requireLive()
}
