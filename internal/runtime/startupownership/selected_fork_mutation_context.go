package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

// SelectedForkMutationContextOwner creates a distinct admitted child operation;
// ordinary operation binding still cannot replace an original generation.
type SelectedForkMutationContextOwner interface {
	BindSelectedForkMutationContext(context.Context) (context.Context, error)
}

func (g *generationGrant) BindSelectedForkMutationContext(ctx context.Context) (context.Context, error) {
	if ctx == nil || g == nil || g.owner == nil {
		return nil, ErrMutationAuthorityMissing
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.requireSelectedChildParent(ctx); err != nil {
		return nil, err
	}
	evidence, err := g.Evidence()
	if err != nil {
		return nil, err
	}
	if err := requireSelectedChildIdentity(ctx, evidence); err != nil {
		return nil, err
	}
	if err := g.ProveCurrent(ctx); err != nil {
		return nil, err
	}
	source, err := correlation.NewSourceArtifactFact(evidence.BundleHash)
	if err != nil {
		return nil, err
	}
	ctx = correlation.WithRuntimeInstanceID(ctx, evidence.RuntimeInstanceID)
	ctx = correlation.WithRunID(ctx, evidence.SelectedFork.ForkRunID)
	ctx = correlation.WithSourceArtifactFact(ctx, source)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash))
	return context.WithValue(ctx, mutationAuthorityContextKey{}, g), nil
}

func requireSelectedChildIdentity(ctx context.Context, evidence GrantEvidence) error {
	binding := evidence.SelectedFork
	authority, found := effects.AuthorityFromContext(ctx)
	admission, admitted := managedexecution.FromContext(ctx)
	if binding == nil || !found || !authority.Valid() || authority.Kind != effects.AuthoritySelectedContractFork ||
		!admitted || !admission.AuthorizesSelected(binding.ExecutionID, binding.ForkRunID, binding.ExecutionGeneration) ||
		admission.BundleHash != evidence.BundleHash || admission.ActorCensusFingerprint != binding.ActorCensusFingerprint {
		return errors.New("selected child context requires its exact issued execution and admission")
	}
	selected := authority.SelectedFork
	if authority.ID != binding.ExecutionID || selected.ExecutionID != binding.ExecutionID ||
		selected.ForkRunID != binding.ForkRunID || selected.Generation != binding.ExecutionGeneration ||
		authority.ExecutionOwner != binding.ExecutionOwner || authority.FenceGeneration != binding.FenceGeneration ||
		selected.AdmissionFingerprint != binding.AdmissionFingerprint || selected.ContainerPlanFingerprint != binding.ContainerPlanFingerprint ||
		selected.ActorCensusFingerprint != binding.ActorCensusFingerprint || selected.EffectiveConfigFingerprint != binding.EffectiveConfigFingerprint {
		return errors.New("selected child context contradicts its issued execution")
	}
	if runID := correlation.RunIDFromContext(ctx); runID != "" && runID != binding.ForkRunID {
		return errors.New("selected child context contradicts its admitted run")
	}
	return validateMutationContext(ctx, evidence)
}

func (g *generationGrant) requireSelectedChildParent(ctx context.Context) error {
	var parent *generationGrant
	switch original := ctx.Value(mutationAuthorityContextKey{}).(type) {
	case MutationAuthority:
		if err := original.Validate(); err != nil {
			return err
		}
		if original.owner != g.owner {
			return errors.New("selected child context has a foreign original process")
		}
		parent = original.grant
		if parent != nil && parent != g {
			current, err := parent.Evidence()
			if err != nil {
				return err
			}
			if current.StateVersion != original.evidence.StateVersion {
				return errors.New("selected child context has stale original custody")
			}
		}
	case *generationGrant:
		if original == nil || original.owner != g.owner {
			return errors.New("selected child context has a foreign original generation")
		}
		parent = original
	case *processCapability:
		if original != g.owner {
			return errors.New("selected child context has a foreign original process")
		}
	case refusedMutationAuthority:
		return original.err
	default:
		return ErrMutationAuthorityMissing
	}
	if parent == nil {
		return g.owner.ProveCurrent(ctx)
	}
	evidence, err := parent.Evidence()
	if err != nil {
		return err
	}
	if parent != g && evidence.SelectedFork != nil {
		return errors.New("selected child context cannot replace another selected operation")
	}
	return parent.ProveCurrent(ctx)
}
