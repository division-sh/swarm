package startupownership

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// AdmitRetainedRunSources extends the initial process plan only with exact,
// artifact-backed coordinates read from this process's active native runs.
func AdmitRetainedRunSources(ctx context.Context, capability ProcessCapability, plan agenttopology.SourceSetPlan) (agenttopology.SourceSetPlan, error) {
	if err := plan.Validate(); err != nil {
		return agenttopology.SourceSetPlan{}, err
	}
	p, ok := capability.(*processCapability)
	if !ok || p == nil {
		return agenttopology.SourceSetPlan{}, ErrProcessNotCurrent
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return agenttopology.SourceSetPlan{}, err
	}
	if p.retainedSources == nil {
		reader, ok := p.session.(interface {
			LoadStartupRunSources(context.Context, []agenttopology.SourceCoordinate) (map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource, error)
		})
		if !ok {
			return agenttopology.SourceSetPlan{}, errors.New("retained source census requires the process's native session")
		}
		retained, err := reader.LoadStartupRunSources(ctx, plan.Sources)
		if err != nil {
			return agenttopology.SourceSetPlan{}, p.retireOnPossessionFailure(err)
		}
		if err := errors.Join(ctx.Err(), p.requireLive()); err != nil {
			return agenttopology.SourceSetPlan{}, err
		}
		admissions := make(map[sourceartifact.RootAdmissionReference]sourceartifact.RootAdmission, len(retained))
		for reference, source := range retained {
			if err := source.Validate(); err != nil {
				return agenttopology.SourceSetPlan{}, fmt.Errorf("admit retained source %s: %w", reference.BundleHash, err)
			}
			if source.Admission().Reference() != reference {
				return agenttopology.SourceSetPlan{}, errors.New("startup census source contradicts its exact coordinate")
			}
			admissions[reference] = source.Admission()
		}
		p.mu.Lock()
		p.retainedSources = admissions
		p.mu.Unlock()
	}
	sources := append([]agenttopology.SourceCoordinate(nil), plan.Sources...)
	seen := make(map[agenttopology.SourceCoordinate]bool, len(sources))
	for _, source := range sources {
		seen[source] = true
	}
	for _, admission := range p.retainedSources {
		coordinate, err := agenttopology.NewSourceCoordinate(admission)
		if err != nil {
			return agenttopology.SourceSetPlan{}, err
		}
		if !seen[coordinate] {
			sources = append(sources, coordinate)
			seen[coordinate] = true
		}
	}
	admitted, err := agenttopology.NewSourceSetPlan(sources, plan.Agents)
	if err = errors.Join(err, ctx.Err(), p.requireLive()); err != nil {
		return agenttopology.SourceSetPlan{}, err
	}
	return admitted, nil
}

// RunSourceObservationContext lends no execution authority. It carries only a
// startup-censused run's own admission for its exact native source observation.
func RunSourceObservationContext(ctx context.Context, runID string, reference sourceartifact.RootAdmissionReference, plan agenttopology.SourceSetPlan) (context.Context, error) {
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	if _, _, err := correlation.RootAdmissionFromContext(ctx); err != nil {
		return nil, err
	}
	if err := correlation.CheckRootAdmissionContext(ctx, reference); err == nil {
		return ctx, nil
	}
	original, err := CaptureMutationAuthority(ctx)
	if err != nil {
		return nil, err
	}
	if original.evidence.SelectedFork != nil {
		return nil, errors.New("selected execution cannot borrow a retained run's root")
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if !original.HasRuntimeGrant() || original.evidence.SourceSetRevision != plan.Revision {
		return nil, errors.New("retained source census requires the current ordinary source set")
	}
	original.owner.mu.Lock()
	admission, found := original.owner.retainedSources[reference]
	original.owner.mu.Unlock()
	if !found || admission.Reference() != reference {
		return nil, fmt.Errorf("run %s source is absent from the startup admission census", runID)
	}
	coordinate, err := agenttopology.NewSourceCoordinate(admission)
	if err != nil {
		return nil, err
	}
	member := false
	for _, source := range plan.Sources {
		member = member || source == coordinate
	}
	if !member {
		return nil, errors.New("retained run source is absent from the admitted source set")
	}
	fact, err := correlation.DecodeSourceArtifactFact(admission.BundleHash())
	if err != nil {
		return nil, err
	}
	return correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx, fact), admission), nil
}
