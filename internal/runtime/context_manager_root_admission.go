package runtime

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func runtimeContextMetadata(contextDef *BundleContext) *BundleContext {
	if contextDef == nil {
		return nil
	}
	copy := *contextDef
	return &copy
}

func validateRuntimeContextRoot(candidate BundleContext) error {
	if err := candidate.RootAdmission.Validate(); err != nil {
		return fmt.Errorf("runtime context rooted admission: %w", err)
	}
	bundle, present := semanticview.Bundle(candidate.Source)
	if !present || bundle == nil {
		return fmt.Errorf("runtime context requires its compiled rooted source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return fmt.Errorf("runtime context compiled root: %w", err)
	}
	if !candidate.RootAdmission.Equal(rooted.Admission()) || candidate.BundleHash() != rooted.Admission().BundleHash() {
		return fmt.Errorf("runtime context root %+v contradicts compiled root %+v", candidate.RootAdmission.Reference(), rooted.Admission().Reference())
	}
	if candidate.Runtime == nil || candidate.Runtime.Bus == nil {
		return fmt.Errorf("runtime context requires its runtime event bus")
	}
	busContext, err := candidate.Runtime.Bus.AdmitSourceArtifactFact(context.Background())
	if err != nil {
		return fmt.Errorf("runtime context event-bus source: %w", err)
	}
	busRoot, present, err := correlation.RootAdmissionFromContext(busContext)
	if err != nil {
		return fmt.Errorf("runtime context event-bus root: %w", err)
	}
	if !present || !busRoot.Equal(candidate.RootAdmission) {
		return fmt.Errorf("runtime context root %+v contradicts frozen event-bus root %+v", candidate.RootAdmission.Reference(), busRoot.Reference())
	}
	return nil
}

func rootedRuntimeContextUse(ctx context.Context, candidate BundleContext) (context.Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("runtime context request context is required")
	}
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	if err := validateRuntimeContextRoot(candidate); err != nil {
		return ctx, err
	}
	if fact, supplied := correlation.SourceArtifactFactFromContext(ctx); supplied && !fact.Matches(candidate.SourceArtifactFact) {
		return ctx, fmt.Errorf("runtime context content fact contradicts the selected rooted source")
	}
	if err := correlation.CheckRootAdmissionContext(ctx, candidate.RootAdmission.Reference()); err != nil {
		return ctx, fmt.Errorf("runtime context rooted execution: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	ctx = correlation.WithSourceArtifactFact(ctx, candidate.SourceArtifactFact)
	return correlation.WithRootAdmission(ctx, candidate.RootAdmission), nil
}

func (m *RuntimeContextManager) runContextEntryLocked(availability runbundle.Availability) (*runtimeContextEntry, RuntimeContextLookup, error) {
	entry := m.contexts[availability.BundleHash]
	lookup := runtimeContextLookupForEntry(entry)
	if !availability.Available() {
		lookup.Context = nil
		lookup.State, lookup.Cause = RuntimeContextStateUnloaded, RuntimeContextCauseUnavailable
		return nil, lookup, nil
	}
	if entry != nil && entry.context != nil && entry.context.RootAdmission.Reference() != availability.RootReference {
		return nil, RuntimeContextLookup{}, fmt.Errorf("run %s root %+v contradicts runtime context root %+v", availability.RunID, availability.RootReference, entry.context.RootAdmission.Reference())
	}
	return entry, lookup, nil
}
