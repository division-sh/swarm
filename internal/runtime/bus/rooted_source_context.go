package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func compiledBusRoot(opts EventBusOptions) (sourceartifact.RootAdmission, error) {
	bundle, present := semanticview.Bundle(opts.ContractBundle)
	if !present || bundle == nil {
		if opts.PipelineObligations != nil {
			return sourceartifact.RootAdmission{}, fmt.Errorf("durable event bus requires its compiled rooted source")
		}
		return sourceartifact.RootAdmission{}, nil
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		if opts.PipelineObligations != nil {
			return sourceartifact.RootAdmission{}, err
		}
		return sourceartifact.RootAdmission{}, nil
	}
	if opts.SourceArtifactFact.Validate() == nil && rooted.Admission().BundleHash() != opts.SourceArtifactFact.BundleHash() {
		return sourceartifact.RootAdmission{}, fmt.Errorf("event bus source fact differs from its compiled rooted source")
	}
	return rooted.Admission(), nil
}

func (eb *EventBus) admitRootedSourceContext(ctx context.Context) (context.Context, error) {
	eb.mu.RLock()
	root, ephemeral := eb.rootAdmission, eb.ephemeral
	eb.mu.RUnlock()
	if root == (sourceartifact.RootAdmission{}) && ephemeral {
		if _, _, err := correlation.RootAdmissionFromContext(ctx); err != nil {
			return ctx, err
		}
		return ctx, nil
	}
	if err := correlation.CheckRootAdmissionContext(ctx, root.Reference()); err != nil {
		return ctx, fmt.Errorf("event bus rooted source context: %w", err)
	}
	return correlation.WithRootAdmission(ctx, root), nil
}
