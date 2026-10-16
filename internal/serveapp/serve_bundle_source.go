package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type serveSourceArtifactPlan struct {
	fact    runtimecorrelation.SourceArtifactFact
	rooted  sourceartifact.RootedSource
	catalog durabledata.Catalog
}

type sourceArtifactDataWriter interface {
	EnsureRootedSourceWithData(context.Context, sourceartifact.RootedSource, durabledata.Catalog) (sourceartifact.RootedEnsureResult, error)
}

func prepareServeSourceArtifact(ctx context.Context, writer sourceArtifactDataWriter, bundle *runtimecontracts.WorkflowContractBundle) (runtimecorrelation.SourceArtifactFact, error) {
	plan, err := planServeSourceArtifact(writer, bundle)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return persistServeSourceArtifactPlan(ctx, writer, plan)
}

func planServeSourceArtifact(writer sourceArtifactDataWriter, bundle *runtimecontracts.WorkflowContractBundle) (serveSourceArtifactPlan, error) {
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return serveSourceArtifactPlan{}, fmt.Errorf("derive canonical bundle hash: %w", err)
	}
	if writer == nil {
		return serveSourceArtifactPlan{}, fmt.Errorf("serve requires the selected bundle ingest writer")
	}
	if bundle == nil || bundle.SourceArtifact == nil {
		return serveSourceArtifactPlan{}, fmt.Errorf("serve requires an admitted source artifact")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return serveSourceArtifactPlan{}, fmt.Errorf("serve requires its compiled rooted source: %w", err)
	}
	catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
	if err != nil {
		return serveSourceArtifactPlan{}, fmt.Errorf("project durable data catalog: %w", err)
	}
	if catalog.BundleHash != bundleHash {
		return serveSourceArtifactPlan{}, fmt.Errorf("durable data catalog hash %q does not match source fact %q", catalog.BundleHash, bundleHash)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		return serveSourceArtifactPlan{}, err
	}
	return serveSourceArtifactPlan{fact: fact, rooted: rooted, catalog: catalog}, nil
}

func persistServeSourceArtifactPlan(ctx context.Context, writer sourceArtifactDataWriter, plan serveSourceArtifactPlan) (runtimecorrelation.SourceArtifactFact, error) {
	if err := plan.fact.Validate(); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("planned source artifact fact: %w", err)
	}
	if err := plan.rooted.Validate(); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("source artifact plan requires its rooted source: %w", err)
	}
	if plan.rooted.Admission().BundleHash() != plan.fact.BundleHash() || plan.catalog.BundleHash != plan.fact.BundleHash() {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("planned source artifact and durable data hashes must match source fact %q", plan.fact.BundleHash())
	}
	if writer == nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("source artifact plan requires a source artifact store")
	}
	result, err := writer.EnsureRootedSourceWithData(ctx, plan.rooted, plan.catalog)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	if err := result.Source.Validate(); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("rooted source ingest returned no admitted source: %w", err)
	}
	if !result.Source.Admission().Equal(plan.rooted.Admission()) {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("rooted source ingest acknowledged a different source root")
	}
	return plan.fact, nil
}
