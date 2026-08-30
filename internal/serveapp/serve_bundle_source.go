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
	fact     runtimecorrelation.SourceArtifactFact
	artifact *sourceartifact.AdmittedSourceArtifact
	catalog  durabledata.Catalog
}

type sourceArtifactDataWriter interface {
	EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
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
	return serveSourceArtifactPlan{fact: fact, artifact: bundle.SourceArtifact, catalog: catalog}, nil
}

func persistServeSourceArtifactPlan(ctx context.Context, writer sourceArtifactDataWriter, plan serveSourceArtifactPlan) (runtimecorrelation.SourceArtifactFact, error) {
	if err := plan.fact.Validate(); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("planned source artifact fact: %w", err)
	}
	if plan.artifact == nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("source artifact plan requires an admitted source artifact")
	}
	if plan.artifact.BundleHash() != plan.fact.BundleHash() || plan.catalog.BundleHash != plan.fact.BundleHash() {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("planned source artifact and durable data hashes must match source fact %q", plan.fact.BundleHash())
	}
	if writer == nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("source artifact plan requires a source artifact store")
	}
	if _, err := writer.EnsureSourceArtifactWithData(ctx, plan.artifact, plan.catalog); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return plan.fact, nil
}
