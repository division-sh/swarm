package runforkexecution

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// admittedFixtureSelectedContractSourceLoader is test infrastructure for local
// source fixtures. It admits the directory once, then compiles exclusively from
// the immutable artifact. Persisted-hash tests use the selected-store loader.
type admittedFixtureSelectedContractSourceLoader struct {
	RepoRoot         string
	SourceRoot       string
	PlatformSpecPath string
}

var admittedFixtureSourceArtifacts sync.Map

func (l admittedFixtureSelectedContractSourceLoader) LoadRunForkSelectedContractSource(ctx context.Context, selection runfork.RunForkContractSelection) (LoadedSelectedContractSource, error) {
	return l.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: selection})
}

func (l admittedFixtureSelectedContractSourceLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	if err := ctx.Err(); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	selection := req.Selection
	if strings.TrimSpace(selection.Mode) != runfork.RunForkContractSelectionModeSelectedContracts {
		return LoadedSelectedContractSource{}, fmt.Errorf("admitted fixture loader supports selected_contracts mode only")
	}
	if err := validateSelectedSourceLoaderSelection(selection); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	artifact, err := sourceartifact.AdmitDirectory(l.SourceRoot)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(l.RepoRoot, artifact, l.PlatformSpecPath, runtimecontracts.WorkflowContractLoadOptions{
		AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	admittedFixtureSourceArtifacts.Store(bundleHash, artifact)
	sourceFact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	source, mockConnectorResponses, effectiveSourceIdentity, err := compileSelectedContractSource(semanticview.Wrap(bundle), sourceFact)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if err := validateSelectedContractSelection("admitted fixture source", selection); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	module := selectedContractWorkflowModule{
		source:         source,
		workflow:       workflow,
		nodes:          nodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}
	return LoadedSelectedContractSource{
		Selection:               selection,
		Source:                  source,
		Module:                  module,
		SourceArtifactFact:      sourceFact,
		EffectiveSourceIdentity: effectiveSourceIdentity,
		MockConnectorResponses:  mockConnectorResponses,
	}, nil
}
