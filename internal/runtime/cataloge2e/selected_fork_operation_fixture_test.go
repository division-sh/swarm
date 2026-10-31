package cataloge2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func executeCatalogLiveFork(t testing.TB, h *runtimeHarness, ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
	return runforkexecution.ExecuteSelectedContractRunFork(ctx, catalogLiveForkRequest(t, h, ctx, request))
}

func catalogLiveForkRequest(t testing.TB, h *runtimeHarness, ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) runforkexecution.SelectedContractExecutionRequest {
	t.Helper()
	var selected interface {
		runforkexecution.SelectedContractForkLifecycle
		runlifecycle.LiveRunReader
	} = h.pg
	if h.sqlite != nil {
		selected = h.sqlite
	}
	if h.liveContexts == nil {
		source := h.rt.Options.WorkflowModule.SemanticSource()
		bundle, ok := semanticview.Bundle(source)
		if !ok {
			t.Fatal("catalog live fork requires its admitted source bundle")
		}
		rooted, err := bundle.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		definition := runtimepkg.BundleContext{
			SourceArtifactFact: h.rt.Options.SourceArtifactFact,
			RootAdmission:      rooted.Admission(), Source: source, Runtime: h.rt, WorkOwner: h.rt.WorkOccurrence(),
		}
		if bundle.PackInventory != nil {
			definition.PackInventoryDigest = bundle.PackInventory.Digest()
			providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
			if err != nil {
				t.Fatal(err)
			}
			scope, err := providers.Scope(semanticview.RootExecutionFlowID(source))
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := scope.TriggerCatalog()
			if err != nil {
				t.Fatal(err)
			}
			definition.ProviderTriggerCatalog = catalog
			definition.ProviderTriggerGeneration = catalog.Generation()
			definition.InstalledTriggerSubjects, err = catalog.InstalledCapabilitySubjects()
			if err != nil {
				t.Fatal(err)
			}
		}
		h.liveContexts, err = runtimepkg.NewRuntimeContextManager(selected, definition)
		if err != nil {
			t.Fatal(err)
		}
	}
	if request.ForkOperation == nil {
		operation := catalogStagedForkOperation(t, ctx, selected, request.ContractSelection, request.SourceRunID, request.At)
		operation.AtStart = request.AtStart
		request.ForkOperation = &operation
	}
	request.LiveRuntime = h.rt
	request.LiveRuns, request.LiveContexts = selected, h.liveContexts
	request.AcquireLiveRuntime = func(ctx context.Context, reference sourceartifact.RootAdmissionReference) (*runtimepkg.RuntimeContextUse, error) {
		hash := reference.BundleHash
		use, lookup, err := h.liveContexts.AcquireBundleHash(ctx, hash)
		if err == nil && !lookup.Loaded() {
			return nil, fmt.Errorf("catalog live fork target %s is not its registered ordinary runtime", hash)
		}
		return use, err
	}
	return request
}

// Only an intended stage/execute continuation uses this request. Fresh staging
// and explicit-child readback keep their operation-free setup.
func catalogStagedForkOperation(t testing.TB, ctx context.Context, selected runforkexecution.SelectedContractForkLifecycle, selection runfork.RunForkContractSelection, sourceRunID, eventID string) runfork.ForkOperationRequest {
	t.Helper()
	store, ok := selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)
	if !ok {
		t.Fatal("catalog operation requires its exact selected source owner")
	}
	loaded, err := (runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootFromCatalogE2E(t), Store: store}).InspectRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{
		Responsibility: runforkexecution.FreshSelectedSource(selection.BundleHash), BundleHash: selection.BundleHash, Selection: selection,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	return runfork.ForkOperationRequest{
		OperationID: id, Actor: "cataloge2e:" + t.Name(), IdempotencyKey: uuid.NewString(), TransportHash: "catalog-staged:" + id,
		SourceRunID: sourceRunID, ForkEventID: eventID, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), TargetRootReference: loaded.RootedSource.Admission().Reference(),
		ContractSelection: selection,
	}
}
