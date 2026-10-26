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
		definition := runtimepkg.BundleContext{
			SourceArtifactFact: h.rt.Options.SourceArtifactFact,
			Source:             h.rt.Options.WorkflowModule.SemanticSource(), Runtime: h.rt, WorkOwner: h.rt.WorkOccurrence(),
		}
		if h.bundle.PackInventory != nil {
			definition.PackInventoryDigest = h.bundle.PackInventory.Digest()
			projection, err := packadmission.FromBundle(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			definition.ProviderTriggerGeneration = projection.ProviderTriggers.Generation()
			definition.InstalledTriggerSubjects, err = projection.ProviderTriggers.InstalledCapabilitySubjects()
			if err != nil {
				t.Fatal(err)
			}
		}
		var err error
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
	request.AcquireLiveRuntime = func(ctx context.Context, hash string) (*runtimepkg.RuntimeContextUse, error) {
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
	bundleHash := selection.BundleHash
	if bundleHash == "" {
		availability, err := selected.LoadRunBundleAvailability(ctx, sourceRunID)
		if err != nil || !availability.Available() {
			t.Fatalf("load exact staged operation source: %+v err=%v", availability, err)
		}
		bundleHash = availability.BundleHash
	}
	id := uuid.NewString()
	return runfork.ForkOperationRequest{
		OperationID: id, Actor: "cataloge2e:" + t.Name(), IdempotencyKey: uuid.NewString(), TransportHash: "catalog-staged:" + id,
		SourceRunID: sourceRunID, ForkEventID: eventID, TargetBundleHash: bundleHash,
		AllowSourceFreeze: true, ContractSelection: selection,
	}
}
