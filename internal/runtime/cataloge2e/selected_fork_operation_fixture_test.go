package cataloge2e

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/google/uuid"
)

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
		AllowSourceFreeze: true, ContractSelection: selection,
	}
}
