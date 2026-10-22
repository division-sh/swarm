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
