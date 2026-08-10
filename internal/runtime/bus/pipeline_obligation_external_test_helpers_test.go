package bus_test

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func claimNextPipelineWork(
	t testing.TB,
	ctx context.Context,
	owner runtimepipelineobligation.Store,
	query runtimepipelineobligation.ClaimQuery,
) (runtimepipelineobligation.ClaimedWork, bool, error) {
	t.Helper()
	request := runtimepipelineobligation.GlobalScanRequest()
	if query.RunID != "" {
		request = runtimepipelineobligation.RunScanRequest(query.RunID)
	}
	request = request.WithExecutionPosture(executionposture.Live)
	scan, err := owner.OpenScan(ctx, request)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, false, err
	}
	scanOpen := true
	defer func() {
		if !scanOpen {
			return
		}
		if closeErr := owner.CloseScan(context.WithoutCancel(ctx), scan); closeErr != nil &&
			!errors.Is(closeErr, runtimepipelineobligation.ErrStaleScan) {
			t.Errorf("close pipeline scan: %v", closeErr)
		}
	}()
	for {
		batch, err := owner.ClaimBatch(ctx, scan, 1)
		if err != nil {
			return runtimepipelineobligation.ClaimedWork{}, false, err
		}
		if len(batch.Work) > 0 {
			selected := batch.Work[0]
			if closeErr := owner.CloseScan(context.WithoutCancel(ctx), scan); closeErr != nil {
				return runtimepipelineobligation.ClaimedWork{}, false, closeErr
			}
			scanOpen = false
			work, err := owner.ClaimEvent(ctx, selected.Event.ID(), selected.Claim.Purpose())
			return work, err == nil, err
		}
		if batch.Exhausted {
			return runtimepipelineobligation.ClaimedWork{}, false, nil
		}
	}
}
