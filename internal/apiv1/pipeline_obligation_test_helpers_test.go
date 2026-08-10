package apiv1

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func claimNextAPIPipelineWork(
	t testing.TB,
	ctx context.Context,
	owner runtimepipelineobligation.Store,
) (runtimepipelineobligation.ClaimedWork, bool, error) {
	t.Helper()
	scan, err := owner.OpenScan(ctx, runtimepipelineobligation.GlobalScanRequest().WithExecutionPosture(executionposture.Live))
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
