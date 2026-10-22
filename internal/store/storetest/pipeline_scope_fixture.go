package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ExactPipelineReceiptOutcomeReason = private.ExactPipelineReceiptOutcomeReason

func ReadExactFanOutBarrier(ctx context.Context, selected any, key fanoutobligation.IntentKey) (fanoutbarrier.Barrier, error) {
	return private.ReadExactFanOutBarrierForTest(ctx, selected, key)
}

func DeleteCommittedReplayScope(ctx context.Context, selected any, eventID string) error {
	return private.DeleteCommittedReplayScopeForTest(ctx, selected, eventID)
}

func ReadExactPipelineReceiptOutcomeReason(ctx context.Context, selected any, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	return private.ReadExactPipelineReceiptOutcomeReasonForTest(ctx, selected, eventID)
}
