package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ExactPipelineReceiptOutcomeReason = private.ExactPipelineReceiptOutcomeReason

func DeleteCommittedReplayScope(ctx context.Context, selected any, eventID string) error {
	return private.DeleteCommittedReplayScopeForTest(ctx, selected, eventID)
}

func ReadExactPipelineReceiptOutcomeReason(ctx context.Context, selected any, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	return private.ReadExactPipelineReceiptOutcomeReasonForTest(ctx, selected, eventID)
}
