package storetest

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ExactPipelineReceiptOutcomeReason = private.ExactPipelineReceiptOutcomeReason

func StampHistoricalReplayScope(ctx context.Context, selected any, runID, eventID string, scope pipelineobligation.CommittedScope, at time.Time) error {
	return private.StampHistoricalReplayScopeForTest(ctx, selected, runID, eventID, scope, at)
}

func CountCopiedSourceReplayScopeStorage(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	return private.CountCopiedSourceReplayScopeStorageForTest(ctx, selected, runID, sourceEventID)
}

func CountForkLocalReplayScopeStorage(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	return private.CountForkLocalReplayScopeStorageForTest(ctx, selected, runID, sourceEventID)
}

func DeleteCommittedReplayScope(ctx context.Context, selected any, eventID string) error {
	return private.DeleteCommittedReplayScopeForTest(ctx, selected, eventID)
}

func ReadExactPipelineReceiptOutcomeReason(ctx context.Context, selected any, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	return private.ReadExactPipelineReceiptOutcomeReasonForTest(ctx, selected, eventID)
}
