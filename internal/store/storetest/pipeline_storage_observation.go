package storetest

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReplyReturnStorageEvidence = private.ReplyReturnStorageEvidence
type FiniteRunStartStorageCounts = private.FiniteRunStartStorageCounts

func ReadFiniteRunStartStorageCounts(ctx context.Context, selected any) (FiniteRunStartStorageCounts, error) {
	return private.ReadFiniteRunStartStorageCountsForTest(ctx, selected)
}

type LatestPipelineReceiptStorage = private.LatestPipelineReceiptStorage
type EventPipelineReceiptObservationRow = private.EventPipelineReceiptObservationRow

type SourceFanOutIntentDiagnosticRow = private.SourceFanOutIntentDiagnosticRow

func CountClosedSourceFanOutIssuance(ctx context.Context, selected any, runID, eventID string, cardinality int) (int, error) {
	return private.CountClosedSourceFanOutIssuanceForTest(ctx, selected, runID, eventID, cardinality)
}

func ReadSourceFanOutIntentDiagnosticRows(ctx context.Context, selected any, runID, eventID string) ([]SourceFanOutIntentDiagnosticRow, error) {
	return private.ReadSourceFanOutIntentDiagnosticRowsForTest(ctx, selected, runID, eventID)
}

func ReadEarliestEventPipelineReceiptRows(ctx context.Context, selected any) ([]EventPipelineReceiptObservationRow, error) {
	return private.ReadEarliestEventPipelineReceiptRowsForTest(ctx, selected)
}

func ReadLatestPlatformPipelineReceiptStorage(ctx context.Context, selected any, eventID string) (LatestPipelineReceiptStorage, error) {
	return private.ReadLatestPlatformPipelineReceiptStorageForTest(ctx, selected, eventID)
}

func ReadPinnedAuthoredMutationStorage(ctx context.Context, selected any, source fanoutobligation.SourceRef) (json.RawMessage, error) {
	return private.ReadPinnedAuthoredMutationStorageForTest(ctx, selected, source)
}

func SetPinnedAuthoredMutationWire(ctx context.Context, selected any, source fanoutobligation.SourceRef, raw []byte) (int64, error) {
	return private.SetPinnedAuthoredMutationWireForTest(ctx, selected, source, raw)
}

type RunPublicationHistoryStorage = private.RunPublicationHistoryStorage

type A2MapHistoryFootprintStorage = private.A2MapHistoryFootprintStorage
type A2RetainedSourceFootprintStorage = private.A2RetainedSourceFootprintStorage

func ReadA2MapHistoryFootprintStorage(ctx context.Context, selected any, runID string) (A2MapHistoryFootprintStorage, error) {
	return private.ReadA2MapHistoryFootprintStorageForTest(ctx, selected, runID)
}

func ReadA2JoinHydrationFootprintStorage(ctx context.Context, selected any, runID string) ([5]int, error) {
	return private.ReadA2JoinHydrationFootprintStorageForTest(ctx, selected, runID)
}

func ReadA2CancellationFootprintStorage(ctx context.Context, selected any, runID string) ([5]int, error) {
	return private.ReadA2CancellationFootprintStorageForTest(ctx, selected, runID)
}

func ReadA2RetainedSourceFootprintStorage(ctx context.Context, selected any, runID string) (A2RetainedSourceFootprintStorage, error) {
	return private.ReadA2RetainedSourceFootprintStorageForTest(ctx, selected, runID)
}

func ReadRunPublicationHistoryStorage(ctx context.Context, selected any, runID string) (RunPublicationHistoryStorage, error) {
	return private.ReadRunPublicationHistoryStorageForTest(ctx, selected, runID)
}

type FanOutPublishedOutcomeStorageEvidence = private.FanOutPublishedOutcomeStorageEvidence
type FanOutRunProgressStorageEvidence = private.FanOutRunProgressStorageEvidence
type FanOutTriggeredIntentStorageEvidence = private.FanOutTriggeredIntentStorageEvidence
type FanOutIntentCompletionStorageEvidence = private.FanOutIntentCompletionStorageEvidence

func ReadReplyReturnStorage(ctx context.Context, selected any, run string) (ReplyReturnStorageEvidence, error) {
	return private.ReadReplyReturnStorageForTest(ctx, selected, run)
}
func ReadFanOutPublishedOutcomeStorage(ctx context.Context, selected any, run string) ([]FanOutPublishedOutcomeStorageEvidence, error) {
	return private.ReadFanOutPublishedOutcomeStorageForTest(ctx, selected, run)
}
func ReadFanOutRunProgressStorage(ctx context.Context, selected any, run string) (FanOutRunProgressStorageEvidence, error) {
	return private.ReadFanOutRunProgressStorageForTest(ctx, selected, run)
}
func ReadFanOutTriggeredIntentStorage(ctx context.Context, selected any, run, event string, node identity.ExecutableNode, ref contracts.FanOutElementRef) ([]FanOutTriggeredIntentStorageEvidence, error) {
	return private.ReadFanOutTriggeredIntentStorageForTest(ctx, selected, run, event, node, ref)
}
func ReadFanOutIntentCompletionStorage(ctx context.Context, selected any, key fanoutobligation.IntentKey) (FanOutIntentCompletionStorageEvidence, error) {
	return private.ReadFanOutIntentCompletionStorageForTest(ctx, selected, key)
}
