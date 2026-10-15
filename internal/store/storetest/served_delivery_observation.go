package storetest

import (
	"context"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"time"
)

type CausalDeliveryStatusCount = private.CausalDeliveryStatusCount

func ReadServedRunDeliverySummary(ctx context.Context, selected any, runID string) (runtimedelivery.RunSummary, error) {
	return private.ReadServedRunDeliverySummaryForTest(ctx, selected, runID)
}

type CausalDeliveryFrontier = private.CausalDeliveryFrontier
type CausalDeliveryFrontierProbe = private.CausalDeliveryFrontierProbe

const (
	FrontierComplete           = private.FrontierComplete
	FrontierMissing            = private.FrontierMissing
	FrontierPending            = private.FrontierPending
	FrontierDuplicateDelivery  = private.FrontierDuplicateDelivery
	FrontierDeadLetterDelivery = private.FrontierDeadLetterDelivery
)

func ReadCausalDeliveryFrontier(ctx context.Context, selected any, runID, eventID string) (CausalDeliveryFrontier, error) {
	return private.ReadCausalDeliveryFrontierForTest(ctx, selected, runID, eventID)
}

func ProbeCausalDeliveryFrontier(ctx context.Context, selected any, probe CausalDeliveryFrontierProbe) (CausalDeliveryFrontier, error) {
	return private.ProbeCausalDeliveryFrontierForTest(ctx, selected, probe)
}

type DeliveryAttemptDiagnosticRow = private.DeliveryAttemptDiagnosticRow

type EventDeliveryDiagnosticRow = private.EventDeliveryDiagnosticRow

func CountGlobalDeliveredNodeRowsStorage(ctx context.Context, selected any, nodeID string) (int, error) {
	return private.CountGlobalDeliveredNodeRowsStorageForTest(ctx, selected, nodeID)
}

func ReadCanonicalRoutingSQLiteDiagnostic(ctx context.Context, selected any) (string, error) {
	return private.ReadCanonicalRoutingSQLiteDiagnosticForTest(ctx, selected)
}

type NodeDeliveryDiagnosticStorage = private.NodeDeliveryDiagnosticStorage

func ReadNodeDeliveryTargetEncoding(ctx context.Context, selected any, eventID, nodeID string) (string, error) {
	return private.ReadNodeDeliveryTargetEncodingForTest(ctx, selected, eventID, nodeID)
}

func ReadNodeDeliveryDiagnosticStorage(ctx context.Context, selected any, eventID string) (NodeDeliveryDiagnosticStorage, error) {
	return private.ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, eventID)
}

func ReadEventDeliveryDiagnosticRows(ctx context.Context, selected any, eventID string) ([]EventDeliveryDiagnosticRow, error) {
	return private.ReadEventDeliveryDiagnosticRowsForTest(ctx, selected, eventID)
}

func ReadSubscriberEventNamesCreatedSince(ctx context.Context, selected any, since time.Time, subscriberID string) ([]string, error) {
	return private.ReadSubscriberEventNamesCreatedSinceForTest(ctx, selected, since, subscriberID)
}

func ReadCausalDeliveryStatusCounts(ctx context.Context, selected any, runID, eventID string) ([]CausalDeliveryStatusCount, error) {
	return private.ReadCausalDeliveryStatusCountsForTest(ctx, selected, runID, eventID)
}

func ReadNonLogRunDeliveryClaimTotals(ctx context.Context, selected any, runID string) (int64, int64, error) {
	return private.ReadNonLogRunDeliveryClaimTotalsForTest(ctx, selected, runID)
}

func ReadDeliveryAttemptDiagnosticRows(ctx context.Context, selected any, deliveryID string) ([]DeliveryAttemptDiagnosticRow, error) {
	return private.ReadDeliveryAttemptDiagnosticRowsForTest(ctx, selected, deliveryID)
}

func ReadServedIncompletePipelineHandoffCount(ctx context.Context, selected any, runID string) (int, error) {
	return private.ReadServedIncompletePipelineHandoffCountForTest(ctx, selected, runID)
}

func CountRunDeadLetterDeliveries(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunDeadLetterDeliveriesForTest(ctx, selected, runID)
}

func ReadMailboxCompletionRunEffects(ctx context.Context, selected any, runID string) ([]string, error) {
	return private.ReadMailboxCompletionRunEffectsForTest(ctx, selected, runID)
}

func ReadServedDeliveryStatusCount(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	return private.ReadServedDeliveryStatusCountForTest(ctx, selected, eventID, subscriberType, subscriberID, statuses...)
}

func ReadServedRunDebugSummary(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadServedRunDebugSummaryForTest(ctx, selected, runID)
}

func ReadServedRuntimeLogDebugSummary(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadServedRuntimeLogDebugSummaryForTest(ctx, selected, runID)
}

func CountExactRunNodeEventDeliveryStorage(ctx context.Context, selected any, runID, eventID, nodeID string) (int, error) {
	return private.CountExactRunNodeEventDeliveryStorageForTest(ctx, selected, runID, eventID, nodeID)
}

func CountCurrentEventDeliveryOutcomeStorage(ctx context.Context, selected any, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	return private.CountCurrentEventDeliveryOutcomeStorageForTest(ctx, selected, eventID, subscriberType, subscriberID, outcome)
}

func CountSubscriberEventReceiptOutcomeStorage(ctx context.Context, selected any, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	return private.CountSubscriberEventReceiptOutcomeStorageForTest(ctx, selected, eventID, subscriberType, subscriberID, outcome)
}

func CountExactRunEventReplayScopeStorage(ctx context.Context, selected any, runID, eventID, scope string) (int, error) {
	return private.CountExactRunEventReplayScopeStorageForTest(ctx, selected, runID, eventID, scope)
}

func CountEventSubscriberReceiptStorage(ctx context.Context, selected any, eventID string, subscribers []string) (int, error) {
	return private.CountEventSubscriberReceiptStorageForTest(ctx, selected, eventID, subscribers)
}
