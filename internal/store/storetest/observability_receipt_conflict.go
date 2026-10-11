package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ObservabilityReceiptConflict = private.ObservabilityReceiptConflict

const (
	PendingDeliveryWithDeadLetterReceipt = private.PendingDeliveryWithDeadLetterReceipt
	FailedDeliveryWithSuccessReceipt     = private.FailedDeliveryWithSuccessReceipt
	FailedDeliveryWithDeadLetterReceipt  = private.FailedDeliveryWithDeadLetterReceipt
)

func SetObservabilityReceiptConflict(ctx context.Context, selected any, eventID string, conflict ObservabilityReceiptConflict, at time.Time) error {
	return private.SetObservabilityReceiptConflictForTest(ctx, selected, eventID, conflict, at)
}
