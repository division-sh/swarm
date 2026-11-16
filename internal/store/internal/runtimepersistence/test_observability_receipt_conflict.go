package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ObservabilityReceiptConflict = pipelinepersistence.ObservabilityReceiptConflict

const (
	PendingDeliveryWithDeadLetterReceipt = pipelinepersistence.PendingDeliveryWithDeadLetterReceipt
	FailedDeliveryWithSuccessReceipt     = pipelinepersistence.FailedDeliveryWithSuccessReceipt
	FailedDeliveryWithDeadLetterReceipt  = pipelinepersistence.FailedDeliveryWithDeadLetterReceipt
)

func SetObservabilityReceiptConflictForTest(ctx context.Context, selected any, eventID string, conflict ObservabilityReceiptConflict, at time.Time) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := validateSelectedForkStorageIdentity(eventID); err != nil {
		return err
	}
	if conflict < PendingDeliveryWithDeadLetterReceipt || conflict > FailedDeliveryWithDeadLetterReceipt || at.IsZero() {
		return fmt.Errorf("observability receipt fault requires a named conflict and exact clock")
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.SetObservabilityReceiptConflictForTest(ctx, eventID, conflict, at)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.SetObservabilityReceiptConflictForTest(ctx, eventID, conflict, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
