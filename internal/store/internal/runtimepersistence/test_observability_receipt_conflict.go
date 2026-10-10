package runtimepersistence

import (
	"context"
	"database/sql"
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
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertObservabilityReceiptConflictForTest(ctx, tx, postgres, eventID, conflict, at)
	})
}
