package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type ObservabilityReceiptConflict uint8

const (
	PendingDeliveryWithDeadLetterReceipt ObservabilityReceiptConflict = iota + 1
	FailedDeliveryWithSuccessReceipt
	FailedDeliveryWithDeadLetterReceipt
)

func InsertObservabilityReceiptConflictForTest(ctx context.Context, tx *sql.Tx, postgres bool, eventID string, conflict ObservabilityReceiptConflict, at time.Time) error {
	var subscriber, outcome, effects string
	var failure any
	switch conflict {
	case PendingDeliveryWithDeadLetterReceipt:
		subscriber, outcome, effects = "agent-pending", "dead_letter", `{"retry_count":9}`
		envelope := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "receipt_should_not_win", "api-test", "read", nil), "api-test", "read")
		raw, err := json.Marshal(&envelope)
		if err != nil {
			return err
		}
		failure = string(raw)
	case FailedDeliveryWithSuccessReceipt:
		subscriber, outcome, effects = "agent-failed", "success", `{"retry_count":0}`
	case FailedDeliveryWithDeadLetterReceipt:
		subscriber, outcome, effects = "agent-failed", "dead_letter", `{"retry_count":7,"error":"receipt-loses"}`
	default:
		return fmt.Errorf("unknown observability receipt conflict %d", conflict)
	}
	query := `INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at) VALUES ($1, 'platform', $2, $3, $4, $5, $6)`
	if postgres {
		query = `INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at) VALUES ($1::uuid, 'platform', $2, $3, $4::jsonb, $5::jsonb, $6)`
	}
	result, err := tx.ExecContext(ctx, query, eventID, subscriber, outcome, effects, failure, at)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("observability receipt conflict requires one exact row, got %d", count)
	}
	return nil
}
