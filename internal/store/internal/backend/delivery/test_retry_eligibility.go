package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// MakeManagerRetryEligibleTx changes the fixture's mutable wake coordinate,
// never the already committed retry outcome or its original cadence.
func MakeManagerRetryEligibleTx(ctx context.Context, tx *sql.Tx, event events.Event, route events.DeliveryRoute) error {
	if tx == nil {
		return fmt.Errorf("manager retry cut requires the original selected transaction")
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE event_deliveries
		SET next_eligible_at=updated_at
		WHERE delivery_id=$1 AND event_id=$2 AND run_id=$3 AND status='failed'
		  AND retry_count>0 AND current_attempt_version IS NULL AND current_attempt_open IS NULL
		  AND EXISTS (SELECT 1 FROM event_delivery_attempts o
		    WHERE o.delivery_id=event_deliveries.delivery_id
		      AND o.claim_version=event_deliveries.claim_version
		      AND o.closure_kind='settled' AND o.outcome='retry_scheduled'
		      AND o.completed_at=event_deliveries.updated_at
		      AND o.retry_eligible_at=event_deliveries.next_eligible_at)`, id, event.ID(), event.RunID())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("manager retry cut requires one exact failed delivery and committed decision: count=%d,error=%v", count, err)
	}
	return nil
}
