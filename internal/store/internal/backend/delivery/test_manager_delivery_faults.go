package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// Only the mutable wake changes; the committed outcome and cadence stay exact.
func (s *DeliveryPostgresOwner) MakeManagerRetryEligibleForTest(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager retry cut requires the original postgres owner")
	}
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
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
	})
}

func (s *DeliverySQLiteOwner) MakeManagerRetryEligibleForTest(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager retry cut requires the original sqlite owner")
	}
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
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
	})
}

// Aging never grants a lease: settlement still renews the original exact token.
func (s *DeliveryPostgresOwner) AgeManagerClaimStartedAtForTest(ctx context.Context, claim deliverylifecycle.Claim, startedAt time.Time) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager claim age cut requires the original postgres owner")
	}
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if tx == nil {
			return fmt.Errorf("manager age cut requires the original selected transaction")
		}
		if err := claim.Validate(); err != nil {
			return err
		}
		if startedAt.IsZero() || !startedAt.Before(time.Now()) {
			return fmt.Errorf("manager age cut requires a past nonzero timestamp")
		}
		result, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET created_at=$1,started_at=$1,updated_at=$1 WHERE delivery_id=$2 AND run_id=$3 AND claim_version=$4 AND status='in_progress'`, startedAt.UTC(), claim.DeliveryID(), claim.RunID(), claim.Version())
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return fmt.Errorf("manager age cut requires one exact in-progress delivery: count=%d,error=%v", count, err)
		}
		result, err = tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET started_at=$1 WHERE delivery_id=$2 AND claim_version=$3 AND claim_token=$4 AND open_marker=TRUE`, startedAt.UTC(), claim.DeliveryID(), claim.Version(), claim.PersistenceToken())
		if err != nil {
			return err
		}
		count, err = result.RowsAffected()
		if err != nil || count != 1 {
			return fmt.Errorf("manager age cut requires one matching open attempt: count=%d,error=%v", count, err)
		}
		return nil
	})
}

func (s *DeliverySQLiteOwner) AgeManagerClaimStartedAtForTest(ctx context.Context, claim deliverylifecycle.Claim, startedAt time.Time) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager claim age cut requires the original sqlite owner")
	}
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		if tx == nil {
			return fmt.Errorf("manager age cut requires the original selected transaction")
		}
		if err := claim.Validate(); err != nil {
			return err
		}
		if startedAt.IsZero() || !startedAt.Before(time.Now()) {
			return fmt.Errorf("manager age cut requires a past nonzero timestamp")
		}
		result, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET created_at=$1,started_at=$1,updated_at=$1 WHERE delivery_id=$2 AND run_id=$3 AND claim_version=$4 AND status='in_progress'`, startedAt.UTC(), claim.DeliveryID(), claim.RunID(), claim.Version())
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return fmt.Errorf("manager age cut requires one exact in-progress delivery: count=%d,error=%v", count, err)
		}
		result, err = tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET started_at=$1 WHERE delivery_id=$2 AND claim_version=$3 AND claim_token=$4 AND open_marker=TRUE`, startedAt.UTC(), claim.DeliveryID(), claim.Version(), claim.PersistenceToken())
		if err != nil {
			return err
		}
		count, err = result.RowsAffected()
		if err != nil || count != 1 {
			return fmt.Errorf("manager age cut requires one matching open attempt: count=%d,error=%v", count, err)
		}
		return nil
	})
}
