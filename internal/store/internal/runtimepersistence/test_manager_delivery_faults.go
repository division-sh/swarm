package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func MakeManagerRetryEligibleForTest(ctx context.Context, selected any, event events.Event, route events.DeliveryRoute) error {
	if err := events.ValidatePersistentEvent(event); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.MakeManagerRetryEligibleForTest(ctx, event, route)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.MakeManagerRetryEligibleForTest(ctx, event, route)
	default:
		return fmt.Errorf("manager retry cut requires the original selected owner")
	}
}

func WaitManagerRetryDecisionForTest(ctx context.Context, selected any, event events.Event, route events.DeliveryRoute) error {
	if err := events.ValidatePersistentEvent(event); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		return err
	}
	owner := selected.(deliverylifecycle.Store)
	snapshot, err := owner.Snapshot(ctx, id)
	if err != nil {
		return err
	}
	outcomes, err := owner.Outcomes(ctx, id)
	if err != nil {
		return err
	}
	if snapshot.Status != deliverylifecycle.StatusFailed || snapshot.EventID != event.ID() || snapshot.RunID != event.RunID() || len(outcomes) == 0 {
		return fmt.Errorf("manager retry wait requires its exact failed delivery and committed decision")
	}
	last := outcomes[len(outcomes)-1]
	if last.Outcome != "retry_scheduled" || last.RetryEligibleAt == nil || !last.RetryEligibleAt.Equal(snapshot.NextEligibleAt) {
		return fmt.Errorf("manager retry wait requires matching immutable due evidence")
	}
	timer := time.NewTimer(time.Until(*last.RetryEligibleAt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func AgeManagerClaimStartedAtForTest(ctx context.Context, selected any, claim deliverylifecycle.Claim, startedAt time.Time) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.deliveryPostgresOwner.AgeManagerClaimStartedAtForTest(ctx, claim, startedAt)
	case *SQLiteRuntimeStore:
		return store.deliverySQLiteOwner.AgeManagerClaimStartedAtForTest(ctx, claim, startedAt)
	default:
		return fmt.Errorf("manager claim age cut requires the original selected owner")
	}
}
