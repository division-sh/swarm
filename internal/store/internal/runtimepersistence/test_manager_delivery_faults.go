package runtimepersistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

func MakeManagerRetryEligibleForTest(ctx context.Context, selected any, event events.Event, route events.DeliveryRoute) error {
	if err := events.ValidatePersistentEvent(event); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.MakeManagerRetryEligibleTx(ctx, tx, event, route)
	})
}

func AgeManagerClaimStartedAtForTest(ctx context.Context, selected any, claim deliverylifecycle.Claim, startedAt time.Time) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.AgeManagerClaimStartedAtTx(ctx, tx, claim, startedAt)
	})
}
