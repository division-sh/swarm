package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (s *DeliveryPostgresOwner) MakeManagerRetryEligibleForTest(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager retry cut requires the original postgres owner")
	}
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return MakeManagerRetryEligibleTx(ctx, tx, event, route)
	})
}

func (s *DeliverySQLiteOwner) MakeManagerRetryEligibleForTest(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager retry cut requires the original sqlite owner")
	}
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return MakeManagerRetryEligibleTx(ctx, tx, event, route)
	})
}

func (s *DeliveryPostgresOwner) AgeManagerClaimStartedAtForTest(ctx context.Context, claim deliverylifecycle.Claim, startedAt time.Time) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager claim age cut requires the original postgres owner")
	}
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return AgeManagerClaimStartedAtTx(ctx, tx, claim, startedAt)
	})
}

func (s *DeliverySQLiteOwner) AgeManagerClaimStartedAtForTest(ctx context.Context, claim deliverylifecycle.Claim, startedAt time.Time) error {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return fmt.Errorf("manager claim age cut requires the original sqlite owner")
	}
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return AgeManagerClaimStartedAtTx(ctx, tx, claim, startedAt)
	})
}
