package storetest

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func MakeManagerRetryEligible(ctx context.Context, selected any, event events.Event, route events.DeliveryRoute) error {
	return private.MakeManagerRetryEligibleForTest(ctx, selected, event, route)
}

func AgeManagerClaimStartedAt(ctx context.Context, selected any, claim deliverylifecycle.Claim, startedAt time.Time) error {
	return private.AgeManagerClaimStartedAtForTest(ctx, selected, claim, startedAt)
}
