package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type LongTransactionClockFixture = private.LongTransactionClockFixture

func CheckLongTransactionDeliveryClock(ctx context.Context, selected any, fixture LongTransactionClockFixture) error {
	return private.CheckLongTransactionDeliveryClockForTest(ctx, selected, fixture)
}
