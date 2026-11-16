package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type LongTransactionClockFixture = delivery.LongTransactionClockFixture

func CheckLongTransactionDeliveryClockForTest(ctx context.Context, selected any, fixture LongTransactionClockFixture) error {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return err
	}
	owner, ok := selected.(*PostgresStore)
	if !ok {
		return fmt.Errorf("long-transaction clock witness requires its original PostgreSQL owner")
	}
	return owner.deliveryPostgresOwner.CheckLongTransactionClockForTest(ctx, fixture)
}
